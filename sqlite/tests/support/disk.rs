//! Een RAM-schijf met stroomuitval: writes zijn pas duurzaam na `sync` van hun
//! bestand; een knip na de N-de write/sync/truncate laat elke niet-duurzame
//! write wegvallen, landen of half landen (per sector van 512 bytes).
#![allow(dead_code)]
use replica_sqlite::{Error, FileId, OpenFlags, Result, Storage};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::ffi::CStr;
use std::rc::Rc;

#[derive(Clone, Debug)]
enum Pend {
    Write(u64, Vec<u8>),
    Trunc(u64),
}

#[derive(Clone, Debug, Default)]
struct File {
    /// Wat een lezer nu ziet.
    data: Vec<u8>,
    /// Wat een stroomuitval overleeft; `None`: nog nooit gesynct (weg bij uitval).
    durable: Option<Vec<u8>>,
    pending: Vec<Pend>,
    /// Verwijderd zonder mapbarrière: weg voor lezers, `durable` blijft.
    gone: bool,
}

/// De gedeelde staat van één schijf.
#[derive(Clone, Debug, Default)]
pub struct State {
    files: BTreeMap<String, File>,
    /// Tellende writes, syncs en truncates.
    pub ops: u64,
    pub syncs: u64,
    pub writes: u64,
    /// Na deze operatie valt de stroom uit.
    pub cut: Option<u64>,
    pub dead: bool,
    rng: u64,
}

impl State {
    fn tick(&mut self) -> Result {
        if self.dead {
            return Err(Error::IO);
        }
        self.ops += 1;
        if self.cut == Some(self.ops) {
            self.dead = true;
        }
        Ok(())
    }

    fn next(&mut self) -> u64 {
        let mut x = self.rng | 1;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    /// De schijf zoals hij na de uitval opstart, met `seed` voor wat landde.
    pub fn crash(&self, seed: u64) -> State {
        let mut rng = State {
            rng: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
            ..State::default()
        };
        let mut files = BTreeMap::new();
        for (name, f) in &self.files {
            let Some(mut data) = f.durable.clone() else {
                continue;
            };
            if !f.gone {
                for p in &f.pending {
                    match (p, rng.next() % 3) {
                        (_, 0) => {}
                        (Pend::Write(off, bytes), 1) => write_at(&mut data, *off, bytes),
                        (Pend::Write(off, bytes), _) => {
                            for (i, part) in bytes.chunks(512).enumerate() {
                                if rng.next().is_multiple_of(2) {
                                    write_at(&mut data, off + i as u64 * 512, part);
                                }
                            }
                        }
                        (Pend::Trunc(n), _) => data.resize(*n as usize, 0),
                    }
                }
            }
            files.insert(
                name.clone(),
                File {
                    data: data.clone(),
                    durable: Some(data),
                    pending: Vec::new(),
                    gone: false,
                },
            );
        }
        State {
            files,
            rng: seed | 1,
            ..State::default()
        }
    }

    /// Een schone schijf met precies wat nu zichtbaar is, alles duurzaam.
    pub fn settled(&self) -> State {
        let mut files = BTreeMap::new();
        for (name, f) in &self.files {
            if f.gone {
                continue;
            }
            files.insert(
                name.clone(),
                File {
                    data: f.data.clone(),
                    durable: Some(f.data.clone()),
                    pending: Vec::new(),
                    gone: false,
                },
            );
        }
        State {
            files,
            rng: self.rng | 1,
            ..State::default()
        }
    }

    /// Maat van een zichtbaar bestand.
    pub fn len(&self, name: &str) -> Option<usize> {
        self.files
            .get(name)
            .filter(|f| !f.gone)
            .map(|f| f.data.len())
    }

    /// Ruwe bytes van een zichtbaar bestand.
    pub fn bytes(&self, name: &str) -> Option<Vec<u8>> {
        self.files
            .get(name)
            .filter(|f| !f.gone)
            .map(|f| f.data.clone())
    }

    /// Zet ruwe bytes (duurzaam).
    pub fn put(&mut self, name: &str, data: Vec<u8>) {
        self.files.insert(
            name.into(),
            File {
                data: data.clone(),
                durable: Some(data),
                pending: Vec::new(),
                gone: false,
            },
        );
    }
}

fn write_at(data: &mut Vec<u8>, off: u64, bytes: &[u8]) {
    let off = off as usize;
    if data.len() < off + bytes.len() {
        data.resize(off + bytes.len(), 0);
    }
    data[off..off + bytes.len()].copy_from_slice(bytes);
}

/// Eén schijf, gedeeld door meerdere opslag-eigenaren.
pub type Disk = Rc<RefCell<State>>;

/// Een nieuwe lege schijf.
pub fn disk(seed: u64) -> Disk {
    Rc::new(RefCell::new(State {
        rng: seed | 1,
        ..State::default()
    }))
}

/// Een opslag-eigenaar op een [`Disk`].
pub struct Vol {
    pub disk: Disk,
    open: Vec<Option<String>>,
}

impl Vol {
    pub fn new(disk: &Disk) -> Self {
        Self {
            disk: disk.clone(),
            open: Vec::new(),
        }
    }
    fn name(&self, id: FileId) -> Result<String> {
        self.open
            .get(id.0 as usize)
            .cloned()
            .flatten()
            .ok_or(Error::IO)
    }
}

impl Storage for Vol {
    fn open(&mut self, name: &CStr, flags: OpenFlags) -> Result<FileId> {
        let name = name.to_str().map_err(|_| Error::CANNOT_OPEN)?.to_string();
        let mut d = self.disk.borrow_mut();
        if d.dead {
            return Err(Error::IO);
        }
        match d.files.get_mut(&name) {
            Some(f) if !f.gone => {}
            Some(f) if flags.is_create() => {
                f.gone = false;
                f.data.clear();
                f.pending = vec![Pend::Trunc(0)];
            }
            None if flags.is_create() => {
                d.files.insert(name.clone(), File::default());
            }
            _ => return Err(Error::CANNOT_OPEN),
        }
        drop(d);
        let slot = self
            .open
            .iter()
            .position(Option::is_none)
            .unwrap_or_else(|| {
                self.open.push(None);
                self.open.len() - 1
            });
        self.open[slot] = Some(name);
        Ok(FileId(slot as u32))
    }
    fn close(&mut self, file: FileId) -> Result {
        self.name(file)?;
        self.open[file.0 as usize] = None;
        Ok(())
    }
    fn read(&mut self, file: FileId, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let name = self.name(file)?;
        let d = self.disk.borrow();
        if d.dead {
            return Err(Error::IO);
        }
        let f = d.files.get(&name).ok_or(Error::IO)?;
        let off = offset as usize;
        if off >= f.data.len() {
            return Ok(0);
        }
        let n = dst.len().min(f.data.len() - off);
        dst[..n].copy_from_slice(&f.data[off..off + n]);
        Ok(n)
    }
    fn write(&mut self, file: FileId, offset: u64, src: &[u8]) -> Result {
        let name = self.name(file)?;
        let mut d = self.disk.borrow_mut();
        d.tick()?;
        d.writes += 1;
        let f = d.files.get_mut(&name).ok_or(Error::IO)?;
        write_at(&mut f.data, offset, src);
        f.pending.push(Pend::Write(offset, src.to_vec()));
        if d.dead { Err(Error::IO) } else { Ok(()) }
    }
    fn truncate(&mut self, file: FileId, size: u64) -> Result {
        let name = self.name(file)?;
        let mut d = self.disk.borrow_mut();
        d.tick()?;
        let f = d.files.get_mut(&name).ok_or(Error::IO)?;
        f.data.resize(size as usize, 0);
        f.pending.push(Pend::Trunc(size));
        if d.dead { Err(Error::IO) } else { Ok(()) }
    }
    fn sync(&mut self, file: FileId, _flags: i32) -> Result {
        let name = self.name(file)?;
        let mut d = self.disk.borrow_mut();
        d.tick()?;
        if d.dead {
            // De knip valt vóór de barrière: niets wordt duurzaam.
            return Err(Error::IO);
        }
        d.syncs += 1;
        let f = d.files.get_mut(&name).ok_or(Error::IO)?;
        f.durable = Some(f.data.clone());
        f.pending.clear();
        Ok(())
    }
    fn size(&mut self, file: FileId) -> Result<u64> {
        let name = self.name(file)?;
        let d = self.disk.borrow();
        if d.dead {
            return Err(Error::IO);
        }
        Ok(d.files.get(&name).ok_or(Error::IO)?.data.len() as u64)
    }
    fn remove(&mut self, name: &CStr, sync_directory: bool) -> Result {
        let name = name.to_str().map_err(|_| Error::IO)?;
        let mut d = self.disk.borrow_mut();
        d.tick()?;
        if sync_directory {
            d.files.remove(name);
        } else if let Some(f) = d.files.get_mut(name) {
            f.gone = true;
            f.data.clear();
        }
        if d.dead { Err(Error::IO) } else { Ok(()) }
    }
    fn exists(&mut self, name: &CStr) -> Result<bool> {
        let name = name.to_str().map_err(|_| Error::IO)?;
        let d = self.disk.borrow();
        if d.dead {
            return Err(Error::IO);
        }
        Ok(d.files.get(name).is_some_and(|f| !f.gone))
    }
    fn random(&mut self, dst: &mut [u8]) -> Result {
        let mut d = self.disk.borrow_mut();
        for b in dst {
            *b = d.next() as u8;
        }
        Ok(())
    }
    fn unix_millis(&mut self) -> Result<i64> {
        Ok(1_700_000_000_000)
    }
}
