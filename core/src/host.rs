//! De objectstore en de lease van de host: een lokale map, voor ontwikkeling
//! en tests (feature `std`), naast de RAM-stores van de toetsen.
//!
//! [`Dir`] is een [`Store`] met hetzelfde contract als S3: een PUT vervangt
//! atomair (een tijdelijk bestand, `fsync`, `rename`, `fsync` van de map), en
//! GET en LIST zien elke bevestigde write meteen. Elke sleutel is één plat
//! bestand: `/` wordt `%2F` en `%` wordt `%25`, zodat een sleutel en een prefix
//! met dezelfde naam naast elkaar bestaan, zoals in S3. Een sleutel die zo
//! langer wordt dan [`NAME_BYTES`] past niet en is [`StoreError::Limit`].
//!
//! [`DirLease`] is de hop-lease (`writer::Backend`) als bestand in die map:
//! de handle is de SHA-256 van de inhoud, en elke schrijf is voorwaardelijk
//! op de vorige, zoals `If-Match` op S3. Eén proces per map: de vergelijking
//! en de vervanging zijn samen niet atomair tegenover een tweede proces, en
//! dat hoeft op de host ook niet (de dev-app is de enige schrijver).
use crate::{
    manifest::hex,
    object::{Object, Store, StoreError},
    writer::{
        Backend, LeaseState,
        discovery::{self, wire},
    },
};
use alloc::{string::String, vec::Vec};
use std::{
    fs::{self, File},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};

/// De langste bestandsnaam van een gecodeerde sleutel.
pub const NAME_BYTES: usize = 240;
/// Tijdelijke bestanden van een PUT beginnen zo; LIST slaat ze over.
const TEMP: &str = ".put-";

/// Een objectstore in één lokale map.
pub struct Dir {
    root: PathBuf,
    /// Telt PUT's, voor unieke tijdelijke namen binnen dit proces.
    puts: u64,
}

impl Dir {
    /// Een store in `root`; de map wordt gemaakt als hij nog niet bestaat.
    pub fn new(root: impl AsRef<Path>) -> Result<Self, StoreError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|_| StoreError::Configuration)?;
        Ok(Self { root, puts: 0 })
    }
    /// De map van deze store.
    pub fn root(&self) -> &Path {
        &self.root
    }
    fn path(&self, key: &str) -> Result<PathBuf, StoreError> {
        if key.is_empty() {
            return Err(StoreError::Configuration);
        }
        let name = encode(key);
        if name.len() > NAME_BYTES {
            return Err(StoreError::Limit);
        }
        Ok(self.root.join(name))
    }
    /// Schrijft `bytes` atomair naar `path`.
    fn replace(&mut self, path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
        self.puts = self.puts.wrapping_add(1);
        let temp = self
            .root
            .join(alloc::format!("{TEMP}{}-{}", std::process::id(), self.puts));
        let written = File::create(&temp)
            .and_then(|mut f| f.write_all(bytes).and_then(|()| f.sync_all()))
            .and_then(|()| fs::rename(&temp, path))
            .and_then(|()| File::open(&self.root).and_then(|d| d.sync_all()));
        if written.is_err() {
            let _ = fs::remove_file(&temp);
            return Err(StoreError::Transport);
        }
        Ok(())
    }
}

/// `/` en `%` gecodeerd, zodat een sleutel één bestandsnaam is.
fn encode(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for c in key.chars() {
        match c {
            '/' => out.push_str("%2F"),
            '%' => out.push_str("%25"),
            c => out.push(c),
        }
    }
    out
}
/// De omkering van [`encode`]; `None` voor een naam die geen sleutel is.
fn decode(name: &str) -> Option<String> {
    let mut out = String::with_capacity(name.len());
    let mut rest = name;
    while let Some(i) = rest.find('%') {
        out.push_str(&rest[..i]);
        match rest.get(i..i + 3)? {
            "%2F" => out.push('/'),
            "%25" => out.push('%'),
            _ => return None,
        }
        rest = &rest[i + 3..];
    }
    out.push_str(rest);
    Some(out)
}

fn io(e: &std::io::Error) -> StoreError {
    match e.kind() {
        ErrorKind::NotFound => StoreError::Missing,
        ErrorKind::PermissionDenied => StoreError::Denied,
        _ => StoreError::Transport,
    }
}

impl Store for Dir {
    fn put(&mut self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        let path = self.path(key)?;
        self.replace(&path, bytes)
    }
    fn get(&mut self, key: &str, limit: usize) -> Result<Vec<u8>, StoreError> {
        let path = self.path(key)?;
        let size = fs::metadata(&path).map_err(|e| io(&e))?.len();
        if size > limit as u64 {
            return Err(StoreError::Limit);
        }
        let bytes = fs::read(&path).map_err(|e| io(&e))?;
        if bytes.len() > limit {
            return Err(StoreError::Limit);
        }
        Ok(bytes)
    }
    fn delete(&mut self, key: &str) -> Result<(), StoreError> {
        let path = self.path(key)?;
        match fs::remove_file(&path) {
            Ok(()) => File::open(&self.root)
                .and_then(|d| d.sync_all())
                .map_err(|e| io(&e)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io(&e)),
        }
    }
    fn list(&mut self, prefix: &str, limit: usize) -> Result<Vec<Object>, StoreError> {
        let mut out = Vec::new();
        for entry in fs::read_dir(&self.root).map_err(|e| io(&e))? {
            let entry = entry.map_err(|e| io(&e))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.starts_with(TEMP) {
                continue;
            }
            let Some(key) = decode(name) else { continue };
            if !key.starts_with(prefix) {
                continue;
            }
            if out.len() == limit {
                return Err(StoreError::Limit);
            }
            let size = entry.metadata().map_err(|e| io(&e))?.len();
            out.push(Object {
                key,
                size: Some(size),
            });
        }
        // Zoals S3: lexicografisch op sleutel.
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }
}

/// De hop-lease als bestand in een lokale map.
pub struct DirLease {
    path: PathBuf,
}

impl DirLease {
    /// De lease op `name` in `dir` (bijvoorbeeld `<namespace>/lease`, gecodeerd
    /// zoals een sleutel van [`Dir`]).
    pub fn new(dir: &Dir, key: &str) -> Result<Self, StoreError> {
        Ok(Self {
            path: dir.path(key)?,
        })
    }
    fn current(&self) -> discovery::Result<Option<(Vec<u8>, String)>> {
        match fs::read(&self.path) {
            Ok(bytes) => {
                let handle = handle(&bytes);
                Ok(Some((bytes, handle)))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(_) => Err(discovery::Error::Unreachable),
        }
    }
}

/// De handle van een leaseversie: de hash van haar bytes, als hex.
fn handle(bytes: &[u8]) -> String {
    let hex = hex(&crate::hash(bytes));
    String::from_utf8_lossy(&hex).into_owned()
}

impl Backend for DirLease {
    fn read(&mut self) -> discovery::Result<(LeaseState, String)> {
        let (bytes, handle) = self.current()?.ok_or(discovery::Error::NoLease)?;
        let state = wire::decode(&bytes).map_err(|_| discovery::Error::Unreachable)?;
        Ok((state, handle))
    }
    fn write(&mut self, prev: &str, state: &LeaseState) -> discovery::Result<String> {
        match (self.current()?, prev.is_empty()) {
            (None, true) => {}
            (Some((_, current)), false) if current == prev => {}
            _ => return Err(discovery::Error::LeaseHeld),
        }
        let bytes = wire::encode(state).map_err(|_| discovery::Error::Unreachable)?;
        let parent = self.path.parent().ok_or(discovery::Error::Unreachable)?;
        let temp = parent.join(alloc::format!("{TEMP}lease-{}", std::process::id()));
        File::create(&temp)
            .and_then(|mut f| f.write_all(&bytes).and_then(|()| f.sync_all()))
            .and_then(|()| fs::rename(&temp, &self.path))
            .map_err(|_| discovery::Error::Unreachable)?;
        Ok(handle(&bytes))
    }
    fn delete(&mut self, handle: &str) -> discovery::Result {
        match self.current()? {
            None => Err(discovery::Error::NoLease),
            Some((_, current)) if current == handle => {
                fs::remove_file(&self.path).map_err(|_| discovery::Error::Unreachable)
            }
            Some(_) => Err(discovery::Error::LeaseHeld),
        }
    }
}
