//! SQLite-bestanden in één exclusief gemounte map; iedere sync gaat naar HopFS.
//!
//! Elke call naar de kern is een ronde door de OS-core (op de M4 ongeveer
//! 55 µs), en SQLite vraagt per pagina van 4 KiB één lees of schrijf. Wat
//! Linux hiertegen heeft, de page cache met write-back en readahead, heeft
//! deze laag in het klein: een schrijfbuffer per schrijfbaar bestand die
//! aaneengesloten writes bundelt tot één call, weggeschreven vóór elke sync,
//! lees, truncate, stat en close (duurzaamheid blijft die van SQLite: niets
//! is beloofd vóór xSync), en één leesvenster dat bij een sequentiële lees
//! (aansluitend op de vorige) vooruitleest, van 64 KiB verdubbelend tot
//! 512 KiB, zoals Linux' readahead-venster. Linux houdt die staat per open
//! bestand; SQLite's sorter leest tot zestien PMA's door elkaar uit één
//! tempbestand (`SORTER_MAX_MERGE_COUNT`, elke lezer 4 KiB per keer op
//! volgorde), dus hier is de staat per stroom: zestien vensters, een lees
//! hoort bij het venster waar hij op aansluit, anders bij het langst
//! ongebruikte. Eén eigenaar schrijft dit domein, dus een venster is altijd
//! actueel: een write erin wordt erin gezet. GEMETEN 04-10 op QEMU: zie
//! apps/sqlite-bench; op de M4 deed de merge van CREATE INDEX met één
//! venster één ronde per pagina.
use alloc::vec::Vec;
use applib::{appnet::SystemClient, stacktask::Suspender, sys};
use core::{
    ffi::CStr,
    future::Future,
    sync::atomic::{AtomicU64, Ordering::Relaxed},
};
use replica_sqlite::{
    Error, FileId, OpenFlags, Result,
    asynchronous::{AsyncStorage, Cancelled, Suspend},
};
/// De applicatie levert klok en entropie; deze adapter verzint geen tijd of randombron.
pub trait Environment {
    /// Vul alle bytes met de platformbron, of faal.
    fn random(&mut self, dst: &mut [u8]) -> Result;
    /// Unixmilliseconden; ongeldige/niet gesynchroniseerde klok mag falen.
    fn unix_millis(&mut self) -> Result<i64>;
}
/// De kleine adapter van de runtime naar SQLite's parkeercontract.
#[derive(Clone, Copy)]
pub struct Wait<'a>(pub &'a Suspender);
impl Suspend for Wait<'_> {
    fn wait<F: Future>(&self, f: F) -> core::result::Result<F::Output, Cancelled> {
        self.0.wait(f).map_err(|_| Cancelled)
    }
}
/// De schrijfbuffer per schrijfbaar bestand: SQLite schrijft bij een commit
/// zijn pagina's op volgorde, dus aaneengesloten runs tot deze maat gaan in
/// één call (256 KiB is 64 pagina's; de kern neemt tot 1 MiB per call).
const WRITE_BUF: usize = 256 << 10;
/// Een leesvenster begint hier bij de eerste sequentiële miss.
const AHEAD_MIN: usize = 64 << 10;
/// En verdubbelt tot hier: 128 pagina's per ronde; zestien vensters zijn dan
/// hoogstens 8 MiB.
const AHEAD_MAX: usize = 512 << 10;
/// Zoveel stromen tegelijk: de merge-breedte van SQLite's sorter.
const WINDOWS: usize = 16;

/// Nog niet weggeschreven bytes van één bestand: één aaneengesloten run.
struct WriteBuf {
    data: Vec<u8>,
    start: u64,
    len: usize,
}
impl WriteBuf {
    const EMPTY: Self = Self {
        data: Vec::new(),
        start: 0,
        len: 0,
    };
    fn end(&self) -> u64 {
        self.start + self.len as u64
    }
    /// Raakt `[off, off + n)` de buffer?
    fn overlaps(&self, off: u64, n: usize) -> bool {
        self.len != 0 && off < self.end() && off + n as u64 > self.start
    }
}

/// Eén stroom: de laatst vooruitgelezen run van één bestand, en los daarvan
/// de vorige lees (de stroomdetectie). Een lees ertussendoor (een interne
/// B-tree-pagina in een scan) hoort bij een andere stroom en laat deze staan.
/// Een dalende scan (SQLite's integrity_check loopt de cellen van achter naar
/// voren, een ORDER BY DESC over een index ook) krijgt een venster dat op de
/// lees eindigt.
struct Window {
    file: u32,
    data: Vec<u8>,
    start: u64,
    len: usize,
    /// Het bestand, het begin en het einde van de vorige lees.
    last: (u32, u64, u64),
    /// De maat van het volgende venster; groeit per sequentiële miss.
    ahead: usize,
    /// Wanneer deze stroom voor het laatst las (de klok van `Files`).
    used: u64,
}
impl Window {
    const EMPTY: Self = Self {
        file: 0,
        data: Vec::new(),
        start: 0,
        len: 0,
        last: (0, 0, 0),
        ahead: 0,
        used: 0,
    };
    /// Ligt de lees in het venster?
    fn covers(&self, id: FileId, off: u64, n: usize) -> bool {
        self.file == id.0 && off >= self.start && off + n as u64 <= self.start + self.len as u64
    }
    /// Sluit de lees aan op de vorige van deze stroom: erna (precies, of een
    /// paar pagina's verder: wat ertussen zat had SQLite in zijn cache), of
    /// ervoor (dalend)?
    fn next_to(&self, id: FileId, off: u64, end: u64) -> (bool, bool) {
        let gap = AHEAD_MIN as u64;
        if self.last.0 != id.0 {
            return (false, false);
        }
        (
            (self.last.2..self.last.2 + gap).contains(&off),
            (self.last.1.saturating_sub(gap)..=self.last.1).contains(&end),
        )
    }
    /// Een write: in het venster als hij erin past, anders het venster weg.
    fn patch(&mut self, id: FileId, off: u64, src: &[u8]) {
        if self.file != id.0 || self.len == 0 {
            return;
        }
        let end = self.start + self.len as u64;
        if off >= self.start && off + src.len() as u64 <= end {
            let at = (off - self.start) as usize;
            self.data[at..at + src.len()].copy_from_slice(src);
        } else if off < end && off + src.len() as u64 > self.start {
            self.len = 0;
        }
    }
    fn drop_file(&mut self, id: FileId) {
        if self.file == id.0 {
            self.len = 0;
            self.last = (0, 0, 0);
        }
    }
}

#[derive(Clone, Copy)]
struct File {
    name: [u8; 256],
    len: usize,
    generation: u32,
    open: bool,
    writable: bool,
}
impl File {
    const EMPTY: Self = Self {
        name: [0; 256],
        len: 0,
        generation: 0,
        open: false,
        writable: false,
    };
}
struct Path {
    bytes: [u8; 512],
    len: usize,
}
impl Path {
    fn text(&self) -> Result<&str> {
        core::str::from_utf8(&self.bytes[..self.len]).map_err(|_| Error::TEXT)
    }
}
/// De meetlat: wat SQLite vroeg (de callbacks) tegenover wat er over de
/// system-client ging (elke call is een ronde door de OS-core). Het verschil
/// is wat deze laag wegneemt; de bench-app drukt beide per stap af. Tellers
/// als atomics: zo leest de app ze terwijl SQLite de
/// eigenaar geleend houdt.
#[derive(Default, Clone, Copy, Debug)]
pub struct Stats {
    /// Read-callbacks.
    pub reads: u64,
    /// Bytes die SQLite las.
    pub read_bytes: u64,
    /// Write-callbacks.
    pub writes: u64,
    /// Bytes die SQLite schreef.
    pub write_bytes: u64,
    /// Sync-callbacks.
    pub syncs: u64,
    /// Leescalls naar de kern (`OP_READ`).
    pub call_reads: u64,
    /// Bytes die de kern terugstuurde.
    pub call_read_bytes: u64,
    /// Schrijfcalls naar de kern (`OP_WRITE`).
    pub call_writes: u64,
    /// Bytes naar de kern.
    pub call_write_bytes: u64,
    /// Barrières (`OP_SYNC`): elk een vastlegging van de hopfs-boom.
    pub call_syncs: u64,
    /// Alle andere calls: stat, truncate, remove.
    pub call_other: u64,
}
static METER: [AtomicU64; 11] = [const { AtomicU64::new(0) }; 11];
const READS: usize = 0;
const READ_BYTES: usize = 1;
const WRITES: usize = 2;
const WRITE_BYTES: usize = 3;
const SYNCS: usize = 4;
const CALL_READS: usize = 5;
const CALL_READ_BYTES: usize = 6;
const CALL_WRITES: usize = 7;
const CALL_WRITE_BYTES: usize = 8;
const CALL_SYNCS: usize = 9;
const CALL_OTHER: usize = 10;
fn count(i: usize, n: u64) {
    if let Some(c) = METER.get(i) {
        c.fetch_add(n, Relaxed);
    }
}
impl Stats {
    /// De stand van nu.
    pub fn snapshot() -> Self {
        let m = |i: usize| METER.get(i).map_or(0, |c| c.load(Relaxed));
        Self {
            reads: m(READS),
            read_bytes: m(READ_BYTES),
            writes: m(WRITES),
            write_bytes: m(WRITE_BYTES),
            syncs: m(SYNCS),
            call_reads: m(CALL_READS),
            call_read_bytes: m(CALL_READ_BYTES),
            call_writes: m(CALL_WRITES),
            call_write_bytes: m(CALL_WRITE_BYTES),
            call_syncs: m(CALL_SYNCS),
            call_other: m(CALL_OTHER),
        }
    }
    /// Alle rondes door de OS-core samen.
    pub fn calls(&self) -> u64 {
        self.call_reads + self.call_writes + self.call_syncs + self.call_other
    }
    /// Het verschil met een eerdere stand, per stap.
    pub fn since(&self, earlier: &Self) -> Self {
        Self {
            reads: self.reads - earlier.reads,
            read_bytes: self.read_bytes - earlier.read_bytes,
            writes: self.writes - earlier.writes,
            write_bytes: self.write_bytes - earlier.write_bytes,
            syncs: self.syncs - earlier.syncs,
            call_reads: self.call_reads - earlier.call_reads,
            call_read_bytes: self.call_read_bytes - earlier.call_read_bytes,
            call_writes: self.call_writes - earlier.call_writes,
            call_write_bytes: self.call_write_bytes - earlier.call_write_bytes,
            call_syncs: self.call_syncs - earlier.call_syncs,
            call_other: self.call_other - earlier.call_other,
        }
    }
}
/// Eén eigenaar van SystemClient en maximaal zestien SQLite-handvatten.
/// Geen andere schrijver mag deze root gebruiken; de jobspec gebruikt recreate.
pub struct Files<'a, E> {
    sys: SystemClient,
    root: &'a str,
    env: E,
    files: [File; 16],
    bufs: [WriteBuf; 16],
    windows: [Window; WINDOWS],
    /// Telt de lezingen: de klok voor de langst ongebruikte stroom.
    clock: u64,
    poisoned: bool,
}
impl<'a, E: Environment> Files<'a, E> {
    /// Root is een canonieke absolute map op het persistente volume.
    pub fn new(sys: SystemClient, root: &'a str, env: E) -> Result<Self> {
        if !root.starts_with('/')
            || root.len() > 255
            || root == "/"
            || root.ends_with('/')
            || !canonical(&root.as_bytes()[1..])
        {
            return Err(Error::CANNOT_OPEN);
        }
        Ok(Self {
            sys,
            root,
            env,
            files: [File::EMPTY; 16],
            bufs: [const { WriteBuf::EMPTY }; 16],
            windows: [const { Window::EMPTY }; WINDOWS],
            clock: 0,
            poisoned: false,
        })
    }
    /// Schrijft de buffer van bestand `i` in één call weg.
    async fn flush(&mut self, i: usize) -> Result {
        let len = self.bufs[i].len;
        if len == 0 {
            return Ok(());
        }
        let f = self.files[i];
        let path = self.path(&f.name[..f.len])?;
        count(CALL_WRITES, 1);
        count(CALL_WRITE_BYTES, len as u64);
        let r = self
            .sys
            .write_at(path.text()?, self.bufs[i].start, &self.bufs[i].data[..len])
            .await;
        self.bufs[i].len = 0;
        if r.map_err(|e| self.error(e))? != len {
            self.poisoned = true;
            return Err(Error::IO);
        }
        Ok(())
    }
    /// Een lees rechtstreeks van de kern, in stappen van hoogstens één call.
    async fn read_direct(&mut self, path: &Path, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let mut done = 0;
        while done < dst.len() {
            let end = dst.len().min(done + sys::MAX_CHUNK);
            let off = offset.checked_add(done as u64).ok_or(Error::RANGE)?;
            count(CALL_READS, 1);
            let n = self
                .sys
                .read_into(path.text()?, off, &mut dst[done..end])
                .await
                .map_err(|e| self.error(e))?;
            count(CALL_READ_BYTES, n as u64);
            if n == 0 {
                break;
            }
            if n > end - done {
                self.poisoned = true;
                return Err(Error::IO);
            }
            done += n;
        }
        Ok(done)
    }
    fn path(&self, name: &[u8]) -> Result<Path> {
        if name.is_empty() || name.len() > 255 || !canonical(name) {
            return Err(Error::CANNOT_OPEN);
        }
        let mut path = Path {
            bytes: [0; 512],
            len: self.root.len() + 1 + name.len(),
        };
        path.bytes[..self.root.len()].copy_from_slice(self.root.as_bytes());
        path.bytes[self.root.len()] = b'/';
        path.bytes[self.root.len() + 1..path.len].copy_from_slice(name);
        Ok(path)
    }
    fn file(&self, id: FileId) -> Result<&File> {
        if self.poisoned {
            return Err(Error::IO);
        }
        self.files
            .get((id.0 & 255) as usize)
            .filter(|f| f.open && f.generation == id.0 >> 8)
            .ok_or(Error::IO)
    }
    fn file_path(&self, id: FileId, writable: bool) -> Result<Path> {
        let f = self.file(id)?;
        if writable && !f.writable {
            return Err(Error { code: 8 });
        }
        self.path(&f.name[..f.len])
    }
    fn error(&mut self, e: sys::Error) -> Error {
        if matches!(
            e,
            sys::Error::Timeout | sys::Error::Transport(_) | sys::Error::Protocol(_)
        ) {
            self.poisoned = true;
        }
        Error::IO
    }
}
fn canonical(name: &[u8]) -> bool {
    core::str::from_utf8(name).is_ok()
        && !name.iter().any(|&b| b < 32 || b == 127 || b == b'\\')
        && name
            .split(|&b| b == b'/')
            .all(|s| !s.is_empty() && s != b"." && s != b"..")
}
impl<E: Environment> AsyncStorage for Files<'_, E> {
    async fn cooperate(&mut self) -> Result {
        if self.poisoned {
            return Err(Error::IO);
        }
        let mut pending = true;
        core::future::poll_fn(|cx| {
            if pending {
                pending = false;
                cx.waker().wake_by_ref();
                core::task::Poll::Pending
            } else {
                core::task::Poll::Ready(())
            }
        })
        .await;
        Ok(())
    }
    fn invalidate(&mut self) {
        self.poisoned = true;
    }
    async fn open(&mut self, name: &CStr, flags: OpenFlags) -> Result<FileId> {
        if self.poisoned {
            return Err(Error::IO);
        }
        let bytes = name.to_bytes();
        let path = self.path(bytes)?;
        if self
            .files
            .iter()
            .any(|f| f.open && &f.name[..f.len] == bytes)
        {
            return Err(Error { code: 5 });
        }
        let index = self
            .files
            .iter()
            .position(|f| !f.open && f.generation < 0xffffff)
            .ok_or(Error::FULL)?;
        count(CALL_OTHER, 1);
        match self.sys.stat(path.text()?).await {
            Ok(_) => {}
            Err(sys::Error::NotFound { .. }) if flags.is_create() && flags.is_writable() => {
                count(CALL_OTHER, 1);
                self.sys
                    .truncate(path.text()?, 0)
                    .await
                    .map_err(|e| self.error(e))?;
            }
            Err(sys::Error::NotFound { .. }) => return Err(Error::CANNOT_OPEN),
            Err(e) => return Err(self.error(e)),
        }
        let f = &mut self.files[index];
        f.generation += 1;
        f.name[..bytes.len()].copy_from_slice(bytes);
        f.len = bytes.len();
        f.open = true;
        f.writable = flags.is_writable();
        Ok(FileId((f.generation << 8) | index as u32))
    }
    async fn close(&mut self, id: FileId) -> Result {
        self.file(id)?;
        let i = (id.0 & 255) as usize;
        self.flush(i).await?;
        self.bufs[i].data = Vec::new();
        for w in &mut self.windows {
            w.drop_file(id);
        }
        self.files[i].open = false;
        Ok(())
    }
    async fn read(&mut self, id: FileId, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let path = self.file_path(id, false)?;
        count(READS, 1);
        count(READ_BYTES, dst.len() as u64);
        self.clock += 1;
        let end = offset + dst.len() as u64;
        if let Some(w) = self
            .windows
            .iter_mut()
            .find(|w| w.covers(id, offset, dst.len()))
        {
            let at = (offset - w.start) as usize;
            dst.copy_from_slice(&w.data[at..at + dst.len()]);
            (w.last, w.used) = ((id.0, offset, end), self.clock);
            return Ok(dst.len());
        }
        // De stroom waar deze lees op aansluit; anders de langst ongebruikte,
        // en dan is dit een losse lees: alleen zichzelf, geen venster.
        let found = self
            .windows
            .iter()
            .position(|w| w.next_to(id, offset, end) != (false, false));
        let k = found.unwrap_or_else(|| {
            self.windows
                .iter()
                .enumerate()
                .min_by_key(|(_, w)| w.used)
                .map_or(0, |(k, _)| k)
        });
        let (forward, _) = self.windows[k].next_to(id, offset, end);
        let i = (id.0 & 255) as usize;
        if found.is_none() {
            let w = &mut self.windows[k];
            (w.len, w.ahead, w.used) = (0, 0, self.clock);
            if self.bufs[i].overlaps(offset, dst.len()) {
                self.flush(i).await?;
            }
            let n = self.read_direct(&path, offset, dst).await?;
            self.windows[k].last = (id.0, offset, offset + n as u64);
            return Ok(n);
        }
        // Een scan: een venster vooruit (of achteruit) lezen, bij elke
        // volgende miss een groter.
        let w = &mut self.windows[k];
        w.ahead = (w.ahead * 2).clamp(AHEAD_MIN, AHEAD_MAX);
        if w.data.len() < w.ahead && w.data.try_reserve_exact(w.ahead - w.data.len()).is_ok() {
            w.data.resize(w.ahead, 0);
        }
        let want = w.ahead.min(w.data.len()).max(dst.len());
        // Vooruit begint het venster bij deze lees, achteruit eindigt het erop.
        let from = if forward {
            offset
        } else {
            end.saturating_sub(want as u64)
        };
        (w.len, w.used) = (0, self.clock);
        if self.bufs[i].overlaps(from, want) {
            self.flush(i).await?;
        }
        if want <= dst.len() {
            // Geen geheugen voor een venster: alleen deze lees.
            let n = self.read_direct(&path, offset, dst).await?;
            self.windows[k].last = (id.0, offset, offset + n as u64);
            return Ok(n);
        }
        let mut data = core::mem::take(&mut self.windows[k].data);
        let r = self.read_direct(&path, from, &mut data[..want]).await;
        self.windows[k].data = data;
        let n = r?;
        let at = (offset - from) as usize;
        let got = n.saturating_sub(at).min(dst.len());
        let w = &mut self.windows[k];
        dst[..got].copy_from_slice(&w.data[at..at + got]);
        if got < dst.len() {
            dst[got..].fill(0);
        }
        (w.file, w.start, w.len) = (id.0, from, n);
        w.last = (id.0, offset, offset + got as u64);
        Ok(got)
    }
    async fn write(&mut self, id: FileId, offset: u64, src: &[u8]) -> Result {
        let path = self.file_path(id, true)?;
        count(WRITES, 1);
        count(WRITE_BYTES, src.len() as u64);
        for w in &mut self.windows {
            w.patch(id, offset, src);
        }
        let i = (id.0 & 255) as usize;
        if self.bufs[i].data.is_empty() {
            let mut v = Vec::new();
            if v.try_reserve_exact(WRITE_BUF).is_ok() {
                v.resize(WRITE_BUF, 0);
            }
            self.bufs[i].data = v;
        }
        let b = &mut self.bufs[i];
        if src.len() > b.data.len() {
            // Te groot voor de buffer (of geen geheugen): eerst wat er lag, dan direct.
            self.flush(i).await?;
            for (k, chunk) in src.chunks(sys::MAX_CHUNK).enumerate() {
                let off = offset
                    .checked_add((k * sys::MAX_CHUNK) as u64)
                    .ok_or(Error::RANGE)?;
                count(CALL_WRITES, 1);
                count(CALL_WRITE_BYTES, chunk.len() as u64);
                let n = self
                    .sys
                    .write_at(path.text()?, off, chunk)
                    .await
                    .map_err(|e| self.error(e))?;
                if n != chunk.len() {
                    self.poisoned = true;
                    return Err(Error::IO);
                }
            }
            return Ok(());
        }
        if b.len != 0 && offset >= b.start && offset + src.len() as u64 <= b.end() {
            // Een overschrijving binnen de run (pagina 1 bij een commit).
            let at = (offset - b.start) as usize;
            b.data[at..at + src.len()].copy_from_slice(src);
            return Ok(());
        }
        if b.len != 0 && (offset != b.end() || b.len + src.len() > b.data.len()) {
            self.flush(i).await?;
        }
        let b = &mut self.bufs[i];
        if b.len == 0 {
            b.start = offset;
        }
        b.data[b.len..b.len + src.len()].copy_from_slice(src);
        b.len += src.len();
        Ok(())
    }
    async fn truncate(&mut self, id: FileId, size: u64) -> Result {
        let p = self.file_path(id, true)?;
        self.flush((id.0 & 255) as usize).await?;
        for w in &mut self.windows {
            w.drop_file(id);
        }
        count(CALL_OTHER, 1);
        self.sys
            .truncate(p.text()?, size)
            .await
            .map_err(|e| self.error(e))
    }
    async fn sync(&mut self, id: FileId, _flags: i32) -> Result {
        let p = self.file_path(id, false)?;
        count(SYNCS, 1);
        self.flush((id.0 & 255) as usize).await?;
        count(CALL_SYNCS, 1);
        self.sys.sync(p.text()?).await.map_err(|e| self.error(e))?;
        Ok(())
    }
    async fn size(&mut self, id: FileId) -> Result<u64> {
        let p = self.file_path(id, false)?;
        self.flush((id.0 & 255) as usize).await?;
        count(CALL_OTHER, 1);
        self.sys.stat(p.text()?).await.map_err(|e| self.error(e))
    }
    async fn remove(&mut self, name: &CStr, sync_directory: bool) -> Result {
        if self.poisoned {
            return Err(Error::IO);
        }
        let bytes = name.to_bytes();
        let mut p = self.path(bytes)?;
        if self
            .files
            .iter()
            .any(|f| f.open && &f.name[..f.len] == bytes)
        {
            return Err(Error { code: 5 });
        }
        count(CALL_OTHER, 1);
        match self.sys.remove(p.text()?).await {
            Ok(()) | Err(sys::Error::NotFound { .. }) => {}
            Err(e) => return Err(self.error(e)),
        }
        if sync_directory {
            count(CALL_SYNCS, 1);
            p.len = p.bytes[..p.len]
                .iter()
                .rposition(|&b| b == b'/')
                .ok_or(Error::CANNOT_OPEN)?;
            self.sys.sync(p.text()?).await.map_err(|e| self.error(e))?;
        }
        Ok(())
    }
    async fn exists(&mut self, name: &CStr) -> Result<bool> {
        if self.poisoned {
            return Err(Error::IO);
        }
        let p = self.path(name.to_bytes())?;
        count(CALL_OTHER, 1);
        match self.sys.stat(p.text()?).await {
            Ok(_) => Ok(true),
            Err(sys::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(self.error(e)),
        }
    }
    fn random(&mut self, dst: &mut [u8]) -> Result {
        if self.poisoned {
            return Err(Error::IO);
        }
        self.env.random(dst)
    }
    fn unix_millis(&mut self) -> Result<i64> {
        if self.poisoned {
            return Err(Error::IO);
        }
        self.env.unix_millis()
    }
}
