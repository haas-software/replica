//! Transparante paginacompressie onder SQLite: [`Compressed`] om elke [`Storage`].
//!
//! Bezit: per gecomprimeerd bestand de paginakaart, de segmentboekhouding en
//! de commit van die twee; de codec ([`crate::lz`]). Niet: de bestanden zelf
//! (de binnenste opslag), SQLite's journal (dat blijft een gewoon bestand) en
//! de keuze welke namen databases zijn ([`Policy`]).
//!
//! Een gecomprimeerd bestand ("container") herken je aan zijn kop
//! ([`MAGIC`]); al het andere gaat ongewijzigd door, zodat bestaande,
//! ongecomprimeerde databases gewoon blijven werken. Nieuwe databases worden
//! gecomprimeerd aangemaakt als [`Policy::create`] dat zegt.
//!
//! # Indeling
//!
//! ```text
//! 0      superblok A (128 bytes, eigen blok van 4 KiB)
//! 4096   superblok B
//! 8192   segment 0, 1, 2, ... van elk SEGMENT bytes
//! ```
//!
//! Een logische pagina (de paginamaat van de container, `page_size`) staat
//! gecomprimeerd in een **slot**: aaneengesloten bytes in één segment,
//! zonder kop. De **kaart** zegt per pagina waar (byte-offset, lengte; nul is
//! een nulpagina, [`RAW`] een onsamendrukbare pagina van `page_size` bytes).
//!
//! Segmenten zijn log-gestructureerd: nieuwe slots worden achter elkaar in het
//! open segment geschreven, nooit over een slot dat in de laatst gecommitte
//! staat leeft. Een segment zonder levende bytes is na de commit weer vrij;
//! de schoonmaker verplaatst de levende slots van het segment met de minste
//! levende bytes (of het hoogste segment, om het bestand te laten krimpen)
//! naar het open segment. Zo blijft het bestand begrensd: dood hoogstens een
//! vijfde, vrije gaten onder het einde hoogstens een tiende (zie
//! `docs/compress.md`).
//!
//! # Commit
//!
//! De kaart is een **snapshot** plus een **log** in een metadatagebied (een
//! reeks segmenten). Elke `sync` van SQLite op een container is één commit:
//! een logrecord met de gewijzigde kaartingangen, de logische maat en een
//! checksum, op een eigen sector, en één `sync` van het binnenste bestand.
//! Past het record niet meer, dan schrijft een checkpoint de hele kaart als
//! nieuw snapshot in vrije segmenten, `sync`, het andere superblok (hogere
//! generatie, nieuw zout), `sync`.
//!
//! Atomiciteit samen met SQLite's rollback-journal: SQLite schrijft het journal
//! en synct het vóór de eerste databasewrite, en ruimt het pas op nadat onze
//! `sync` terugkwam. Een stroomstoring vóór die `sync` laat de container op de
//! vorige commit staan (de nieuwe slots stonden in vrije ruimte, het record is
//! niet of half geschreven en zijn checksum klopt niet); het hete journal zet
//! dan ook de pagina's terug die er wel stonden. Een storing tijdens de `sync`
//! kan het record wel en een slot niet duurzaam maken: dat slot hoort bij een
//! pagina die in het journal staat (of voorbij de oude maat ligt, die het
//! journal afkapt), dus het terugspelen overschrijft hem. Na het opruimen van
//! het journal is de commit al duurzaam. De binnenste opslag moet daarvoor
//! alleen beloven wat SQLite ook vraagt: `sync` maakt voorafgaande writes
//! duurzaam. Een nieuwe commit schrijft nooit in een sector waarin een
//! duurzame commit iets heeft (slots en records beginnen na een commit op
//! een nieuwe sector van 4 KiB), dus ook zonder "powersafe overwrite" kan een
//! halve write geen eerdere commit raken.
//!
//! # Meerdere eigenaren van één map
//!
//! Twee [`Compressed`] op dezelfde map (de SQL-kant en de sync-kant van
//! replica-core) houden elk hun eigen kaart. Een `open` van een bestand dat in
//! deze eigenaar niet open is, leest de superblokken en speelt alleen de
//! nieuwe logrecords bij (of laadt opnieuw na een checkpoint): het bestand is
//! de enige waarheid. Er schrijft er maar één tegelijk, zoals het
//! [`Storage`]-contract al eist.
use crate::{Error, FileId, OpenFlags, Result, Storage, lz};
use alloc::vec::Vec;
use core::ffi::CStr;

/// De eerste acht bytes van een container (en van elk superblok).
pub const MAGIC: [u8; 8] = *b"ReplicaZ";
/// Formaatversie van superblok en log.
const VERSION: u32 = 1;
/// Bytes vóór segment 0: twee superblokken van elk een eigen blok.
pub const HEADER: u64 = 8192;
/// Waar superblok B staat.
const SLOT_B: u64 = 4096;
/// Bytes van één superblok.
const SB_LEN: usize = 128;
/// Bytes per segment; een slot past altijd in één segment.
pub const SEGMENT: u32 = 65536;
/// De sector: een commit begint slots en records op een nieuwe sector.
const SECTOR: u64 = 4096;
/// Kop van een logrecord.
const RECORD_HEAD: usize = 48;
/// Eén kaartingang in een record: pagina (u32), ingang (u64) en de checksum
/// van de slotbytes (u32).
const ENTRY: usize = 16;
/// Kleinste en grootste log van een metadatagebied.
const MIN_LOG: u64 = 60 << 10;
const MAX_LOG: u64 = 64 << 20;
/// Lengtecode van een onsamendrukbare pagina (`page_size` bytes).
pub const RAW: u64 = 0xFFFF;
/// Byte-offset van een slot in de onderste 48 bits van een ingang.
const OFFSET_MASK: u64 = (1 << 48) - 1;
/// Magic van een logrecord ("RLOG").
const RECORD_MAGIC: u32 = 0x474F_4C52;
/// De meeste pagina's van één container (paginanummers zijn u32).
const MAX_PAGES: u64 = 1 << 31;
/// `SQLITE_CORRUPT`: een container die zijn eigen formaat schendt.
pub const CORRUPT: Error = Error { code: 11 };
/// Lees- en schrijfstap voor snapshots en logs.
const CHUNK: usize = 1 << 20;

/// Welke nieuwe bestanden een container worden.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    /// Nieuwe databasebestanden gecomprimeerd aanmaken. Bestaande bestanden
    /// houden altijd hun formaat (de kop beslist).
    pub create: bool,
    /// Paginamaat van een nieuwe container als de eerste write geen
    /// SQLite-kop heeft; met kop neemt de container SQLite's paginamaat.
    pub page_size: u32,
}

impl Policy {
    /// Comprimeren, standaard 16 KiB.
    pub const fn compressed() -> Self {
        Self {
            create: true,
            page_size: 16384,
        }
    }
    /// Niets nieuws comprimeren; bestaande containers blijven leesbaar.
    pub const fn plain() -> Self {
        Self {
            create: false,
            page_size: 16384,
        }
    }
}

/// Het formaat van een bestand volgens zijn kop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    /// Leeg of afwezig.
    Empty,
    /// Een container van deze module.
    Compressed,
    /// Iets anders, bijvoorbeeld een gewone SQLite-database.
    Plain,
}

/// Leest de kop van `name` in `inner` (zonder [`Compressed`] ertussen).
pub fn detect<S: Storage>(inner: &mut S, name: &CStr) -> Result<Format> {
    if !inner.exists(name)? {
        return Ok(Format::Empty);
    }
    let id = inner.open(name, OpenFlags(2))?;
    let mut head = [0u8; 8];
    let got = inner.read(id, 0, &mut head);
    let size = inner.size(id);
    inner.close(id)?;
    let (got, size) = (got?, size?);
    Ok(if size == 0 {
        Format::Empty
    } else if got == 8 && head == MAGIC {
        Format::Compressed
    } else {
        Format::Plain
    })
}

/// Een naam die nooit een container wordt: SQLite's journals en tijdelijke
/// bestanden en replica-core's sidecars (behalve de restore-scratch, die een
/// database is).
pub fn is_sidecar(name: &[u8]) -> bool {
    let last = name.rsplit(|&b| b == b'/').next().unwrap_or(name);
    const ENDS: [&[u8]; 3] = [b"-journal", b"-wal", b"-shm"];
    if ENDS.iter().any(|e| last.ends_with(e)) || last.starts_with(b"etilqs_") {
        return true;
    }
    if last.ends_with(b".replica-restore-data") {
        return false;
    }
    contains(last, b".replica") || contains(last, b"-mj")
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

/// SQLite's bestandssoorten die geen hoofddatabase zijn.
const TEMP_KINDS: i32 = 0x200 | 0x400 | 0x800 | 0x1000 | 0x2000 | 0x4000 | 0x80000;

/// Paginanummer en ingang: waar het slot staat en hoe lang het is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Slot {
    off: u64,
    len: u32,
}

fn slot_of(entry: u64, page_size: u32) -> Option<Slot> {
    let code = entry >> 48;
    if code == 0 {
        return None;
    }
    let len = if code == RAW { page_size } else { code as u32 };
    Some(Slot {
        off: entry & OFFSET_MASK,
        len,
    })
}

fn entry_of(off: u64, len: u32, page_size: u32) -> u64 {
    let code = if len == page_size {
        RAW
    } else {
        u64::from(len)
    };
    (code << 48) | (off & OFFSET_MASK)
}

const fn align(n: u64, to: u64) -> u64 {
    n.div_ceil(to) * to
}

fn pages_of(size: u64, page_size: u32) -> u64 {
    size.div_ceil(u64::from(page_size))
}

fn valid_page_size(n: u32) -> bool {
    (512..=65536).contains(&n) && n.is_power_of_two()
}

/// Een 64-bits checksum voor superblokken, snapshots en logrecords (geen
/// cryptografie: hij vangt halve writes en verkeerde bytes).
#[derive(Clone, Copy)]
struct Sum {
    h: u64,
    len: u64,
    tail: u64,
    fill: u32,
}

impl Sum {
    const K1: u64 = 0x9E37_79B9_7F4A_7C15;
    const K2: u64 = 0xC2B2_AE3D_27D4_EB4F;
    fn new() -> Self {
        Self {
            h: 0x5245_504C_4943_415A,
            len: 0,
            tail: 0,
            fill: 0,
        }
    }
    fn word(&mut self, w: u64) {
        self.h = (self.h ^ w.wrapping_mul(Self::K2))
            .rotate_left(29)
            .wrapping_mul(Self::K1);
    }
    fn update(&mut self, mut bytes: &[u8]) {
        self.len = self.len.wrapping_add(bytes.len() as u64);
        while self.fill != 0 {
            let Some((&b, rest)) = bytes.split_first() else {
                return;
            };
            self.tail |= u64::from(b) << (8 * self.fill);
            self.fill += 1;
            bytes = rest;
            if self.fill == 8 {
                let t = self.tail;
                self.word(t);
                self.tail = 0;
                self.fill = 0;
            }
        }
        let mut words = bytes.chunks_exact(8);
        for w in &mut words {
            let mut a = [0u8; 8];
            a.copy_from_slice(w);
            self.word(u64::from_le_bytes(a));
        }
        for &b in words.remainder() {
            self.tail |= u64::from(b) << (8 * self.fill);
            self.fill += 1;
        }
    }
    fn finish(mut self) -> u64 {
        if self.fill != 0 {
            let t = self.tail;
            self.word(t);
        }
        let mut h = self.h ^ self.len;
        h ^= h >> 33;
        h = h.wrapping_mul(Self::K2);
        h ^= h >> 29;
        h = h.wrapping_mul(Self::K1);
        h ^ (h >> 32)
    }
}

fn sum(parts: &[&[u8]]) -> u64 {
    let mut s = Sum::new();
    for p in parts {
        s.update(p);
    }
    s.finish()
}

fn get_u32(b: &[u8], at: usize) -> u32 {
    let mut a = [0u8; 4];
    if let Some(s) = b.get(at..at + 4) {
        a.copy_from_slice(s);
    }
    u32::from_le_bytes(a)
}

fn get_u64(b: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    if let Some(s) = b.get(at..at + 8) {
        a.copy_from_slice(s);
    }
    u64::from_le_bytes(a)
}

fn put(b: &mut [u8], at: usize, v: &[u8]) {
    if let Some(s) = b.get_mut(at..at + v.len()) {
        s.copy_from_slice(v);
    }
}

/// Eén superblok: de duurzame staat van het laatste checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Super {
    page_size: u32,
    segment: u32,
    file_id: u64,
    generation: u64,
    salt: u64,
    seq: u64,
    size: u64,
    region_off: u64,
    region_len: u64,
    snapshot_len: u64,
    snapshot_sum: u64,
    log_off: u64,
    log_cap: u64,
}

impl Super {
    fn encode(&self) -> [u8; SB_LEN] {
        let mut b = [0u8; SB_LEN];
        put(&mut b, 0, &MAGIC);
        put(&mut b, 8, &VERSION.to_le_bytes());
        put(&mut b, 12, &self.page_size.to_le_bytes());
        put(&mut b, 16, &self.segment.to_le_bytes());
        for (i, v) in [
            self.file_id,
            self.generation,
            self.salt,
            self.seq,
            self.size,
            self.region_off,
            self.region_len,
            self.snapshot_len,
            self.snapshot_sum,
            self.log_off,
            self.log_cap,
        ]
        .iter()
        .enumerate()
        {
            put(&mut b, 24 + 8 * i, &v.to_le_bytes());
        }
        let s = sum(&[b.get(..120).unwrap_or(&[])]);
        put(&mut b, 120, &s.to_le_bytes());
        b
    }

    /// Een geldig superblok, of `None` (leeg, half geschreven of vreemd).
    fn decode(b: &[u8]) -> Option<Self> {
        if b.get(..8)? != MAGIC || get_u32(b, 8) != VERSION {
            return None;
        }
        if sum(&[b.get(..120)?]) != get_u64(b, 120) {
            return None;
        }
        let s = Self {
            page_size: get_u32(b, 12),
            segment: get_u32(b, 16),
            file_id: get_u64(b, 24),
            generation: get_u64(b, 32),
            salt: get_u64(b, 40),
            seq: get_u64(b, 48),
            size: get_u64(b, 56),
            region_off: get_u64(b, 64),
            region_len: get_u64(b, 72),
            snapshot_len: get_u64(b, 80),
            snapshot_sum: get_u64(b, 88),
            log_off: get_u64(b, 96),
            log_cap: get_u64(b, 104),
        };
        s.is_consistent().then_some(s)
    }

    fn is_consistent(&self) -> bool {
        let pages = pages_of(self.size, self.page_size.max(1));
        valid_page_size(self.page_size)
            && self.segment == segment_for(self.page_size)
            && pages < MAX_PAGES
            && self.snapshot_len == pages * 8
            && self.region_off >= HEADER
            && (self.region_off - HEADER).is_multiple_of(u64::from(self.segment))
            && self.region_len.is_multiple_of(u64::from(self.segment))
            && self.log_off == self.region_off + align(self.snapshot_len, SECTOR)
            && self.region_len >= self.log_off - self.region_off
            && self.log_cap == self.region_len - (self.log_off - self.region_off)
            && self.region_off.checked_add(self.region_len).is_some()
    }
}

fn segment_for(page_size: u32) -> u32 {
    SEGMENT.max(page_size)
}

/// Wat een segment bevat.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Use {
    /// Geen levende bytes in de gecommitte staat; mag opnieuw.
    Free,
    /// Slots.
    Data,
    /// Het metadatagebied (snapshot en log).
    Meta,
}

/// Boekhouding van één segment.
#[derive(Clone, Copy, Debug)]
struct Seg {
    /// Levende slotbytes volgens de huidige kaart.
    live: u32,
    /// Hoogste geschreven byte binnen het segment.
    end: u32,
    usage: Use,
}

/// De schrijfkant van een container: segmenten en het open segment. Wordt
/// pas bij de eerste write uit de kaart opgebouwd.
struct Writer {
    segs: Vec<Seg>,
    open: Option<u32>,
    /// Volgende schrijfpositie in het open segment (absoluut).
    wp: u64,
    /// Bytes geschreven in deze transactie (budget van de schoonmaker).
    written: u64,
}

/// Eén containerbestand in deze eigenaar.
struct Container {
    name: [u8; 256],
    name_len: usize,
    opens: u32,
    /// De kaart in het geheugen hoort bij het bestand (anders: opnieuw laden).
    valid: bool,
    /// Een commit faalde halverwege; alles op dit bestand faalt tot heropenen.
    broken: bool,
    /// Nul: nog geen enkele write (een nieuw, leeg bestand).
    page_size: u32,
    segment: u32,
    file_id: u64,
    generation: u64,
    salt: u64,
    /// Sequence van de laatste commit.
    seq: u64,
    /// Actief superblok (0: A, 1: B).
    active: u8,
    /// Nog geen tweede superblok: de eerste commit is een checkpoint.
    fresh: bool,
    size: u64,
    map: Vec<u64>,
    region_off: u64,
    region_len: u64,
    snapshot_len: u64,
    log_off: u64,
    log_cap: u64,
    /// Gebruikt deel van het log, sectorgealigneerd.
    log_end: u64,
    /// Bekende maat van het binnenste bestand.
    physical: u64,
    writer: Option<Writer>,
    /// Sinds de laatste commit gewijzigde pagina's met de checksum van hun
    /// nieuwe slot (met dubbelen; de laatste telt).
    dirty: Vec<(u32, u32)>,
    /// Laagste paginatelling in deze transactie (afkappen).
    trunc: Option<u64>,
    pending: bool,
}

impl Container {
    fn new(name: &[u8]) -> Result<Self> {
        let mut buf = [0u8; 256];
        buf.get_mut(..name.len())
            .ok_or(Error::CANNOT_OPEN)?
            .copy_from_slice(name);
        Ok(Self {
            name: buf,
            name_len: name.len(),
            opens: 0,
            valid: false,
            broken: false,
            page_size: 0,
            segment: SEGMENT,
            file_id: 0,
            generation: 0,
            salt: 0,
            seq: 0,
            active: 0,
            fresh: true,
            size: 0,
            map: Vec::new(),
            region_off: HEADER,
            region_len: 0,
            snapshot_len: 0,
            log_off: HEADER,
            log_cap: 0,
            log_end: 0,
            physical: 0,
            writer: None,
            dirty: Vec::new(),
            trunc: None,
            pending: false,
        })
    }
    fn name(&self) -> &[u8] {
        self.name.get(..self.name_len).unwrap_or(&[])
    }
    /// Terug naar een leeg, nog niet geïnitialiseerd bestand.
    fn reset_empty(&mut self) {
        self.page_size = 0;
        self.generation = 0;
        self.fresh = true;
        self.size = 0;
        self.map = Vec::new();
        self.writer = None;
        self.dirty.clear();
        self.trunc = None;
        self.pending = false;
        self.log_end = 0;
        self.seq = 0;
        self.valid = true;
    }
    fn seg_base(&self, i: u32) -> u64 {
        HEADER + u64::from(i) * u64::from(self.segment)
    }
    fn seg_of(&self, off: u64) -> Option<u32> {
        u32::try_from(off.checked_sub(HEADER)? / u64::from(self.segment)).ok()
    }
    fn superblock(&self) -> Super {
        Super {
            page_size: self.page_size,
            segment: self.segment,
            file_id: self.file_id,
            generation: self.generation,
            salt: self.salt,
            seq: self.seq,
            size: self.size,
            region_off: self.region_off,
            region_len: self.region_len,
            snapshot_len: self.snapshot_len,
            snapshot_sum: 0,
            log_off: self.log_off,
            log_cap: self.log_cap,
        }
    }
}

/// Wat [`Compressed::stats`] over een container zegt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Logische maat (wat SQLite ziet).
    pub size: u64,
    /// Maat van het binnenste bestand.
    pub physical: u64,
    /// Bytes in levende slots.
    pub live: u64,
    /// Paginamaat van de container.
    pub page_size: u32,
    /// Segmenten met slots, vrij, en van het metadatagebied (na de eerste write).
    pub data_segments: u64,
    /// Vrije segmenten onder het einde.
    pub free_segments: u64,
    /// Segmenten van het metadatagebied.
    pub meta_segments: u64,
    /// Geschreven bytes in datasegmenten (levend plus dood).
    pub written: u64,
}

/// Een handvat van deze eigenaar.
#[derive(Clone, Copy)]
struct Handle {
    inner: FileId,
    /// `Some(i)`: container `i`, anders gewoon doorgeven.
    container: Option<usize>,
    /// Het journal (`<naam>-journal`) van container `i`: zijn afronding is een commit.
    journal_of: Option<usize>,
}

/// Werkbuffers, één keer per eigenaar.
struct Work {
    enc: Option<lz::Encoder>,
    /// Gecomprimeerde bytes van één slot.
    stored: Vec<u8>,
    /// Een gedecodeerde pagina voor deelreads en read-modify-write.
    page: Vec<u8>,
    /// Welke pagina `page` nu bevat (container, pagina).
    cached: Option<(usize, u32)>,
}

/// Transparante paginacompressie om `S` (zie de moduledocs).
///
/// # Invariants
/// Per container: `map.len() == pages_of(size)`; elk slot in de kaart ligt in
/// een `Data`-segment en telt mee in diens `live`; geen slot van de laatst
/// gecommitte staat ligt in een `Free`-segment.
pub struct Compressed<S: Storage> {
    inner: S,
    policy: Policy,
    handles: Vec<Option<Handle>>,
    /// Containers by slot; a handle keeps its slot number, so a slot is emptied
    /// (`None`) but never moved.
    files: Vec<Option<Container>>,
    work: Work,
    /// `false` tijdens een commit die SQLite niet met `sync` vroeg (de afronding van
    /// het journal bij `synchronous=OFF`): het record zonder `sync`.
    durable: bool,
}

impl<S: Storage> Compressed<S> {
    /// `inner` met `policy` voor nieuwe bestanden.
    pub fn new(inner: S, policy: Policy) -> Self {
        Self {
            inner,
            policy,
            handles: Vec::new(),
            files: Vec::new(),
            work: Work {
                enc: None,
                stored: Vec::new(),
                page: Vec::new(),
                cached: None,
            },
            durable: true,
        }
    }
    /// De binnenste opslag.
    pub fn inner(&self) -> &S {
        &self.inner
    }
    /// De binnenste opslag, veranderlijk (geen I/O op open containers).
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }
    /// Geeft de binnenste opslag terug.
    pub fn into_inner(self) -> S {
        self.inner
    }
    /// De regels voor nieuwe bestanden.
    pub fn policy(&self) -> Policy {
        self.policy
    }

    fn handle(&self, id: FileId) -> Result<Handle> {
        self.handles
            .get(id.0 as usize)
            .copied()
            .flatten()
            .ok_or(Error::IO)
    }

    fn container(&mut self, i: usize) -> Result<&mut Container> {
        let c = self
            .files
            .get_mut(i)
            .and_then(Option::as_mut)
            .ok_or(Error::IO)?;
        if c.broken { Err(Error::IO) } else { Ok(c) }
    }

    /// Statistiek van een open container, voor toetsen en meetprogramma's;
    /// `None` voor een gewoon bestand.
    pub fn stats(&mut self, id: FileId) -> Result<Option<Stats>> {
        let Some(ci) = self.handle(id)?.container else {
            return Ok(None);
        };
        let c = self.container(ci)?;
        let page = c.page_size;
        let live = c
            .map
            .iter()
            .filter_map(|&e| slot_of(e, page))
            .map(|s| u64::from(s.len))
            .sum();
        let mut out = Stats {
            size: c.size,
            physical: c.physical,
            live,
            page_size: c.page_size,
            ..Stats::default()
        };
        if let Some(w) = &c.writer {
            for g in &w.segs {
                match g.usage {
                    Use::Free => out.free_segments += 1,
                    Use::Meta => out.meta_segments += 1,
                    Use::Data => {
                        out.data_segments += 1;
                        out.written += u64::from(g.end);
                    }
                }
            }
        }
        Ok(Some(out))
    }

    fn buffers(&mut self, page_size: u32) -> Result {
        let need = page_size as usize;
        if self.work.page.len() < need {
            self.work.page.clear();
            self.work
                .page
                .try_reserve_exact(need)
                .map_err(|_| Error::MEMORY)?;
            self.work.page.resize(need, 0);
            self.work.cached = None;
        }
        if self.work.stored.len() < need {
            self.work.stored.clear();
            self.work
                .stored
                .try_reserve_exact(need)
                .map_err(|_| Error::MEMORY)?;
            self.work.stored.resize(need, 0);
        }
        Ok(())
    }
}

/// Leest precies `dst.len()` bytes; EOF ervoor is beschadiging.
fn read_exact<S: Storage>(inner: &mut S, id: FileId, mut off: u64, mut dst: &mut [u8]) -> Result {
    while !dst.is_empty() {
        let n = inner.read(id, off, dst)?;
        if n == 0 || n > dst.len() {
            return Err(CORRUPT);
        }
        off += n as u64;
        dst = dst.get_mut(n..).ok_or(CORRUPT)?;
    }
    Ok(())
}

/// Leest zoveel als er is; kortere uitvoer is EOF (de rest wordt nul).
fn read_some<S: Storage>(inner: &mut S, id: FileId, mut off: u64, dst: &mut [u8]) -> Result<usize> {
    let mut done = 0;
    while done < dst.len() {
        let n = inner.read(id, off, dst.get_mut(done..).ok_or(Error::IO)?)?;
        if n == 0 {
            break;
        }
        done += n;
        off += n as u64;
    }
    if let Some(rest) = dst.get_mut(done..) {
        rest.fill(0);
    }
    Ok(done)
}

/// Decodeert pagina `page` van `c` naar `out` (`page_size` bytes).
fn load_page<S: Storage>(
    inner: &mut S,
    stored: &mut [u8],
    c: &Container,
    id: FileId,
    page: u32,
    out: &mut [u8],
) -> Result {
    let entry = c.map.get(page as usize).copied().unwrap_or(0);
    let Some(slot) = slot_of(entry, c.page_size) else {
        out.fill(0);
        return Ok(());
    };
    if slot.len == c.page_size {
        return read_exact(inner, id, slot.off, out);
    }
    let buf = stored.get_mut(..slot.len as usize).ok_or(CORRUPT)?;
    read_exact(inner, id, slot.off, buf)?;
    match lz::decompress(buf, out) {
        Ok(n) if n == out.len() => Ok(()),
        _ => Err(CORRUPT),
    }
}

/// Leest de superblokken; `Ok(None)`: geen geldige, en het bestand is nooit
/// verder gekomen dan zijn eerste commit (blok B leeg, A leeg of half).
fn read_super<S: Storage>(inner: &mut S, id: FileId) -> Result<Option<(Super, u8)>> {
    let mut a = [0u8; SB_LEN];
    let mut b = [0u8; SB_LEN];
    read_some(inner, id, 0, &mut a)?;
    read_some(inner, id, SLOT_B, &mut b)?;
    let (sa, sb) = (Super::decode(&a), Super::decode(&b));
    match (sa, sb) {
        (Some(x), Some(y)) if y.generation > x.generation => Ok(Some((y, 1))),
        (Some(x), _) => Ok(Some((x, 0))),
        (None, Some(y)) => Ok(Some((y, 1))),
        (None, None) => {
            let a_blank = a.iter().all(|&x| x == 0) || a.get(..8) == Some(&MAGIC[..]);
            if b.iter().all(|&x| x == 0) && a_blank {
                Ok(None)
            } else {
                Err(CORRUPT)
            }
        }
    }
}

/// Speelt geldige logrecords vanaf `c.log_end` bij; het aantal records.
///
/// Met `verify_last` worden de slots van het laatste record eerst gecontroleerd:
/// alleen dat record kan duurzaam zijn terwijl een slot van dezelfde commit
/// het niet werd (een stroomstoring tijdens zijn `sync`). Klopt een slot niet,
/// dan telt die commit niet; SQLite's hete journal maakt dat consistent, want
/// de `sync` kwam nooit terug.
fn replay<S: Storage>(
    inner: &mut S,
    c: &mut Container,
    id: FileId,
    verify_last: bool,
) -> Result<u64> {
    let mut head = [0u8; RECORD_HEAD];
    let mut applied = 0;
    let mut held: Option<([u8; RECORD_HEAD], Vec<u8>, u64)> = None;
    let mut pos = c.log_end;
    let mut seq = c.seq;
    while pos + RECORD_HEAD as u64 <= c.log_cap {
        let at = c.log_off + pos;
        if read_some(inner, id, at, &mut head)? < RECORD_HEAD {
            break;
        }
        let count = u64::from(get_u32(&head, 4));
        let len = RECORD_HEAD as u64 + count * ENTRY as u64;
        if get_u32(&head, 0) != RECORD_MAGIC
            || get_u64(&head, 8) != c.salt
            || get_u64(&head, 16) != seq + 1
            || pos + len > c.log_cap
        {
            break;
        }
        let n = usize::try_from(count * ENTRY as u64).map_err(|_| CORRUPT)?;
        let mut body = Vec::new();
        body.try_reserve_exact(n).map_err(|_| Error::MEMORY)?;
        body.resize(n, 0);
        if read_some(inner, id, at + RECORD_HEAD as u64, &mut body)? < n {
            break;
        }
        let mut sealed = head;
        put(&mut sealed, 40, &[0; 8]);
        if sum(&[&sealed, &body]) != get_u64(&head, 40) {
            break;
        }
        pos += align(len, SECTOR);
        seq += 1;
        if verify_last {
            if let Some((h, b, end)) = held.take() {
                apply(c, &h, &b)?;
                c.log_end = end;
                c.seq += 1;
                applied += 1;
            }
            held = Some((head, body, pos));
        } else {
            apply(c, &head, &body)?;
            c.log_end = pos;
            c.seq += 1;
            applied += 1;
        }
    }
    if let Some((h, b, end)) = held
        && slots_intact(inner, c, id, &b)?
    {
        apply(c, &h, &b)?;
        c.log_end = end;
        c.seq += 1;
        applied += 1;
    }
    Ok(applied)
}

/// Hebben alle slots van een record de checksum uit het record?
fn slots_intact<S: Storage>(inner: &mut S, c: &Container, id: FileId, body: &[u8]) -> Result<bool> {
    let mut buf = Vec::new();
    for e in body.chunks_exact(ENTRY) {
        let entry = get_u64(e, 4);
        let Some(s) = slot_of(entry, c.page_size) else {
            continue;
        };
        if check_entry(c, entry).is_err() {
            return Ok(false);
        }
        buf.clear();
        buf.try_reserve_exact(s.len as usize)
            .map_err(|_| Error::MEMORY)?;
        buf.resize(s.len as usize, 0);
        if read_some(inner, id, s.off, &mut buf)? < buf.len() || check(&buf) != get_u32(e, 12) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// De checksum van slotbytes in een record.
fn check(bytes: &[u8]) -> u32 {
    sum(&[bytes]) as u32
}

/// Past één gevalideerd record toe op de kaart.
fn apply(c: &mut Container, head: &[u8], body: &[u8]) -> Result {
    let size = get_u64(head, 24);
    let trunc = get_u64(head, 32);
    let pages = pages_of(size, c.page_size);
    if pages >= MAX_PAGES {
        return Err(CORRUPT);
    }
    if trunc < c.map.len() as u64 {
        c.map.truncate(trunc as usize);
    }
    for e in body.chunks_exact(ENTRY) {
        let page = get_u32(e, 0) as usize;
        let entry = get_u64(e, 4);
        check_entry(c, entry)?;
        if page >= c.map.len() {
            grow(&mut c.map, page + 1)?;
        }
        if let Some(m) = c.map.get_mut(page) {
            *m = entry;
        }
    }
    c.map.truncate(pages as usize);
    grow(&mut c.map, pages as usize)?;
    c.size = size;
    Ok(())
}

/// Een ingang die binnen het bestand en in één segment ligt.
fn check_entry(c: &Container, entry: u64) -> Result {
    let Some(s) = slot_of(entry, c.page_size) else {
        return if entry == 0 { Ok(()) } else { Err(CORRUPT) };
    };
    let seg = c.seg_of(s.off).ok_or(CORRUPT)?;
    let end = s.off + u64::from(s.len);
    if s.len == 0 || s.len > c.page_size || end > c.seg_base(seg) + u64::from(c.segment) {
        return Err(CORRUPT);
    }
    Ok(())
}

/// Vergroot `v` faalbaar tot `n` nullen.
fn grow(v: &mut Vec<u64>, n: usize) -> Result {
    if n > v.len() {
        v.try_reserve(n - v.len()).map_err(|_| Error::MEMORY)?;
        v.resize(n, 0);
    }
    Ok(())
}

/// Laadt de hele staat uit superblok `sb`.
fn load<S: Storage>(inner: &mut S, c: &mut Container, id: FileId, sb: Super, active: u8) -> Result {
    let pages = usize::try_from(sb.snapshot_len / 8).map_err(|_| CORRUPT)?;
    if sb.region_off + sb.snapshot_len > c.physical {
        return Err(CORRUPT);
    }
    c.page_size = sb.page_size;
    c.segment = sb.segment;
    c.file_id = sb.file_id;
    c.generation = sb.generation;
    c.salt = sb.salt;
    c.seq = sb.seq;
    c.active = active;
    c.size = sb.size;
    c.region_off = sb.region_off;
    c.region_len = sb.region_len;
    c.snapshot_len = sb.snapshot_len;
    c.log_off = sb.log_off;
    c.log_cap = sb.log_cap;
    c.log_end = 0;
    c.writer = None;
    c.dirty.clear();
    c.trunc = None;
    c.pending = false;
    let mut map = Vec::new();
    map.try_reserve_exact(pages).map_err(|_| Error::MEMORY)?;
    let mut buf = Vec::new();
    buf.try_reserve_exact(CHUNK.min(pages * 8))
        .map_err(|_| Error::MEMORY)?;
    let mut s = Sum::new();
    let mut at = 0usize;
    while at < pages * 8 {
        let n = CHUNK.min(pages * 8 - at);
        buf.clear();
        buf.resize(n, 0);
        read_exact(inner, id, sb.region_off + at as u64, &mut buf)?;
        s.update(&buf);
        for e in buf.chunks_exact(8) {
            map.push(get_u64(e, 0));
        }
        at += n;
    }
    if s.finish() != sb.snapshot_sum {
        return Err(CORRUPT);
    }
    c.map = map;
    for &e in &c.map {
        check_entry(c, e)?;
    }
    replay(inner, c, id, true)?;
    c.fresh = false;
    c.valid = true;
    Ok(())
}

/// Brengt een container in deze eigenaar bij met het bestand.
fn refresh<S: Storage>(inner: &mut S, c: &mut Container, id: FileId) -> Result {
    c.physical = inner.size(id)?;
    if c.physical == 0 {
        c.reset_empty();
        return Ok(());
    }
    match read_super(inner, id)? {
        None => {
            c.reset_empty();
            Ok(())
        }
        Some((sb, active)) => {
            if c.valid
                && c.page_size != 0
                && sb.file_id == c.file_id
                && sb.generation == c.generation
                && sb.salt == c.salt
            {
                if replay(inner, c, id, false)? > 0 {
                    c.writer = None;
                }
                c.active = active;
                Ok(())
            } else {
                load(inner, c, id, sb, active)
            }
        }
    }
}

/// Bouwt de segmentboekhouding uit de kaart (eerste write na laden).
fn build_writer(c: &mut Container) -> Result {
    if c.writer.is_some() {
        return Ok(());
    }
    let seg = u64::from(c.segment);
    let mut count = c.physical.saturating_sub(HEADER).div_ceil(seg);
    count = count.max((c.region_off + c.region_len - HEADER).div_ceil(seg));
    let mut segs = Vec::new();
    let n = usize::try_from(count).map_err(|_| Error::MEMORY)?;
    segs.try_reserve_exact(n).map_err(|_| Error::MEMORY)?;
    segs.resize(
        n,
        Seg {
            live: 0,
            end: 0,
            usage: Use::Free,
        },
    );
    for &e in &c.map {
        let Some(s) = slot_of(e, c.page_size) else {
            continue;
        };
        let i = c.seg_of(s.off).ok_or(CORRUPT)? as usize;
        let base = c.seg_base(i as u32);
        let g = segs.get_mut(i).ok_or(CORRUPT)?;
        g.live += s.len;
        g.end = g
            .end
            .max(u32::try_from(s.off + u64::from(s.len) - base).map_err(|_| CORRUPT)?);
        g.usage = Use::Data;
    }
    if c.region_len > 0 {
        let first = c.seg_of(c.region_off).ok_or(CORRUPT)?;
        for k in 0..(c.region_len / seg) as u32 {
            let g = segs.get_mut((first + k) as usize).ok_or(CORRUPT)?;
            if g.usage == Use::Data {
                return Err(CORRUPT);
            }
            g.usage = Use::Meta;
        }
    }
    // Verder schrijven waar de laatste levende bytes eindigen, als daar nog
    // ruimte is: een klein bestand groeit dan niet met een segment per open.
    let mut open = None;
    let mut wp = 0;
    let mut best = 0;
    for (i, g) in segs.iter().enumerate() {
        let end = c.seg_base(i as u32) + u64::from(g.end);
        if g.usage == Use::Data && end > best {
            best = end;
            let next = align(end, SECTOR);
            if next < c.seg_base(i as u32) + seg {
                open = Some(i as u32);
                wp = next;
            } else {
                open = None;
            }
        }
    }
    c.writer = Some(Writer {
        segs,
        open,
        wp,
        written: 0,
    });
    Ok(())
}

/// Ruimte voor `len` bytes in het open segment; opent zo nodig het laagste
/// vrije segment (of een nieuw aan het eind).
fn alloc(c: &mut Container, len: u32) -> Result<u64> {
    let seg = u64::from(c.segment);
    let base_of = |i: u32| HEADER + u64::from(i) * seg;
    let w = c.writer.as_mut().ok_or(Error::MISUSE)?;
    if let Some(i) = w.open
        && w.wp + u64::from(len) <= base_of(i) + seg
    {
        let off = w.wp;
        w.wp += u64::from(len);
        let g = w.segs.get_mut(i as usize).ok_or(Error::IO)?;
        g.end = u32::try_from(w.wp - base_of(i)).map_err(|_| Error::IO)?;
        return Ok(off);
    }
    let i = match w.segs.iter().position(|g| g.usage == Use::Free) {
        Some(i) => i,
        None => {
            w.segs.try_reserve(1).map_err(|_| Error::MEMORY)?;
            w.segs.push(Seg {
                live: 0,
                end: 0,
                usage: Use::Free,
            });
            w.segs.len() - 1
        }
    };
    let i = u32::try_from(i).map_err(|_| Error::FULL)?;
    let g = w.segs.get_mut(i as usize).ok_or(Error::IO)?;
    g.usage = Use::Data;
    g.end = len;
    g.live = 0;
    w.open = Some(i);
    w.wp = base_of(i) + u64::from(len);
    Ok(base_of(i))
}

/// Telt een slot af of bij in de boekhouding van zijn segment.
fn account(c: &mut Container, entry: u64, add: bool) -> Result {
    let Some(s) = slot_of(entry, c.page_size) else {
        return Ok(());
    };
    let i = c.seg_of(s.off).ok_or(Error::IO)? as usize;
    let w = c.writer.as_mut().ok_or(Error::MISUSE)?;
    let g = w.segs.get_mut(i).ok_or(Error::IO)?;
    g.live = if add {
        g.live.checked_add(s.len).ok_or(Error::IO)?
    } else {
        g.live.checked_sub(s.len).ok_or(Error::IO)?
    };
    Ok(())
}

/// Zet `entry` als nieuwe ingang van `page` en boekt het oude slot af.
fn set_entry(c: &mut Container, page: u32, entry: u64, sum: u32) -> Result {
    let p = page as usize;
    if p >= c.map.len() {
        grow(&mut c.map, p + 1)?;
    }
    let old = c.map.get(p).copied().unwrap_or(0);
    account(c, old, false)?;
    account(c, entry, true)?;
    if let Some(m) = c.map.get_mut(p) {
        *m = entry;
    }
    c.dirty.try_reserve(1).map_err(|_| Error::MEMORY)?;
    c.dirty.push((page, sum));
    c.pending = true;
    Ok(())
}

/// Comprimeert en schrijft één hele pagina.
fn store<S: Storage>(
    inner: &mut S,
    enc: &mut lz::Encoder,
    stored: &mut [u8],
    c: &mut Container,
    id: FileId,
    page: u32,
    data: &[u8],
) -> Result {
    if u64::from(page) >= MAX_PAGES {
        return Err(Error::FULL);
    }
    if data.iter().all(|&b| b == 0) {
        return set_entry(c, page, 0, 0);
    }
    // Alleen gecomprimeerd als het kleiner is dan de pagina en in de
    // lengtecode past; anders onsamendrukbaar, rauw.
    let limit = (c.page_size as usize - 1).min(RAW as usize - 1);
    let (bytes, len) = match enc.compress(data, stored.get_mut(..limit).ok_or(Error::IO)?) {
        Some(n) => (stored.get(..n).ok_or(Error::IO)?, n as u32),
        None => (data, c.page_size),
    };
    let off = alloc(c, len)?;
    inner.write(id, off, bytes)?;
    c.physical = c.physical.max(off + u64::from(len));
    if let Some(w) = c.writer.as_mut() {
        w.written += u64::from(len);
    }
    set_entry(c, page, entry_of(off, len, c.page_size), check(bytes))
}

impl<S: Storage> Compressed<S> {
    fn new_handle(&mut self, h: Handle) -> Result<FileId> {
        let slot = match self.handles.iter().position(Option::is_none) {
            Some(i) => i,
            None => {
                self.handles.try_reserve(1).map_err(|_| Error::MEMORY)?;
                self.handles.push(None);
                self.handles.len() - 1
            }
        };
        if let Some(s) = self.handles.get_mut(slot) {
            *s = Some(h);
        }
        Ok(FileId(u32::try_from(slot).map_err(|_| Error::FULL)?))
    }

    /// Opent of hergebruikt container `name` met handvat `id` van de binnenste opslag.
    fn open_container(&mut self, name: &[u8], id: FileId, create: bool) -> Result<Option<usize>> {
        let size = self.inner.size(id)?;
        let ci = self
            .files
            .iter()
            .position(|c| c.as_ref().is_some_and(|c| c.name() == name));
        if let Some(i) = ci
            && let Some(Some(c)) = self.files.get(i)
            && c.opens > 0
        {
            // Al open in deze eigenaar: die kaart is de waarheid.
            return Ok(Some(i));
        }
        let mut head = [0u8; 8];
        let got = if size > 0 {
            read_some(&mut self.inner, id, 0, &mut head)?
        } else {
            0
        };
        let ours = got == 8 && head == MAGIC;
        let blank = size > 0 && head.iter().all(|&b| b == 0);
        if !(ours || (create && (size == 0 || blank))) {
            if let Some(i) = ci {
                if let Some(slot) = self.files.get_mut(i) {
                    *slot = None;
                }
                self.work.cached = None;
            }
            return Ok(None);
        }
        let i = match ci {
            Some(i) => i,
            None => {
                let c = Container::new(name)?;
                match self.files.iter().position(Option::is_none) {
                    Some(free) => {
                        if let Some(slot) = self.files.get_mut(free) {
                            *slot = Some(c);
                        }
                        free
                    }
                    None => {
                        self.files.try_reserve(1).map_err(|_| Error::MEMORY)?;
                        self.files.push(Some(c));
                        self.files.len() - 1
                    }
                }
            }
        };
        self.work.cached = None;
        let c = self
            .files
            .get_mut(i)
            .and_then(Option::as_mut)
            .ok_or(Error::IO)?;
        c.broken = false;
        if let Err(e) = refresh(&mut self.inner, c, id) {
            c.valid = false;
            return Err(e);
        }
        if c.page_size == 0 && !create {
            // Leeg zonder toestemming om te comprimeren: gewoon bestand.
            if let Some(slot) = self.files.get_mut(i) {
                *slot = None;
            }
            return Ok(None);
        }
        Ok(Some(i))
    }
}

impl<S: Storage> Compressed<S> {
    /// Maakt van een leeg bestand een container met paginamaat uit `src`.
    fn init(&mut self, ci: usize, id: FileId, offset: u64, src: &[u8]) -> Result {
        let mut page_size = self.policy.page_size;
        if offset == 0 && src.len() >= 100 && src.get(..16) == Some(b"SQLite format 3\0") {
            let raw = u16::from_be_bytes([src[16], src[17]]);
            page_size = if raw == 1 { 65536 } else { u32::from(raw) };
        }
        if !valid_page_size(page_size) {
            return Err(Error::MISUSE);
        }
        let mut ids = [0u8; 16];
        self.inner.random(&mut ids)?;
        let c = self.container(ci)?;
        c.page_size = page_size;
        c.segment = segment_for(page_size);
        c.file_id = get_u64(&ids, 0);
        c.salt = get_u64(&ids, 8);
        c.generation = 1;
        c.seq = 0;
        c.active = 0;
        c.fresh = true;
        c.size = 0;
        c.map.clear();
        c.region_off = HEADER;
        c.region_len = u64::from(c.segment);
        c.snapshot_len = 0;
        c.log_off = HEADER;
        c.log_cap = c.region_len;
        c.log_end = 0;
        c.writer = None;
        let mut sb = c.superblock();
        sb.snapshot_sum = sum(&[]);
        let bytes = sb.encode();
        c.physical = c.physical.max(SB_LEN as u64);
        // Superblok A van de lege staat; blok B volgt bij de eerste commit.
        self.inner.write(id, 0, &bytes)?;
        let c = self.container(ci)?;
        let mut segs = Vec::new();
        segs.try_reserve_exact(1).map_err(|_| Error::MEMORY)?;
        segs.push(Seg {
            live: 0,
            end: 0,
            usage: Use::Meta,
        });
        c.writer = Some(Writer {
            segs,
            open: None,
            wp: 0,
            written: 0,
        });
        c.pending = true;
        Ok(())
    }

    fn encoder(&mut self) -> Result {
        if self.work.enc.is_none() {
            self.work.enc = Some(lz::Encoder::new().ok_or(Error::MEMORY)?);
        }
        Ok(())
    }

    fn write_container(&mut self, ci: usize, id: FileId, offset: u64, src: &[u8]) -> Result {
        if self.container(ci)?.page_size == 0 {
            self.init(ci, id, offset, src)?;
        }
        let page_size = self.container(ci)?.page_size;
        self.buffers(page_size)?;
        self.encoder()?;
        build_writer(self.container(ci)?)?;
        let p = u64::from(page_size);
        let end = offset.checked_add(src.len() as u64).ok_or(Error::FULL)?;
        if pages_of(end, page_size) >= MAX_PAGES {
            return Err(Error::FULL);
        }
        self.work.cached = None;
        let mut pos = 0usize;
        while pos < src.len() {
            let at = offset + pos as u64;
            let page = u32::try_from(at / p).map_err(|_| Error::FULL)?;
            let within = (at % p) as usize;
            let n = (page_size as usize - within).min(src.len() - pos);
            let piece = src.get(pos..pos + n).ok_or(Error::IO)?;
            let Self {
                inner, files, work, ..
            } = self;
            let c = files
                .get_mut(ci)
                .and_then(Option::as_mut)
                .ok_or(Error::IO)?;
            let enc = work.enc.as_mut().ok_or(Error::MEMORY)?;
            if n == page_size as usize {
                store(inner, enc, &mut work.stored, c, id, page, piece)?;
            } else {
                // Read-modify-write van een deel van een pagina.
                let scratch = work.page.get_mut(..page_size as usize).ok_or(Error::IO)?;
                if u64::from(page) * p < c.size {
                    load_page(inner, &mut work.stored, c, id, page, scratch)?;
                    // Voorbij de logische maat is alles nul, ook na een eerdere krimp.
                    let visible = (c.size - u64::from(page) * p).min(p) as usize;
                    if let Some(t) = scratch.get_mut(visible..) {
                        t.fill(0);
                    }
                } else {
                    scratch.fill(0);
                }
                scratch
                    .get_mut(within..within + n)
                    .ok_or(Error::IO)?
                    .copy_from_slice(piece);
                store(inner, enc, &mut work.stored, c, id, page, scratch)?;
            }
            pos += n;
        }
        let c = self.container(ci)?;
        if end > c.size {
            c.size = end;
            grow(&mut c.map, pages_of(end, page_size) as usize)?;
        }
        c.pending = true;
        Ok(())
    }

    fn read_container(
        &mut self,
        ci: usize,
        id: FileId,
        offset: u64,
        dst: &mut [u8],
    ) -> Result<usize> {
        let (size, page_size) = {
            let c = self.container(ci)?;
            (c.size, c.page_size)
        };
        if offset >= size || page_size == 0 {
            return Ok(0);
        }
        self.buffers(page_size)?;
        let p = u64::from(page_size);
        let total = (dst.len() as u64).min(size - offset) as usize;
        let mut pos = 0usize;
        while pos < total {
            let at = offset + pos as u64;
            let page = u32::try_from(at / p).map_err(|_| Error::IO)?;
            let within = (at % p) as usize;
            let n = (page_size as usize - within).min(total - pos);
            let Self {
                inner, files, work, ..
            } = self;
            let c = files.get(ci).and_then(Option::as_ref).ok_or(Error::IO)?;
            let out = dst.get_mut(pos..pos + n).ok_or(Error::IO)?;
            if n == page_size as usize {
                load_page(inner, &mut work.stored, c, id, page, out)?;
            } else {
                let scratch = work.page.get_mut(..page_size as usize).ok_or(Error::IO)?;
                if work.cached != Some((ci, page)) {
                    work.cached = None;
                    load_page(inner, &mut work.stored, c, id, page, scratch)?;
                    work.cached = Some((ci, page));
                }
                out.copy_from_slice(scratch.get(within..within + n).ok_or(Error::IO)?);
            }
            pos += n;
        }
        Ok(total)
    }

    fn truncate_container(&mut self, ci: usize, id: FileId, size: u64) -> Result {
        let c = self.container(ci)?;
        if c.page_size == 0 {
            if size == 0 {
                return Ok(());
            }
            self.init(ci, id, 1, &[])?;
        }
        let c = self.container(ci)?;
        if size >= c.size {
            if size > c.size {
                let page_size = c.page_size;
                c.size = size;
                grow(&mut c.map, pages_of(size, page_size) as usize)?;
                c.pending = true;
            }
            return Ok(());
        }
        let page_size = c.page_size;
        self.work.cached = None;
        build_writer(self.container(ci)?)?;
        let p = u64::from(page_size);
        let keep = pages_of(size, page_size);
        // Een halve laatste pagina: de staart wordt nul (een latere groei leest nul).
        if !size.is_multiple_of(p) {
            let last = (size / p) as u32;
            let within = (size % p) as usize;
            self.buffers(page_size)?;
            self.encoder()?;
            let Self {
                inner, files, work, ..
            } = self;
            let c = files
                .get_mut(ci)
                .and_then(Option::as_mut)
                .ok_or(Error::IO)?;
            let scratch = work.page.get_mut(..page_size as usize).ok_or(Error::IO)?;
            load_page(inner, &mut work.stored, c, id, last, scratch)?;
            if let Some(t) = scratch.get_mut(within..) {
                t.fill(0);
            }
            let enc = work.enc.as_mut().ok_or(Error::MEMORY)?;
            store(inner, enc, &mut work.stored, c, id, last, scratch)?;
        }
        let c = self.container(ci)?;
        let gone: Vec<u64> = c
            .map
            .get(keep as usize..)
            .map(<[u64]>::to_vec)
            .unwrap_or_default();
        for e in gone {
            account(c, e, false)?;
        }
        c.map.truncate(keep as usize);
        c.size = size;
        c.trunc = Some(c.trunc.map_or(keep, |t| t.min(keep)));
        c.pending = true;
        Ok(())
    }
}

impl<S: Storage> Compressed<S> {
    /// Eén commit van container `ci` (zie de moduledocs).
    fn commit(&mut self, ci: usize, id: FileId) -> Result {
        let c = self.container(ci)?;
        if !c.pending {
            return self.inner.sync(id, 3);
        }
        let out = self.commit_inner(ci, id);
        if out.is_err()
            && let Some(Some(c)) = self.files.get_mut(ci)
        {
            // Onbekend wat er duurzaam is: pas na heropenen verder.
            c.broken = true;
            c.valid = false;
        }
        out
    }

    /// Een commit zonder `sync` op het moment dat SQLite zijn transactie afrondt
    /// zonder de database te synchroniseren (`synchronous=OFF`, een rollback): het
    /// journal wordt verwijderd, op nul gekapt of zijn kop gewist. Zo ziet een andere
    /// eigenaar (een ander proces) wat er geschreven is, zoals bij een gewoon bestand;
    /// duurzaam is het pas na een volgende `sync`, zoals SQLite bij `OFF` ook belooft.
    /// Een checkpoint blijft wel gesynct: het superblok mag nooit naar een half
    /// snapshot wijzen.
    fn settle(&mut self, ci: usize) -> Result {
        let pending = self
            .files
            .get(ci)
            .and_then(Option::as_ref)
            .is_some_and(|c| c.pending && !c.broken && c.opens > 0);
        if !pending {
            return Ok(());
        }
        let Some(id) = self
            .handles
            .iter()
            .flatten()
            .find(|h| h.container == Some(ci))
            .map(|h| h.inner)
        else {
            return Ok(());
        };
        self.durable = false;
        let out = self.commit(ci, id);
        self.durable = true;
        out
    }

    /// De container met open handvatten waarvan `name` het journal is.
    fn journal_owner(&self, name: &[u8]) -> Option<usize> {
        let base = name.strip_suffix(b"-journal")?;
        self.files
            .iter()
            .position(|c| c.as_ref().is_some_and(|c| c.opens > 0 && c.name() == base))
    }

    fn commit_inner(&mut self, ci: usize, id: FileId) -> Result {
        build_writer(self.container(ci)?)?;
        self.clean(ci, id)?;
        let c = self.container(ci)?;
        if c.fresh {
            return self.checkpoint(ci, id);
        }
        // De laatste write van een pagina telt: stabiel sorteren, dan per
        // pagina de laatste houden.
        c.dirty.sort_by_key(|&(p, _)| p);
        let mut keep: Vec<(u32, u32)> = Vec::new();
        keep.try_reserve_exact(c.dirty.len())
            .map_err(|_| Error::MEMORY)?;
        for &(p, s) in &c.dirty {
            match keep.last_mut() {
                Some(last) if last.0 == p => last.1 = s,
                _ => keep.push((p, s)),
            }
        }
        c.dirty = keep;
        let count = c.dirty.len();
        let len = RECORD_HEAD + count * ENTRY;
        if (c.log_end + len as u64) > c.log_cap {
            return self.checkpoint(ci, id);
        }
        let mut rec = Vec::new();
        rec.try_reserve_exact(len).map_err(|_| Error::MEMORY)?;
        rec.resize(len, 0);
        put(&mut rec, 0, &RECORD_MAGIC.to_le_bytes());
        put(&mut rec, 4, &(count as u32).to_le_bytes());
        put(&mut rec, 8, &c.salt.to_le_bytes());
        put(&mut rec, 16, &(c.seq + 1).to_le_bytes());
        put(&mut rec, 24, &c.size.to_le_bytes());
        put(&mut rec, 32, &c.trunc.unwrap_or(u64::MAX).to_le_bytes());
        for (k, &(page, check)) in c.dirty.iter().enumerate() {
            let at = RECORD_HEAD + k * ENTRY;
            put(&mut rec, at, &page.to_le_bytes());
            let e = c.map.get(page as usize).copied().unwrap_or(0);
            put(&mut rec, at + 4, &e.to_le_bytes());
            put(&mut rec, at + 12, &check.to_le_bytes());
        }
        let s = sum(&[&rec]);
        put(&mut rec, 40, &s.to_le_bytes());
        let at = c.log_off + c.log_end;
        self.inner.write(id, at, &rec)?;
        if self.durable {
            self.inner.sync(id, 3)?;
        }
        let c = self.container(ci)?;
        c.physical = c.physical.max(at + len as u64);
        c.log_end += align(len as u64, SECTOR);
        c.seq += 1;
        self.after_commit(ci, id)
    }

    /// Hele kaart als nieuw snapshot, dan het andere superblok.
    fn checkpoint(&mut self, ci: usize, id: FileId) -> Result {
        let c = self.container(ci)?;
        let seg = u64::from(c.segment);
        let pages = c.map.len() as u64;
        let snapshot_len = pages * 8;
        let snap = align(snapshot_len, SECTOR);
        let log = align(snapshot_len / 2, SECTOR).clamp(MIN_LOG, MAX_LOG);
        let region_len = align(snap + log, seg);
        let k = (region_len / seg) as usize;
        // De eerste commit mag zijn lege gebied hergebruiken: niets daarin
        // telt (blok A beschrijft een lege staat met een ander zout).
        let in_place = c.fresh && region_len <= c.region_len;
        let old = (c.region_off, c.region_len);
        let region_off = if in_place {
            c.region_off
        } else {
            let w = c.writer.as_mut().ok_or(Error::MISUSE)?;
            let open = w.open.map(|o| o as usize);
            let mut run = 0usize;
            let mut start = None;
            for (i, g) in w.segs.iter().enumerate() {
                if g.usage == Use::Free && Some(i) != open {
                    run += 1;
                    if run == k {
                        start = Some(i + 1 - k);
                        break;
                    }
                } else {
                    run = 0;
                }
            }
            let first = match start {
                Some(s) => s,
                None => {
                    // Aan het eind, eventueel aansluitend op een vrije staart.
                    let tail = w
                        .segs
                        .iter()
                        .rev()
                        .take_while(|g| g.usage == Use::Free)
                        .count()
                        .min(k);
                    let tail = if open.is_some_and(|o| o + tail >= w.segs.len()) {
                        0
                    } else {
                        tail
                    };
                    let s = w.segs.len() - tail;
                    w.segs.try_reserve(k - tail).map_err(|_| Error::MEMORY)?;
                    while w.segs.len() < s + k {
                        w.segs.push(Seg {
                            live: 0,
                            end: 0,
                            usage: Use::Free,
                        });
                    }
                    s
                }
            };
            for g in w.segs.iter_mut().skip(first).take(k) {
                g.usage = Use::Meta;
                g.live = 0;
                g.end = 0;
            }
            HEADER + first as u64 * seg
        };
        // Het snapshot, in stappen geschreven en gesomd.
        let mut s = Sum::new();
        let mut buf = Vec::new();
        buf.try_reserve_exact(CHUNK.min(snapshot_len as usize).max(8))
            .map_err(|_| Error::MEMORY)?;
        let mut at = 0usize;
        while at < pages as usize {
            let n = (CHUNK / 8).min(pages as usize - at);
            buf.clear();
            let c = self.container(ci)?;
            for &e in c.map.get(at..at + n).ok_or(Error::IO)? {
                buf.extend_from_slice(&e.to_le_bytes());
            }
            s.update(&buf);
            self.inner.write(id, region_off + at as u64 * 8, &buf)?;
            at += n;
        }
        self.inner.sync(id, 3)?;
        let mut salt = [0u8; 8];
        self.inner.random(&mut salt)?;
        let c = self.container(ci)?;
        c.physical = c.physical.max(region_off + snapshot_len);
        let mut sb = c.superblock();
        sb.generation = c.generation + 1;
        sb.salt = u64::from_le_bytes(salt);
        sb.seq = c.seq + 1;
        sb.region_off = region_off;
        sb.region_len = region_len;
        sb.snapshot_len = snapshot_len;
        sb.snapshot_sum = s.finish();
        sb.log_off = region_off + snap;
        sb.log_cap = region_len - snap;
        let target = if c.active == 0 { SLOT_B } else { 0 };
        self.inner.write(id, target, &sb.encode())?;
        self.inner.sync(id, 3)?;
        let c = self.container(ci)?;
        c.generation = sb.generation;
        c.salt = sb.salt;
        c.seq = sb.seq;
        c.active = if target == 0 { 0 } else { 1 };
        c.region_off = sb.region_off;
        c.region_len = sb.region_len;
        c.snapshot_len = sb.snapshot_len;
        c.log_off = sb.log_off;
        c.log_cap = sb.log_cap;
        c.log_end = 0;
        c.fresh = false;
        if !in_place {
            let first = c.seg_of(old.0).ok_or(Error::IO)? as usize;
            let w = c.writer.as_mut().ok_or(Error::MISUSE)?;
            for g in w.segs.iter_mut().skip(first).take((old.1 / seg) as usize) {
                if g.usage == Use::Meta {
                    g.usage = Use::Free;
                    g.end = 0;
                }
            }
        }
        self.after_commit(ci, id)
    }

    /// Na een duurzame commit: lege segmenten vrij, volgende writes op een
    /// nieuwe sector, eventueel een kortere staart.
    fn after_commit(&mut self, ci: usize, id: FileId) -> Result {
        let c = self.container(ci)?;
        c.dirty.clear();
        c.trunc = None;
        c.pending = false;
        let seg = u64::from(c.segment);
        let meta_end = c.log_off + c.log_end;
        let w = c.writer.as_mut().ok_or(Error::MISUSE)?;
        w.written = 0;
        for (i, g) in w.segs.iter_mut().enumerate() {
            if g.usage == Use::Data && g.live == 0 && w.open != Some(i as u32) {
                g.usage = Use::Free;
                g.end = 0;
            }
        }
        if let Some(i) = w.open {
            w.wp = align(w.wp, SECTOR);
            if w.wp >= HEADER + (u64::from(i) + 1) * seg {
                w.open = None;
            }
        }
        let mut target = HEADER.max(meta_end);
        for (i, g) in w.segs.iter().enumerate() {
            if g.usage == Use::Data {
                target = target.max(HEADER + i as u64 * seg + u64::from(g.end));
            }
        }
        while w.segs.last().is_some_and(|g| g.usage == Use::Free) {
            w.segs.pop();
        }
        if let Some(i) = w.open
            && i as usize >= w.segs.len()
        {
            w.open = None;
        }
        if target < c.physical {
            self.inner.truncate(id, target)?;
            self.container(ci)?.physical = target;
        }
        Ok(())
    }

    /// De schoonmaker: binnen een budget levende slots verplaatsen uit segmenten
    /// die vooral dood zijn, of uit de staart als er onderin veel vrij is.
    fn clean(&mut self, ci: usize, id: FileId) -> Result {
        let c = self.container(ci)?;
        let seg = u64::from(c.segment);
        let budget_start = {
            let w = c.writer.as_ref().ok_or(Error::MISUSE)?;
            // Twee keer wat deze transactie schreef houdt de schoonmaker bij;
            // staat er onder het einde veel vrij (na VACUUM of een grote
            // DELETE), dan krimpt het bestand met ruim 2 MiB per commit.
            let last = w
                .segs
                .iter()
                .rposition(|g| g.usage == Use::Data)
                .unwrap_or(0);
            let free = w
                .segs
                .iter()
                .take(last)
                .filter(|g| g.usage == Use::Free)
                .count() as u64;
            let floor = if free * 4 > last as u64 + 1 {
                32 * seg
            } else {
                4 * seg
            };
            (2 * w.written).max(floor)
        };
        let mut budget = budget_start;
        for _ in 0..64 {
            let c = self.container(ci)?;
            let w = c.writer.as_mut().ok_or(Error::MISUSE)?;
            let open = w.open;
            let mut used = 0u64;
            let mut live = 0u64;
            let mut last_data = None;
            for (i, g) in w.segs.iter().enumerate() {
                if g.usage == Use::Data {
                    used += u64::from(g.end);
                    live += u64::from(g.live);
                    last_data = Some(i);
                }
            }
            let Some(last) = last_data else {
                return Ok(());
            };
            let free_below = w
                .segs
                .iter()
                .take(last)
                .filter(|g| g.usage == Use::Free)
                .count() as u64;
            let dead = used - live;
            let victim = if dead * 5 > used && dead > 4 * seg {
                w.segs
                    .iter()
                    .enumerate()
                    .filter(|&(i, g)| g.usage == Use::Data && Some(i as u32) != open && g.live > 0)
                    .min_by_key(|&(_, g)| g.live)
                    .map(|(i, _)| i)
            } else if free_below * 10 > last as u64 + 1 && free_below > 4 {
                // Krimpen: het hoogste segment naar beneden, via een open
                // segment onder hem.
                if open.is_some_and(|o| o as usize >= last) {
                    w.open = None;
                }
                let lowest = w.segs.iter().position(|g| g.usage == Use::Free);
                lowest.filter(|&l| l < last).map(|_| last)
            } else {
                None
            };
            let Some(victim) = victim else {
                return Ok(());
            };
            if Some(victim as u32) == w.open {
                return Ok(());
            }
            let moved = self.evacuate(ci, id, victim as u32)?;
            budget = budget.saturating_sub(moved.max(seg / 4));
            if budget == 0 {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Verplaatst alle levende slots van segment `victim` naar het open segment.
    fn evacuate(&mut self, ci: usize, id: FileId, victim: u32) -> Result<u64> {
        let (page_size, base, end) = {
            let c = self.container(ci)?;
            let b = c.seg_base(victim);
            (c.page_size, b, b + u64::from(c.segment))
        };
        self.buffers(page_size)?;
        let mut moved = 0u64;
        let count = self.container(ci)?.map.len();
        for page in 0..count {
            let Self {
                inner, files, work, ..
            } = self;
            let c = files
                .get_mut(ci)
                .and_then(Option::as_mut)
                .ok_or(Error::IO)?;
            let e = c.map.get(page).copied().unwrap_or(0);
            let Some(s) = slot_of(e, page_size) else {
                continue;
            };
            if s.off < base || s.off >= end {
                continue;
            }
            let buf = work.stored.get_mut(..s.len as usize).ok_or(Error::IO)?;
            read_exact(inner, id, s.off, buf)?;
            let off = alloc(c, s.len)?;
            if c.seg_of(off) == Some(victim) {
                return Err(Error::IO);
            }
            inner.write(id, off, buf)?;
            c.physical = c.physical.max(off + u64::from(s.len));
            let sum = check(buf);
            set_entry(c, page as u32, entry_of(off, s.len, page_size), sum)?;
            moved += u64::from(s.len);
        }
        Ok(moved)
    }
}

impl<S: Storage> Storage for Compressed<S> {
    fn cooperate(&mut self) -> Result {
        self.inner.cooperate()
    }
    fn open(&mut self, name: &CStr, flags: OpenFlags) -> Result<FileId> {
        let id = self.inner.open(name, flags)?;
        let bytes = name.to_bytes();
        let kinds = flags.0 & TEMP_KINDS;
        let container = if kinds != 0 || is_sidecar(bytes) || bytes.len() > 255 {
            None
        } else {
            let create = self.policy.create && flags.is_writable();
            match self.open_container(bytes, id, create) {
                Ok(c) => c,
                Err(e) => {
                    let _ = self.inner.close(id);
                    return Err(e);
                }
            }
        };
        if let Some(ci) = container
            && let Some(Some(c)) = self.files.get_mut(ci)
        {
            c.opens += 1;
        }
        let journal_of = if container.is_none() {
            self.journal_owner(bytes)
        } else {
            None
        };
        match self.new_handle(Handle {
            inner: id,
            container,
            journal_of,
        }) {
            Ok(h) => Ok(h),
            Err(e) => {
                if let Some(ci) = container
                    && let Some(Some(c)) = self.files.get_mut(ci)
                {
                    c.opens -= 1;
                }
                let _ = self.inner.close(id);
                Err(e)
            }
        }
    }
    fn close(&mut self, file: FileId) -> Result {
        let h = self.handle(file)?;
        if let Some(s) = self.handles.get_mut(file.0 as usize) {
            *s = None;
        }
        let mut out = Ok(());
        if let Some(ci) = h.container
            && let Some(Some(c)) = self.files.get_mut(ci)
        {
            c.opens = c.opens.saturating_sub(1);
            if c.opens == 0 {
                if c.broken {
                    c.broken = false;
                    c.valid = false;
                } else if c.pending {
                    // Ook zonder sync van SQLite (synchronous=OFF, VACUUM INTO)
                    // is het bestand na sluiten compleet.
                    out = self.commit(ci, h.inner);
                    if let Some(Some(c)) = self.files.get_mut(ci)
                        && c.broken
                    {
                        c.broken = false;
                        c.valid = false;
                    }
                }
            }
        }
        let closed = self.inner.close(h.inner);
        out.and(closed)
    }
    fn read(&mut self, file: FileId, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let h = self.handle(file)?;
        match h.container {
            None => self.inner.read(h.inner, offset, dst),
            Some(ci) => self.read_container(ci, h.inner, offset, dst),
        }
    }
    fn write(&mut self, file: FileId, offset: u64, src: &[u8]) -> Result {
        let h = self.handle(file)?;
        match h.container {
            None => {
                self.inner.write(h.inner, offset, src)?;
                // SQLite wist de journalkop: de transactie is af.
                if let Some(ci) = h.journal_of
                    && offset == 0
                    && src.iter().all(|&b| b == 0)
                {
                    self.settle(ci)?;
                }
                Ok(())
            }
            Some(ci) => self.write_container(ci, h.inner, offset, src),
        }
    }
    fn truncate(&mut self, file: FileId, size: u64) -> Result {
        let h = self.handle(file)?;
        match h.container {
            None => {
                self.inner.truncate(h.inner, size)?;
                if let Some(ci) = h.journal_of
                    && size == 0
                {
                    self.settle(ci)?;
                }
                Ok(())
            }
            Some(ci) => self.truncate_container(ci, h.inner, size),
        }
    }
    fn sync(&mut self, file: FileId, flags: i32) -> Result {
        let h = self.handle(file)?;
        match h.container {
            None => self.inner.sync(h.inner, flags),
            Some(ci) => self.commit(ci, h.inner),
        }
    }
    fn size(&mut self, file: FileId) -> Result<u64> {
        let h = self.handle(file)?;
        match h.container {
            None => self.inner.size(h.inner),
            Some(ci) => Ok(self.container(ci)?.size),
        }
    }
    fn remove(&mut self, name: &CStr, sync_directory: bool) -> Result {
        let bytes = name.to_bytes();
        if let Some(slot) = self
            .files
            .iter_mut()
            .find(|c| c.as_ref().is_some_and(|c| c.name() == bytes))
        {
            if slot.as_ref().is_some_and(|c| c.opens > 0) {
                return Err(Error { code: 5 });
            }
            *slot = None;
            self.work.cached = None;
        }
        self.inner.remove(name, sync_directory)?;
        if let Some(ci) = self.journal_owner(bytes) {
            self.settle(ci)?;
        }
        Ok(())
    }
    fn exists(&mut self, name: &CStr) -> Result<bool> {
        self.inner.exists(name)
    }
    fn random(&mut self, dst: &mut [u8]) -> Result {
        self.inner.random(dst)
    }
    fn unix_millis(&mut self) -> Result<i64> {
        self.inner.unix_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn superblock_roundtrip_and_torn() {
        let sb = Super {
            page_size: 16384,
            segment: SEGMENT,
            file_id: 7,
            generation: 3,
            salt: 9,
            seq: 11,
            size: 16384 * 3,
            region_off: HEADER + u64::from(SEGMENT),
            region_len: u64::from(SEGMENT),
            snapshot_len: 24,
            snapshot_sum: 5,
            log_off: HEADER + u64::from(SEGMENT) + SECTOR,
            log_cap: u64::from(SEGMENT) - SECTOR,
        };
        let b = sb.encode();
        assert_eq!(Super::decode(&b), Some(sb));
        for i in 0..SB_LEN {
            let mut t = b;
            t[i] ^= 1;
            assert_eq!(Super::decode(&t), None, "bit in byte {i}");
        }
    }

    #[test]
    fn sum_is_streaming() {
        let data: Vec<u8> = (0..1000u32).map(|i| (i * 7) as u8).collect();
        let whole = sum(&[&data]);
        for split in [0, 1, 3, 7, 8, 9, 500, 999, 1000] {
            assert_eq!(sum(&[&data[..split], &data[split..]]), whole);
        }
        assert_ne!(sum(&[&data[..999]]), whole);
    }

    #[test]
    fn sidecars() {
        assert!(is_sidecar(b"main.db-journal"));
        assert!(is_sidecar(b"t/main.db.replica"));
        assert!(is_sidecar(b"main.db.replica-dirty-a"));
        assert!(is_sidecar(b"main.db-mj0A1B2C3D"));
        assert!(is_sidecar(b"etilqs_0123456789abcdef"));
        assert!(!is_sidecar(b"main.db"));
        assert!(!is_sidecar(b"2024.db"));
        assert!(!is_sidecar(b"main.db.replica-restore-data"));
    }

    #[test]
    fn entries() {
        let e = entry_of(HEADER + 100, 300, 16384);
        assert_eq!(
            slot_of(e, 16384),
            Some(Slot {
                off: HEADER + 100,
                len: 300
            })
        );
        let r = entry_of(HEADER, 16384, 16384);
        assert_eq!(
            slot_of(r, 16384),
            Some(Slot {
                off: HEADER,
                len: 16384
            })
        );
        let r = entry_of(HEADER, 65536, 65536);
        assert_eq!(
            slot_of(r, 65536),
            Some(Slot {
                off: HEADER,
                len: 65536
            })
        );
        assert_eq!(slot_of(0, 4096), None);
    }
}
