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
//! Eigenaren op dezelfde map (de SQL-kant en de sync-kant van replica-core,
//! leessessies) die één [`Maps`] delen ([`Compressed::shared`]), delen per
//! bestand één kaart: 8 B per pagina één keer per bestand in plaats van per
//! eigenaar (een productie-`main.db`: 17,2 MB; meting 21 nov 2026). De ene schrijver
//! werkt hem bij; de anderen zien tijdens zijn transactie de gecommitte staat
//! (de oude ingangen van wat hij veranderde, bewaard zodra de `Maps` meer
//! houders heeft) en na zijn commit meteen de nieuwe. Er schrijft er maar één tegelijk, zoals het [`Storage`]-contract al
//! eist; een tweede krijgt `SQLITE_BUSY`. Sluit het laatste handvat op een
//! bestand, dan gaat zijn kaart terug naar de heap.
//!
//! Een `open` van een bestand dat in deze `Maps` niet open is, leest de
//! superblokken, het snapshot en het log: het bestand is de enige waarheid.
//! Een eigenaar met eigen kaarten ([`Compressed::new`]) is een `Maps` van één.
use crate::{Error, FileId, OpenFlags, Result, Storage, lz};
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::RefCell;
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

/// Een gewijzigde-paginalijst die na de commit groter is dan dit, gaat terug
/// naar de heap (32 KiB; een herstel liet hem op 33,5 MB staan, meting 21 nov 2026).
const DIRTY_KEEP: usize = 4096;

/// Eén containerbestand: de kaart, de segmentboekhouding en de lopende transactie.
struct Container {
    name: [u8; 256],
    name_len: usize,
    /// Een commit faalde halverwege; de schrijver faalt tot hij het bestand sluit.
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
    /// Heap van de kaart en van de rest (segmenten, gewijzigde pagina's).
    fn heap(&self) -> (u64, u64) {
        let segs = self
            .writer
            .as_ref()
            .map_or(0, |w| w.segs.capacity() * core::mem::size_of::<Seg>());
        let dirty = self.dirty.capacity() * core::mem::size_of::<(u32, u32)>();
        ((self.map.capacity() * 8) as u64, (segs + dirty) as u64)
    }
    /// Na een commit: een kaart met veel lege capaciteit (na een herstel, een
    /// grote groei of een krimp) en een grote gewijzigde-paginalijst terug naar
    /// de heap. `before` is de paginatelling vóór de transactie.
    fn trim(&mut self, before: usize) {
        if self.dirty.capacity() > DIRTY_KEEP {
            self.dirty = Vec::new();
        }
        let len = self.map.len();
        let slack = self.map.capacity() - len;
        // Gewone groei laat hoogstens een achtste speling (`grow`); meer is een
        // herstel of een krimp.
        if slack > len / 8 + 64 || (len > 2 * before && slack > 64) {
            let mut fit = Vec::new();
            // Lukt het niet, dan blijft de ruime kaart: geen fout, alleen geheugen.
            if fit.try_reserve_exact(len).is_ok() {
                fit.extend_from_slice(&self.map);
                self.map = fit;
            }
        }
    }
}

/// De kaarten van één opslagdomein, gedeeld door alle [`Compressed`] erop.
///
/// Eén kaart per bestand: de SQL-kant, de sync-kant en elke leessessie van
/// dezelfde map lezen dezelfde kaart, en de ene schrijver werkt hem bij. Een
/// bestand verdwijnt uit de `Maps` zodra zijn laatste handvat (van welke
/// eigenaar ook) sluit. Geef één `Maps` alleen aan eigenaren op hetzelfde
/// domein: de sleutel is de bestandsnaam.
///
/// Gedeeld eigendom (`Rc`) met reden: de eigenaren zijn opslagen van
/// verschillende verbindingen op verschillende stacks. Een lening van het
/// register overleeft nooit een aanroep van de binnenste opslag (die kan
/// parkeren); [`Compressed`] leent alleen in `with`, dat geen opslag krijgt.
#[derive(Clone, Default)]
pub struct Maps(Rc<RefCell<Registry>>);

/// Heap van de kaarten in een [`Maps`], voor toetsen en meetlatten.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Heap {
    /// Bestanden met een kaart (open in minstens één eigenaar).
    pub files: u64,
    /// Bytes van de kaarten (capaciteit × 8).
    pub map: u64,
    /// Bytes van de rest: segmentboekhouding, gewijzigde pagina's en de
    /// gecommitte staat voor lezers tijdens een transactie.
    pub other: u64,
}

impl Maps {
    /// Een leeg register.
    pub fn new() -> Self {
        Self::default()
    }
    /// Wat de kaarten nu kosten.
    pub fn heap(&self) -> Heap {
        let mut out = Heap::default();
        if let Ok(reg) = self.0.try_borrow() {
            for k in reg.cores.iter().flatten() {
                let (map, other) = k.c.heap();
                let undo = k.undo.as_ref().map_or(0, |u| {
                    u.list.capacity() * core::mem::size_of::<(u32, u64)>()
                });
                out.files += 1;
                out.map += map;
                out.other += other + undo as u64;
            }
        }
        out
    }
    fn owner(&self) -> u32 {
        self.0.try_borrow_mut().map_or(0, |mut r| {
            r.owners = r.owners.wrapping_add(1);
            r.owners
        })
    }
}

impl core::fmt::Debug for Maps {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let h = self.heap();
        f.debug_struct("Maps")
            .field("files", &h.files)
            .field("map", &h.map)
            .finish()
    }
}

/// Het register achter [`Maps`].
#[derive(Default)]
struct Registry {
    /// Kernen op slot; een eigenaar houdt het slotnummer, dus een slot wordt
    /// geleegd (`None`) maar nooit verschoven.
    cores: Vec<Option<Core>>,
    /// Laatst uitgegeven eigenaarnummer.
    owners: u32,
}

impl Registry {
    /// De vindbare kern met naam `name`.
    fn find(&self, name: &[u8]) -> Option<usize> {
        self.cores
            .iter()
            .position(|k| k.as_ref().is_some_and(|k| k.listed && k.c.name() == name))
    }
    fn insert(&mut self, core: Core) -> Result<usize> {
        if let Some(free) = self.cores.iter().position(Option::is_none) {
            if let Some(slot) = self.cores.get_mut(free) {
                *slot = Some(core);
            }
            return Ok(free);
        }
        self.cores.try_reserve(1).map_err(|_| Error::MEMORY)?;
        self.cores.push(Some(core));
        Ok(self.cores.len() - 1)
    }
    /// Eigenaar `me` laat kern `i` los; de laatste geeft de kaart terug.
    fn detach(&mut self, i: usize, me: u32) {
        let Some(slot) = self.cores.get_mut(i) else {
            return;
        };
        let Some(k) = slot.as_mut() else {
            return;
        };
        k.attached = k.attached.saturating_sub(1);
        if k.tx == Some(me) {
            // Ongecommitte writes in de kaart: alleen het bestand weet nog wat geldt.
            k.tx = None;
            k.undo = None;
            k.stale = true;
            k.version = k.version.wrapping_add(1);
        }
        if k.attached == 0 {
            *slot = None;
        }
    }
}

/// De gecommitte staat van vóór de lopende transactie, voor de andere eigenaren.
#[derive(Clone, Copy, Default)]
struct Base {
    pages: usize,
    size: u64,
    page_size: u32,
}

/// De oude ingangen van pagina's die de lopende transactie veranderde.
#[derive(Default)]
struct Undo {
    /// (pagina, oude ingang), in volgorde van wijzigen; per pagina telt de eerste.
    list: Vec<(u32, u64)>,
    sorted: bool,
}

impl Undo {
    fn push(&mut self, page: u32, old: u64) -> Result {
        self.list.try_reserve(1).map_err(|_| Error::MEMORY)?;
        self.list.push((page, old));
        self.sorted = false;
        Ok(())
    }
    fn find(&mut self, page: u32) -> Option<u64> {
        if !self.sorted {
            // Stabiel: van dubbelen blijft de oudste vooraan.
            self.list.sort_by_key(|e| e.0);
            self.sorted = true;
        }
        let i = self.list.partition_point(|e| e.0 < page);
        self.list.get(i).filter(|e| e.0 == page).map(|e| e.1)
    }
}

/// Eén bestand in een [`Maps`]: zijn container en wie hem gebruikt.
///
/// # Invariants
/// Loopt er een transactie (`tx`) in een gedeelde [`Maps`], dan is `undo` er
/// en bevat hij de oude ingang van elke pagina onder `base.pages` die de
/// transactie veranderde: `base` plus `undo` plus de kaart is de gecommitte
/// staat.
struct Core {
    c: Container,
    /// Vindbaar op naam; `false` na een `remove` terwijl een ander hem open
    /// had: die houdt de kaart van het oude bestand, een nieuw bestand met deze
    /// naam krijgt een eigen kern.
    listed: bool,
    /// Eigenaren met een open handvat.
    attached: u32,
    /// De eigenaar met ongecommitte writes in de kaart.
    tx: Option<u32>,
    base: Base,
    undo: Option<Undo>,
    /// Hoogt op bij elke nieuwe gecommitte staat (een lezer ververst zijn paginabuffer).
    version: u64,
    /// De kaart volgt het bestand niet meer (een schrijver ging weg midden in
    /// een transactie): de volgende toegang laadt opnieuw.
    stale: bool,
}

impl Core {
    fn new(c: Container) -> Self {
        Self {
            c,
            listed: true,
            attached: 0,
            tx: None,
            base: Base::default(),
            undo: None,
            version: 0,
            stale: false,
        }
    }
    /// Ziet `me` de gecommitte staat (een ander schrijft) in plaats van de kaart?
    fn is_reader(&self, me: u32) -> bool {
        self.tx.is_some_and(|t| t != me)
    }
    /// Mag `me` dit bestand gebruiken?
    fn check(&self, me: u32) -> Result {
        if self.stale || (self.c.broken && self.tx == Some(me)) {
            Err(Error::IO)
        } else {
            Ok(())
        }
    }
    fn size(&self, me: u32) -> u64 {
        if self.is_reader(me) {
            self.base.size
        } else {
            self.c.size
        }
    }
    fn page_size(&self, me: u32) -> u32 {
        if self.is_reader(me) {
            self.base.page_size
        } else {
            self.c.page_size
        }
    }
    /// De ingang van `page` zoals `me` hem ziet.
    fn entry(&mut self, me: u32, page: u32) -> Result<u64> {
        let p = page as usize;
        if self.is_reader(me) {
            if p >= self.base.pages {
                return Ok(0);
            }
            // Geen oude ingangen: de `Maps` was niet gedeeld toen de schrijver
            // begon, en `me` kwam er midden in zijn transactie bij.
            if let Some(old) = self.undo.as_mut().ok_or(BUSY)?.find(page) {
                return Ok(old);
            }
        }
        Ok(self.c.map.get(p).copied().unwrap_or(0))
    }
    /// Begint (of vervolgt) de transactie van `me`; één schrijver tegelijk.
    /// `shared`: de `Maps` heeft meer houders, er kan een lezer bij komen.
    fn begin(&mut self, me: u32, shared: bool) -> Result {
        self.check(me)?;
        match self.tx {
            Some(t) if t == me => Ok(()),
            Some(_) => Err(BUSY),
            None => {
                self.tx = Some(me);
                self.base = Base {
                    pages: self.c.map.len(),
                    size: self.c.size,
                    page_size: self.c.page_size,
                };
                // Oude ingangen zodra een ander kan meelezen, ook als hij pas
                // tijdens de transactie opent: zo deelt elke eigenaar altijd de
                // ene kaart. Kost 16 B per veranderde bestaande pagina, tot de
                // commit; een herstel naar een leeg bestand bewaart niets.
                self.undo = shared.then(Undo::default);
                Ok(())
            }
        }
    }
    /// Bewaart de gecommitte ingang van `page` vóór hij verandert.
    fn remember(&mut self, page: u32) -> Result {
        if (page as usize) < self.base.pages
            && let Some(u) = self.undo.as_mut()
        {
            let old = self.c.map.get(page as usize).copied().unwrap_or(0);
            u.push(page, old)?;
        }
        Ok(())
    }
    /// Zet `entry` als nieuwe ingang van `page` en boekt het oude slot af.
    fn set_entry(&mut self, page: u32, entry: u64, sum: u32) -> Result {
        self.remember(page)?;
        set_entry(&mut self.c, page, entry, sum)
    }
    /// De transactie is gecommit: dit is de nieuwe gecommitte staat.
    fn end(&mut self) {
        let before = self.base.pages;
        self.tx = None;
        self.undo = None;
        self.version = self.version.wrapping_add(1);
        self.c.trim(before);
    }
}

/// `SQLITE_BUSY`: een ander schrijft, of het bestand is nog open.
const BUSY: Error = Error { code: 5 };

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
    /// `Some(i)`: het bestand in slot `i` van [`Owner::files`], anders gewoon doorgeven.
    container: Option<usize>,
    /// Het journal (`<naam>-journal`) van slot `i`: zijn afronding is een commit.
    journal_of: Option<usize>,
}

/// Werkbuffers, één keer per eigenaar.
struct Work {
    enc: Option<lz::Encoder>,
    /// Gecomprimeerde bytes van één slot.
    stored: Vec<u8>,
    /// Een gedecodeerde pagina voor deelreads en read-modify-write.
    page: Vec<u8>,
    /// Welke pagina `page` nu bevat (kern, pagina, versie van de kern).
    cached: Option<(usize, u32, u64)>,
}

/// Een open bestand van deze eigenaar: zijn kern in de [`Maps`].
#[derive(Clone, Copy)]
struct Open {
    core: usize,
    opens: u32,
}

/// Wat één [`Compressed`] in zijn [`Maps`] heeft. `Drop` laat alles los.
struct Owner {
    maps: Maps,
    me: u32,
    /// Open bestanden op slot; een handvat houdt het slotnummer.
    files: Vec<Option<Open>>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        if let Ok(mut reg) = self.maps.0.try_borrow_mut() {
            for s in self.files.iter().flatten() {
                reg.detach(s.core, self.me);
            }
        }
    }
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
    owner: Owner,
    work: Work,
    /// `false` tijdens een commit die SQLite niet met `sync` vroeg (de afronding van
    /// het journal bij `synchronous=OFF`): het record zonder `sync`.
    durable: bool,
}

impl<S: Storage> Compressed<S> {
    /// `inner` met `policy` voor nieuwe bestanden, met eigen kaarten.
    pub fn new(inner: S, policy: Policy) -> Self {
        Self::shared(inner, policy, &Maps::new())
    }
    /// Als [`Compressed::new`], met de kaarten van `maps`: alle eigenaren op
    /// dezelfde `Maps` delen per bestand één kaart.
    pub fn shared(inner: S, policy: Policy, maps: &Maps) -> Self {
        Self {
            inner,
            policy,
            handles: Vec::new(),
            owner: Owner {
                maps: maps.clone(),
                me: maps.owner(),
                files: Vec::new(),
            },
            work: Work {
                enc: None,
                stored: Vec::new(),
                page: Vec::new(),
                cached: None,
            },
            durable: true,
        }
    }
    /// De kaarten van deze eigenaar.
    pub fn maps(&self) -> &Maps {
        &self.owner.maps
    }
    /// Heeft de `Maps` van deze eigenaar meer houders (andere eigenaren)?
    fn is_shared(&self) -> bool {
        Rc::strong_count(&self.owner.maps.0) > 1
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

    /// Leent de kern van slot `si` voor `f`, met het nummer van deze eigenaar.
    ///
    /// `f` krijgt geen opslag (`self` is gedeeld geleend), dus geen lening van
    /// het register overleeft een park in de binnenste opslag: de compiler
    /// bewijst dat een andere eigenaar op een andere stack nooit een geleend
    /// register treft.
    fn with<R>(&self, si: usize, f: impl FnOnce(&mut Core, u32) -> R) -> Result<R> {
        let slot = self
            .owner
            .files
            .get(si)
            .copied()
            .flatten()
            .ok_or(Error::IO)?;
        let mut reg = self
            .owner
            .maps
            .0
            .try_borrow_mut()
            .map_err(|_| Error::MISUSE)?;
        let k = reg
            .cores
            .get_mut(slot.core)
            .and_then(Option::as_mut)
            .ok_or(Error::IO)?;
        Ok(f(k, self.owner.me))
    }

    /// Statistiek van een open container, voor toetsen en meetprogramma's;
    /// `None` voor een gewoon bestand.
    pub fn stats(&mut self, id: FileId) -> Result<Option<Stats>> {
        let Some(si) = self.handle(id)?.container else {
            return Ok(None);
        };
        self.with(si, |k, me| {
            k.check(me)?;
            let c = &k.c;
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
        })?
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

/// Decodeert de pagina met ingang `entry` naar `out` (`page_size` bytes).
fn load_slot<S: Storage>(
    inner: &mut S,
    stored: &mut [u8],
    entry: u64,
    page_size: u32,
    id: FileId,
    out: &mut [u8],
) -> Result {
    let Some(slot) = slot_of(entry, page_size) else {
        out.fill(0);
        return Ok(());
    };
    if slot.len == page_size {
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

/// Waar het log van een container staat en tot waar het gelezen is.
#[derive(Clone, Copy)]
struct LogAt {
    salt: u64,
    seq: u64,
    log_off: u64,
    log_cap: u64,
    log_end: u64,
}

impl LogAt {
    fn of(c: &Container) -> Self {
        Self {
            salt: c.salt,
            seq: c.seq,
            log_off: c.log_off,
            log_cap: c.log_cap,
            log_end: c.log_end,
        }
    }
}

/// Een gelezen logrecord: kop, ingangen en de positie erna.
type Record = ([u8; RECORD_HEAD], Vec<u8>, u64);

/// Het geldige logrecord op `pos` dat `seq` voortzet: kop, ingangen en de
/// volgende positie; `None` als daar geen staat.
fn read_record<S: Storage>(
    inner: &mut S,
    id: FileId,
    at: LogAt,
    pos: u64,
    seq: u64,
) -> Result<Option<Record>> {
    let mut head = [0u8; RECORD_HEAD];
    if pos + RECORD_HEAD as u64 > at.log_cap
        || read_some(inner, id, at.log_off + pos, &mut head)? < RECORD_HEAD
    {
        return Ok(None);
    }
    let count = u64::from(get_u32(&head, 4));
    let len = RECORD_HEAD as u64 + count * ENTRY as u64;
    if get_u32(&head, 0) != RECORD_MAGIC
        || get_u64(&head, 8) != at.salt
        || get_u64(&head, 16) != seq + 1
        || pos + len > at.log_cap
    {
        return Ok(None);
    }
    let n = usize::try_from(count * ENTRY as u64).map_err(|_| CORRUPT)?;
    let mut body = Vec::new();
    body.try_reserve_exact(n).map_err(|_| Error::MEMORY)?;
    body.resize(n, 0);
    if read_some(inner, id, at.log_off + pos + RECORD_HEAD as u64, &mut body)? < n {
        return Ok(None);
    }
    let mut sealed = head;
    put(&mut sealed, 40, &[0; 8]);
    if sum(&[&sealed, &body]) != get_u64(&head, 40) {
        return Ok(None);
    }
    Ok(Some((head, body, pos + align(len, SECTOR))))
}

/// Speelt geldige logrecords vanaf `c.log_end` bij; het aantal records.
///
/// De slots van het laatste record worden eerst gecontroleerd: alleen dat
/// record kan duurzaam zijn terwijl een slot van dezelfde commit het niet werd
/// (een stroomstoring tijdens zijn `sync`). Klopt een slot niet, dan telt die
/// commit niet; SQLite's hete journal maakt dat consistent, want de `sync`
/// kwam nooit terug.
fn replay<S: Storage>(inner: &mut S, c: &mut Container, id: FileId) -> Result<u64> {
    let mut applied = 0;
    let mut held: Option<Record> = None;
    let at = LogAt::of(c);
    let mut pos = c.log_end;
    let mut seq = c.seq;
    while let Some((head, body, next)) = read_record(inner, id, at, pos, seq)? {
        pos = next;
        seq += 1;
        if let Some((h, b, end)) = held.take() {
            apply(c, &h, &b)?;
            c.log_end = end;
            c.seq += 1;
            applied += 1;
        }
        held = Some((head, body, pos));
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

/// Vergroot `v` faalbaar tot `n` nullen. Is er geen ruimte, dan komt er een
/// achtste bij in plaats van een verdubbeling: de kaart van een groot bestand
/// staat dan niet tot de helft leeg (een productie-`main.db`: 17 MB).
fn grow(v: &mut Vec<u64>, n: usize) -> Result {
    if n > v.len() {
        let need = n - v.len();
        if need > v.capacity() - v.len() {
            v.try_reserve_exact(need.max(v.len() / 8))
                .map_err(|_| Error::MEMORY)?;
        }
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
    replay(inner, c, id)?;
    c.fresh = false;
    Ok(())
}

/// Leest container `c` (nieuw, nog leeg) van het bestand: het bestand is de
/// enige waarheid.
fn load_file<S: Storage>(inner: &mut S, c: &mut Container, id: FileId) -> Result {
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
        Some((sb, active)) => load(inner, c, id, sb, active),
    }
}

/// Wat een open kern van zijn bestand denkt te weten, om het na te kijken.
#[derive(Clone, Copy)]
struct Known {
    page_size: u32,
    file_id: u64,
    generation: u64,
    log: LogAt,
}

/// Staat het bestand nog waar de kern het kent? Een andere schrijver (buiten
/// deze [`Maps`]) gaf een nieuw superblok of zette het log voort.
fn is_current<S: Storage>(inner: &mut S, id: FileId, k: Known) -> Result<bool> {
    let Some((sb, _)) = read_super(inner, id)? else {
        return Ok(k.page_size == 0);
    };
    if k.page_size == 0
        || sb.file_id != k.file_id
        || sb.generation != k.generation
        || sb.salt != k.log.salt
    {
        return Ok(false);
    }
    Ok(read_record(inner, id, k.log, k.log.log_end, k.log.seq)?.is_none())
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

    /// Het slot van deze eigenaar met bestand `name`.
    fn slot_named(&self, name: &[u8]) -> Option<usize> {
        let reg = self.owner.maps.0.try_borrow().ok()?;
        self.owner.files.iter().position(|s| {
            s.is_some_and(|s| {
                reg.cores
                    .get(s.core)
                    .and_then(Option::as_ref)
                    .is_some_and(|k| k.c.name() == name)
            })
        })
    }

    /// Neemt kern `core` op in een slot van deze eigenaar (met één open).
    fn take_slot(&mut self, core: usize) -> Result<usize> {
        self.work.cached = None;
        let slot = Some(Open { core, opens: 1 });
        if let Some(i) = self.owner.files.iter().position(Option::is_none) {
            if let Some(s) = self.owner.files.get_mut(i) {
                *s = slot;
            }
            return Ok(i);
        }
        self.owner.files.try_reserve(1).map_err(|_| Error::MEMORY)?;
        self.owner.files.push(slot);
        Ok(self.owner.files.len() - 1)
    }

    /// Hecht deze eigenaar aan kern `core`; het slot.
    fn attach(&mut self, core: usize) -> Result<usize> {
        {
            let mut reg = self
                .owner
                .maps
                .0
                .try_borrow_mut()
                .map_err(|_| Error::MISUSE)?;
            let k = reg
                .cores
                .get_mut(core)
                .and_then(Option::as_mut)
                .ok_or(Error::IO)?;
            k.attached += 1;
        }
        match self.take_slot(core) {
            Ok(si) => Ok(si),
            Err(e) => {
                if let Ok(mut reg) = self.owner.maps.0.try_borrow_mut() {
                    reg.detach(core, self.owner.me);
                }
                Err(e)
            }
        }
    }

    /// Eén open minder op slot `si`; bij nul laat deze eigenaar de kern los.
    fn release(&mut self, si: usize) {
        let Some(Some(slot)) = self.owner.files.get_mut(si) else {
            return;
        };
        slot.opens = slot.opens.saturating_sub(1);
        if slot.opens > 0 {
            return;
        }
        let core = slot.core;
        if let Some(s) = self.owner.files.get_mut(si) {
            *s = None;
        }
        self.work.cached = None;
        if let Ok(mut reg) = self.owner.maps.0.try_borrow_mut() {
            reg.detach(core, self.owner.me);
        }
    }

    /// Opent container `name` met handvat `id` van de binnenste opslag (een
    /// open erbij); `None` als het geen container is of wordt.
    fn open_container(&mut self, name: &[u8], id: FileId, create: bool) -> Result<Option<usize>> {
        // Al open in deze eigenaar: die kaart is de waarheid.
        if let Some(si) = self.slot_named(name) {
            if let Some(Some(s)) = self.owner.files.get_mut(si) {
                s.opens += 1;
            }
            return Ok(Some(si));
        }
        // Open in een andere eigenaar van deze `Maps`: dezelfde kaart.
        let found = {
            let reg = self.owner.maps.0.try_borrow().map_err(|_| Error::MISUSE)?;
            reg.find(name)
        };
        if let Some(core) = found {
            let si = self.attach(core)?;
            if let Err(e) = self.revalidate(si, id) {
                self.release(si);
                return Err(e);
            }
            return Ok(Some(si));
        }
        let size = self.inner.size(id)?;
        let mut head = [0u8; 8];
        let got = if size > 0 {
            read_some(&mut self.inner, id, 0, &mut head)?
        } else {
            0
        };
        let ours = got == 8 && head == MAGIC;
        let blank = size > 0 && head.iter().all(|&b| b == 0);
        if !(ours || (create && (size == 0 || blank))) {
            return Ok(None);
        }
        let mut c = Container::new(name)?;
        load_file(&mut self.inner, &mut c, id)?;
        if c.page_size == 0 && !create {
            // Leeg zonder toestemming om te comprimeren: gewoon bestand.
            return Ok(None);
        }
        // Nam een ander hem intussen (tijdens het laden) op, dan diens kaart.
        let core = {
            let mut reg = self
                .owner
                .maps
                .0
                .try_borrow_mut()
                .map_err(|_| Error::MISUSE)?;
            match reg.find(name) {
                Some(i) => i,
                None => reg.insert(Core::new(c))?,
            }
        };
        self.attach(core).map(Some)
    }

    /// Kijkt een kern die een andere eigenaar al open had na tegen het bestand,
    /// en laadt opnieuw als een schrijver buiten deze `Maps` hem inhaalde.
    fn revalidate(&mut self, si: usize, id: FileId) -> Result {
        let known = self.with(si, |k, _| {
            (k.tx.is_none() && !k.stale).then(|| Known {
                page_size: k.c.page_size,
                file_id: k.c.file_id,
                generation: k.c.generation,
                log: LogAt::of(&k.c),
            })
        })?;
        let behind = match known {
            Some(known) => !is_current(&mut self.inner, id, known)?,
            None => false,
        };
        if behind {
            self.reload(si, id)
        } else {
            self.fresh(si, id)
        }
    }

    /// Laadt een kern die het bestand niet meer volgt opnieuw.
    fn fresh(&mut self, si: usize, id: FileId) -> Result {
        if self.with(si, |k, _| k.stale && k.tx.is_none())? {
            self.reload(si, id)?;
        }
        Ok(())
    }

    /// Leest kern `si` opnieuw van het bestand, als intussen niemand hem verving
    /// of erin schreef.
    fn reload(&mut self, si: usize, id: FileId) -> Result {
        let (mut c, version) = self.with(si, |k, _| {
            Container::new(k.c.name()).map(|c| (c, k.version))
        })??;
        load_file(&mut self.inner, &mut c, id)?;
        self.with(si, |k, _| {
            if k.tx.is_none() && k.version == version {
                k.c = c;
                k.stale = false;
                k.version = k.version.wrapping_add(1);
            }
        })?;
        self.work.cached = None;
        Ok(())
    }
}

impl<S: Storage> Compressed<S> {
    /// Maakt van een leeg bestand een container met paginamaat uit `src`.
    fn init(&mut self, si: usize, id: FileId, offset: u64, src: &[u8]) -> Result {
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
        let bytes = self.with(si, |k, _| {
            let c = &mut k.c;
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
            c.physical = c.physical.max(SB_LEN as u64);
            sb.encode()
        })?;
        // Superblok A van de lege staat; blok B volgt bij de eerste commit.
        self.inner.write(id, 0, &bytes)?;
        self.with(si, |k, _| {
            let mut segs = Vec::new();
            segs.try_reserve_exact(1).map_err(|_| Error::MEMORY)?;
            segs.push(Seg {
                live: 0,
                end: 0,
                usage: Use::Meta,
            });
            k.c.writer = Some(Writer {
                segs,
                open: None,
                wp: 0,
                written: 0,
            });
            k.c.pending = true;
            Ok(())
        })?
    }

    fn encoder(&mut self) -> Result {
        if self.work.enc.is_none() {
            self.work.enc = Some(lz::Encoder::new().ok_or(Error::MEMORY)?);
        }
        Ok(())
    }

    /// Comprimeert en schrijft één hele pagina.
    fn store(&mut self, si: usize, id: FileId, page: u32, page_size: u32, data: &[u8]) -> Result {
        if u64::from(page) >= MAX_PAGES {
            return Err(Error::FULL);
        }
        if data.iter().all(|&b| b == 0) {
            return self.with(si, |k, _| k.set_entry(page, 0, 0))?;
        }
        // Alleen gecomprimeerd als het kleiner is dan de pagina en in de
        // lengtecode past; anders onsamendrukbaar, rauw.
        let limit = (page_size as usize - 1).min(RAW as usize - 1);
        let enc = self.work.enc.as_mut().ok_or(Error::MEMORY)?;
        let stored = self.work.stored.get_mut(..limit).ok_or(Error::IO)?;
        let len = match enc.compress(data, stored) {
            Some(n) => n as u32,
            None => page_size,
        };
        let off = self.with(si, |k, _| alloc(&mut k.c, len))??;
        let bytes = if len == page_size {
            data
        } else {
            self.work.stored.get(..len as usize).ok_or(Error::IO)?
        };
        self.inner.write(id, off, bytes)?;
        let sum = check(bytes);
        self.with(si, |k, _| {
            k.c.physical = k.c.physical.max(off + u64::from(len));
            if let Some(w) = k.c.writer.as_mut() {
                w.written += u64::from(len);
            }
            k.set_entry(page, entry_of(off, len, page_size), sum)
        })?
    }

    /// Vult `scratch` met pagina `page` zoals deze eigenaar hem ziet, nul voorbij
    /// de logische maat.
    fn load_page(&mut self, si: usize, id: FileId, page: u32, scratch: &mut [u8]) -> Result {
        let (entry, size, page_size) = self.with(si, |k, me| {
            Ok((k.entry(me, page)?, k.size(me), k.page_size(me)))
        })??;
        let p = u64::from(page_size);
        if u64::from(page) * p >= size {
            scratch.fill(0);
            return Ok(());
        }
        load_slot(
            &mut self.inner,
            &mut self.work.stored,
            entry,
            page_size,
            id,
            scratch,
        )?;
        // Voorbij de logische maat is alles nul, ook na een eerdere krimp.
        let visible = (size - u64::from(page) * p).min(p) as usize;
        if let Some(t) = scratch.get_mut(visible..) {
            t.fill(0);
        }
        Ok(())
    }

    fn write_container(&mut self, si: usize, id: FileId, offset: u64, src: &[u8]) -> Result {
        self.fresh(si, id)?;
        let shared = self.is_shared();
        let empty = self.with(si, |k, me| k.begin(me, shared).map(|()| k.c.page_size == 0))??;
        if empty {
            self.init(si, id, offset, src)?;
        }
        let page_size = self.with(si, |k, _| k.c.page_size)?;
        self.buffers(page_size)?;
        self.encoder()?;
        self.with(si, |k, _| build_writer(&mut k.c))??;
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
            if n == page_size as usize {
                self.store(si, id, page, page_size, piece)?;
            } else {
                // Read-modify-write van een deel van een pagina.
                let mut scratch = core::mem::take(&mut self.work.page);
                let out = self.rmw(si, id, at, page_size, piece, &mut scratch);
                self.work.page = scratch;
                out?;
            }
            pos += n;
        }
        self.with(si, |k, _| {
            let c = &mut k.c;
            if end > c.size {
                c.size = end;
                grow(&mut c.map, pages_of(end, page_size) as usize)?;
            }
            c.pending = true;
            Ok(())
        })?
    }

    /// Schrijft `piece` op logische offset `at`, binnen één pagina
    /// (read-modify-write).
    fn rmw(
        &mut self,
        si: usize,
        id: FileId,
        at: u64,
        page_size: u32,
        piece: &[u8],
        buf: &mut [u8],
    ) -> Result {
        let p = u64::from(page_size);
        let page = u32::try_from(at / p).map_err(|_| Error::FULL)?;
        let within = (at % p) as usize;
        let scratch = buf.get_mut(..page_size as usize).ok_or(Error::IO)?;
        self.load_page(si, id, page, scratch)?;
        scratch
            .get_mut(within..within + piece.len())
            .ok_or(Error::IO)?
            .copy_from_slice(piece);
        self.store(si, id, page, page_size, scratch)
    }

    fn read_container(
        &mut self,
        si: usize,
        id: FileId,
        offset: u64,
        dst: &mut [u8],
    ) -> Result<usize> {
        self.fresh(si, id)?;
        let (size, page_size) = self.with(si, |k, me| {
            k.check(me)?;
            Ok((k.size(me), k.page_size(me)))
        })??;
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
            let core = self
                .owner
                .files
                .get(si)
                .copied()
                .flatten()
                .ok_or(Error::IO)?
                .core;
            let (entry, version) =
                self.with(si, |k, me| k.entry(me, page).map(|e| (e, k.version)))??;
            let out = dst.get_mut(pos..pos + n).ok_or(Error::IO)?;
            if n == page_size as usize {
                load_slot(
                    &mut self.inner,
                    &mut self.work.stored,
                    entry,
                    page_size,
                    id,
                    out,
                )?;
            } else {
                let key = Some((core, page, version));
                let scratch = self
                    .work
                    .page
                    .get_mut(..page_size as usize)
                    .ok_or(Error::IO)?;
                if self.work.cached != key {
                    self.work.cached = None;
                    load_slot(
                        &mut self.inner,
                        &mut self.work.stored,
                        entry,
                        page_size,
                        id,
                        scratch,
                    )?;
                    self.work.cached = key;
                }
                out.copy_from_slice(scratch.get(within..within + n).ok_or(Error::IO)?);
            }
            pos += n;
        }
        Ok(total)
    }

    fn truncate_container(&mut self, si: usize, id: FileId, size: u64) -> Result {
        self.fresh(si, id)?;
        let page_size = self.with(si, |k, me| k.check(me).map(|()| k.c.page_size))??;
        if page_size == 0 {
            if size == 0 {
                return Ok(());
            }
            let shared = self.is_shared();
            self.with(si, |k, me| k.begin(me, shared))??;
            self.init(si, id, 1, &[])?;
        }
        let shared = self.is_shared();
        let (page_size, old) = self.with(si, |k, me| {
            k.begin(me, shared)?;
            Ok((k.c.page_size, k.c.size))
        })??;
        if size >= old {
            if size > old {
                self.with(si, |k, _| {
                    k.c.size = size;
                    k.c.pending = true;
                    grow(&mut k.c.map, pages_of(size, page_size) as usize)
                })??;
            }
            return Ok(());
        }
        self.work.cached = None;
        self.with(si, |k, _| build_writer(&mut k.c))??;
        let p = u64::from(page_size);
        let keep = pages_of(size, page_size);
        // Een halve laatste pagina: de staart wordt nul (een latere groei leest nul).
        if !size.is_multiple_of(p) {
            let last = (size / p) as u32;
            let within = (size % p) as usize;
            self.buffers(page_size)?;
            self.encoder()?;
            let mut buf = core::mem::take(&mut self.work.page);
            let out = (|| {
                let scratch = buf.get_mut(..page_size as usize).ok_or(Error::IO)?;
                self.load_page(si, id, last, scratch)?;
                if let Some(t) = scratch.get_mut(within..) {
                    t.fill(0);
                }
                self.store(si, id, last, page_size, scratch)
            })();
            self.work.page = buf;
            out?;
        }
        self.with(si, |k, _| {
            let keep = keep as usize;
            let len = k.c.map.len();
            for p in keep..len {
                if p < k.base.pages {
                    k.remember(p as u32)?;
                }
                let e = k.c.map.get(p).copied().unwrap_or(0);
                account(&mut k.c, e, false)?;
            }
            let c = &mut k.c;
            c.map.truncate(keep);
            c.size = size;
            c.trunc = Some(c.trunc.map_or(keep as u64, |t| t.min(keep as u64)));
            c.pending = true;
            Ok(())
        })?
    }
}

impl<S: Storage> Compressed<S> {
    /// Eén commit van slot `si` (zie de moduledocs). Alleen de schrijver commit;
    /// voor een ander is het een gewone `sync`.
    fn commit(&mut self, si: usize, id: FileId) -> Result {
        let ours = self.with(si, |k, me| {
            if k.tx != Some(me) {
                return Ok(false);
            }
            k.check(me)?;
            if !k.c.pending {
                // Begonnen maar niets veranderd: niets te committen.
                k.end();
                return Ok(false);
            }
            Ok(true)
        })??;
        if !ours {
            return self.inner.sync(id, 3);
        }
        let out = self.commit_inner(si, id);
        if out.is_err() {
            // Onbekend wat er duurzaam is: de schrijver faalt tot hij sluit, de
            // anderen lezen de vorige gecommitte staat. Viel de fout na de
            // commit (het afkappen van de staart), dan is er niets onbekends.
            self.with(si, |k, me| {
                if k.tx == Some(me) {
                    k.c.broken = true;
                }
            })?;
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
    fn settle(&mut self, si: usize) -> Result {
        let pending = self
            .with(si, |k, me| k.tx == Some(me) && k.c.pending && !k.c.broken)
            .unwrap_or(false);
        if !pending {
            return Ok(());
        }
        let Some(id) = self
            .handles
            .iter()
            .flatten()
            .find(|h| h.container == Some(si))
            .map(|h| h.inner)
        else {
            return Ok(());
        };
        self.durable = false;
        let out = self.commit(si, id);
        self.durable = true;
        out
    }

    /// Het slot van deze eigenaar waarvan `name` het journal is.
    fn journal_owner(&self, name: &[u8]) -> Option<usize> {
        self.slot_named(name.strip_suffix(b"-journal")?)
    }

    fn commit_inner(&mut self, si: usize, id: FileId) -> Result {
        self.with(si, |k, _| build_writer(&mut k.c))??;
        self.clean(si, id)?;
        let record = self.with(si, |k, _| -> Result<Option<(Vec<u8>, u64)>> {
            let c = &mut k.c;
            if c.fresh {
                return Ok(None);
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
                return Ok(None);
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
            Ok(Some((rec, c.log_off + c.log_end)))
        })??;
        let Some((rec, at)) = record else {
            return self.checkpoint(si, id);
        };
        self.inner.write(id, at, &rec)?;
        if self.durable {
            self.inner.sync(id, 3)?;
        }
        self.with(si, |k, _| {
            let c = &mut k.c;
            c.physical = c.physical.max(at + rec.len() as u64);
            c.log_end += align(rec.len() as u64, SECTOR);
            c.seq += 1;
        })?;
        self.after_commit(si, id)
    }

    /// Hele kaart als nieuw snapshot, dan het andere superblok.
    fn checkpoint(&mut self, si: usize, id: FileId) -> Result {
        let plan = self.with(si, |k, _| plan_checkpoint(&mut k.c))??;
        // Het snapshot, in stappen geschreven en gesomd.
        let mut s = Sum::new();
        let mut buf = Vec::new();
        buf.try_reserve_exact(CHUNK.min(plan.snapshot_len as usize).max(8))
            .map_err(|_| Error::MEMORY)?;
        let pages = (plan.snapshot_len / 8) as usize;
        let mut at = 0usize;
        while at < pages {
            let n = (CHUNK / 8).min(pages - at);
            buf.clear();
            self.with(si, |k, _| -> Result {
                for &e in k.c.map.get(at..at + n).ok_or(Error::IO)? {
                    buf.extend_from_slice(&e.to_le_bytes());
                }
                Ok(())
            })??;
            s.update(&buf);
            self.inner
                .write(id, plan.region_off + at as u64 * 8, &buf)?;
            at += n;
        }
        self.inner.sync(id, 3)?;
        let mut salt = [0u8; 8];
        self.inner.random(&mut salt)?;
        let (sb, target) = self.with(si, |k, _| {
            let c = &mut k.c;
            c.physical = c.physical.max(plan.region_off + plan.snapshot_len);
            let snap = align(plan.snapshot_len, SECTOR);
            let mut sb = c.superblock();
            sb.generation = c.generation + 1;
            sb.salt = u64::from_le_bytes(salt);
            sb.seq = c.seq + 1;
            sb.region_off = plan.region_off;
            sb.region_len = plan.region_len;
            sb.snapshot_len = plan.snapshot_len;
            sb.snapshot_sum = s.finish();
            sb.log_off = plan.region_off + snap;
            sb.log_cap = plan.region_len - snap;
            (sb, if c.active == 0 { SLOT_B } else { 0 })
        })?;
        self.inner.write(id, target, &sb.encode())?;
        self.inner.sync(id, 3)?;
        self.with(si, |k, _| {
            let c = &mut k.c;
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
            if !plan.in_place {
                let seg = u64::from(c.segment);
                let first = c.seg_of(plan.old.0).ok_or(Error::IO)? as usize;
                let w = c.writer.as_mut().ok_or(Error::MISUSE)?;
                for g in w
                    .segs
                    .iter_mut()
                    .skip(first)
                    .take((plan.old.1 / seg) as usize)
                {
                    if g.usage == Use::Meta {
                        g.usage = Use::Free;
                        g.end = 0;
                    }
                }
            }
            Ok(())
        })??;
        self.after_commit(si, id)
    }

    /// Na een duurzame commit: de nieuwe gecommitte staat voor iedereen, lege
    /// segmenten vrij, volgende writes op een nieuwe sector, eventueel een
    /// kortere staart.
    fn after_commit(&mut self, si: usize, id: FileId) -> Result {
        let (target, physical) = self.with(si, |k, _| -> Result<(u64, u64)> {
            let c = &mut k.c;
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
            let physical = c.physical;
            k.end();
            Ok((target, physical))
        })??;
        if target < physical {
            self.inner.truncate(id, target)?;
            self.with(si, |k, _| k.c.physical = target)?;
        }
        Ok(())
    }

    /// De schoonmaker: binnen een budget levende slots verplaatsen uit segmenten
    /// die vooral dood zijn, of uit de staart als er onderin veel vrij is.
    fn clean(&mut self, si: usize, id: FileId) -> Result {
        let mut budget = self.with(si, |k, _| clean_budget(&k.c))??;
        for _ in 0..64 {
            let Some((victim, seg)) = self.with(si, |k, _| pick_victim(&mut k.c))?? else {
                return Ok(());
            };
            let moved = self.evacuate(si, id, victim)?;
            budget = budget.saturating_sub(moved.max(seg / 4));
            if budget == 0 {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Verplaatst alle levende slots van segment `victim` naar het open segment.
    fn evacuate(&mut self, si: usize, id: FileId, victim: u32) -> Result<u64> {
        let (page_size, base, end) = self.with(si, |k, _| {
            let b = k.c.seg_base(victim);
            (k.c.page_size, b, b + u64::from(k.c.segment))
        })?;
        self.buffers(page_size)?;
        let mut moved = 0u64;
        let mut from = 0usize;
        loop {
            // De volgende pagina met een slot in het segment, in één lening.
            let next = self.with(si, |k, _| {
                let map = &k.c.map;
                (from..map.len()).find_map(|p| {
                    let s = slot_of(map.get(p).copied().unwrap_or(0), page_size)?;
                    (s.off >= base && s.off < end).then_some((p, s))
                })
            })?;
            let Some((page, s)) = next else {
                break;
            };
            let buf = self
                .work
                .stored
                .get_mut(..s.len as usize)
                .ok_or(Error::IO)?;
            read_exact(&mut self.inner, id, s.off, buf)?;
            let off = self.with(si, |k, _| {
                let off = alloc(&mut k.c, s.len)?;
                if k.c.seg_of(off) == Some(victim) {
                    return Err(Error::IO);
                }
                Ok(off)
            })??;
            let buf = self.work.stored.get(..s.len as usize).ok_or(Error::IO)?;
            self.inner.write(id, off, buf)?;
            let sum = check(buf);
            self.with(si, |k, _| {
                k.c.physical = k.c.physical.max(off + u64::from(s.len));
                k.set_entry(page as u32, entry_of(off, s.len, page_size), sum)
            })??;
            moved += u64::from(s.len);
            from = page + 1;
        }
        Ok(moved)
    }
}

/// Waar een checkpoint zijn nieuwe metadatagebied legt.
#[derive(Clone, Copy)]
struct Plan {
    region_off: u64,
    region_len: u64,
    snapshot_len: u64,
    /// Het lege gebied van de eerste commit hergebruikt.
    in_place: bool,
    /// Het oude gebied (offset, lengte).
    old: (u64, u64),
}

/// Kiest het metadatagebied van een checkpoint en zet zijn segmenten op `Meta`.
fn plan_checkpoint(c: &mut Container) -> Result<Plan> {
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
    Ok(Plan {
        region_off,
        region_len,
        snapshot_len,
        in_place,
        old,
    })
}

/// Het budget van de schoonmaker voor deze commit.
fn clean_budget(c: &Container) -> Result<u64> {
    let seg = u64::from(c.segment);
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
    Ok((2 * w.written).max(floor))
}

/// Het volgende segment voor de schoonmaker en de segmentmaat; `None` als er
/// niets te doen is.
fn pick_victim(c: &mut Container) -> Result<Option<(u32, u64)>> {
    let seg = u64::from(c.segment);
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
        return Ok(None);
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
        return Ok(None);
    };
    if Some(victim as u32) == w.open {
        return Ok(None);
    }
    Ok(Some((victim as u32, seg)))
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
                if let Some(si) = container {
                    self.release(si);
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
        if let Some(si) = h.container {
            let last = self
                .owner
                .files
                .get(si)
                .copied()
                .flatten()
                .is_some_and(|s| s.opens == 1);
            let pending = last
                && self
                    .with(si, |k, me| k.tx == Some(me) && k.c.pending && !k.c.broken)
                    .unwrap_or(false);
            if pending {
                // Ook zonder sync van SQLite (synchronous=OFF, VACUUM INTO)
                // is het bestand na sluiten compleet.
                out = self.commit(si, h.inner);
            }
            // Na een mislukte commit leest de volgende opener het bestand opnieuw.
            self.release(si);
        }
        let closed = self.inner.close(h.inner);
        out.and(closed)
    }
    fn read(&mut self, file: FileId, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let h = self.handle(file)?;
        match h.container {
            None => self.inner.read(h.inner, offset, dst),
            Some(si) => self.read_container(si, h.inner, offset, dst),
        }
    }
    fn write(&mut self, file: FileId, offset: u64, src: &[u8]) -> Result {
        let h = self.handle(file)?;
        match h.container {
            None => {
                self.inner.write(h.inner, offset, src)?;
                // SQLite wist de journalkop: de transactie is af.
                if let Some(si) = h.journal_of
                    && offset == 0
                    && src.iter().all(|&b| b == 0)
                {
                    self.settle(si)?;
                }
                Ok(())
            }
            Some(si) => self.write_container(si, h.inner, offset, src),
        }
    }
    fn truncate(&mut self, file: FileId, size: u64) -> Result {
        let h = self.handle(file)?;
        match h.container {
            None => {
                self.inner.truncate(h.inner, size)?;
                if let Some(si) = h.journal_of
                    && size == 0
                {
                    self.settle(si)?;
                }
                Ok(())
            }
            Some(si) => self.truncate_container(si, h.inner, size),
        }
    }
    fn sync(&mut self, file: FileId, flags: i32) -> Result {
        let h = self.handle(file)?;
        match h.container {
            None => self.inner.sync(h.inner, flags),
            Some(si) => self.commit(si, h.inner),
        }
    }
    fn size(&mut self, file: FileId) -> Result<u64> {
        let h = self.handle(file)?;
        match h.container {
            None => self.inner.size(h.inner),
            Some(si) => {
                self.fresh(si, h.inner)?;
                self.with(si, |k, me| k.check(me).map(|()| k.size(me)))?
            }
        }
    }
    fn remove(&mut self, name: &CStr, sync_directory: bool) -> Result {
        let bytes = name.to_bytes();
        if self.slot_named(bytes).is_some() {
            return Err(BUSY);
        }
        // Open bij een andere eigenaar: die houdt zijn kaart van het oude
        // bestand (zoals zijn handvat), een nieuw bestand met deze naam krijgt
        // een eigen kern.
        if let Ok(mut reg) = self.owner.maps.0.try_borrow_mut()
            && let Some(i) = reg.find(bytes)
            && let Some(Some(k)) = reg.cores.get_mut(i)
        {
            k.listed = false;
        }
        self.inner.remove(name, sync_directory)?;
        if let Some(si) = self.journal_owner(bytes) {
            self.settle(si)?;
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
