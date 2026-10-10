//! Begrensde lokale spool: database-eigendom vrijgeven vóór de netwerkupload.
use crate::{
    Error, Result,
    coverage::growth_gap,
    hash,
    local::{File, Name},
    manifest::{MAX_PARTS, Part, field, hex, json_error, uint, unhex},
    object::{self, Store},
    reserve, segment,
    shadow::{Shadow, Shared},
    string,
    tracking::Tracking,
};
use alloc::vec::Vec;
use hop_types::json::{Object, Value};
struct Chunk {
    offset: u64,
    len: usize,
    hash: [u8; 32],
}
/// Waar de tijd van een capture heen ging, in milliseconden van de opslagklok:
/// pagina's lezen, segmenten coderen en hashen, de spool schrijven.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    /// Pagina's uit de database lezen.
    pub read_ms: u64,
    /// Segmenten coderen en hashen.
    pub hash_ms: u64,
    /// De spool schrijven en synchroniseren.
    pub write_ms: u64,
    /// Geschreven segmentbytes.
    pub bytes: u64,
}
fn millis<B: replica_sqlite::Storage>(b: &mut B) -> u64 {
    b.unix_millis()
        .map_or(0, |ms| u64::try_from(ms).unwrap_or(0))
}
/// Eén consistent databasebeeld of dirty-setbeeld, los van de actieve SQLite-engine.
/// De eigenaar laat maximaal één capture tegelijk publiceren; een nieuwere spool
/// kan door de hashes nooit stil als de oudere capture worden verstuurd.
pub struct Capture {
    spool: Name,
    chunks: Vec<Chunk>,
    /// Databasegrootte van dit beeld.
    pub size: u64,
    /// SQLite-paginamaten.
    pub page_size: u32,
    /// Revisie op het moment van capture; latere writes blijven dirty bij bevestiging.
    pub revision: u64,
    /// Een volledig snapshot in plaats van een increment.
    pub snapshot: bool,
    pub(crate) counter: u32,
    pub(crate) witness: Option<(u32, [u8; 32])>,
    /// De meegenomen pagina's van een increment, oplopend; leeg bij een snapshot
    /// (dat neemt alles mee). De bevestiging laat alleen díe pagina's los.
    pub(crate) pages: Vec<u32>,
    /// Tijdsbesteding van het lezen van dit beeld.
    pub stats: Stats,
}
impl Capture {
    /// Hoeveel pagina's dit beeld meeneemt.
    pub fn page_count(&self) -> u64 {
        if self.snapshot {
            self.size / u64::from(self.page_size.max(1))
        } else {
            self.pages.len() as u64
        }
    }
    /// SQLite moet gesloten zijn of de eigenaar moet een echte leestransactie
    /// vasthouden. Alle writes gaan via Tracking; deze exclusieve opslaglening
    /// mag niet naast een verborgen schrijver of padalias bestaan.
    pub fn read<B: replica_sqlite::Storage>(
        b: &mut B,
        path: Name,
        tracking: &Tracking,
        snapshot: bool,
        previous_size: u64,
        segment_bytes: usize,
    ) -> Result<Self> {
        match Plan::new(b, path, tracking, snapshot, previous_size, segment_bytes)? {
            Some(plan) => plan.run(b),
            None => Ok(Self::empty(path, tracking)?),
        }
    }
    fn empty(path: Name, tracking: &Tracking) -> Result<Self> {
        Ok(Self {
            spool: path.suffix(".replica-capture")?,
            chunks: Vec::new(),
            size: 0,
            page_size: 0,
            revision: tracking.revision(),
            snapshot: false,
            counter: 0,
            witness: None,
            pages: Vec::new(),
            stats: Stats::default(),
        })
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
    /// Het beeld als record naast de spool, zodat een herstart de upload van een
    /// snapshot hervat. Alleen snapshots: die hebben geen paginalijst.
    pub(crate) fn record(&self) -> Result<Value> {
        if !self.snapshot || !self.pages.is_empty() {
            return Err(Error::State);
        }
        let push = |o: &mut Object, k, v| o.push(k, v).map_err(json_error);
        let mut o = Object::new();
        push(&mut o, "size", Value::uint(self.size))?;
        push(&mut o, "page_size", Value::uint(u64::from(self.page_size)))?;
        push(&mut o, "revision", Value::uint(self.revision))?;
        push(&mut o, "counter", Value::uint(u64::from(self.counter)))?;
        if let Some((page, digest)) = &self.witness {
            push(&mut o, "witness_page", Value::uint(u64::from(*page)))?;
            push(&mut o, "witness_hash", Value::String(hex_string(digest)?))?;
        }
        let mut chunks = Vec::new();
        reserve(&mut chunks, self.chunks.len())?;
        for chunk in &self.chunks {
            let mut c = Object::new();
            push(&mut c, "offset", Value::uint(chunk.offset))?;
            push(&mut c, "len", Value::uint(chunk.len as u64))?;
            push(&mut c, "sha256", Value::String(hex_string(&chunk.hash)?))?;
            chunks.push(Value::Object(c));
        }
        push(&mut o, "chunks", Value::Array(chunks))?;
        Ok(Value::Object(o))
    }
    pub(crate) fn from_record(o: &Object, spool: Name) -> Result<Self> {
        let array = field(o, "chunks")?.as_array().ok_or(Error::Corrupt)?;
        if array.is_empty() || array.len() > MAX_PARTS {
            return Err(Error::Corrupt);
        }
        let mut chunks = Vec::new();
        reserve(&mut chunks, array.len())?;
        let mut end = 0_u64;
        for v in array {
            let c = v.as_object().ok_or(Error::Corrupt)?;
            let offset = uint(c, "offset")?;
            let len = usize::try_from(uint(c, "len")?).map_err(|_| Error::Corrupt)?;
            if offset != end {
                return Err(Error::Corrupt);
            }
            end = offset.checked_add(len as u64).ok_or(Error::Corrupt)?;
            chunks.push(Chunk {
                offset,
                len,
                hash: unhex(field(c, "sha256")?.as_str().ok_or(Error::Corrupt)?)?,
            });
        }
        let page_size = u32::try_from(uint(o, "page_size")?).map_err(|_| Error::Corrupt)?;
        if !segment::valid_page_size(page_size) {
            return Err(Error::Corrupt);
        }
        let witness = match (o.get("witness_page"), o.get("witness_hash")) {
            (Some(_), Some(h)) => Some((
                u32::try_from(uint(o, "witness_page")?).map_err(|_| Error::Corrupt)?,
                unhex(h.as_str().ok_or(Error::Corrupt)?)?,
            )),
            _ => None,
        };
        Ok(Self {
            spool,
            chunks,
            size: uint(o, "size")?,
            page_size,
            revision: uint(o, "revision")?,
            snapshot: true,
            counter: u32::try_from(uint(o, "counter")?).map_err(|_| Error::Corrupt)?,
            witness,
            pages: Vec::new(),
            stats: Stats::default(),
        })
    }
    /// Waar de spool eindigt; een hervatting eist dat het bestand zo groot is.
    pub(crate) fn spool_end(&self) -> u64 {
        self.chunks
            .last()
            .map_or(0, |c| c.offset.saturating_add(c.len as u64))
    }
    pub(crate) fn spool(&self) -> &Name {
        &self.spool
    }
    /// Een hervatting na een herstart telt alles wat sindsdien dirty werd als
    /// ná dit beeld geschreven; de revisie is dan de huidige.
    pub(crate) fn resumed_at(&mut self, revision: u64) {
        self.revision = revision;
    }
    /// Uploadt naar een verse `.../data/<poging>/`-prefix, manifest komt later.
    /// Het geheugen houdt hoogstens één segment, niet een tweede hele database.
    pub fn upload<B: replica_sqlite::Storage, S: Store>(
        &self,
        b: &mut B,
        store: &mut S,
        prefix: &str,
    ) -> Result<Vec<Part>> {
        if !prefix.ends_with('/') || prefix.contains("..") {
            return Err(Error::State);
        }
        let mut parts = Vec::new();
        reserve(&mut parts, self.chunks.len())?;
        // Wat een eerdere, onderbroken poging al onder deze prefix zette, blijft
        // staan: zelfde sleutel en grootte is hetzelfde deel (de hash komt uit de
        // spool die sindsdien niet veranderde).
        let mut present = store.list(prefix, MAX_PARTS)?;
        present.sort_unstable_by(|a, c| a.key.cmp(&c.key));
        for (i, chunk) in self.chunks.iter().enumerate() {
            let mut name = *b"000000.seg";
            let mut n = i + 1;
            for b in name[..6].iter_mut().rev() {
                *b = b'0' + (n % 10) as u8;
                n /= 10;
            }
            let key = object::key(
                prefix,
                core::str::from_utf8(&name).map_err(|_| Error::State)?,
            )?;
            let uploaded = present
                .binary_search_by(|o| o.key.as_str().cmp(key.as_str()))
                .is_ok_and(|at| present[at].size == Some(chunk.len as u64));
            if !uploaded {
                let mut bytes = Vec::new();
                reserve(&mut bytes, chunk.len)?;
                bytes.resize(chunk.len, 0);
                let mut source = File::open(b, &self.spool, false)?;
                source.read(chunk.offset, &mut bytes)?;
                source.close()?;
                // encode valideerde het segment; de hash bewijst dat de spool gelijk bleef.
                if hash(&bytes) != chunk.hash {
                    return Err(Error::Corrupt);
                }
                store.put(&key, &bytes)?;
            }
            parts.push(Part {
                key,
                size: chunk.len as u64,
                hash: chunk.hash,
            });
        }
        Ok(parts)
    }
}
/// Een capture in stappen: de kop, de broncontrole en de paginalijst op de
/// eigenaar (SQLite dicht), het lezen van de segmenten waar dan ook, met de
/// [`Shadow`] als beeld van het begin. Eén segment per [`Plan::step`].
pub struct Plan {
    path: Name,
    spool: Name,
    size: u64,
    page_size: u32,
    revision: u64,
    snapshot: bool,
    counter: u32,
    per_segment: usize,
    chunk_count: usize,
    index: usize,
    offset: u64,
    witness: Option<(u32, [u8; 32])>,
    chunks: Vec<Chunk>,
    stats: Stats,
    shadow: Shared,
}
impl Plan {
    /// `None`: niets veranderde en de grootte bleef gelijk; er is geen capture.
    pub fn new<B: replica_sqlite::Storage>(
        b: &mut B,
        path: Name,
        tracking: &Tracking,
        snapshot: bool,
        previous_size: u64,
        segment_bytes: usize,
    ) -> Result<Option<Self>> {
        if !(65536..=16 << 20).contains(&segment_bytes) {
            return Err(Error::Limit);
        }
        if tracking.needs_snapshot() && !snapshot {
            return Err(Error::State);
        }
        let mut file = File::open(b, &path, false)?;
        let size = file.size()?;
        let mut header = [0u8; 100];
        file.read(0, &mut header)?;
        file.close()?;
        if &header[..16] != b"SQLite format 3\0" {
            return Err(Error::Corrupt);
        }
        let raw = u16::from_be_bytes([header[16], header[17]]);
        let page_size = if raw == 1 { 65536 } else { u32::from(raw) };
        if !segment::valid_page_size(page_size) || !size.is_multiple_of(u64::from(page_size)) {
            return Err(Error::Corrupt);
        }
        let count = size / u64::from(page_size);
        if count > u64::from(tracking.page_limit()) {
            return Err(Error::Limit);
        }
        if !snapshot && tracking.page_size() != 0 && tracking.page_size() != page_size {
            return Err(Error::State);
        }
        let counter = u32::from_be_bytes([header[24], header[25], header[26], header[27]]);
        if !snapshot {
            if tracking.source_counter.is_some_and(|old| old != counter)
                && tracking.pending()?.is_empty()
            {
                return Err(Error::ForeignWrite);
            }
            if let Some((page, expected)) = tracking.witness
                && !tracking.is_dirty(page)
                && u64::from(page) <= count
            {
                let mut bytes = Vec::new();
                reserve(&mut bytes, page_size as usize)?;
                bytes.resize(page_size as usize, 0);
                let mut source = File::open(b, &path, false)?;
                source.read(u64::from(page - 1) * u64::from(page_size), &mut bytes)?;
                source.close()?;
                if hash(&bytes) != expected {
                    return Err(Error::ForeignWrite);
                }
            }
        }
        // Een snapshot is een bereik, geen paginanummervector ter grootte van de DB.
        let mut pages = if snapshot {
            Vec::new()
        } else {
            tracking.pending()?
        };
        pages.retain(|&page| u64::from(page) <= count);
        if !snapshot
            && growth_gap(
                previous_size,
                size,
                page_size,
                &pages,
                tracking.page_limit(),
            )?
            .is_some()
        {
            return Err(Error::Gap);
        }
        if !snapshot && pages.is_empty() && size == previous_size {
            return Ok(None);
        }
        let per_segment = segment_bytes / page_size as usize;
        let page_count = if snapshot {
            count as usize
        } else {
            pages.len()
        };
        let chunk_count = page_count.div_ceil(per_segment).max(1);
        if chunk_count > MAX_PARTS {
            return Err(Error::Limit);
        }
        let spool = path.suffix(".replica-capture")?;
        let mut output = File::open(b, &spool, true)?;
        output.truncate(0)?;
        output.close()?;
        let shadow_name = path.suffix(".replica-shadow")?;
        let mut output = File::open(b, &shadow_name, true)?;
        output.truncate(0)?;
        output.close()?;
        let mut chunks = Vec::new();
        reserve(&mut chunks, chunk_count)?;
        Ok(Some(Self {
            path,
            spool,
            size,
            page_size,
            revision: tracking.revision(),
            snapshot,
            counter,
            per_segment,
            chunk_count,
            index: 0,
            offset: 0,
            witness: None,
            chunks,
            stats: Stats::default(),
            shadow: Shadow::new(shadow_name, page_size, count as u32, pages, snapshot),
        }))
    }
    /// Het handvat dat de eigenaar in zijn tracking zet zolang dit beeld leest.
    pub fn shadow(&self) -> Shared {
        self.shadow.clone()
    }
    /// Alle segmenten achter elkaar.
    pub fn run<B: replica_sqlite::Storage>(mut self, b: &mut B) -> Result<Capture> {
        while self.step(b)? {}
        self.finish(b)
    }
    /// Eén segment: lezen uit de database, de schaduw eroverheen, coderen,
    /// hashen, naar de spool. `false` als alle segmenten gelezen zijn.
    pub fn step<B: replica_sqlite::Storage>(&mut self, b: &mut B) -> Result<bool> {
        if self.index >= self.chunk_count {
            return Ok(false);
        }
        let page_size = self.page_size;
        let numbers = {
            let shadow = self.shadow.borrow();
            if shadow.is_broken() {
                return Err(Error::Gap);
            }
            shadow.segment(self.index, self.per_segment)
        };
        let mut data = Vec::new();
        reserve(&mut data, numbers.len() * page_size as usize)?;
        data.resize(numbers.len() * page_size as usize, 0);
        let t0 = millis(b);
        let mut source = File::open(b, &self.path, false)?;
        let mut start = 0;
        while start < numbers.len() {
            let mut end = start + 1;
            while end < numbers.len() && numbers[end] == numbers[end - 1] + 1 {
                end += 1;
            }
            source.read(
                u64::from(numbers[start] - 1) * u64::from(page_size),
                &mut data[start * page_size as usize..end * page_size as usize],
            )?;
            start = end;
        }
        source.close()?;
        // Wat de eigenaar sinds het begin overschreef, staat oud in de schaduw;
        // ook als de write landde terwijl wij hierboven lazen.
        let slots: Vec<(usize, u32)> = {
            let shadow = self.shadow.borrow();
            if shadow.is_broken() {
                return Err(Error::Gap);
            }
            numbers
                .iter()
                .enumerate()
                .filter_map(|(i, &page)| shadow.slot(page).map(|slot| (i, slot)))
                .collect()
        };
        if !slots.is_empty() {
            let name = *self.shadow.borrow().file();
            let mut file = File::open(b, &name, false)?;
            for (i, slot) in slots {
                file.read(
                    u64::from(slot) * u64::from(page_size),
                    &mut data[i * page_size as usize..(i + 1) * page_size as usize],
                )?;
            }
            file.close()?;
        }
        if let Some(&last) = numbers.last() {
            self.shadow.borrow_mut().advance(last);
        }
        let t1 = millis(b);
        if let Some(&last) = numbers.last() {
            self.witness = Some((last, hash(&data[data.len() - page_size as usize..])));
        }
        let mut records = Vec::new();
        reserve(&mut records, numbers.len())?;
        for (&page, bytes) in numbers.iter().zip(data.chunks_exact(page_size as usize)) {
            records.push((page, bytes));
        }
        let encoded = segment::encode(page_size, self.size, &records)?;
        let checksum = hash(&encoded);
        let len = encoded.len();
        let t2 = millis(b);
        let mut output = File::open(b, &self.spool, false)?;
        output.write(self.offset, &encoded)?;
        output.close()?;
        let t3 = millis(b);
        self.stats.read_ms = self.stats.read_ms.saturating_add(t1.saturating_sub(t0));
        self.stats.hash_ms = self.stats.hash_ms.saturating_add(t2.saturating_sub(t1));
        self.stats.write_ms = self.stats.write_ms.saturating_add(t3.saturating_sub(t2));
        self.stats.bytes = self.stats.bytes.saturating_add(len as u64);
        self.chunks.push(Chunk {
            offset: self.offset,
            len,
            hash: checksum,
        });
        self.offset = self.offset.checked_add(len as u64).ok_or(Error::Limit)?;
        self.index += 1;
        Ok(true)
    }
    /// De spool duurzaam; de schaduw hoeft niets meer te bewaren.
    pub fn finish<B: replica_sqlite::Storage>(mut self, b: &mut B) -> Result<Capture> {
        if self.index < self.chunk_count {
            return Err(Error::State);
        }
        let t4 = millis(b);
        let mut output = File::open(b, &self.spool, false)?;
        output.sync()?;
        output.close()?;
        self.stats.write_ms = self
            .stats
            .write_ms
            .saturating_add(millis(b).saturating_sub(t4));
        self.shadow.borrow_mut().finished();
        let pages = {
            let mut shadow = self.shadow.borrow_mut();
            core::mem::take(&mut shadow.pages)
        };
        Ok(Capture {
            spool: self.spool,
            chunks: self.chunks,
            size: self.size,
            page_size: self.page_size,
            revision: self.revision,
            snapshot: self.snapshot,
            counter: self.counter,
            witness: self.witness,
            pages,
            stats: self.stats,
        })
    }
}
fn hex_string(hash: &[u8; 32]) -> Result<alloc::string::String> {
    string(core::str::from_utf8(&hex(hash)).map_err(|_| Error::Corrupt)?)
}
