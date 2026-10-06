//! Publicatie van één capture; markerbeslissingen blijven duurzaam vóór onbekende PUTs.
use crate::{
    Error, Result,
    capture::Capture,
    local::{self, Name},
    manifest::{Manifest, Part},
    marker::{self, LocalMarker, Marker},
    object::{self, Store, StoreError},
    reserve, string,
    time::Time,
    tracking::Tracking,
};
use alloc::{string::String, vec::Vec};
use core::fmt::Write;
use hop_types::json::{self, Object, Value};
use replica_sqlite::Storage;
/// Het record van een onderbroken snapshot-upload naast de spool.
const PENDING_SUFFIX: &str = ".replica-pending";
/// Ruim onder de spoolgrootte; 32768 delen van ~100 bytes.
const PENDING_BYTES: usize = 4 << 20;
/// Een capture blijft aan precies één generatie en voorganger gekoppeld.
pub struct Batch {
    capture: Capture,
    id: [u8; 16],
    generation: String,
    previous_sequence: u64,
}
/// Prefix van een generatie, gelijk aan Go's `prefix/domain/generations/id/`.
pub fn generation_prefix(namespace: &str, generation: &str) -> Result<String> {
    if namespace.is_empty() || namespace.ends_with('/') || namespace.contains("..") {
        return Err(Error::State);
    }
    marker::generation_time(generation)?;
    object::key(
        &object::key(namespace, "/generations/")?,
        &object::key(generation, "/")?,
    )
}
/// Begint een verse snapshotpoging, met Previous bewaard totdat current bevestigd is.
pub fn renew<B: Storage>(b: &mut B, local: &mut LocalMarker, now: Time) -> Result {
    if local.value.uncertain != 0 {
        return Err(Error::State);
    }
    let generation = marker::new_generation(b, now)?;
    let mut next = Marker::new(&local.value.destination, &generation, now)?;
    next.repair_from = string(&local.value.repair_from)?;
    let old = if local.value.complete {
        Some(&local.value)
    } else {
        local.value.previous.first()
    };
    if let Some(old) = old {
        let mut previous = old.duplicate()?;
        previous.previous.clear();
        reserve(&mut next.previous, 1)?;
        next.previous.push(previous);
    }
    local.value = next;
    local.save(b)
}
impl Batch {
    /// Hoeveel pagina's deze batch meeneemt.
    pub fn page_count(&self) -> u64 {
        self.capture.page_count()
    }
    /// Waar de tijd van de capture heen ging.
    pub fn stats(&self) -> crate::capture::Stats {
        self.capture.stats
    }
    /// Capture vóór netwerk-I/O; None wanneer broncontrole geen wijzigingen vindt.
    pub fn capture<B: Storage>(
        b: &mut B,
        path: Name,
        tracking: &Tracking,
        local: &LocalMarker,
        segment_bytes: usize,
    ) -> Result<Option<Self>> {
        if local.value.generation.is_empty() || local.value.uncertain != 0 {
            return Err(Error::State);
        }
        if !local.value.complete && local.value.sequence != 0 {
            return Err(Error::State);
        }
        let generation = string(&local.value.generation)?;
        let capture = Capture::read(
            b,
            path,
            tracking,
            !local.value.complete,
            local.value.size,
            segment_bytes,
        )?;
        if capture.is_empty() {
            return Ok(None);
        }
        // Een retry kan dezelfde generatie en voorganger hebben. Eigen entropie
        // bindt staged/uploaded ook dan aan precies deze capture.
        let mut id = [0u8; 16];
        b.random(&mut id)?;
        Ok(Some(Self {
            capture,
            id,
            generation,
            previous_sequence: local.value.sequence,
        }))
    }
    /// Legt een snapshot-capture met zijn poging-prefix duurzaam naast de spool,
    /// zodat een herstart de upload hervat waar hij ophield. Een increment is
    /// goedkoop en begint gewoon opnieuw: dan wordt het record leeg.
    pub fn save_pending<B: Storage>(&self, b: &mut B, path: Name, staged: &Staged) -> Result {
        let name = path.suffix(PENDING_SUFFIX)?;
        if !self.capture.snapshot {
            return local::write(b, &name, b"");
        }
        let push = |o: &mut Object, k, v| o.push(k, v).map_err(crate::manifest::json_error);
        let mut o = Object::new();
        push(&mut o, "version", Value::uint(1))?;
        push(&mut o, "id", Value::String(hex_id(&self.id)?))?;
        push(
            &mut o,
            "generation",
            Value::String(string(&self.generation)?),
        )?;
        push(&mut o, "previous_seq", Value::uint(self.previous_sequence))?;
        push(
            &mut o,
            "namespace",
            Value::String(string(&staged.namespace)?),
        )?;
        push(&mut o, "prefix", Value::String(string(&staged.prefix)?))?;
        push(
            &mut o,
            "data_prefix",
            Value::String(string(&staged.data_prefix)?),
        )?;
        push(&mut o, "capture", self.capture.record()?)?;
        let text = json::to_string(&Value::Object(o)).map_err(crate::manifest::json_error)?;
        if text.len() > PENDING_BYTES {
            return Err(Error::Limit);
        }
        local::write(b, &name, text.as_bytes())
    }
    /// Geen hervatting meer mogelijk of nodig.
    pub fn clear_pending<B: Storage>(b: &mut B, path: Name) -> Result {
        let name = path.suffix(PENDING_SUFFIX)?;
        if b.exists(name.cstr()?)? {
            local::write(b, &name, b"")?;
        }
        Ok(())
    }
    /// Het record van een onderbroken snapshot bij precies deze marker (zelfde
    /// generatie en voorganger, geen onzekere commit) met een intacte spool;
    /// anders niets, en dan begint de snapshot opnieuw.
    pub fn load_pending<B: Storage>(
        b: &mut B,
        path: Name,
        local: &LocalMarker,
    ) -> Result<Option<(Self, Staged)>> {
        let name = path.suffix(PENDING_SUFFIX)?;
        if local.value.complete || !b.exists(name.cstr()?)? {
            return Ok(None);
        }
        let bytes = local::read(b, &name, PENDING_BYTES)?;
        if bytes.is_empty() {
            return Ok(None);
        }
        let value = json::parse(&bytes).map_err(crate::manifest::json_error)?;
        let o = value.as_object().ok_or(Error::Corrupt)?;
        if crate::manifest::uint(o, "version")? != 1 {
            return Ok(None);
        }
        let text = |k| -> Result<String> {
            string(
                crate::manifest::field(o, k)?
                    .as_str()
                    .ok_or(Error::Corrupt)?,
            )
        };
        let generation = text("generation")?;
        let previous_sequence = crate::manifest::uint(o, "previous_seq")?;
        if generation != local.value.generation
            || previous_sequence != local.value.sequence
            || local.value.uncertain != 0
        {
            return Ok(None);
        }
        let record = crate::manifest::field(o, "capture")?
            .as_object()
            .ok_or(Error::Corrupt)?;
        let capture = Capture::from_record(record, path.suffix(".replica-capture")?)?;
        let mut spool = local::File::open(b, capture.spool(), false)?;
        let intact = spool.size()? >= capture.spool_end();
        spool.close()?;
        if !intact {
            return Ok(None);
        }
        let id = unhex_id(&text("id")?)?;
        let batch = Self {
            capture,
            id,
            generation,
            previous_sequence,
        };
        let staged = Staged {
            batch_id: id,
            namespace: text("namespace")?,
            prefix: text("prefix")?,
            data_prefix: text("data_prefix")?,
        };
        Ok(Some((batch, staged)))
    }
    /// De paginagrootte van het beeld, voor een marker die er nog geen heeft.
    pub fn page_size(&self) -> u32 {
        self.capture.page_size
    }
    /// Een hervatte capture na een herstart (zie [`Tracking::resume_capture`]).
    pub fn resumed_at(&mut self, revision: u64) {
        self.capture.resumed_at(revision);
    }
    fn check(&self, local: &LocalMarker) -> Result {
        if local.value.generation != self.generation
            || local.value.sequence != self.previous_sequence
            || local.value.uncertain != 0
        {
            return Err(Error::State);
        }
        Ok(())
    }
    /// Kiest de generatieprefix en een verse attempt-prefix voor de delen; geen netwerk.
    pub fn stage<B: Storage>(
        &self,
        b: &mut B,
        namespace: &str,
        local: &LocalMarker,
        now: Time,
    ) -> Result<Staged> {
        self.check(local)?;
        let prefix = generation_prefix(namespace, &self.generation)?;
        let attempt = marker::new_generation(b, now)?;
        let data_prefix = object::key(
            &object::key(&prefix, "data/")?,
            &object::key(&attempt, "/")?,
        )?;
        Ok(Staged {
            batch_id: self.id,
            namespace: string(namespace)?,
            prefix,
            data_prefix,
        })
    }
    /// Uploadt de delen. Dit raakt alleen de spool en de store, dus het mag
    /// buiten de eigenaar lopen terwijl SQL via de VFS doorschrijft: die
    /// pagina's blijven dirty tot een volgende capture.
    pub fn upload<B: Storage, S: Store>(
        &self,
        b: &mut B,
        store: &mut S,
        staged: &Staged,
    ) -> Result<Uploaded> {
        if staged.batch_id != self.id {
            return Err(Error::State);
        }
        Ok(Uploaded {
            batch_id: self.id,
            namespace: string(&staged.namespace)?,
            prefix: string(&staged.prefix)?,
            parts: self.capture.upload(b, store, &staged.data_prefix)?,
        })
    }
    /// Manifest laatst, met de marker duurzaam vóór die onbekende PUT, en
    /// daarna de bevestiging van de tracking. Kort, en op de eigenaar.
    pub fn finish<B: Storage, S: Store>(
        self,
        b: &mut B,
        store: &mut S,
        uploaded: Uploaded,
        local: &mut LocalMarker,
        tracking: &mut Tracking,
        now: Time,
    ) -> Result {
        self.check(local)?;
        if uploaded.batch_id != self.id {
            return Err(Error::State);
        }
        let Uploaded {
            batch_id: _,
            namespace,
            prefix,
            parts,
        } = uploaded;
        let seq = self
            .previous_sequence
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(Error::Limit)?;
        let at = now
            .max(local.value.at.next()?)
            .max(local.value.sealed_at.next()?);
        let manifest = Manifest {
            min_size: self.capture.size,
            first: seq,
            sequence: seq,
            at,
            level: 0,
            start: Time::ZERO,
            end: Time::ZERO,
            parts,
        };
        let key = if self.capture.snapshot {
            object::key(&prefix, "snapshot")?
        } else {
            raw_key(&prefix, seq, at)?
        };
        let added = manifest
            .parts
            .iter()
            .try_fold(0u64, |n, p| n.checked_add(p.size).ok_or(Error::Limit))?;
        let bytes = local
            .value
            .bytes
            .checked_add(added)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(Error::Limit)?;
        // Vóór de PUT: een crash na serveracceptatie mag de sequence nooit vrijgeven.
        local.value.uncertain = seq;
        local.value.clean = false;
        local.save(b)?;
        object::publish(store, &key, &manifest, &prefix)?;
        if self.capture.snapshot {
            let current = object::key(&namespace, "/current")?;
            if let Err(original) = store.put(&current, self.generation.as_bytes()) {
                match store.get(&current, 255) {
                    Ok(found) if found == self.generation.as_bytes() => {}
                    _ => return Err(original.into()),
                }
            }
        }
        local.value.sequence = seq;
        local.value.at = at;
        local.value.size = self.capture.size;
        local.value.page_size = self.capture.page_size;
        local.value.bytes = bytes;
        local.value.complete = true;
        local.value.uncertain = 0;
        local.value.previous.clear();
        local.value.repair_from.clear();
        local.save(b)?;
        tracking.source_counter = Some(self.capture.counter);
        if self.capture.witness.is_some() {
            tracking.witness = self.capture.witness;
        }
        let clean = tracking.acknowledge_capture(b, &self.generation, seq, &self.capture)?;
        local.value.clean = clean;
        local.save(b)
    }
    /// Onderdelen eerst, manifest laatst, in één beurt op de eigenaar: stage,
    /// upload en finish. SQL mag tussen capture en deze call schrijven; die
    /// pagina's blijven dirty.
    pub fn publish<B: Storage, S: Store>(
        self,
        b: &mut B,
        store: &mut S,
        namespace: &str,
        local: &mut LocalMarker,
        tracking: &mut Tracking,
        now: Time,
    ) -> Result {
        let staged = self.stage(b, namespace, local, now)?;
        let uploaded = self.upload(b, store, &staged)?;
        self.finish(b, store, uploaded, local, tracking, now)
    }
}
/// De objectsleutels van één publicatiepoging, gekozen vóór de upload van de delen.
pub struct Staged {
    batch_id: [u8; 16],
    namespace: String,
    prefix: String,
    data_prefix: String,
}
/// De geüploade delen van één capture; het manifest volgt in [`Batch::finish`].
pub struct Uploaded {
    batch_id: [u8; 16],
    namespace: String,
    prefix: String,
    parts: Vec<Part>,
}
/// Bestaande raw-keyvorm; tijden buiten Go's veilige UnixNano-bereik worden niet gepubliceerd.
pub fn raw_key(prefix: &str, sequence: u64, at: Time) -> Result<String> {
    let ns = at
        .seconds()
        .checked_mul(1_000_000_000)
        .and_then(|n| n.checked_add(i64::from(at.nanos())))
        .ok_or(Error::Limit)?;
    if ns < 0 || sequence > i64::MAX as u64 {
        return Err(Error::Limit);
    }
    let mut suffix = String::new();
    suffix.try_reserve_exact(64).map_err(|_| Error::Memory)?;
    // Alle velden samen zijn hoogstens 47 bytes; deze fmt-write kan niet groeien.
    write!(&mut suffix, "L0/{sequence:012}-{ns:020}.json").map_err(|_| Error::Memory)?;
    object::key(prefix, &suffix)
}
/// Lost de onzekere sequence op vóór een nieuwe capture. Een transportfout laat
/// de marker onzeker; alleen een volledige listing mag 'niet geland' bewijzen.
pub fn resolve<B: Storage, S: Store>(
    b: &mut B,
    store: &mut S,
    namespace: &str,
    local: &mut LocalMarker,
) -> Result<bool> {
    let seq = local.value.uncertain;
    if seq == 0 {
        return Ok(false);
    }
    let prefix = generation_prefix(namespace, &local.value.generation)?;
    let key = if !local.value.complete {
        let current = object::key(namespace, "/current")?;
        match store.get(&current, 255) {
            Ok(bytes) if bytes == local.value.generation.as_bytes() => {
                Some(object::key(&prefix, "snapshot")?)
            }
            Ok(bytes) => {
                let current = core::str::from_utf8(&bytes)
                    .map_err(|_| Error::Corrupt)?
                    .trim();
                if (!local.value.repair_from.is_empty() && local.value.repair_from == current)
                    || local
                        .value
                        .previous
                        .first()
                        .is_some_and(|p| p.generation == current)
                {
                    None
                } else {
                    return Err(Error::State);
                }
            }
            Err(StoreError::Missing) => None,
            Err(e) => return Err(e.into()),
        }
    } else {
        let mut suffix = String::new();
        suffix.try_reserve_exact(32).map_err(|_| Error::Memory)?;
        write!(&mut suffix, "L0/{seq:012}-").map_err(|_| Error::Memory)?;
        let query = object::key(&prefix, &suffix)?;
        let list = store.list(&query, 2)?;
        if list.len() > 1 {
            return Err(Error::Corrupt);
        }
        match list.into_iter().next() {
            Some(o) if o.key.starts_with(&query) && o.key.ends_with(".json") => Some(o.key),
            Some(_) => return Err(Error::Corrupt),
            None => None,
        }
    };
    let landed = key.is_some();
    if let Some(key) = key {
        let manifest = object::read_manifest(store, &key, &prefix)?;
        if manifest.level != 0 || manifest.first != seq || manifest.sequence != seq {
            return Err(Error::Corrupt);
        }
        let last = manifest.parts.last().ok_or(Error::Corrupt)?;
        let bytes = object::committed(store, &last.key, last.size as usize)?;
        let segment = last.read(&bytes)?;
        let bytes = manifest.parts.iter().try_fold(local.value.bytes, |n, p| {
            n.checked_add(p.size)
                .filter(|n| *n <= i64::MAX as u64)
                .ok_or(Error::Limit)
        })?;
        local.value.sequence = seq;
        local.value.at = local.value.at.max(manifest.at);
        local.value.size = segment.database_size();
        local.value.page_size = segment.page_size();
        local.value.complete = true;
        local.value.previous.clear();
        local.value.repair_from.clear();
        local.value.bytes = bytes;
    }
    local.value.uncertain = 0;
    local.value.clean = false;
    local.save(b)?;
    Ok(landed)
}
fn hex_id(id: &[u8; 16]) -> Result<String> {
    let mut out = String::new();
    for b in id {
        write!(out, "{b:02x}").map_err(|_| Error::Memory)?;
    }
    Ok(out)
}
fn unhex_id(s: &str) -> Result<[u8; 16]> {
    if s.len() != 32 {
        return Err(Error::Corrupt);
    }
    let mut out = [0; 16];
    for (dst, pair) in out.iter_mut().zip(s.as_bytes().chunks_exact(2)) {
        let nibble = |b: u8| match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            _ => Err(Error::Corrupt),
        };
        *dst = nibble(pair[0])? * 16 + nibble(pair[1])?;
    }
    Ok(out)
}
