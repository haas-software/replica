//! De lezer van een gerepliceerde database: een lokale kopie die volgt wat
//! de schrijver publiceert, zonder de schrijverlease en zonder ooit naar de
//! store te schrijven (TODO "Volgen"; uit EasyFlor, 21-11-2026).
//!
//! [`crate::owner`] heeft de kant van de schrijver (Prepare zet een kopie
//! voort of herstelt hem onder de lease) en [`crate::restore`] een volledig
//! herstel, maar niets dat een kopie voortzet die *achterloopt*: Prepare op
//! een achterlopende kopie betekent dat zijn eigen writes nog niet
//! gepubliceerd zijn (`NewSnapshot`). Deze lezer bouwt op het publieke
//! formaat (de manifesten, hun plan, de segmenten), met dezelfde regels:
//!
//! - **Herstel**: geen bruikbare lokale kopie, of een andere generatie:
//!   `restore::stage` naar scratch, de integriteitscontrole, dan de kopie
//!   over het bestand (de eigen intentie en scratch van replica).
//! - **Incrementen**: dezelfde generatie en een lagere sequence: de commits
//!   na de lokale worden op het bestand zelf toegepast, in planvolgorde (een
//!   compactievenster dat vóór de lokale sequence begint, gaat in zijn geheel:
//!   zijn pagina's zijn hun staat aan zijn eind). Eerst gaat er een
//!   intentiebestand (`.replica-restoring`) neer, dat na de sync weggaat;
//!   een onderbroken toepassing wordt de volgende keer volledig hersteld.
//! - **Peilen**: `current` en één listing van de manifesten van de generatie;
//!   alleen manifesten die nog niet gezien zijn worden opgehaald (ze blijven
//!   per generatie bewaard), dus volgen kost twee verzoeken per database
//!   als er niets veranderde.
//!
//! De lokale kopie krijgt de marker en dirty log die een herstel de
//! schrijver geeft (compleet, schoon, de sequence die hij heeft), plus de
//! sidecar [`SIDECAR`]: een schrijver die later dezelfde map opent, brengt
//! hem eerst naar de nieuwste staat (en haalt de sidecar weg met
//! [`release`]), zodat een achterlopende kopie nooit als nieuwe snapshot
//! gepubliceerd wordt.
//!
//! Elke stap valt tussen verbindingen: de SQL-kant heeft het bestand dicht
//! (de host heropent zijn verbinding na een update), dus geen paginacache en
//! geen kaart van de gecomprimeerde container is verouderd.
use crate::local::{self, File, Name};
use crate::manifest::{Layout, MAX_COMMITS, MAX_MANIFEST_BYTES, Manifest};
use crate::marker::{self, LocalMarker, Marker};
use crate::object::{self, Store, StoreError};
use crate::tracking::Tracking;
use crate::{Error, Result, replication, restore, string};
use alloc::string::String;
use alloc::vec::Vec;
use replica_sqlite::Storage;
/// De sidecar van een lokale kopie van een lezer (leeg bestand `<db>.replica-follower`).
pub const SIDECAR: &str = ".replica-follower";
/// Wat [`Follow::update`] deed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// De store heeft geen generatie van de database (een lokale kopie blijft).
    Absent,
    /// De lokale kopie is de nieuwste staat.
    Current,
    /// Volledig hersteld: `bytes` aan segmenten.
    Restored {
        /// Opgehaalde segmentbytes.
        bytes: u64,
    },
    /// Incrementen toegepast van sequence `from` tot `to`: `bytes` aan segmenten.
    Updated {
        /// De sequence van de kopie ervoor.
        from: u64,
        /// De sequence van de kopie erna.
        to: u64,
        /// Opgehaalde segmentbytes.
        bytes: u64,
    },
}
/// Eén manifest van de gevolgde generatie: zijn bytes en zijn laatste sequence.
struct Cached {
    key: String,
    bytes: Vec<u8>,
    sequence: u64,
}
/// De lezer van één database in één namespace.
pub struct Follow {
    namespace: String,
    destination: String,
    path: Name,
    pages: u32,
    /// De generatie waar de manifesten hieronder bij horen.
    generation: String,
    snapshot: Option<Vec<u8>>,
    commits: Vec<Cached>,
    /// Generatie en sequence van de lokale kopie, zodra bekend.
    local: Option<(String, u64)>,
}
fn store_error(e: StoreError) -> Error {
    Error::Object(e)
}
/// `<namespace>/current`, `None` als de store geen generatie van de database heeft.
fn current<S: Store>(store: &mut S, namespace: &str) -> Result<Option<String>> {
    match store.get(&object::key(namespace, "/current")?, 255) {
        Ok(bytes) => {
            let id = core::str::from_utf8(&bytes)
                .map_err(|_| Error::Corrupt)?
                .trim();
            marker::generation_time(id)?;
            Ok(Some(string(id)?))
        }
        Err(StoreError::Missing) => Ok(None),
        Err(e) => Err(store_error(e)),
    }
}
impl Follow {
    /// De lezer van `path` in `namespace`; `destination` zoals de marker van
    /// de schrijver hem heeft (`marker::destination`), `pages` het budget.
    pub fn new(namespace: &str, destination: &str, path: Name, pages: u32) -> Result<Self> {
        Ok(Follow {
            namespace: string(namespace)?,
            destination: string(destination)?,
            path,
            pages,
            generation: String::new(),
            snapshot: None,
            commits: Vec::new(),
            local: None,
        })
    }
    /// De manifesten van `generation`: bewaarde blijven, nieuwe worden
    /// opgehaald, verdwenen gaan weg.
    fn refresh<S: Store>(&mut self, store: &mut S, generation: &str) -> Result<String> {
        let prefix = replication::generation_prefix(&self.namespace, generation)?;
        if self.generation != generation {
            self.generation = string(generation)?;
            self.snapshot = None;
            self.commits.clear();
        }
        if self.snapshot.is_none() {
            let key = object::key(&prefix, "snapshot")?;
            self.snapshot = Some(object::committed(store, &key, MAX_MANIFEST_BYTES)?);
        }
        let listing = store
            .list(&object::key(&prefix, "L")?, MAX_COMMITS)
            .map_err(store_error)?;
        let keys: Vec<&str> = listing
            .iter()
            .map(|o| o.key.as_str())
            .filter(|k| {
                // De manifesten die `object::layout` leest: ruwe commits en complete vensters.
                k.strip_prefix(prefix.as_str()).is_some_and(|rest| {
                    (rest.starts_with("L0/") && rest.ends_with(".json"))
                        || (rest.starts_with('L') && rest.ends_with("/complete"))
                })
            })
            .collect();
        self.commits.retain(|c| keys.contains(&c.key.as_str()));
        for key in keys {
            if self.commits.iter().any(|c| c.key == key) {
                continue;
            }
            let bytes = object::committed(store, key, MAX_MANIFEST_BYTES)?;
            let sequence = Manifest::decode(&bytes, &prefix)?.sequence;
            crate::reserve(&mut self.commits, 1)?;
            self.commits.push(Cached {
                key: string(key)?,
                bytes,
                sequence,
            });
        }
        Ok(prefix)
    }
    /// De layout van de bewaarde manifesten.
    fn layout(&self, prefix: &str) -> Result<Layout> {
        let snapshot = Manifest::decode(self.snapshot.as_deref().ok_or(Error::State)?, prefix)?;
        let mut commits = Vec::new();
        crate::reserve(&mut commits, self.commits.len())?;
        for c in &self.commits {
            commits.push(Manifest::decode(&c.bytes, prefix)?);
        }
        Layout::new(snapshot, commits, prefix)
    }
    /// De marker van de lokale kopie als deze lezer hem kan voortzetten: het
    /// bestand is er, geen herstel is onderbroken, en de marker is compleet
    /// voor deze bestemming.
    fn usable<B: Storage>(&self, b: &mut B) -> Result<Option<Marker>> {
        if !b.exists(self.path.cstr()?)?
            || b.exists(self.path.suffix(".replica-restoring")?.cstr()?)?
            || !b.exists(self.path.suffix(".replica")?.cstr()?)?
        {
            return Ok(None);
        }
        match LocalMarker::load(b, self.path) {
            Ok(m) if m.value.destination == self.destination && m.value.complete => {
                Ok(Some(m.value))
            }
            Ok(_) | Err(Error::Corrupt | Error::Legacy | Error::Limit) => Ok(None),
            Err(e) => Err(e),
        }
    }
    /// Loopt de store voor op de lokale kopie? `current` en één listing;
    /// nieuwe manifesten blijven bewaard.
    pub fn poll<S: Store>(&mut self, store: &mut S) -> Result<bool> {
        let Some(generation) = current(store, &self.namespace)? else {
            return Ok(false);
        };
        let Some((local, sequence)) = self.local.as_ref() else {
            return Ok(true);
        };
        if *local != generation {
            return Ok(true);
        }
        let sequence = *sequence;
        self.refresh(store, &generation)?;
        Ok(self.commits.iter().any(|c| c.sequence > sequence))
    }
    /// Brengt de lokale kopie naar de nieuwste staat van de store (zie de
    /// moduledocs). De SQL-kant heeft het bestand dicht. `verify` controleert
    /// een herstelde scratchdatabase.
    pub fn update<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        verify: impl FnOnce(&mut B, &Name) -> Result,
    ) -> Result<Outcome> {
        let Some(generation) = current(store, &self.namespace)? else {
            return Ok(if b.exists(self.path.cstr()?)? {
                Outcome::Current
            } else {
                Outcome::Absent
            });
        };
        let prefix = self.refresh(store, &generation)?;
        let layout = self.layout(&prefix)?;
        let target = layout
            .plan(None)?
            .last()
            .map(|m| m.sequence)
            .ok_or(Error::Corrupt)?;
        let outcome = match self.usable(b)? {
            Some(m) if m.generation == generation && m.sequence == target => Outcome::Current,
            Some(m) if m.generation == generation && m.sequence < target => {
                match self.apply(b, store, &layout, m)? {
                    Some(o) => o,
                    None => self.restore(b, store, &layout, &generation, verify)?,
                }
            }
            _ => self.restore(b, store, &layout, &generation, verify)?,
        };
        self.local = Some((generation, target));
        Ok(outcome)
    }
    /// Een volledig herstel van `generation` (de eigen stage, verify en
    /// publish van replica), dan de marker, de dirty log en de sidecar van de
    /// kopie van een lezer.
    fn restore<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        layout: &Layout,
        generation: &str,
        verify: impl FnOnce(&mut B, &Name) -> Result,
    ) -> Result<Outcome> {
        let result = restore::stage(b, store, layout, None, self.path, self.pages)?
            .verify(b, verify)?
            .publish(b, generation)?;
        let mut value = Marker::new(
            &self.destination,
            generation,
            marker::generation_time(generation)?,
        )?;
        value.sequence = result.sequence;
        value.at = result.at;
        value.size = result.size;
        value.page_size = result.page_size;
        value.bytes = result.bytes;
        value.sealed_at = layout.frontier();
        value.complete = true;
        value.clean = true;
        self.settle(b, value)?;
        Ok(Outcome::Restored {
            bytes: result.bytes,
        })
    }
    /// Schrijft `value` als lokale marker, de bijbehorende dirty log en de sidecar.
    fn settle<B: Storage>(&self, b: &mut B, value: Marker) -> Result {
        let generation = string(&value.generation)?;
        let (sequence, page_size) = (value.sequence, value.page_size);
        LocalMarker::new(self.path, value)?.save(b)?;
        Tracking::new(self.path, self.pages)?.reset_confirmed(
            b,
            &generation,
            sequence,
            page_size,
        )?;
        local::write(b, &self.path.suffix(SIDECAR)?, b"")
    }
    /// De commits na `local.sequence`, toegepast op het bestand. `None`: het
    /// plan heeft geen commit die de lokale sequence voortzet (de aanroeper
    /// herstelt).
    fn apply<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        layout: &Layout,
        mut local: Marker,
    ) -> Result<Option<Outcome>> {
        let from = local.sequence;
        let plan = layout.plan(None)?;
        let chain: Vec<&Manifest> = plan.into_iter().filter(|m| m.sequence > from).collect();
        if chain.first().is_none_or(|m| m.first > from + 1) || local.page_size == 0 {
            return Ok(None);
        }
        let page_size = local.page_size;
        let intent = self.path.suffix(".replica-restoring")?;
        local::write(b, &intent, local.generation.as_bytes())?;
        let mut bytes = 0u64;
        let mut page: Vec<u8> = Vec::new();
        crate::reserve(&mut page, page_size as usize)?;
        page.resize(page_size as usize, 0);
        {
            let mut file = File::open(b, &self.path, false)?;
            for m in &chain {
                if m.min_size > u64::from(self.pages) * u64::from(page_size) {
                    return Err(Error::Limit);
                }
                file.truncate(m.min_size)?;
                for part in &m.parts {
                    let limit = usize::try_from(part.size).map_err(|_| Error::Limit)?;
                    let data = object::committed(store, &part.key, limit)?;
                    let seg = part.read(&data)?;
                    if seg.page_size() != page_size {
                        return Err(Error::Corrupt);
                    }
                    if seg.database_size() / u64::from(page_size) > u64::from(self.pages) {
                        return Err(Error::Limit);
                    }
                    for (number, stored) in seg.pages() {
                        stored.read(&mut page)?;
                        file.write(u64::from(number - 1) * u64::from(page_size), &page)?;
                    }
                    file.truncate(seg.database_size())?;
                    local.size = seg.database_size();
                    bytes = bytes.saturating_add(part.size);
                }
                local.sequence = m.sequence;
                local.at = m.at;
            }
            file.sync()?;
            file.close()?;
        }
        b.remove(intent.cstr()?, true)?;
        let to = local.sequence;
        local.bytes = local.bytes.saturating_add(bytes);
        local.sealed_at = layout.frontier();
        local.clean = true;
        self.settle(b, local)?;
        Ok(Some(Outcome::Updated { from, to, bytes }))
    }
}
/// Haalt de sidecar van `path` weg (een schrijver nam de kopie over).
pub fn release<B: Storage>(b: &mut B, path: Name) -> Result {
    let sidecar = path.suffix(SIDECAR)?;
    if b.exists(sidecar.cstr()?)? {
        b.remove(sidecar.cstr()?, true)?;
    }
    Ok(())
}
/// Is de kopie van `path` die van een lezer (de sidecar staat er)? Een
/// schrijver brengt hem dan eerst naar de nieuwste staat.
pub fn is_follower<B: Storage>(b: &mut B, path: Name) -> Result<bool> {
    Ok(b.exists(path.suffix(SIDECAR)?.cstr()?)?)
}
