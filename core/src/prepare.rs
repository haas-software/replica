//! Boot vóór SQLite openen: lineage controleren, log hervatten of offline herstellen.
//!
//! **Werk van een ander.** Een tenant draait op A, dan op B, dan weer op A met
//! A's oude volume (of een teruggezet volume). De store is dan in dezelfde
//! generatie verder dan A's marker, door B. Een store die verder is, is alleen
//! van onszelf als het precies het ene manifest is dat de marker als onzeker
//! (`uncertain`) wegstuurde, met ons token erin ([`Marker::uncertain_lease`]):
//! de marker is duurzaam vóór elke PUT, dus een andere achterstand bestaat
//! voor ons eigen volume niet. Is het van een ander, dan herstelt een schone
//! database uit de store; heeft de lokale database eigen ongepubliceerd werk,
//! dan opent hij niet ([`Error::ForeignAhead`]) en blijven de bestanden staan.
//! Zo ook wanneer ons eigen bevestigde manifest door de fencing afviel (een
//! ander manifest met dezelfde sequence en een ander token), en wanneer een
//! onderbroken generatiewissel terug zou vallen op een generatie waarin de
//! store sindsdien verder is.
use crate::{
    Error, Result,
    local::{File, Name},
    manifest::{Layout, Manifest},
    marker::{self, LocalMarker, Marker},
    object::{self, Store, StoreError},
    replication,
    replication::Batch,
    restore, string,
    time::Time,
    tracking::Tracking,
};
use alloc::string::String;
use replica_sqlite::Storage;
/// Invoer van de exclusieve database-eigenaar, vóór er SQL-taken worden toegelaten.
pub struct Options<'a> {
    /// Namespace `prefix/domain`, zonder afsluitende slash.
    pub namespace: &'a str,
    /// Gepinde bestemmingshash van endpoint/bucket/prefix/domain.
    pub destination: &'a str,
    /// Canoniek lokaal databasepad.
    pub path: Name,
    /// Maximum aantal databasepagina's in deze eigenaar.
    pub page_limit: u32,
    /// Betrouwbare UTC-bootklok.
    pub now: Time,
    /// Expliciete migratie: lokale data zonder marker mag een bestaande replica overnemen.
    pub adopt_local: bool,
}
/// Eén marker en trackingstaat voor de SQL-eigenaar; geen verborgen achtergrondtaken.
pub struct Prepared {
    /// Duurzame lokale marker en zijn pad.
    pub marker: LocalMarker,
    /// De VFS-adapter gebruikt deze staat vóór elke databasewrite.
    pub tracking: Tracking,
    /// Waarom een nieuwe snapshot nodig is, of waarom de bestaande generatie doorgaat.
    pub reason: Reason,
}
/// Reden voor status/logging zonder verborgen netwerkdetails of vrije fouttekst.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// Lege database/bucket of een expliciet toegestane lokale bootstrap.
    Bootstrap,
    /// Een geldige generatie gaat door met de genoteerde dirty pagina's.
    Continued,
    /// De huidige remote generatie is volledig teruggezet.
    Restored,
    /// Een onvolledige snapshot of onbetrouwbaar dirty log vraagt een nieuwe poging.
    NewSnapshot,
    /// Bewezen schade in de eigen remote generatie wordt uit de lokale bron gerepareerd.
    Repair,
    /// Configuratie wijst naar een andere bestemming.
    DestinationChanged,
}
struct Tip {
    generation: String,
    layout: Layout,
    sequence: u64,
}
fn tip<S: Store>(store: &mut S, namespace: &str, generation: String) -> Result<Tip> {
    let prefix = replication::generation_prefix(namespace, &generation)?;
    let layout = object::layout(store, &prefix)?;
    let plan = layout.plan(None)?;
    let last = plan.last().ok_or(Error::Corrupt)?;
    let part = last.parts.last().ok_or(Error::Corrupt)?;
    let bytes = object::committed(store, &part.key, part.size as usize)?;
    part.read(&bytes)?;
    let out = Tip {
        generation,
        sequence: last.sequence,
        layout,
    };
    Ok(out)
}
impl Tip {
    /// Het raw-manifest (of de snapshot) met `sequence` in de keten, als het er
    /// nog staat; bij twee met dezelfde sequence is dit het winnende token.
    fn raw(&self, sequence: u64) -> Option<&Manifest> {
        let snapshot = self.layout.snapshot();
        if snapshot.sequence == sequence {
            return Some(snapshot);
        }
        self.layout
            .commits()
            .iter()
            .find(|m| m.level == 0 && m.sequence == sequence)
    }
}
/// Is de store verder dan `marker` door ons eigen onzekere manifest? Alleen
/// dat ene manifest, met ons token; een marker zonder token (een oudere
/// versie) houdt de oude regel voor zijn onzekere sequence.
fn ours(marker: &Marker, tip: &Tip) -> bool {
    let seq = marker.uncertain;
    seq != 0
        && seq == tip.sequence
        && marker.sequence.checked_add(1) == Some(seq)
        && tip
            .raw(seq)
            .is_some_and(|m| marker.uncertain_lease == 0 || m.lease == marker.uncertain_lease)
}
/// Viel ons laatst bevestigde manifest af tegen een ander met dezelfde
/// sequence (een late PUT na een overname)? Dan staat er lokaal werk dat
/// nooit in de keten kwam. Zonder token of zonder raw-manifest: niet te zeggen.
fn fenced_out(marker: &Marker, tip: &Tip) -> bool {
    marker.lease != 0
        && marker.complete
        && tip
            .raw(marker.sequence)
            .is_some_and(|m| m.lease != 0 && m.lease != marker.lease)
}
fn fresh<B: Storage>(
    b: &mut B,
    options: &Options<'_>,
    repair: &str,
    reason: Reason,
) -> Result<Prepared> {
    // Zonder een betrouwbaar log kan Previous geen veilige fallback zijn.
    let mut value = Marker::new(options.destination, "", Time::ZERO)?;
    value.repair_from = string(repair)?;
    let mut local = LocalMarker::new(options.path, value)?;
    replication::renew(b, &mut local, options.now)?;
    Ok(Prepared {
        marker: local,
        tracking: Tracking::new(options.path, options.page_limit)?,
        reason,
    })
}
fn restored<B: Storage, S: Store>(
    b: &mut B,
    store: &mut S,
    options: &Options<'_>,
    tip: Tip,
    verify: &mut impl FnMut(&mut B, &Name) -> Result,
) -> Result<Prepared> {
    let staged = restore::stage(
        b,
        store,
        &tip.layout,
        None,
        options.path,
        options.page_limit,
    )?;
    let result = staged.verify(b, verify)?.publish(b, &tip.generation)?;
    let mut value = Marker::new(
        options.destination,
        &tip.generation,
        marker::generation_time(&tip.generation)?,
    )?;
    value.sequence = result.sequence;
    value.at = result.at;
    value.size = result.size;
    value.page_size = result.page_size;
    value.bytes = result.bytes;
    value.sealed_at = tip.layout.frontier();
    value.complete = true;
    value.clean = true;
    let mut local = LocalMarker::new(options.path, value)?;
    local.save(b)?;
    let mut tracking = Tracking::new(options.path, options.page_limit)?;
    tracking.reset_confirmed(b, &tip.generation, result.sequence, result.page_size)?;
    Ok(Prepared {
        marker: local,
        tracking,
        reason: Reason::Restored,
    })
}
/// Bereidt de database voor volgens de lineage-regels. I/O-fouten zijn retries,
/// geen reden om met een lege DB te beginnen. De verificatiecallback krijgt de
/// complete scratchdatabase vóór de bestaande bestemming verandert.
pub fn run<B: Storage, S: Store>(
    b: &mut B,
    store: &mut S,
    options: Options<'_>,
    mut verify: impl FnMut(&mut B, &Name) -> Result,
) -> Result<Prepared> {
    if options.now.seconds() < 1_577_836_800
        || options.page_limit == 0
        || options.namespace.is_empty()
        || options.namespace.ends_with('/')
        || options.namespace.contains("..")
    {
        return Err(Error::State);
    }
    let current = match store.get(&object::key(options.namespace, "/current")?, 255) {
        Ok(bytes) => {
            if bytes.len() > 255 {
                return Err(Error::Limit);
            }
            let id = core::str::from_utf8(&bytes)
                .map_err(|_| Error::Corrupt)?
                .trim();
            marker::generation_time(id)?;
            Some(string(id)?)
        }
        Err(StoreError::Missing) => None,
        Err(e) => return Err(e.into()),
    };
    let exists = b.exists(options.path.cstr()?)?;
    let interrupted = b.exists(options.path.suffix(".replica-restoring")?.cstr()?)?;
    if !exists || interrupted {
        if let Some(generation) = current {
            let remote = tip(store, options.namespace, generation)?;
            return restored(b, store, &options, remote, &mut verify);
        }
        if interrupted {
            return Err(Error::Unproven);
        }
        // Zelfs zonder current kan een archief echte gegevens bevatten.
        match store.list(&object::key(options.namespace, "/generations/")?, 1) {
            Ok(entries) if entries.is_empty() => {}
            Ok(_) | Err(StoreError::Limit) => return Err(Error::Unproven),
            Err(e) => return Err(e.into()),
        }
        return fresh(b, &options, "", Reason::Bootstrap);
    }
    let stored = if b.exists(options.path.suffix(".replica")?.cstr()?)? {
        match LocalMarker::load(b, options.path) {
            Ok(local) => Some(local),
            Err(Error::Corrupt | Error::Legacy | Error::Limit) => None,
            Err(e) => return Err(e),
        }
    } else {
        None
    };
    let Some(mut local) = stored else {
        if current.is_some() && !options.adopt_local {
            return Err(Error::Unproven);
        }
        return fresh(b, &options, "", Reason::Bootstrap);
    };
    if local.value.destination != options.destination {
        return fresh(b, &options, "", Reason::DestinationChanged);
    }
    if current.is_none() && local.value.complete {
        return fresh(b, &options, &local.value.generation, Reason::Repair);
    }
    let remote = match current {
        Some(generation) => {
            let ours =
                local.value.generation == generation || local.value.repair_from == generation;
            match tip(store, options.namespace, string(&generation)?) {
                Ok(tip) => Some(tip),
                Err(Error::Corrupt | Error::Gap | Error::Legacy) if ours => {
                    return fresh(b, &options, &generation, Reason::Repair);
                }
                Err(e) => return Err(e),
            }
        }
        None => None,
    };
    if let Some(tip) = &remote {
        if local.value.generation == tip.generation && local.value.sequence > tip.sequence {
            // Een ontbrekende staart heeft nog steeds een geldig herstelplan,
            // maar mist eerder bevestigde writes uit onze lokale database.
            return fresh(b, &options, &tip.generation, Reason::Repair);
        }
        if !local.value.complete && local.value.repair_from == tip.generation {
            return fresh(b, &options, &tip.generation, Reason::Repair);
        }
        if local.value.generation == tip.generation && fenced_out(&local.value, tip) {
            return Err(Error::ForeignAhead);
        }
        if local.value.generation == tip.generation && local.value.sequence < tip.sequence {
            if ours(&local.value, tip) {
                // Ons manifest gecommit, lokale marker onzeker gebleven: publiceer
                // de lokale bron opnieuw zonder afhankelijkheid van het oude dirty log.
                return fresh(b, &options, &tip.generation, Reason::NewSnapshot);
            }
            // Een ander publiceerde verder. Zonder eigen werk: diens stand
            // herstellen; met eigen werk: niet openen en niets aanraken.
            if local.value.complete && local.value.clean {
                return restored(b, store, &options, remote.ok_or(Error::State)?, &mut verify);
            }
            return Err(Error::ForeignAhead);
        }
        if !local.value.complete
            && local.value.previous.first().is_some_and(|old| {
                old.generation == tip.generation
                    && (old.sequence < tip.sequence || fenced_out(old, tip))
            })
        {
            // Een onderbroken generatiewissel valt terug op (of hervat vanaf)
            // de vorige generatie, maar daarin is een ander sindsdien verder.
            return Err(Error::ForeignAhead);
        }
        if !local.value.repair_from.is_empty() {
            return Err(Error::Unproven);
        }
        let previous_matches = !local.value.complete
            && local
                .value
                .previous
                .first()
                .is_some_and(|old| old.generation == tip.generation);
        if local.value.generation < tip.generation && !previous_matches {
            if local.value.complete && local.value.clean {
                return restored(b, store, &options, remote.ok_or(Error::State)?, &mut verify);
            }
            return Err(Error::Unproven);
        }
    }
    // Een onderbroken snapshot met een intacte spool en record wordt hervat:
    // de marker blijft bij die generatie. Het dirty log draagt de laatste
    // bevestigde commit (de vorige generatie, of niets vóór de eerste snapshot),
    // dus daar herstelt de tracking van; de paginagrootte komt uit het record
    // zolang de marker er nog geen heeft.
    let resumable = if local.value.complete {
        None
    } else {
        Batch::load_pending(b, options.path, &local)?.map(|(batch, _)| batch.page_size())
    };
    if resumable.is_some() {
        local.value.clean = false;
    }
    if !local.value.complete && resumable.is_none() {
        let previous = local.value.previous.first().filter(|old| {
            remote
                .as_ref()
                .is_some_and(|tip| old.generation == tip.generation && old.sequence == tip.sequence)
        });
        match previous {
            Some(old) => {
                local.value = old.duplicate()?;
                local.value.clean = false;
            }
            None => return fresh(b, &options, "", Reason::NewSnapshot),
        }
    }
    // Na de terugval naar de vorige generatie: het log draagt de laatste
    // bevestigde commit, bij een hervatte eerste snapshot nog niets.
    let (recover_generation, recover_sequence) = match (&resumable, local.value.previous.first()) {
        (Some(_), Some(old)) => (string(&old.generation)?, old.sequence),
        (Some(_), None) => (String::new(), 0),
        (None, _) => (string(&local.value.generation)?, local.value.sequence),
    };
    let mut tracking = Tracking::new(options.path, options.page_limit)?;
    let page_size = match resumable {
        Some(size) if local.value.page_size == 0 => size,
        _ => local.value.page_size,
    };
    match tracking.recover(b, &recover_generation, recover_sequence, page_size) {
        Ok(()) => {}
        Err(Error::Corrupt | Error::Limit) if local.value.clean => {
            tracking = Tracking::new(options.path, options.page_limit)?;
            tracking.reset_confirmed(
                b,
                &local.value.generation,
                local.value.sequence,
                local.value.page_size,
            )?;
        }
        Err(Error::Corrupt | Error::Limit) => {
            return fresh(b, &options, &local.value.generation, Reason::NewSnapshot);
        }
        Err(e) => return Err(e),
    }
    if !tracking.pending()?.is_empty() {
        local.value.clean = false;
    }
    if local.value.uncertain != 0 {
        replication::resolve(b, store, options.namespace, &mut local)?;
    }
    local.save(b)?;
    learn_counter(b, options.path, &mut tracking)?;
    Ok(Prepared {
        marker: local,
        tracking,
        reason: Reason::Continued,
    })
}
fn learn_counter<B: Storage>(b: &mut B, path: Name, tracking: &mut Tracking) -> Result {
    let mut header = [0u8; 28];
    let mut file = File::open(b, &path, false)?;
    file.read(0, &mut header)?;
    file.close()?;
    if &header[..16] != b"SQLite format 3\0" {
        return Err(Error::Corrupt);
    }
    tracking.source_counter = Some(u32::from_be_bytes([
        header[24], header[25], header[26], header[27],
    ]));
    Ok(())
}
