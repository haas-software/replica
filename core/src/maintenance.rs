//! Het bestaande uitdunningsschema en veilige verwijdering van verlopen generaties.
use crate::{
    Error, Result,
    compact::{self, Window},
    manifest::{Layout, MAX_COMMITS, Manifest},
    marker::{self, LocalMarker, Marker},
    object::{self, Store},
    replication, reserve,
    time::Time,
};
use alloc::vec::Vec;
use replica_sqlite::Storage;
/// Eén niveau: gehele seconden, vensters oplopend en onderling deelbaar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    /// Grootte van een gesloten venster.
    pub window: u64,
    /// Bewaartermijn op dit niveau; het hoogste niveau leeft met zijn generatie mee.
    pub keep: u64,
}
/// Gevalideerd schema, maximaal 32 niveaus.
pub struct Schedule {
    levels: Vec<Level>,
}
impl Schedule {
    /// Go's defaults: 15m:2h, 1h:24h, 24h:168h.
    pub fn defaults() -> Result<Self> {
        Self::new(&[
            Level {
                window: 900,
                keep: 7200,
            },
            Level {
                window: 3600,
                keep: 86400,
            },
            Level {
                window: 86400,
                keep: 604800,
            },
        ])
    }
    /// Alle vensters zijn ten minste één minuut en passen in Go's durationbereik.
    pub fn new(levels: &[Level]) -> Result<Self> {
        if levels.is_empty() || levels.len() > 32 {
            return Err(Error::Limit);
        }
        let mut previous = 0;
        for l in levels {
            if l.window < 60
                || l.keep < l.window
                || l.keep > i64::MAX as u64 / 1_000_000_000
                || (previous != 0 && (l.window <= previous || !l.window.is_multiple_of(previous)))
            {
                return Err(Error::State);
            }
            previous = l.window;
        }
        let mut out = Vec::new();
        reserve(&mut out, levels.len())?;
        out.extend_from_slice(levels);
        Ok(Self { levels: out })
    }
    /// Leest dezelfde window:keep-notatie als Go, met begrensde invoer.
    pub fn parse(text: &str) -> Result<Self> {
        if text.len() > 4096 {
            return Err(Error::Limit);
        }
        let text = text.trim();
        if text.is_empty() {
            return Self::defaults();
        }
        let mut levels = Vec::new();
        for item in text.split(',') {
            if levels.len() >= 32 {
                return Err(Error::Limit);
            }
            let (window, keep) = item.trim().split_once(':').ok_or(Error::State)?;
            let duration = |s: &str| -> Result<u64> {
                let n = hop_types::time::parse_duration(s.trim()).map_err(|_| Error::State)?;
                if !n.is_multiple_of(1_000_000_000) {
                    return Err(Error::State);
                }
                Ok(n / 1_000_000_000)
            };
            crate::grow(&mut levels, 1, 32)?;
            levels.push(Level {
                window: duration(window)?,
                keep: duration(keep)?,
            });
        }
        Self::new(&levels)
    }
    /// De ingestelde niveaus, in volgorde van fijn naar grof.
    pub fn levels(&self) -> &[Level] {
        &self.levels
    }
    /// Nog niet gepubliceerde gesloten vensters; grenspunten horen bij het venster ervoor.
    pub fn elapsed(&self, layout: &Layout, level: u32, now: Time) -> Result<Vec<(Time, Time)>> {
        let config = self
            .levels
            .get(level.checked_sub(1).ok_or(Error::State)? as usize)
            .ok_or(Error::State)?;
        let size = config.window as i64;
        let mut starts = Vec::new();
        reserve(&mut starts, layout.commits().len())?;
        let mut merged = Vec::new();
        reserve(&mut merged, layout.commits().len())?;
        merged.extend(
            layout
                .commits()
                .iter()
                .filter(|m| m.level == level)
                .map(|m| m.start),
        );
        merged.sort_unstable();
        // time.Time.Truncate is verankerd op jaar 0001, ook voor weekvensters.
        const ANCHOR: i64 = 62135596800;
        for m in layout.commits().iter().filter(|m| m.level + 1 == level) {
            let at = if level == 1 { m.at } else { m.end };
            let absolute = at.seconds() + ANCHOR;
            let mut start = absolute.div_euclid(size) * size - ANCHOR;
            if start == at.seconds() && at.nanos() == 0 {
                start -= size;
            }
            let start = Time::unix(start, 0)?;
            let end = Time::unix(start.seconds() + size, 0)?;
            if end <= now && merged.binary_search(&start).is_err() {
                starts.push(start);
            }
        }
        starts.sort_unstable();
        starts.dedup();
        let mut out = Vec::new();
        reserve(&mut out, starts.len())?;
        for start in starts {
            out.push((start, Time::unix(start.seconds() + size, 0)?));
        }
        Ok(out)
    }
}
/// Generaties vernieuwen op leeftijd, nooit alleen omdat veel bytes zijn verstuurd.
pub fn renewal_due(marker: &Marker, now: Time, generation_seconds: u64) -> Result<bool> {
    if now.seconds() < 1_577_836_800
        || generation_seconds == 0
        || generation_seconds > i64::MAX as u64
    {
        return Err(Error::State);
    }
    Ok(!marker.complete
        || marker.generation.is_empty()
        || now.seconds().saturating_sub(marker.started_at.seconds()) > generation_seconds as i64)
}
/// Meetlat van een onderhoudsronde.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    /// Gepubliceerde nieuwe vensters.
    pub merged: usize,
    /// Verwijderde fijnere manifesten; orphan data kan na een fout blijven bestaan.
    pub pruned: usize,
}
/// Geheugengrenzen voor één onderhoudsronde.
pub struct Budget {
    /// Maximale databasepagina's in de winnende-paginakaart.
    pub pages: u32,
    /// Paginabytes per uitvoersegment.
    pub segment_bytes: usize,
}
/// Eén actorronde; de app plant dit hoogstens eens per minuut en bewaart het tijdstip
/// ook na een fout, zodat een onbereikbare bucket geen hot retrylus veroorzaakt.
pub fn run<B: Storage, S: Store>(
    b: &mut B,
    store: &mut S,
    namespace: &str,
    local: &mut LocalMarker,
    schedule: &Schedule,
    now: Time,
    budget: Budget,
) -> Result<Report> {
    if now.seconds() < 1_577_836_800 {
        return Err(Error::State);
    }
    let prefix = replication::generation_prefix(namespace, &local.value.generation)?;
    let mut layout = object::layout(store, &prefix)?;
    let mut report = Report::default();
    for level in 1..=schedule.levels.len() as u32 {
        for (start, end) in schedule.elapsed(&layout, level, now)? {
            let mut inputs: Vec<&Manifest> = Vec::new();
            reserve(&mut inputs, layout.commits().len())?;
            for m in layout.commits().iter().filter(|m| m.level + 1 == level) {
                if (level == 1 && m.at > start && m.at <= end)
                    || (level > 1 && m.start >= start && m.end <= end)
                {
                    inputs.push(m);
                }
            }
            inputs.sort_unstable_by_key(|m| m.first);
            if inputs.is_empty() {
                continue;
            }
            let manifest = compact::merge(
                b,
                store,
                namespace,
                local,
                Window {
                    level,
                    start,
                    end,
                    page_limit: budget.pages,
                    segment_bytes: budget.segment_bytes,
                },
                &inputs,
            )?;
            layout.add_window(manifest, &prefix)?;
            report.merged += 1;
        }
    }
    let mut live = Vec::new();
    reserve(&mut live, layout.commits().len())?;
    live.extend(layout.commits());
    for old in layout.commits() {
        let expired = if old.level == 0 {
            old.at
                < Time::unix(
                    now.seconds() - schedule.levels[0].window as i64,
                    now.nanos(),
                )?
        } else if (old.level as usize) < schedule.levels.len() {
            old.end
                < Time::unix(
                    now.seconds() - schedule.levels[old.level as usize - 1].keep as i64,
                    now.nanos(),
                )?
        } else {
            false
        };
        if !expired {
            continue;
        }
        let Some(replacement) = layout.commits().iter().find(|m| {
            m.level == old.level + 1 && m.first <= old.first && m.sequence >= old.sequence
        }) else {
            continue;
        };
        let key = if old.level == 0 {
            replication::raw_key(&prefix, old.sequence, old.at)?
        } else {
            compact::window_key(&prefix, old.level, old.start, old.end)?
        };
        // Alleen overblijvende referenties beschermen delen; het te verwijderen
        // manifest zelf mag zijn eigen cleanup niet verhinderen.
        live.retain(|m| !core::ptr::eq(*m, old));
        compact::prune(store, &key, old, replacement, &live)?;
        report.pruned += 1;
    }
    Ok(report)
}
/// Verwijdert verlopen generaties in volgorde snapshot -> manifesten -> data.
/// Iedere fout stopt vóór verdere dependencies verdwijnen. Current wordt nooit gepruned.
pub fn generations<S: Store>(
    store: &mut S,
    namespace: &str,
    keep: &str,
    now: Time,
    retention_seconds: u64,
) -> Result<usize> {
    if now.seconds() < 1_577_836_800
        || retention_seconds == 0
        || retention_seconds > i64::MAX as u64
    {
        return Err(Error::State);
    }
    marker::generation_time(keep)?;
    let current = store.get(&object::key(namespace, "/current")?, 255)?;
    if core::str::from_utf8(&current)
        .map_err(|_| Error::Corrupt)?
        .trim()
        != keep
    {
        return Err(Error::Unproven);
    }
    let prefix = object::key(namespace, "/generations/")?;
    let mut objects = store.list(&prefix, MAX_COMMITS)?;
    if objects.len() > MAX_COMMITS {
        return Err(Error::Limit);
    }
    for object in &objects {
        if object.key.len() > 1024 || !object.key.starts_with(&prefix) {
            return Err(Error::Corrupt);
        }
    }
    objects.sort_unstable_by(|a, b| a.key.cmp(&b.key));
    let mut removed = 0;
    let mut first = 0;
    while first < objects.len() {
        let rest = objects[first]
            .key
            .strip_prefix(&prefix)
            .ok_or(Error::Corrupt)?;
        let Some((id, _)) = rest.split_once('/') else {
            first += 1;
            continue;
        };
        let generation = object::key(&prefix, &object::key(id, "/")?)?;
        let mut end = first + 1;
        while end < objects.len() && objects[end].key.starts_with(&generation) {
            end += 1;
        }
        let group = &objects[first..end];
        first = end;
        if id == keep {
            continue;
        }
        let Ok(created) = marker::generation_time(id) else {
            continue;
        };
        let complete = group
            .iter()
            .any(|o| o.key.strip_prefix(&generation) == Some("snapshot"));
        let expired = now.seconds().saturating_sub(created.seconds()) > retention_seconds as i64;
        let abandoned = !complete && now.seconds().saturating_sub(created.seconds()) > 86400;
        if !expired && !abandoned {
            continue;
        }
        // Drie passes sparen een tweede grote objectlijst uit.
        for rank in 0..3 {
            for o in group {
                let Some(rest) = o.key.strip_prefix(&generation) else {
                    continue;
                };
                let r = if rest == "snapshot" {
                    0
                } else if rest.starts_with("data/") {
                    2
                } else {
                    1
                };
                if r == rank {
                    store.delete(&o.key)?;
                    removed += 1;
                }
            }
        }
    }
    Ok(removed)
}
