//! De bestaande objectgrens: atomic replace, sterke GET/LIST-consistentie, één schrijver.
use crate::{
    Error, Result,
    manifest::{Layout, MAX_COMMITS, Manifest, Part},
    reserve, string,
};
use alloc::{collections::VecDeque, string::String, vec::Vec};
/// Fouten blijven herkenbaar; alleen Missing op een committed object bewijst schade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    /// GET vindt de sleutel niet; DELETE van een ontbrekend object mag slagen.
    Missing,
    /// Verbinding, timeout of onbekende PUT-uitkomst.
    Transport,
    /// Authenticatie/autorisatie; geen bewijs van ontbrekende data.
    Denied,
    /// Begrensde body of listing past niet in het gevraagde budget.
    Limit,
    /// De eigenaar heeft de operatie gestopt.
    Cancelled,
    /// Verplichte configuratie of een betrouwbare klok ontbreekt.
    Configuration,
    /// [`Store::put_new`]: de sleutel bestaat al (S3: 412 op `If-None-Match: *`).
    Exists,
}
impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Self::Object(e)
    }
}
/// Sleutel en grootte uit een begrensde lijstaanvraag.
pub struct Object {
    /// Volledige sleutel, geen URL.
    pub key: String,
    /// Grootte indien het listingtransport die levert; hashes valideren altijd GET.
    pub size: Option<u64>,
}
/// Hoeveel GETs een store tegelijk doet, en hoeveel bytes die samen mogen zijn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parallel {
    /// Gelijktijdige GETs; één is na elkaar.
    pub requests: usize,
    /// Geheugenbudget van één ronde; één deel past altijd.
    pub bytes: u64,
}
/// Antwoorden van [`Store::get_all`], in de volgorde van de sleutels.
pub type Replies = Vec<core::result::Result<Vec<u8>, StoreError>>;
/// Een objectstore voor één namespace-eigenaar.
///
/// Implementaties mogen op de private stack async I/O parkeren. Put vervangt
/// atomair; get/list moeten alle bevestigde writes en deletes direct zien.
/// Een fout na PUT is onbekend, nooit automatisch "niet geschreven".
pub trait Store {
    /// Vervangt één volledig object; onderdelen krijgen per poging verse sleutels.
    fn put(&mut self, key: &str, bytes: &[u8]) -> core::result::Result<(), StoreError>;
    /// Maakt één volledig object alleen aan als de sleutel nog niet bestaat,
    /// in één atomaire stap in de backend (S3: `If-None-Match: *`, zoals de
    /// lease). Bestaat hij al, dan [`StoreError::Exists`] en blijft het oude
    /// object staan. Nooit nadoen met een GET vóór de PUT: dan is het weer
    /// eerst kijken, dan doen, en precies dat gat sluit deze aanroep.
    fn put_new(&mut self, key: &str, bytes: &[u8]) -> core::result::Result<(), StoreError>;
    /// Weiger bodies boven `limit` vóór ze in hun geheel worden gealloceerd.
    fn get(&mut self, key: &str, limit: usize) -> core::result::Result<Vec<u8>, StoreError>;
    /// Hoeveel [`Store::get_all`] tegelijk mag ophalen; standaard één.
    fn parallel(&self) -> Parallel {
        Parallel {
            requests: 1,
            bytes: 0,
        }
    }
    /// Haalt hoogstens `parallel().requests` sleutels op, ieder met zijn eigen
    /// grens zoals [`Store::get`]. Ieder antwoord staat op de plaats van zijn
    /// sleutel; de buitenste fout geldt voor de hele ronde. Standaard na elkaar.
    fn get_all(&mut self, keys: &[(&str, usize)]) -> core::result::Result<Replies, StoreError> {
        let mut out = Vec::new();
        out.try_reserve_exact(keys.len())
            .map_err(|_| StoreError::Limit)?;
        for &(key, limit) in keys {
            out.push(self.get(key, limit));
        }
        Ok(out)
    }
    /// Het einde van een reeks [`Store::get_all`]-rondes: een store die er
    /// extra verbindingen voor opende, laat ze nu vallen in plaats van te
    /// wachten op zijn volgende call. [`crate::restore::stage`] roept dit aan
    /// zodra een herstel klaar is (ook als het faalt). Standaard niets.
    fn rest(&mut self) {}
    /// Duurzaam verwijderen; afwezig mag succes zijn.
    fn delete(&mut self, key: &str) -> core::result::Result<(), StoreError>;
    /// Alle pagina's onder deze prefix, maximaal `limit` objecten; nooit stil afkappen.
    fn list(&mut self, prefix: &str, limit: usize)
    -> core::result::Result<Vec<Object>, StoreError>;
    /// Directe kindprefixen; remote stores gebruiken een delimiterlisting.
    fn directories(
        &mut self,
        prefix: &str,
        limit: usize,
    ) -> core::result::Result<Vec<String>, StoreError> {
        let objects = self.list(prefix, MAX_COMMITS)?;
        let mut out = Vec::new();
        for object in objects {
            let rest = object
                .key
                .strip_prefix(prefix)
                .ok_or(StoreError::Transport)?;
            let Some(end) = rest.find('/') else { continue };
            let child = &object.key[..prefix.len() + end + 1];
            if out.iter().any(|s: &String| s == child) {
                continue;
            }
            if out.len() == limit {
                return Err(StoreError::Limit);
            }
            out.try_reserve(1).map_err(|_| StoreError::Limit)?;
            out.push(string(child).map_err(|_| StoreError::Limit)?);
        }
        Ok(out)
    }
    /// Eerste begrensde batch; afkappen is hier bedoeld voor hervatbare opruiming.
    fn list_batch(
        &mut self,
        prefix: &str,
        limit: usize,
    ) -> core::result::Result<Vec<Object>, StoreError> {
        if limit == 0 {
            return Err(StoreError::Limit);
        }
        let mut objects = self.list(prefix, MAX_COMMITS)?;
        objects.truncate(limit);
        Ok(objects)
    }
}
/// Begrensd samenvoegen, zodat sleutels niet tijdens publicatie onbegrensd groeien.
pub fn key(prefix: &str, suffix: &str) -> Result<String> {
    let n = prefix.len().checked_add(suffix.len()).ok_or(Error::Limit)?;
    if n > 1024 {
        return Err(Error::Limit);
    }
    let mut out = String::new();
    out.try_reserve_exact(n).map_err(|_| Error::Memory)?;
    out.push_str(prefix);
    out.push_str(suffix);
    Ok(out)
}
/// Laadt uitsluitend manifesten; miljoenen data-objecten horen niet in de layoutlisting.
pub fn layout<S: Store>(store: &mut S, prefix: &str) -> Result<Layout> {
    let snapshot = read_manifest(store, &key(prefix, "snapshot")?, prefix)?;
    let listing = store.list(&key(prefix, "L")?, MAX_COMMITS)?;
    if listing.len() > MAX_COMMITS {
        return Err(Error::Limit);
    }
    let mut commits = Vec::new();
    reserve(&mut commits, listing.len())?;
    let mut seen = Vec::new();
    reserve(&mut seen, listing.len())?;
    for object in &listing {
        let rest = object.key.strip_prefix(prefix).ok_or(Error::Corrupt)?;
        let raw = rest.starts_with("L0/") && rest.ends_with(".json");
        let window = rest.starts_with('L') && rest.ends_with("/complete");
        if !raw && !window {
            continue;
        }
        if object.key.len() > 1024 {
            return Err(Error::Limit);
        }
        seen.push(object.key.as_str());
        let m = read_manifest(store, &object.key, prefix)?;
        if (raw && m.level != 0) || (window && m.level == 0) {
            return Err(Error::Corrupt);
        }
        commits.push(m);
    }
    seen.sort_unstable();
    if seen.windows(2).any(|p| p[0] == p[1]) {
        return Err(Error::Corrupt);
    }
    Layout::new(snapshot, commits, prefix)
}
/// Een onvindbaar gecommit object is beschadiging, een transportfout blijft transport.
/// Een transportfout krijgt [`SEGMENT_RETRIES`] herkansingen: een committed object
/// verandert niet, dus opnieuw lezen is veilig.
pub fn committed<S: Store>(store: &mut S, key: &str, limit: usize) -> Result<Vec<u8>> {
    let reply = store.get(key, limit);
    settle(store, key, limit, reply)
}
/// Het oordeel over één antwoord op een committed object, met herkansingen na
/// een transportfout: een gestroomde body die halverwege breekt, komt zo terug.
fn settle<S: Store>(
    store: &mut S,
    key: &str,
    limit: usize,
    mut reply: core::result::Result<Vec<u8>, StoreError>,
) -> Result<Vec<u8>> {
    for _ in 0..SEGMENT_RETRIES {
        if reply != Err(StoreError::Transport) {
            break;
        }
        reply = store.get(key, limit);
    }
    match reply {
        Ok(bytes) if bytes.len() > limit => Err(Error::Limit),
        Ok(bytes) => Ok(bytes),
        Err(StoreError::Missing) => Err(Error::Corrupt),
        Err(e) => Err(e.into()),
    }
}
/// Haalt de committed delen van een herstel- of compactieplan vooruit op, in
/// rondes binnen [`Store::parallel`], en geeft ze in planvolgorde terug.
/// Toepassen blijft dus volgordelijk: een later deel overschrijft nooit een
/// eerder. Een fout komt pas terug bij het deel waar hij hoort.
pub(crate) struct Prefetch<'a> {
    parts: &'a [&'a Part],
    next: usize,
    ready: VecDeque<(usize, core::result::Result<Vec<u8>, StoreError>)>,
}
/// Zo vaak wordt een committed object opnieuw gelezen na een transportfout.
/// Een gestroomde body die halverwege breekt, komt als transportfout terug;
/// een committed segment is idempotent en wordt op zijn hash getoetst, dus
/// opnieuw ophalen is veilig en scheelt een heel herstel.
pub(crate) const SEGMENT_RETRIES: usize = 3;
impl<'a> Prefetch<'a> {
    pub(crate) fn new(parts: &'a [&'a Part]) -> Self {
        Self {
            parts,
            next: 0,
            ready: VecDeque::new(),
        }
    }
    /// De bytes van het volgende deel, zoals [`committed`] ze geeft.
    pub(crate) fn next<S: Store>(&mut self, store: &mut S) -> Result<Vec<u8>> {
        if self.ready.is_empty() {
            self.fill(store)?;
        }
        let (index, reply) = self.ready.pop_front().ok_or(Error::State)?;
        let part = self.parts.get(index).ok_or(Error::State)?;
        let limit = usize::try_from(part.size).map_err(|_| Error::Limit)?;
        settle(store, &part.key, limit, reply)
    }
    fn fill<S: Store>(&mut self, store: &mut S) -> Result {
        let parallel = store.parallel();
        let mut keys = Vec::new();
        let mut bytes = 0u64;
        for part in &self.parts[self.next..] {
            if keys.len() >= parallel.requests.max(1)
                || (!keys.is_empty() && bytes.saturating_add(part.size) > parallel.bytes)
            {
                break;
            }
            let limit = usize::try_from(part.size).map_err(|_| Error::Limit)?;
            reserve(&mut keys, 1)?;
            keys.push((part.key.as_str(), limit));
            bytes = bytes.saturating_add(part.size);
        }
        if keys.is_empty() {
            return Err(Error::State);
        }
        let replies = store.get_all(&keys)?;
        if replies.len() != keys.len() {
            return Err(Error::State);
        }
        self.ready
            .try_reserve(replies.len())
            .map_err(|_| Error::Memory)?;
        for (offset, reply) in replies.into_iter().enumerate() {
            self.ready.push_back((self.next + offset, reply));
        }
        self.next += keys.len();
        Ok(())
    }
}
pub(crate) fn read_manifest<S: Store>(store: &mut S, key: &str, prefix: &str) -> Result<Manifest> {
    Manifest::decode(
        &committed(store, key, crate::manifest::MAX_MANIFEST_BYTES)?,
        prefix,
    )
}
/// Publiceert het manifest nadat de aanroeper alle onderdelen heeft bevestigd.
/// Verloren PUT-antwoorden worden alleen met exact gelijk teruggelezen bytes opgelost.
/// De lokale marker moet vóór deze aanroep de onzekere sequence bewaren.
///
/// Eerst alleen aanmaken ([`Store::put_new`]). Staat de sleutel er al, dan
/// beslist het fencing-token ([`Manifest::lease`]): een hoger token is van een
/// nieuwere schrijver en dan is dit [`Error::LeaseLost`]; een gelijk of lager
/// token (onze eigen eerdere poging, zoals een snapshot die opnieuw gaat
/// omdat current nog niet stond) wordt vervangen. Zo overschrijft een oude
/// schrijver nooit een vaste sleutel (`snapshot`, een venster) van een nieuwe.
pub fn publish<S: Store>(store: &mut S, key: &str, manifest: &Manifest, prefix: &str) -> Result {
    let bytes = manifest.encode(prefix)?;
    let original = match store.put_new(key, &bytes) {
        Ok(()) => return Ok(()),
        Err(StoreError::Exists) => match store.get(key, crate::manifest::MAX_MANIFEST_BYTES) {
            Ok(found) if found == bytes => return Ok(()),
            Ok(found) => {
                let lease = Manifest::decode(&found, prefix).map_or(0, |m| m.lease);
                if lease > manifest.lease {
                    return Err(Error::LeaseLost);
                }
                match store.put(key, &bytes) {
                    Ok(()) => return Ok(()),
                    Err(e) => e,
                }
            }
            Err(e) => return Err(e.into()),
        },
        Err(e) => e,
    };
    match store.get(key, bytes.len()) {
        Ok(found) if found == bytes => Ok(()),
        _ => Err(original.into()),
    }
}
/// Gevalideerde kopie van een sleutel, bruikbaar bij een aparte publicatiefase.
pub fn owned_key(value: &str) -> Result<String> {
    if value.len() > 1024 || value.is_empty() {
        return Err(Error::Limit);
    }
    string(value)
}
