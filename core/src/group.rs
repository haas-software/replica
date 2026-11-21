//! Meerdere databases onder één schrijverlease: een groep.
//!
//! Een app met meer dan één database per domein (EasyFlor: per tenant
//! `main.db` en een bestand per jaar) repliceert elke database als een eigen
//! namespace, `<prefix>/<domain>/<file>`: snapshots en incrementen van zijn
//! pagina's, een `current`-generatie. Eén schrijverlease per domein
//! (`<prefix>/<domain>/lease`) wordt gedeeld door de eigenaars van de
//! databases ([`Replica::prepare_shared`]); zijn generatie is het
//! fencing-token in de store. De host claimt hem ([`crate::writer::claim`])
//! vóór hij iets ophaalt, en geeft hem de groep.
//!
//! Wat de groep doet, en wat elke host met meer databases anders zelf moest:
//!
//! - **Prepare per database** ([`Group::prepare`]): herstel uit de store of
//!   voortzetten van de lokale kopie, de eerste keer dat de host hem opent
//!   (lui per database). Tijdens een lang herstel wordt de lease vernieuwd
//!   vóór elke store-call en tijdens de integriteitscontrole ([`Ticking`]),
//!   en gaan de GET's in rondes die zich aanpassen aan hun duur: een herstel
//!   langer dan twee derde lease verloor anders de lease bij de volgende
//!   database (EasyFlor, 16-11-2026, 256 MiB op QEMU).
//! - **De lease bijhouden** ([`Group::keep_lease`]): hoogstens eens per
//!   seconde gekeken, vernieuwd op leeftijd, ook uit de progresscallback van
//!   een lang statement. Voorbij de schrijfdeadline maar vóór het verlopen
//!   wachten writes ([`Standing::Held`]) en wordt de lease heroverd zodra de
//!   store antwoordt ([`crate::writer::renew_or_reclaim`]); een andere houder
//!   of een verlopen lease is verlies ([`Standing::Lost`]).
//! - **Publiceren in rust** ([`Group::rest`], [`Group::flush`]): elke database
//!   op zijn eigen interval, alles vóór het sluiten. Een store die niet
//!   antwoordt is geen verloren lease: de stap wordt gemeld en na een backoff
//!   opnieuw geprobeerd (1, 2, 4 s, dan elke zesde lease), de ronde stopt bij
//!   de eerste zo'n fout, de host bedient door; de wijzigingen blijven in de
//!   dirty logs tot een publicatie lukt ([`Report::recovered`]).
//! - **Banen** ([`Group::with_offload`], zie [`crate::lanes`]): de uploads,
//!   het onderhoud en de lease buiten de stack van de eigenaar. De eigenaar
//!   doet in rust alleen de capture (lokaal) en het manifest na een gelande
//!   upload (één kleine PUT, en alleen als een baan de store net hoorde).
//! - **Welke writes de store heeft** ([`Publications`]): de nummers die de
//!   eigenaar van elke database zijn captures geeft ([`Replica::begun`],
//!   [`Synced::capture`]), gedeeld met wie wacht.
//! - **Vrijgeven** ([`release`]): de lease terug bij het sluiten, ook na een
//!   eerste DELETE die op een stale verbinding faalt, en bij naam als een
//!   vernieuwing onderweg de [`Writer`] nog deelt.
//!
//! Niet atomair over databases heen: elke database publiceert op zijn eigen
//! moment; een herstel kan databases van verschillende momenten combineren.
//!
//! De groep leent niets over een `.await`: hij leeft op de stack van zijn
//! eigenaar, de SQL-kant (de [`Vfs`] per database) en de banen delen alleen
//! `RefCell`'s die per stap kort geleend worden (het rustprotocol van
//! [`crate::owner`]). De host meldt wat er gebeurt via [`Report`].
use crate::lanes::{DynStorage, DynStore, Job, Lane, LaneSide, Offload};
use crate::local::Name;
use crate::object::{Object, Parallel, Replies, Store, StoreError};
use crate::owner::{Config as ReplicaConfig, Maintenance, Pending, Replica, Synced, Vfs};
use crate::prepare::Reason;
use crate::publications::Publications;
use crate::replication::Uploaded;
use crate::time::Time;
use crate::writer::{self, Backend, Held, Writer};
use crate::{Error, Result, marker};
use alloc::{boxed::Box, format, rc::Rc, string::String, vec::Vec};
use core::cell::{Cell, RefCell};
use core::ffi::CStr;
use replica_sqlite::{FileId, OpenFlags, Storage};
/// De objectstore van een groep met de hop-lease erover (S3, of een map op
/// een host).
pub trait Bucket: Store {
    /// Het leaseobject, dat de store leent.
    type Lease<'l>: Backend
    where
        Self: 'l;
    /// De lease op `key`.
    fn lease<'l>(
        &'l mut self,
        key: &'l str,
        ttl_ms: u64,
    ) -> core::result::Result<Self::Lease<'l>, StoreError>;
    /// Endpoint en bucket: met prefix en database de bestemmingshash.
    fn identity(&self) -> (String, String);
}
/// Hoe de databases van een groep repliceren.
#[derive(Clone, Debug)]
pub struct Config {
    /// De sleutelprefix in de bucket.
    pub prefix: String,
    /// Het domein onder de prefix (een tenant, een klant).
    pub domain: String,
    /// De schrijverlease (minstens [`writer::MIN_TTL_MS`]).
    pub ttl_ms: u64,
    /// Het meeste aantal pagina's dat één database mag hebben (trackingbudget).
    pub pages: u32,
    /// Seconden tussen twee publicaties van één database.
    pub interval: u32,
    /// Gecomprimeerde segmenten (`SPINSEG2`).
    pub compress: bool,
}
impl Config {
    /// De lease-sleutel: `<prefix>/<domain>/lease`.
    pub fn lease_key(&self) -> String {
        format!("{}/{}/lease", self.prefix, self.domain)
    }
    /// De namespace van `file`: `<prefix>/<domain>/<file>`.
    pub fn namespace(&self, file: &str) -> String {
        format!("{}/{}/{file}", self.prefix, self.domain)
    }
    /// De bestemmingshash van `file` bij `endpoint` en `bucket`.
    pub fn destination(&self, endpoint: &str, bucket: &str, file: &str) -> Result<String> {
        let domain = format!("{}/{file}", self.domain);
        marker::destination(endpoint, bucket, &self.prefix, &domain)
    }
}
/// Waarom een store-stap van de schrijver mislukte: wat beslist tussen
/// opnieuw proberen en fencen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    /// De store antwoordde niet (time-out, verbinding, 5xx): na een backoff
    /// opnieuw. De host bedient door; zijn writes zijn lokaal duurzaam en
    /// gaan de store in als die weer antwoordt.
    Transport,
    /// `LeaseLost` terwijl niemand anders de lease houdt: de schrijfdeadline
    /// (twee derde lease) is voorbij omdat de vernieuwingen mislukten, of
    /// omdat een ronde publicaties zo lang duurde. Writes wachten
    /// ([`Standing::Held`]); de lease wordt heroverd als de store weer antwoordt.
    Deadline,
    /// Een ander houdt de lease: een voorwaardelijke schrijf verloor, of de
    /// lease noemt een andere eigenaar of generatie. Fencen.
    Taken,
    /// De lease is verlopen (de termijn die wij het laatst schreven) zonder
    /// vernieuwing: een andere node mag hem vanaf nu nemen. Fencen.
    Expired,
    /// Al het andere (een beschadigd bestand, een limiet): gemeld, de host gaat door.
    Other,
}
impl Failure {
    /// De naam in een logregel (`transport`, `deadline`, ...).
    pub fn name(self) -> &'static str {
        match self {
            Failure::Transport => "transport",
            Failure::Deadline => "deadline",
            Failure::Taken => "taken",
            Failure::Expired => "expired",
            Failure::Other => "other",
        }
    }
}
/// Waar de schrijverlease staat voor de verbinding van de host.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Standing {
    /// Writes gaan door.
    Valid,
    /// Voorbij de schrijfdeadline, vóór het verlopen: lezen gaat door, writes
    /// wachten (Retry-After in ms).
    Held(u64),
    /// Overgenomen of verlopen: fencen.
    Lost,
}
/// Wat een groep de host meldt (zijn log). Elke methode krijgt de database
/// (`lease` voor de lease zelf) en de stap (`renew`, `sync`, `upload`,
/// `capture`, `maintain`, `prepare`).
pub trait Report {
    /// Een store-stap mislukte en wordt na `next_ms` opnieuw geprobeerd; de
    /// `retry`-de fout op rij.
    fn retry(&self, file: &str, step: &str, error: Error, cause: Failure, retry: u32, next_ms: i64);
    /// De store antwoordt weer na `retries` fouten in `ms` milliseconden: de
    /// lease is vernieuwd en elke database die faalde heeft sindsdien gepubliceerd.
    fn recovered(&self, retries: u32, ms: i64);
    /// De lease is overgenomen of verlopen (eenmaal per groep); `error` is
    /// [`Error::LeaseLost`] als geen store-stap de oorzaak was.
    fn lost(&self, file: &str, step: &str, error: Error, cause: Failure, retries: u32);
    /// Een andere fout van `step` (een beschadigd bestand, een limiet).
    fn failed(&self, file: &str, step: &str, error: Error);
    /// Een capture van `file` is gepubliceerd.
    fn published(&self, file: &str);
}
/// Controleert een herstelde scratchdatabase vóór die iets vervangt (de
/// callback van Prepare). Het derde argument roept de controle aan terwijl
/// hij loopt (bijvoorbeeld bij elke pagina die hij leest, via [`Ticking`]):
/// een `quick_check` van een grote database duurt minuten zonder één
/// store-call, dus de lease wordt van daaruit vernieuwd.
pub type Verify<'e, D> = dyn Fn(&mut D, &Name, &dyn Fn()) -> Result + 'e;
/// Een opslag die `tick` aanroept bij elke read en write en telkens als
/// SQLite de eigenaar laat ademen (`cooperate`).
pub struct Ticking<'a, D> {
    /// De opslag eronder.
    pub inner: &'a mut D,
    /// Wordt bij elke read, write en `cooperate` aangeroepen; houd hem goedkoop.
    pub tick: &'a dyn Fn(),
}
impl<D: Storage> Storage for Ticking<'_, D> {
    fn cooperate(&mut self) -> replica_sqlite::Result {
        (self.tick)();
        self.inner.cooperate()
    }
    fn open(&mut self, name: &CStr, flags: OpenFlags) -> replica_sqlite::Result<FileId> {
        self.inner.open(name, flags)
    }
    fn close(&mut self, file: FileId) -> replica_sqlite::Result {
        self.inner.close(file)
    }
    fn read(&mut self, file: FileId, offset: u64, dst: &mut [u8]) -> replica_sqlite::Result<usize> {
        // `quick_check` is één SQLite-stap (`OP_IntegrityCk`): de progresscallback
        // loopt niet terwijl hij elke pagina leest, dus tikken de reads ook.
        (self.tick)();
        self.inner.read(file, offset, dst)
    }
    fn write(&mut self, file: FileId, offset: u64, src: &[u8]) -> replica_sqlite::Result {
        (self.tick)();
        self.inner.write(file, offset, src)
    }
    fn truncate(&mut self, file: FileId, size: u64) -> replica_sqlite::Result {
        self.inner.truncate(file, size)
    }
    fn sync(&mut self, file: FileId, flags: i32) -> replica_sqlite::Result {
        self.inner.sync(file, flags)
    }
    fn size(&mut self, file: FileId) -> replica_sqlite::Result<u64> {
        self.inner.size(file)
    }
    fn remove(&mut self, name: &CStr, sync_directory: bool) -> replica_sqlite::Result {
        self.inner.remove(name, sync_directory)
    }
    fn exists(&mut self, name: &CStr) -> replica_sqlite::Result<bool> {
        self.inner.exists(name)
    }
    fn random(&mut self, dst: &mut [u8]) -> replica_sqlite::Result {
        self.inner.random(dst)
    }
    fn unix_millis(&mut self) -> replica_sqlite::Result<i64> {
        self.inner.unix_millis()
    }
}
/// Wat een [`Lane::Data`]-job van één database terugbrengt naar de eigenaar.
#[allow(
    clippy::large_enum_variant,
    reason = "één per database onderweg, één keer verplaatst: een box voegt alleen een allocatie toe"
)]
enum Landed {
    /// De delen van een capture staan er (of niet): de eigenaar schrijft het
    /// manifest ([`Replica::finish`]).
    Upload(Pending, Result<Uploaded>),
    /// Een onderhoudsbeurt liep ([`Replica::maintenance_finish`]).
    Maintained(Result<Synced>),
}
/// Het antwoordvak van een job: `None` zolang hij loopt.
type Slot<T> = Rc<RefCell<Option<T>>>;
/// Eén database van de groep.
struct Member<D> {
    name: String,
    replica: Replica,
    /// Het eigen handvat van de sync-kant op de map.
    sync: D,
    /// Een [`Lane::Data`]-job van deze database die de eigenaar nog niet
    /// terugnam (een upload of een onderhoudsbeurt; hoogstens één).
    flight: Option<Slot<Landed>>,
}
/// De store tijdens een herstel in [`Replica::prepare_shared`] of een
/// publicatie: vóór elke call vernieuwt hij de lease als die aan de beurt is.
///
/// Replica herstelt in rondes: tot `parallel().requests` GET's tegelijk, dan
/// elk deel uitpakken, hashen en schrijven, dan de volgende ronde. Binnen een
/// ronde wordt niets vernieuwd, dus haar duur beslist: bij een herstel
/// (`adapt`) beginnen de rondes bij één GET en verdubbelen ze zolang een hele
/// ronde korter duurt dan een vierentwintigste lease, tot het eigen aantal
/// van de store, en halveren ze als er een langer duurt dan een zesde. Alleen
/// een enkel segment dat langer duurt dan de marge van de lease kan hem dan
/// nog kosten. Een publicatie gebruikt het eigen aantal van de store.
struct Renewing<'a, S: Bucket> {
    /// Per call geleend, zodat de controle tussendoor kan vernieuwen.
    store: &'a RefCell<S>,
    /// Vernieuwt de lease als die aan de beurt is ([`Group::keep_lease`]).
    tick: &'a dyn Fn(),
    ttl_ms: u64,
    /// Unix-milliseconden, voor de duur van een ronde.
    millis: fn() -> i64,
    /// Rondes die zich aan hun duur aanpassen (een herstel).
    adapt: bool,
    /// GET's in de volgende ronde.
    requests: usize,
    /// Wanneer de vorige ronde begon (Unix-ms).
    round_at: Option<i64>,
}
impl<S: Bucket> Renewing<'_, S> {
    fn renew(&mut self) {
        (self.tick)();
    }
}
impl<S: Bucket> Store for Renewing<'_, S> {
    fn put(&mut self, key: &str, bytes: &[u8]) -> core::result::Result<(), StoreError> {
        self.renew();
        self.store.borrow_mut().put(key, bytes)
    }
    fn get(&mut self, key: &str, limit: usize) -> core::result::Result<Vec<u8>, StoreError> {
        self.renew();
        self.store.borrow_mut().get(key, limit)
    }
    fn parallel(&self) -> Parallel {
        let own = self.store.borrow().parallel();
        if !self.adapt {
            return own;
        }
        Parallel {
            requests: self.requests,
            ..own
        }
    }
    fn get_all(&mut self, keys: &[(&str, usize)]) -> core::result::Result<Replies, StoreError> {
        if !self.adapt {
            self.renew();
            return self.store.borrow_mut().get_all(keys);
        }
        let now = (self.millis)();
        if let Some(at) = self.round_at {
            let round = now.saturating_sub(at);
            let slow = i64::try_from(self.ttl_ms / 6).unwrap_or(i64::MAX);
            let most = self.store.borrow().parallel().requests.max(1);
            self.requests = if round > slow {
                (self.requests / 2).max(1)
            } else if round < slow / 4 {
                (self.requests * 2).min(most)
            } else {
                self.requests
            };
        }
        self.round_at = Some(now);
        self.renew();
        self.store.borrow_mut().get_all(keys)
    }
    fn delete(&mut self, key: &str) -> core::result::Result<(), StoreError> {
        self.renew();
        self.store.borrow_mut().delete(key)
    }
    fn list(
        &mut self,
        prefix: &str,
        limit: usize,
    ) -> core::result::Result<Vec<Object>, StoreError> {
        self.renew();
        self.store.borrow_mut().list(prefix, limit)
    }
    fn directories(
        &mut self,
        prefix: &str,
        limit: usize,
    ) -> core::result::Result<Vec<String>, StoreError> {
        self.renew();
        self.store.borrow_mut().directories(prefix, limit)
    }
    fn list_batch(
        &mut self,
        prefix: &str,
        limit: usize,
    ) -> core::result::Result<Vec<Object>, StoreError> {
        self.renew();
        self.store.borrow_mut().list_batch(prefix, limit)
    }
}
/// Hoe lang een flush op de banen wacht voordat hij de rest op de eigenaar
/// publiceert (ms). Een stop op HopOS heeft in totaal 2,5 s: een upload die
/// dan nog loopt, blijft in de dirty log (en zijn pending-record) voor de
/// volgende eigenaar.
pub const SETTLE_MS: u64 = 1_500;
/// Hoe lang een sluiting op de banen wacht voordat hij de lease vrijgeeft
/// (ms): een vernieuwing onderweg houdt de Writer; zonder hem gaat de lease
/// bij naam terug ([`Held::Named`]).
pub const CLOSE_SETTLE_MS: u64 = 500;
/// De store antwoordde een baan binnen zo lang (ms): hij antwoordt.
const ANSWERED_MS: i64 = 10_000;
/// De langste backoff tussen twee pogingen na een fout: een zesde lease, zodat
/// een store die weer antwoordt binnen een paar seconden gebruikt wordt, ruim
/// vóór de lease verloopt.
fn backoff_ms(retries: u32, ttl_ms: u64) -> i64 {
    let most = i64::try_from(ttl_ms / 6).unwrap_or(i64::MAX).max(1000);
    let shift = retries.saturating_sub(1).min(16);
    (1000_i64 << shift).min(most)
}
/// `ms` (Unix-milliseconden) als de tijd van replica.
pub fn time_at(ms: i64) -> Result<Time> {
    Time::unix(
        ms.div_euclid(1000),
        u32::try_from(ms.rem_euclid(1000) * 1_000_000).unwrap_or(0),
    )
}
/// De databases van één domein onder één schrijverlease (zie de moduledocs).
pub struct Group<'h, D: Storage, S: Bucket> {
    config: Config,
    store: Rc<RefCell<S>>,
    millis: fn() -> i64,
    report: Box<dyn Report + 'h>,
    writer: Rc<RefCell<Writer>>,
    files: RefCell<Vec<Member<D>>>,
    /// Wat de store per database bevestigde.
    publications: Rc<Publications>,
    /// Wanneer [`Group::keep_lease`] het laatst naar de lease keek (Unix-ms).
    lease_tried: Cell<i64>,
    /// Store-stappen (vernieuwingen, publicaties) die op rij mislukten en
    /// opnieuw geprobeerd worden; 0 zolang de store antwoordt.
    retries: Cell<u32>,
    /// Geen store-stap vóór dit moment (Unix-ms): de backoff na een fout.
    retry_at: Cell<i64>,
    /// Wanneer de eerste van die fouten gebeurde (Unix-ms).
    failing_since: Cell<i64>,
    /// De laatste vernieuwing van de lease mislukte.
    lease_failing: Cell<bool>,
    /// De databases waarvan de laatste publicatie mislukte.
    files_failing: RefCell<Vec<String>>,
    /// De lease is overgenomen of verlopen, en dat is gemeld.
    fenced: Cell<bool>,
    /// Waar het store-werk buiten de stack van de eigenaar loopt; `None`: erop.
    offload: Option<&'h dyn Offload>,
    /// Een [`Lane::Lease`]-vernieuwing die de eigenaar nog niet terugnam.
    lease_flight: RefCell<Option<Slot<Result>>>,
    /// Wanneer een baan de store het laatst hoorde antwoorden (Unix-ms): de
    /// eigenaar roept de store in rust (een onzeker manifest) alleen aan
    /// zolang die antwoordt.
    answered_at: Cell<i64>,
}
impl<'h, D: Storage, S: Bucket> Group<'h, D, S> {
    /// De groep van `config.domain` met de geclaimde lease `writer`
    /// ([`crate::writer::claim`]), op `store`. `millis` is de wandklok
    /// (Unix-milliseconden), `report` hoort wat er gebeurt.
    pub fn new(
        config: Config,
        store: Rc<RefCell<S>>,
        writer: Rc<RefCell<Writer>>,
        millis: fn() -> i64,
        report: Box<dyn Report + 'h>,
    ) -> Self {
        Group {
            config,
            store,
            millis,
            report,
            writer,
            files: RefCell::new(Vec::new()),
            publications: Rc::new(Publications::new()),
            lease_tried: Cell::new(i64::MIN),
            retries: Cell::new(0),
            retry_at: Cell::new(i64::MIN),
            failing_since: Cell::new(0),
            lease_failing: Cell::new(false),
            files_failing: RefCell::new(Vec::new()),
            fenced: Cell::new(false),
            offload: None,
            lease_flight: RefCell::new(None),
            answered_at: Cell::new(i64::MIN),
        }
    }
    /// De uploads, het onderhoud en de lease lopen op de banen van `offload`
    /// (zie [`crate::lanes`]); zonder op de eigenaar.
    #[must_use]
    pub fn with_offload(mut self, offload: &'h dyn Offload) -> Self {
        self.offload = Some(offload);
        self
    }
    /// De publicaties in `publications` bijhouden (gedeeld met wie wacht)
    /// in plaats van in een eigen [`Publications`].
    #[must_use]
    pub fn with_publications(mut self, publications: Rc<Publications>) -> Self {
        self.publications = publications;
        self
    }
    /// Wat de store per database bevestigde.
    pub fn publications(&self) -> &Rc<Publications> {
        &self.publications
    }
    /// De configuratie.
    pub fn config(&self) -> &Config {
        &self.config
    }
    /// De lease van de groep.
    pub fn writer(&self) -> &Rc<RefCell<Writer>> {
        &self.writer
    }
    fn now(&self) -> Result<Time> {
        time_at((self.millis)())
    }
    /// Is `file` al voorbereid?
    pub fn has(&self, file: &str) -> bool {
        self.files.borrow().iter().any(|f| f.name == file)
    }
    /// Heeft een database iets te publiceren ([`Replica::has_changes`])?
    pub fn has_changes(&self) -> bool {
        self.files.borrow().iter().any(|f| f.replica.has_changes())
    }
    /// De getrackte VFS van `file` over `b` ([`Replica::vfs`]).
    pub fn vfs<'a, B: Storage>(&self, file: &str, b: &'a mut B) -> Result<Vfs<'a, B>> {
        let files = self.files.borrow();
        let member = files.iter().find(|f| f.name == file).ok_or(Error::State)?;
        member.replica.vfs(b)
    }
    /// Eén vernieuwing (of herovering) van de lease, nu, op de store van de eigenaar.
    fn renew(&self) -> Result {
        let now = self.now()?;
        let mut store = self.store.try_borrow_mut().map_err(|_| Error::Busy)?;
        let key = self.config.lease_key();
        let mut lease = store
            .lease(&key, self.config.ttl_ms)
            .map_err(Error::Object)?;
        writer::renew_or_reclaim(&self.writer, &mut lease, now)
    }
    /// Het eind van de lease zoals wij hem het laatst schreven (Unix-ms): de
    /// schrijfdeadline plus de marge van een derde termijn.
    fn expiry(&self) -> Option<i64> {
        let w = self.writer.try_borrow().ok()?;
        let end = w.deadline().saturating_add(self.config.ttl_ms / 3);
        Some(i64::try_from(end).unwrap_or(i64::MAX))
    }
    fn is_lost(&self) -> bool {
        self.writer.try_borrow().is_ok_and(|w| w.is_lost())
    }
    /// Waar de lease nu staat (zie [`Standing`]).
    pub fn standing(&self) -> Standing {
        let at = (self.millis)();
        let Ok(w) = self.writer.try_borrow() else {
            // Alleen geleend voor de boekhouding van een vernieuwing: zoals het was.
            return Standing::Valid;
        };
        if w.is_lost() || self.expiry().is_some_and(|end| at >= end) {
            return Standing::Lost;
        }
        let valid = self.now().is_ok_and(|now| w.check(now).is_ok());
        if valid {
            Standing::Valid
        } else {
            let wait = self.retry_at.get().saturating_sub(at).max(1000);
            Standing::Held(u64::try_from(wait).unwrap_or(1000))
        }
    }
    /// Wat een mislukte store-stap van de schrijver betekent (zie [`Failure`]).
    fn classify(&self, e: &Error) -> Failure {
        if self.is_lost() {
            return Failure::Taken;
        }
        let expired = self.expiry().is_some_and(|end| (self.millis)() >= end);
        match e {
            Error::LeaseLost | Error::Object(_) if expired => Failure::Expired,
            Error::LeaseLost => Failure::Deadline,
            Error::Object(_) => Failure::Transport,
            _ => Failure::Other,
        }
    }
    /// Een store-stap die mislukte en opnieuw geprobeerd wordt: de backoff en
    /// [`Report::retry`]. De lease na de backoff; de publicatie van een
    /// database ook na zijn eigen interval (een mislukte poging krijgt het
    /// interval ook), dus `next_ms` is voor een database het latere van beide.
    fn retry(&self, file: &str, step: &str, e: Error, failure: Failure) {
        let at = (self.millis)();
        let n = self.retries.get().saturating_add(1);
        if n == 1 {
            self.failing_since.set(at);
        }
        self.retries.set(n);
        let mut backoff = backoff_ms(n, self.config.ttl_ms);
        if matches!(self.standing(), Standing::Held(_)) {
            // Writes wachten: de lease snel terug zodra de store antwoordt.
            backoff = backoff.min(2000);
        }
        self.retry_at.set(at.saturating_add(backoff));
        let next = if file == "lease" {
            self.lease_failing.set(true);
            backoff
        } else {
            let mut failing = self.files_failing.borrow_mut();
            if !failing.iter().any(|f| f == file) {
                failing.push(String::from(file));
            }
            backoff.max(i64::from(self.config.interval) * 1000)
        };
        self.report.retry(file, step, e, failure, n, next);
    }
    /// De store antwoordt weer na fouten: als de lease vernieuwd is en elke
    /// database die faalde sindsdien publiceerde (of niets te publiceren had),
    /// [`Report::recovered`].
    fn recovered(&self) {
        if self.retries.get() == 0
            || self.lease_failing.get()
            || !self.files_failing.borrow().is_empty()
        {
            return;
        }
        let n = self.retries.replace(0);
        self.retry_at.set(i64::MIN);
        let ms = (self.millis)().saturating_sub(self.failing_since.get());
        self.report.recovered(n, ms);
    }
    /// De lease is overgenomen of verlopen: eenmaal gemeld, dan
    /// [`Error::LeaseLost`] (de host fencet).
    fn lost(&self, file: &str, step: &str, e: Option<Error>, failure: Failure) -> Error {
        if !self.fenced.replace(true) {
            let e = e.unwrap_or(Error::LeaseLost);
            self.report.lost(file, step, e, failure, self.retries.get());
        }
        Error::LeaseLost
    }
    /// Meldt een fout en geeft hem terug.
    fn failed(&self, file: &str, step: &str, e: Error) -> Error {
        self.report.failed(file, step, e);
        e
    }
    /// Vernieuwt de lease als die aan de beurt is (eens per zesde termijn),
    /// hoogstens eens per seconde bekeken: de host roept dit waar SQLite lang
    /// loopt tussen twee rustmomenten (de progresscallback van een verzoek),
    /// dus het kost een klokleesbeurt. De schrijfdeadline ligt twee derde
    /// termijn na de laatste vernieuwing (20 s van 30); een verzoek mag langer
    /// lopen, en rust komt pas tussen verzoeken. Na een fout wacht dit de
    /// backoff af, zodat een store die niet antwoordt niet elk verzoek bij
    /// elke controle ophoudt.
    ///
    /// Met banen is de vernieuwing een [`Lane::Lease`]-job: hier in de rij gezet
    /// (en de banen krijgen een beurt), nooit afgewacht; de uitkomst komt
    /// bij de volgende keer terug.
    pub fn keep_lease(&self) {
        let at = (self.millis)();
        if at.saturating_sub(self.lease_tried.get()) < 1000 || at < self.retry_at.get() {
            return;
        }
        self.lease_tried.set(at);
        if self.standing() == Standing::Lost {
            // Gefencet bij de volgende controle van de host; geen store-call
            // voor een lease die we niet kunnen houden.
            return;
        }
        if let Some(off) = self.offload {
            // Een verloren lease wordt hier gemeld en bij de volgende
            // controle van de host gefencet ([`Group::standing`]).
            let _ = self.lease_landed();
            if self.standing() != Standing::Lost && self.lease_due(at) && self.push_lease(off) {
                off.breathe();
            }
            return;
        }
        match self.renew() {
            Ok(()) => self.lease_failing.set(false),
            Err(Error::Busy) => {}
            Err(e) => match self.classify(&e) {
                f @ (Failure::Transport | Failure::Deadline) => self.retry("lease", "renew", e, f),
                f @ (Failure::Taken | Failure::Expired) => {
                    self.lost("lease", "renew", Some(e), f);
                }
                Failure::Other => {
                    self.failed("lease", "renew", e);
                }
            },
        }
    }
    /// Is een vernieuwing aan de beurt op `at` (Unix-ms)? Zoals de Writer het
    /// beslist: een zesde termijn na de laatste bevestiging (dus ook zodra de
    /// schrijfdeadline voorbij is).
    fn lease_due(&self, at: i64) -> bool {
        let Ok(w) = self.writer.try_borrow() else {
            return false;
        };
        let ttl = self.config.ttl_ms;
        // `Writer::deadline` is de laatste bevestiging plus twee derde termijn.
        let confirmed = w.deadline().saturating_add(ttl / 3).saturating_sub(ttl);
        u64::try_from(at).is_ok_and(|at| at.saturating_sub(confirmed) >= ttl / 6)
    }
    /// Zet een vernieuwing op [`Lane::Lease`], tenzij er een onderweg is; `true`: gezet.
    fn push_lease(&self, off: &dyn Offload) -> bool {
        if self.lease_flight.borrow().is_some() {
            return false;
        }
        let writer = self.writer.clone();
        let key = self.config.lease_key();
        let ttl = self.config.ttl_ms;
        let millis = self.millis;
        let slot: Slot<Result> = Rc::new(RefCell::new(None));
        let back = slot.clone();
        *self.lease_flight.borrow_mut() = Some(slot);
        let job = Job::of(move |side: &mut dyn LaneSide| {
            // De tijd waarop hij loopt: de baan kan hem even na het zetten beginnen.
            let out = match time_at(millis()) {
                Ok(now) => {
                    let mut out = Err(Error::State);
                    match side.lease(&key, ttl, &mut |lease| {
                        out = writer::renew_or_reclaim(&writer, lease, now);
                    }) {
                        Ok(()) => out,
                        Err(e) => Err(Error::Object(e)),
                    }
                }
                Err(_) => Err(Error::State),
            };
            *back.borrow_mut() = Some(out);
        });
        off.push(Lane::Lease, job);
        true
    }
    /// Neemt een vernieuwing terug die [`Lane::Lease`] afmaakte, zoals de
    /// eigenaar de zijne nam: [`Error::LeaseLost`] als een ander de lease
    /// heeft of hij verliep.
    fn lease_landed(&self) -> Result {
        let out = {
            let flight = self.lease_flight.borrow();
            let Some(slot) = flight.as_ref() else {
                return Ok(());
            };
            let Some(out) = slot.borrow_mut().take() else {
                // Nog onderweg.
                return Ok(());
            };
            out
        };
        *self.lease_flight.borrow_mut() = None;
        match out {
            Ok(()) => {
                self.lease_failing.set(false);
                self.answered_at.set((self.millis)());
                Ok(())
            }
            Err(Error::Busy) => Ok(()),
            Err(e) => match self.classify(&e) {
                f @ (Failure::Transport | Failure::Deadline) => {
                    self.retry("lease", "renew", e, f);
                    Ok(())
                }
                f @ (Failure::Taken | Failure::Expired) => {
                    Err(self.lost("lease", "renew", Some(e), f))
                }
                Failure::Other => {
                    self.failed("lease", "renew", e);
                    Ok(())
                }
            },
        }
    }
    /// Zet de upload van `pending` op [`Lane::Data`].
    fn push_upload(&self, off: &dyn Offload, pending: Pending) -> Slot<Landed> {
        let slot: Slot<Landed> = Rc::new(RefCell::new(None));
        let back = slot.clone();
        let domain = self.config.domain.clone();
        off.push(
            Lane::Data,
            Job::of(move |side: &mut dyn LaneSide| {
                let mut out = Err(Error::State);
                let opened = side.files(&domain, &mut |b, s| {
                    out = pending.upload(&mut DynStorage(b), &mut DynStore(s));
                });
                if opened.is_err() {
                    out = Err(Error::Storage(replica_sqlite::Error::CANNOT_OPEN));
                }
                *back.borrow_mut() = Some(Landed::Upload(pending, out));
            }),
        );
        slot
    }
    /// Zet een onderhoudsbeurt op [`Lane::Data`].
    fn push_maintenance(&self, off: &dyn Offload, round: Maintenance) -> Slot<Landed> {
        let slot: Slot<Landed> = Rc::new(RefCell::new(None));
        let back = slot.clone();
        let domain = self.config.domain.clone();
        off.push(
            Lane::Data,
            Job::of(move |side: &mut dyn LaneSide| {
                let mut out = Err(Error::State);
                let opened = side.files(&domain, &mut |b, s| {
                    out = round.run(&mut DynStorage(b), &mut DynStore(s));
                });
                if opened.is_err() {
                    out = Err(Error::Storage(replica_sqlite::Error::CANNOT_OPEN));
                }
                *back.borrow_mut() = Some(Landed::Maintained(out));
            }),
        );
        slot
    }
    fn replica_config(&self, file: &str) -> Result<ReplicaConfig> {
        let (endpoint, bucket) = self.store.borrow().identity();
        let destination = self.config.destination(&endpoint, &bucket, file)?;
        let mut c = ReplicaConfig::new(
            &self.config.namespace(file),
            &destination,
            Name::new(file)?,
            self.config.pages,
        )?;
        c.interval = self.config.interval;
        c.compress = self.config.compress;
        // Met banen is het onderhoud een eigen job ([`Group::rest`]); inline
        // liep het binnen een publicatie op de eigenaar, eens per minuut per database.
        c.background_maintenance = self.offload.is_some();
        Ok(c)
    }
    /// Bereidt `file` eenmaal voor op zijn sync-handvat `sync`: hersteld uit
    /// de store als hij lokaal ontbreekt, anders voortgezet, aangemaakt bij de
    /// eerste opening als geen van beide hem heeft. `verify` controleert een
    /// herstelde scratchdatabase. `true`: hersteld. Al voorbereid: `false`.
    ///
    /// Elke store-call van het herstel vernieuwt de lease als die aan de beurt
    /// is, de GET's in rondes die zich aan hun duur aanpassen (zie de
    /// moduledocs), en elke opslagcall en elke pagina die `verify` leest
    /// tikt ook ([`Ticking`]). Een mislukte vernieuwing is niet de fout van
    /// het herstel: een verloren lease faalt bij de volgende controle.
    pub fn prepare(&self, file: &str, mut sync: D, verify: &Verify<'_, D>) -> Result<bool> {
        if self.has(file) {
            return Ok(false);
        }
        let config = self.replica_config(file)?;
        let now = self.now()?;
        let replica = {
            let tick = || self.keep_lease();
            let mut renewing = Renewing {
                store: &self.store,
                tick: &tick,
                ttl_ms: self.config.ttl_ms,
                millis: self.millis,
                adapt: true,
                requests: 1,
                round_at: None,
            };
            // Ook elke opslagcall van Prepare tikt: na de controle kopieert
            // hij de scratch over het doel (64 KiB per keer), even lang.
            let mut storage = Ticking {
                inner: &mut sync,
                tick: &tick,
            };
            Replica::prepare_shared(
                &mut storage,
                &mut renewing,
                self.writer.clone(),
                config,
                now,
                |b, name| verify(&mut *b.inner, name, &tick),
            )
        }
        .map_err(|e| self.failed(file, "prepare", e))?;
        let restored = replica.status().reason == Reason::Restored;
        crate::reserve(&mut self.files.borrow_mut(), 1)?;
        self.files.borrow_mut().push(Member {
            name: crate::string(file)?,
            replica,
            sync,
            flight: None,
        });
        self.record(file);
        Ok(restored)
    }
    /// De nummers van de eigenaar van `file` in [`Publications`].
    fn record(&self, file: &str) {
        if let Some(f) = self.files.borrow().iter().find(|f| f.name == file) {
            Self::mirror(&self.publications, f);
        }
    }
    fn mirror(publications: &Publications, f: &Member<D>) {
        publications.record(&f.name, f.replica.begun(), f.replica.confirmed());
    }
    /// Een rustmoment (tussen twee verzoeken): de lease als die aan de beurt
    /// is, dan elke database waarvan het interval voorbij is. Met banen: zie
    /// [`Group::with_offload`]; dan doet dit alleen wat rust vraagt en wacht
    /// het nooit op een store die niet antwoordt.
    ///
    /// Alleen [`Error::LeaseLost`] betekent fencen; een store die niet
    /// antwoordt, wordt na een backoff opnieuw geprobeerd en is hier `Ok`.
    pub fn rest(&self) -> Result {
        self.sync(false)
    }
    /// Vóór het sluiten: alles publiceren. Met banen wacht dit eerst even op
    /// de banen ([`SETTLE_MS`]), neemt terug wat ze deden en publiceert de
    /// rest op de eigenaar. Een store die niet antwoordt is hier wel een fout.
    pub fn flush(&self) -> Result {
        self.sync(true)
    }
    fn sync(&self, all: bool) -> Result {
        if let Some(off) = self.offload {
            self.sync_lanes(off, all)?;
            if !all {
                return Ok(());
            }
        }
        match self.standing() {
            Standing::Lost => {
                let failure = if self.is_lost() {
                    Failure::Taken
                } else {
                    Failure::Expired
                };
                return Err(self.lost("lease", "renew", None, failure));
            }
            // In de backoff: het volgende rustmoment daarna probeert opnieuw.
            _ if !all && (self.millis)() < self.retry_at.get() => return Ok(()),
            _ => {}
        }
        if let Err(e) = self.renew() {
            return match self.classify(&e) {
                f @ (Failure::Transport | Failure::Deadline) => {
                    self.retry("lease", "renew", e, f);
                    if all { Err(e) } else { Ok(()) }
                }
                f @ (Failure::Taken | Failure::Expired) => {
                    Err(self.lost("lease", "renew", Some(e), f))
                }
                Failure::Other => Err(self.failed("lease", "renew", e)),
            };
        }
        self.lease_failing.set(false);
        // Elke store-call van een publicatie vernieuwt de lease als die aan de
        // beurt is: één ronde van meer databases (of één grote snapshot) kan
        // langer duren dan de schrijfdeadline.
        let tick = || self.keep_lease();
        let mut files = self.files.borrow_mut();
        let mut first: Option<Error> = None;
        let mut failing = false;
        for f in files.iter_mut() {
            if self.publications.is_held(&f.name) {
                // Een toets houdt hem tegen: niets ervan bereikt de store.
                continue;
            }
            if f.flight.is_some() {
                // Een baan heeft zijn upload (of onderhoud) nog: niet in rust.
                continue;
            }
            // Per database: een vernieuwing tijdens de vorige publicatie schoof de lease op.
            let now = self.now()?;
            let mut store = Renewing {
                store: &self.store,
                tick: &tick,
                ttl_ms: self.config.ttl_ms,
                millis: self.millis,
                adapt: false,
                requests: 1,
                round_at: None,
            };
            let out = if all {
                if f.replica.has_changes() {
                    f.replica.sync(&mut f.sync, &mut store, now).map(Some)
                } else {
                    Ok(None)
                }
            } else {
                f.replica.tick(&mut f.sync, &mut store, now)
            };
            if matches!(out, Ok(Some(_))) {
                // Een beurt van deze database lukte: de store antwoordt hem weer.
                self.files_failing.borrow_mut().retain(|n| *n != f.name);
            }
            // De nummers van de eigenaar: welke capture begon en welke landde.
            Self::mirror(&self.publications, f);
            match out {
                Ok(Some(s)) if s.published => {
                    self.report.published(&f.name);
                }
                Ok(_) => {}
                Err(e) => match self.classify(&e) {
                    fl @ (Failure::Transport | Failure::Deadline) => {
                        self.retry(&f.name, "sync", e, fl);
                        failing = true;
                        first.get_or_insert(e);
                        // De store antwoordt niet: de andere databases wachten op de volgende ronde.
                        break;
                    }
                    fl @ (Failure::Taken | Failure::Expired) => {
                        return Err(self.lost(&f.name, "sync", Some(e), fl));
                    }
                    Failure::Other => {
                        let e = self.failed(&f.name, "sync", e);
                        first.get_or_insert(e);
                    }
                },
            }
        }
        if failing {
            // Na de backoff opnieuw; de host bedient intussen door.
            return if all {
                first.map_or(Ok(()), Err)
            } else {
                Ok(())
            };
        }
        self.recovered();
        first.map_or(Ok(()), Err)
    }
    /// Het rustmoment van een groep met banen: alleen de stappen die de
    /// eigenaar in rust vragen lopen hier, en geen wacht op een store die niet
    /// antwoordt.
    ///
    /// 1. Een vernieuwing of een job die een baan afmaakte, komt terug: gemeld
    ///    en geteld zoals die van de eigenaar zelf. Na een upload schrijft de
    ///    eigenaar het manifest ([`Replica::finish`]): één kleine PUT, de enige
    ///    store-call in rust.
    /// 2. De lease gaat op [`Lane::Lease`] als een vernieuwing aan de beurt is.
    /// 3. Per database, tenzij er een job van hem onderweg is: de capture van
    ///    wat veranderde als zijn interval voorbij is ([`Replica::begin`]:
    ///    lokaal) en zijn upload op [`Lane::Data`]; anders een onderhoudsbeurt
    ///    als die aan de beurt is ([`Replica::maintenance_begin`]).
    ///
    /// Niets nieuws begint zolang de lease voorbij zijn schrijfdeadline is of
    /// in de backoff na een fout. `all` (een flush): eerst even wachten op de
    /// banen ([`Offload::settle`]), terugnemen wat ze deden, niets beginnen.
    fn sync_lanes(&self, off: &dyn Offload, all: bool) -> Result {
        if self.standing() == Standing::Lost {
            let failure = if self.is_lost() {
                Failure::Taken
            } else {
                Failure::Expired
            };
            return Err(self.lost("lease", "renew", None, failure));
        }
        if all {
            off.settle(SETTLE_MS);
        }
        self.lease_landed()?;
        let at = (self.millis)();
        let backoff = at < self.retry_at.get();
        if !all && !backoff && self.lease_due(at) && self.push_lease(off) {
            self.lease_tried.set(at);
        }
        let valid = self.standing() == Standing::Valid;
        let mut files = self.files.borrow_mut();
        let mut first: Option<Error> = None;
        for f in files.iter_mut() {
            if self.publications.is_held(&f.name) {
                // Een toets houdt hem tegen: niets ervan bereikt de store.
                continue;
            }
            let landed = match f.flight.as_ref().map(|s| s.borrow_mut().take()) {
                // Onderweg.
                Some(None) => continue,
                Some(Some(l)) => {
                    f.flight = None;
                    Some(l)
                }
                None => None,
            };
            let now = self.now()?;
            if let Some(l) = landed {
                let (step, out) = match l {
                    Landed::Upload(pending, uploaded) => {
                        if uploaded.is_ok() {
                            self.answered_at.set(at);
                        }
                        let mut store = self.store.borrow_mut();
                        let out =
                            f.replica
                                .finish(&mut f.sync, &mut *store, pending, uploaded, now);
                        ("upload", out)
                    }
                    Landed::Maintained(round) => {
                        if round.is_ok() {
                            self.answered_at.set(at);
                        }
                        ("maintain", f.replica.maintenance_finish(&mut f.sync, round))
                    }
                };
                // Alleen het eigen nummer van de capture landt: een capture die
                // onderweg was toen een write kwam, bevestigt die write niet.
                Self::mirror(&self.publications, f);
                match out {
                    Ok(s) => {
                        self.files_failing.borrow_mut().retain(|n| *n != f.name);
                        if s.published {
                            self.report.published(&f.name);
                        }
                    }
                    Err(e) => {
                        if let Some(e) = self.lane_failed(&f.name, step, e)? {
                            first.get_or_insert(e);
                        }
                        continue;
                    }
                }
            }
            if all || !valid || backoff {
                continue;
            }
            if f.replica.is_due(now) {
                // Een onzeker manifest wordt in `begin` opgelost, in rust, met
                // store-calls: alleen zolang de banen de store horen antwoorden.
                let uncertain = f.replica.try_marker().is_ok_and(|m| m.uncertain != 0);
                if uncertain && at.saturating_sub(self.answered_at.get()) > ANSWERED_MS {
                    continue;
                }
                let begun = {
                    let mut store = self.store.borrow_mut();
                    f.replica.begin(&mut f.sync, &mut *store, now)
                };
                // De capture bevat elke write van vóór nu (genomen in rust),
                // een hervatte upload die van een vorig leven: de eigenaar nummert.
                Self::mirror(&self.publications, f);
                match begun {
                    Ok(Some(pending)) => {
                        f.flight = Some(self.push_upload(off, pending));
                    }
                    // Niets veranderd: niets van deze database wacht op de store.
                    Ok(None) => self.files_failing.borrow_mut().retain(|n| *n != f.name),
                    Err(e) => {
                        if let Some(e) = self.lane_failed(&f.name, "capture", e)? {
                            first.get_or_insert(e);
                        }
                    }
                }
            } else {
                match f.replica.maintenance_begin(&mut f.sync, now) {
                    Ok(Some(round)) => f.flight = Some(self.push_maintenance(off, round)),
                    Ok(None) => {}
                    Err(e) => {
                        if let Some(e) = self.lane_failed(&f.name, "maintain", e)? {
                            first.get_or_insert(e);
                        }
                    }
                }
            }
        }
        drop(files);
        self.recovered();
        first.map_or(Ok(()), Err)
    }
    /// Een mislukte stap van `file` met banen: een store die niet antwoordt
    /// wordt na de backoff opnieuw geprobeerd (`Ok(None)`), een verloren
    /// lease is [`Error::LeaseLost`], al het andere wordt gemeld en
    /// teruggegeven (`Ok(Some(e))`).
    fn lane_failed(
        &self,
        file: &str,
        step: &str,
        e: Error,
    ) -> core::result::Result<Option<Error>, Error> {
        match self.classify(&e) {
            f @ (Failure::Transport | Failure::Deadline) => {
                self.retry(file, step, e, f);
                Ok(None)
            }
            f @ (Failure::Taken | Failure::Expired) => Err(self.lost(file, step, Some(e), f)),
            Failure::Other => Ok(Some(self.failed(file, step, e))),
        }
    }
    /// Sluit elke eigenaar (elke VFS weigert vanaf nu writes) en geeft de
    /// lease terug zoals [`Held::take`] hem vindt. Wat niet gepubliceerd is,
    /// blijft in de dirty logs: de volgende eigenaar op dezelfde mappen zet
    /// het voort (`Continued`). Geef de lease daarna vrij met [`release`].
    pub fn close(self) -> Option<Held> {
        let Group { files, writer, .. } = self;
        for mut f in files.into_inner() {
            f.replica.close();
        }
        Held::take(writer)
    }
}
/// Geeft de lease op `key` terug zodat de volgende houder hem meteen
/// claimt, met het aantal pogingen. De vrijgave van de Writer is één
/// voorwaardelijke DELETE, en een transport als `leans3http` doet één poging
/// per verzoek, nooit een herhaling. Bij een stop is de keep-alive-verbinding
/// vaak een tijd stil geweest en faalt dat eerste verzoek met
/// `Object(Transport)` zonder de store te bereiken (EasyFlor, 17-11-2026: elke
/// stop in de recreate-proef; de opvolger wachtte tot de lease verliep). Na
/// een transportfout dus nog hoogstens twee keer, op een nieuwe verbinding,
/// bij naam ([`writer::release_named`]).
///
/// [`Held::Named`] (een vernieuwing onderweg houdt de Writer nog) gaat vanaf
/// de eerste poging bij naam.
pub fn release<S: Bucket>(store: &mut S, key: &str, ttl_ms: u64, held: Held) -> (Result, u32) {
    let transport = Error::Object(StoreError::Transport);
    let (first, owner, generation, mut attempts) = match held {
        Held::Writer(writer) => {
            let owner = match crate::string(writer.owner()) {
                Ok(o) => o,
                Err(e) => return (Err(e), 0),
            };
            let generation = writer.generation();
            let first = store
                .lease(key, ttl_ms)
                .map_err(Error::Object)
                .and_then(|mut lease| writer.release(&mut lease));
            if first != Err(transport) {
                return (first, 1);
            }
            (first, owner, generation, 1)
        }
        Held::Named { owner, generation } => (Err(transport), owner, generation, 0),
    };
    let mut last = first;
    while attempts < 3 && last.is_err() {
        attempts += 1;
        last = store
            .lease(key, ttl_ms)
            .map_err(Error::Object)
            .and_then(|mut lease| writer::release_named(&mut lease, &owner, generation));
    }
    (last, attempts)
}
