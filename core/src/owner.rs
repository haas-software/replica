//! De expliciete Replica-levensloop van één app-eigenaar; geen verborgen threads.
//!
//! # De eigenaar los van de VFS
//!
//! De SQL-kant (een verbinding op haar eigen stack) en de Sync-kant (capture,
//! upload, onderhoud en de lease, op een andere stack) delen twee dingen: de
//! tracking met de lokale marker ([`State`], in een `Rc<RefCell<..>>`) en de
//! lease ([`Writer`], ook gedeeld, zoals de leasetaak hem al deelde). De
//! [`Vfs`] die [`Replica::vfs`] (of [`Tracker::vfs`]) geeft, leent de eigenaar
//! dus niet meer: hij houdt eigen handvatten op die gedeelde staat, en de
//! eigenaar blijft vrij voor `tick`, `begin`, `finish`, `renew` en het
//! onderhoud terwijl de verbinding openstaat.
//!
//! Het handboek (§1.2) noemt gedeeld eigendom een teken dat de eigenaar nog
//! niet gekozen is. Hier is hij gekozen, alleen niet in de tijd: in rust is de
//! staat van de eigenaar, midden in een SQL-callback van de VFS. Dat is het
//! rustprotocol, en de `RefCell` is de controle erop.
//!
//! ## Het rustprotocol
//!
//! - **De VFS** leent de staat alleen binnen één callback (open, write,
//!   truncate, sync, remove), en houdt die lening wél vast over de opslag-I/O
//!   van die callback heen (de dirty log, de marker, de schaduw). Een callback
//!   die de staat niet kan lenen, geeft `SQLITE_BUSY` zonder I/O.
//! - **De eigenaar** leent de staat in [`Replica::sync`], [`Replica::tick`],
//!   [`Replica::begin`], [`Replica::begin_capture`], [`Replica::abandon`],
//!   [`Replica::finish`], [`Replica::maintenance_begin`] en
//!   [`Replica::maintenance_finish`], van begin tot eind, ook over hun I/O.
//!   Kan hij niet lenen, dan geven ze [`Error::Busy`] zonder iets te wijzigen.
//!   Ze mogen dus alleen lopen terwijl de SQL-kant **in rust** is: de host
//!   sluit de verbinding, of laat haar actor wachten tussen twee verzoeken (geen
//!   lopend statement, geen open transactie), en hervat haar daarna. Wat
//!   ertussen hoort, is lokaal en kort: de capture naar de spool, het manifest
//!   en de marker.
//! - **Zonder rust** mogen: [`Pending::upload`] (alleen spool en store),
//!   [`Capturing::run`] (database, spool en schaduw), [`Maintenance::run`]
//!   (alleen de store), [`Replica::renew`] (alleen de lease) en
//!   [`Replica::status`]. Zo blokkeert een upload van megabytes geen enkel
//!   verzoek.
//! - **Geen lening over een `.await`**: alle leningen hier zijn synchroon en
//!   eindigen binnen hun functie; een parkeerpunt (een `Suspend::wait` in de
//!   opslag) ligt alleen binnen de lening van de kant die op dat moment de
//!   staat heeft, en de andere kant mag er dan per protocol niet aan komen.
//!   De lint `await_holding_refcell_ref` staat op deny; de lease wordt in
//!   stappen vernieuwd ([`Writer::renew_request`]), zodat de VFS zijn deadline
//!   altijd kan lezen.
use crate::{
    Error, Result,
    capture::Plan,
    local::{self, Name},
    maintenance::{self, Budget, Report, Schedule},
    marker::{LocalMarker, Marker},
    object::Store,
    prepare::{self, Reason},
    replication::{self, Batch, Staged, Uploaded},
    string,
    time::Time,
    tracking::{Tracked, Tracking, sqlite_error},
    writer::{Backend, Renewed, Writer},
};
use alloc::rc::Rc;
use alloc::string::String;
use core::cell::{Cell, Ref, RefCell};
use core::ffi::CStr;
use replica_sqlite::{FileId, OpenFlags, Storage};
/// Configuratie vóór Prepare. De app bezit transport, klok en entropie.
pub struct Config {
    /// `prefix/domain` in objectopslag.
    pub namespace: String,
    /// Bestemmingshash uit `marker::destination`.
    pub destination: String,
    /// Canoniek databasepad in de VFS.
    pub path: Name,
    /// Hard maximum voor tracking/coverage-pagina's.
    pub pages: u32,
    /// Paginabytes per segment: 64 KiB..16 MiB, standaard 4 MiB.
    pub segment_bytes: usize,
    /// Synchronisatie-interval in hele seconden, standaard 15.
    pub interval: u32,
    /// Generatieleeftijd in seconden, standaard één week.
    pub generation: u64,
    /// Generatiebewaartermijn in seconden, standaard vier weken.
    pub retention: u64,
    /// Bewaarschema voor incrementen binnen één generatie.
    pub schedule: Schedule,
    /// Expliciete toestemming voor lokale adoptie zonder bestaande marker.
    pub adopt_local: bool,
    /// Het onderhoud (compactie, uitdunnen, verlopen generaties) niet binnen
    /// `begin`/`finish` op de eigenaar, maar als losse taak via
    /// [`Replica::maintenance_begin`]: dan legt geen venster de database stil.
    pub background_maintenance: bool,
}
impl Config {
    /// Defaults zoals Go, met expliciet databasebudget en bestemming.
    pub fn new(namespace: &str, destination: &str, path: Name, pages: u32) -> Result<Self> {
        let out = Self {
            namespace: string(namespace)?,
            destination: string(destination)?,
            path,
            pages,
            segment_bytes: 4 << 20,
            interval: 15,
            generation: 7 * 86400,
            retention: 28 * 86400,
            schedule: Schedule::defaults()?,
            adopt_local: false,
            background_maintenance: false,
        };
        out.validate()?;
        Ok(out)
    }
    fn validate(&self) -> Result {
        if self.namespace.is_empty()
            || self.namespace.ends_with('/')
            || self.namespace.contains("..")
            || self.namespace.len() > 512
            || self.pages == 0
            || self.interval == 0
            || !(65536..=16 << 20).contains(&self.segment_bytes)
            || self.generation == 0
            || self.retention == 0
            || self.generation > i64::MAX as u64
            || self.retention > i64::MAX as u64
        {
            return Err(Error::State);
        }
        Marker::new(&self.destination, "", Time::ZERO)?;
        self.path.suffix(".replica-restore-data")?;
        Ok(())
    }
}
/// Eén afgeronde beurt; onderhoud kan ook bij een ongewijzigde database plaatsvinden.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Synced {
    /// Er is een snapshot of increment bevestigd.
    pub published: bool,
    /// Aantal gecompacteerde en geprunede manifesten.
    pub maintenance: Report,
    /// Verwijderde objecten uit verlopen generaties.
    pub expired_objects: usize,
}
/// Status zonder strings uit een externe foutmelding of geleende buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    /// Reden van de laatste Prepare of snapshotvernieuwing.
    pub reason: Reason,
    /// Tijd van de laatste synchronisatiepoging, ook bij een fout.
    pub attempted: Option<Time>,
    /// Tijd van de laatste volledig geslaagde beurt.
    pub synced: Option<Time>,
    /// Laatste fout; transportfouten maken de generatie niet beschadigd.
    pub error: Option<Error>,
}
/// De staat die eigenaar en VFS delen: de lokale marker en de tracking.
pub struct State {
    marker: LocalMarker,
    tracking: Tracking,
}
/// Eén eigenaar van marker, dirty logs en cadans. Alle SQL-writes lopen via `vfs`.
///
/// Zie de moduledocs voor het rustprotocol: de VFS mag openblijven, maar de
/// stappen die de tracking raken lopen alleen terwijl de SQL-kant in rust is.
///
/// Alleen de houder van de schrijverlease maakt een eigenaar. Na de deadline van
/// die lease weigeren VFS, synchronisatie en publicatie hard; de app roept
/// [`Replica::renew`] daarom ook aan terwijl opslag of netwerk parkeert.
/// Deze bibliotheek verstopt geen achtergrondtaak of runtime.
pub struct Replica {
    config: Config,
    /// Gedeeld met de leasetaak van de host en met elke VFS: die leest er de
    /// deadline uit, de leasetaak vernieuwt hem op zijn eigen stack.
    writer: Rc<RefCell<Writer>>,
    /// Gedeeld met elke VFS, onder het rustprotocol.
    state: Rc<RefCell<State>>,
    /// Gedeeld met elke VFS: dicht weigert iedere write.
    closed: Rc<Cell<bool>>,
    status: Status,
    maintenance_at: Option<Time>,
    pending: bool,
    maintaining: bool,
    /// Een onderbroken snapshot-upload van vóór de herstart; [`Replica::begin`]
    /// geeft hem terug in plaats van een nieuwe capture te maken.
    resumable: Option<Pending>,
}
/// Een onderhoudsbeurt als losse taak: alleen de store (en willekeur voor
/// poging-id's), nooit SQLite, marker of tracking. De frontier is bij
/// [`Replica::maintenance_begin`] al duurzaam vastgelegd; commits die
/// intussen landen, vallen erbuiten.
pub struct Maintenance {
    namespace: String,
    generation: String,
    sequence: u64,
    sealed_at: Time,
    now: Time,
    levels: alloc::vec::Vec<crate::maintenance::Level>,
    budget: (u32, usize),
    retention: u64,
}
impl Maintenance {
    /// Compactie, uitdunnen en verlopen generaties; mag lang duren.
    pub fn run<B: Storage, S: Store>(&self, b: &mut B, store: &mut S) -> Result<Synced> {
        let schedule = Schedule::new(&self.levels)?;
        let maintenance = maintenance::run_sealed(
            b,
            store,
            &self.namespace,
            &self.generation,
            self.sequence,
            self.sealed_at,
            &schedule,
            self.now,
            Budget {
                pages: self.budget.0,
                segment_bytes: self.budget.1,
            },
        )?;
        let expired_objects = maintenance::generations(
            store,
            &self.namespace,
            &self.generation,
            self.now,
            self.retention,
        )?;
        Ok(Synced {
            published: false,
            maintenance,
            expired_objects,
        })
    }
}
/// Een capture waarvan de delen buiten de eigenaar worden geüpload, tussen
/// [`Replica::begin`] en [`Replica::finish`].
pub struct Pending {
    batch: Batch,
    staged: Staged,
}
impl Pending {
    /// Hoeveel pagina's deze capture meeneemt.
    pub fn pages(&self) -> u64 {
        self.batch.page_count()
    }
    /// Waar de tijd van de capture heen ging.
    pub fn stats(&self) -> crate::capture::Stats {
        self.batch.stats()
    }
    /// Uploadt de delen; raakt alleen de spool en de store, nooit SQLite,
    /// marker of tracking, dus dit mag op een eigen stack naast de eigenaar.
    pub fn upload<B: Storage, S: Store>(&self, b: &mut B, store: &mut S) -> Result<Uploaded> {
        self.batch.upload(b, store, &self.staged)
    }
}
/// Wat [`Replica::begin_capture`] teruggeeft: een beeld om te lezen, of een
/// onderbroken snapshot waarvan alleen de upload nog moet.
#[allow(clippy::large_enum_variant)]
pub enum Begun {
    /// Een nieuw beeld: lees het met [`Capturing::run`], upload dan.
    Capture(Capturing),
    /// Een hervatte snapshot: alleen [`Pending::upload`] nog.
    Upload(Pending),
}
/// Een capture die buiten de eigenaar leest: alleen de database, de spool en
/// de schaduw, nooit SQLite, marker of tracking.
pub struct Capturing {
    plan: Plan,
    namespace: String,
    generation: String,
    previous_sequence: u64,
    path: Name,
    now: Time,
}
impl Capturing {
    /// Eén segment; `false` als alles gelezen is. Voor wie tussen de segmenten
    /// door iets anders wil doen; [`Capturing::run`] doet ze achter elkaar.
    pub fn step<B: Storage>(&mut self, b: &mut B) -> Result<bool> {
        self.plan.step(b)
    }
    /// Leest het beeld naar de spool en zet de upload klaar (prefix en het
    /// hervattingsrecord), zodat [`Pending::upload`] meteen kan volgen.
    pub fn run<B: Storage>(mut self, b: &mut B) -> Result<Pending> {
        while self.step(b)? {}
        let capture = self.plan.finish(b)?;
        let batch = Batch::new(b, capture, &self.generation, self.previous_sequence)?;
        let staged = batch.stage_at(b, &self.namespace, self.now)?;
        batch.save_pending(b, self.path, &staged)?;
        Ok(Pending { batch, staged })
    }
}
/// De SQL-kant van een eigenaar: wat een VFS nodig heeft, zonder de eigenaar
/// zelf. Een host geeft dit aan zijn SQL-taak en houdt de [`Replica`] bij zijn
/// Sync-taak; [`Tracker::vfs`] maakt er per verbinding een [`Vfs`] van.
#[derive(Clone)]
pub struct Tracker {
    path: Name,
    writer: Rc<RefCell<Writer>>,
    state: Rc<RefCell<State>>,
    closed: Rc<Cell<bool>>,
}
impl Tracker {
    /// De getrackte VFS over `b`, met dezelfde controles als [`Replica::vfs`].
    pub fn vfs<'a, B: Storage>(&self, b: &'a mut B) -> Result<Vfs<'a, B>> {
        if self.closed.get() {
            return Err(Error::State);
        }
        let deadline = fence(&self.writer, b)?;
        Ok(Vfs {
            tracker: self.clone(),
            storage: b,
            main: None,
            deadline,
        })
    }
    /// Het canonieke pad van de database in deze opslag.
    pub fn path(&self) -> Name {
        self.path
    }
}
/// De deadline op de klok van de opslag, of een harde fout als die voorbij is.
fn fence<B: Storage>(writer: &RefCell<Writer>, b: &mut B) -> Result<i64> {
    let deadline = {
        let w = writer.try_borrow().map_err(|_| Error::Busy)?;
        if w.is_lost() {
            return Err(Error::LeaseLost);
        }
        i64::try_from(w.deadline()).map_err(|_| Error::Limit)?
    };
    if b.unix_millis()? >= deadline {
        return Err(Error::LeaseLost);
    }
    Ok(deadline)
}
/// SQLITE_BUSY: de gedeelde staat is geleend (zie het rustprotocol).
const BUSY: replica_sqlite::Error = replica_sqlite::Error { code: 5 };
/// De getrackte VFS van één verbinding. Leent de eigenaar niet: per callback
/// leent hij de gedeelde staat (zie het rustprotocol), en de deadline van de
/// lease leest hij telkens opnieuw, zodat een vernieuwing ook een verbinding
/// die al uren openstaat laat doorschrijven.
pub struct Vfs<'a, B: Storage> {
    tracker: Tracker,
    storage: &'a mut B,
    main: Option<FileId>,
    /// De laatst gelezen deadline: geldt ook als de leasetaak de lease net
    /// vernieuwt (en dus leent); een vernieuwing schuift hem alleen op.
    deadline: i64,
}
impl<B: Storage> Vfs<'_, B> {
    /// Dicht of verloren: geen write meer. Anders de actuele deadline.
    fn deadline(&mut self) -> replica_sqlite::Result<i64> {
        if self.tracker.closed.get() {
            return Err(replica_sqlite::Error::READONLY);
        }
        if let Ok(w) = self.tracker.writer.try_borrow() {
            if w.is_lost() {
                return Err(replica_sqlite::Error::READONLY);
            }
            self.deadline = i64::try_from(w.deadline()).unwrap_or(i64::MAX);
        }
        Ok(self.deadline)
    }
    /// Eén callback via de tracking-VFS, met de staat geleend voor precies
    /// deze callback.
    fn tracked<R>(
        &mut self,
        f: impl FnOnce(&mut Tracked<'_, B, LocalMarker>) -> replica_sqlite::Result<R>,
    ) -> replica_sqlite::Result<R> {
        let deadline = self.deadline()?;
        let state = Rc::clone(&self.tracker.state);
        let mut guard = state.try_borrow_mut().map_err(|_| BUSY)?;
        let st = &mut *guard;
        let mut tracked = st
            .tracking
            .wrap(&mut *self.storage, &mut st.marker)
            .until(deadline);
        tracked.main = self.main;
        let result = f(&mut tracked);
        self.main = tracked.main;
        result
    }
}
impl<B: Storage> Storage for Vfs<'_, B> {
    fn cooperate(&mut self) -> replica_sqlite::Result {
        self.storage.cooperate()
    }
    fn open(&mut self, name: &CStr, flags: OpenFlags) -> replica_sqlite::Result<FileId> {
        let main = name == self.tracker.path.cstr().map_err(sqlite_error)?;
        if !main {
            return self.storage.open(name, flags);
        }
        self.tracked(|t| t.open(name, flags))
    }
    fn close(&mut self, file: FileId) -> replica_sqlite::Result {
        if self.main == Some(file) {
            self.main = None;
        }
        self.storage.close(file)
    }
    fn read(&mut self, file: FileId, offset: u64, dst: &mut [u8]) -> replica_sqlite::Result<usize> {
        self.storage.read(file, offset, dst)
    }
    fn write(&mut self, file: FileId, offset: u64, src: &[u8]) -> replica_sqlite::Result {
        self.tracked(|t| t.write(file, offset, src))
    }
    fn truncate(&mut self, file: FileId, size: u64) -> replica_sqlite::Result {
        self.tracked(|t| t.truncate(file, size))
    }
    fn sync(&mut self, file: FileId, flags: i32) -> replica_sqlite::Result {
        self.tracked(|t| t.sync(file, flags))
    }
    fn size(&mut self, file: FileId) -> replica_sqlite::Result<u64> {
        self.storage.size(file)
    }
    fn remove(&mut self, name: &CStr, dir: bool) -> replica_sqlite::Result {
        self.tracked(|t| t.remove(name, dir))
    }
    fn exists(&mut self, name: &CStr) -> replica_sqlite::Result<bool> {
        self.storage.exists(name)
    }
    fn random(&mut self, dst: &mut [u8]) -> replica_sqlite::Result {
        self.storage.random(dst)
    }
    fn unix_millis(&mut self) -> replica_sqlite::Result<i64> {
        self.storage.unix_millis()
    }
}
/// Leent de gedeelde staat voor één stap van de eigenaar, of [`Error::Busy`].
fn rest(state: &Rc<RefCell<State>>) -> Result<core::cell::RefMut<'_, State>> {
    state.try_borrow_mut().map_err(|_| Error::Busy)
}
impl Replica {
    /// Prepare vóór SQLite openen, inclusief geverifieerde restore waar nodig.
    /// De [`Writer`] uit [`crate::writer::claim`] is het bewijs van de lease.
    pub fn prepare<B: Storage, S: Store>(
        b: &mut B,
        store: &mut S,
        writer: Writer,
        config: Config,
        now: Time,
        verify: impl FnMut(&mut B, &Name) -> Result,
    ) -> Result<Self> {
        Self::prepare_shared(b, store, Rc::new(RefCell::new(writer)), config, now, verify)
    }
    /// Als [`Replica::prepare`], met een lease die de host al deelt met zijn
    /// leasetaak: die vernieuwt dan ook tijdens een lang herstel in Prepare.
    pub fn prepare_shared<B: Storage, S: Store>(
        b: &mut B,
        store: &mut S,
        writer: Rc<RefCell<Writer>>,
        config: Config,
        now: Time,
        verify: impl FnMut(&mut B, &Name) -> Result,
    ) -> Result<Self> {
        config.validate()?;
        writer.try_borrow().map_err(|_| Error::Busy)?.check(now)?;
        let mut prepared = prepare::run(
            b,
            store,
            prepare::Options {
                namespace: &config.namespace,
                destination: &config.destination,
                path: config.path,
                page_limit: config.pages,
                now,
                adopt_local: config.adopt_local,
            },
            verify,
        )?;
        let resumable = match Batch::load_pending(b, config.path, &prepared.marker)? {
            Some((mut batch, staged)) => {
                // Wat sinds de capture dirty werd, blijft dirty bij de bevestiging:
                // het beeld krijgt de huidige revisie, de tracking gaat er één voorbij.
                batch.resumed_at(prepared.tracking.revision());
                prepared.tracking.resume_capture()?;
                Some(Pending { batch, staged })
            }
            None => None,
        };
        let status = Status {
            reason: prepared.reason,
            attempted: None,
            synced: None,
            error: None,
        };
        Ok(Self {
            config,
            writer,
            state: Rc::new(RefCell::new(State {
                marker: prepared.marker,
                tracking: prepared.tracking,
            })),
            closed: Rc::new(Cell::new(false)),
            status,
            maintenance_at: None,
            pending: false,
            maintaining: false,
            resumable,
        })
    }
    /// Of de volgende [`Replica::begin`] een onderbroken snapshot hervat.
    pub fn resumes(&self) -> bool {
        self.resumable.is_some()
    }
    /// De getrackte VFS. Leent de eigenaar niet (zie het rustprotocol): de
    /// eigenaar kan synchroniseren terwijl de verbinding openstaat. Na de
    /// leasedeadline weigert de VFS iedere write met `SQLITE_READONLY`.
    pub fn vfs<'a, B: Storage>(&self, b: &'a mut B) -> Result<Vfs<'a, B>> {
        self.tracker()?.vfs(b)
    }
    /// De SQL-kant van deze eigenaar, voor een VFS op een andere stack.
    pub fn tracker(&self) -> Result<Tracker> {
        if self.closed.get() {
            return Err(Error::State);
        }
        Ok(Tracker {
            path: self.config.path,
            writer: Rc::clone(&self.writer),
            state: Rc::clone(&self.state),
            closed: Rc::clone(&self.closed),
        })
    }
    /// De deadline op de klok van de opslag, of een harde fout als die voorbij is.
    fn fence<B: Storage>(&self, b: &mut B) -> Result<i64> {
        fence(&self.writer, b)
    }
    /// Vernieuwt de schrijverlease op leeftijd; zie [`Writer::renew`]. Na
    /// verlies is deze eigenaar permanent dicht.
    ///
    /// In stappen: de lening op de [`Writer`] loopt nooit over de store-I/O
    /// heen, zodat een VFS ook tijdens een trage PUT zijn deadline leest.
    /// Mag zonder rust lopen.
    pub fn renew<L: Backend>(&mut self, lease: &mut L, now: Time) -> Result {
        let result = renew_shared(&self.writer, lease, now);
        if result == Err(Error::LeaseLost) {
            self.closed.set(true);
        }
        result
    }
    /// De lease van deze eigenaar, met het fencing-token.
    pub fn writer(&self) -> Ref<'_, Writer> {
        self.writer.borrow()
    }
    /// Sluit deze eigenaar en geeft de lease vrij, zodat een lezer meteen kan
    /// overnemen. SQLite is dan al gesloten en elke [`Vfs`] gedropt; leeft er
    /// nog een (of een [`Tracker`]), dan verloopt de lease vanzelf.
    pub fn release<L: Backend>(self, lease: &mut L) -> Result {
        self.closed.set(true);
        let Self { writer, .. } = self;
        match Rc::try_unwrap(writer) {
            Ok(writer) => writer.into_inner().release(lease),
            // De leasetaak of een VFS houdt het handvat nog vast: laat de lease verlopen.
            Err(_) => Ok(()),
        }
    }
    /// Onveranderlijke marker voor status, archiefcatalogus en diagnose.
    ///
    /// # Panics
    /// Als de VFS de gedeelde staat op dit moment leent (buiten het
    /// rustprotocol); [`Replica::try_marker`] geeft dan een fout.
    pub fn marker(&self) -> Ref<'_, Marker> {
        Ref::map(self.state.borrow(), |s| &s.marker.value)
    }
    /// Als [`Replica::marker`], met [`Error::Busy`] in plaats van een panic.
    pub fn try_marker(&self) -> Result<Ref<'_, Marker>> {
        let state = self.state.try_borrow().map_err(|_| Error::Busy)?;
        Ok(Ref::map(state, |s| &s.marker.value))
    }
    /// De laatste beurt, ook na sluiten.
    pub fn status(&self) -> Status {
        self.status
    }
    /// Zet de eigenaar permanent dicht: elke VFS weigert vanaf nu writes; de
    /// host sluit SQLite en geeft daarna de lease vrij met [`Replica::release`].
    pub fn close(&mut self) {
        self.closed.set(true);
    }
    /// Is deze eigenaar dicht (gesloten, of de lease verloren)?
    pub fn is_closed(&self) -> bool {
        self.closed.get()
    }
    /// Hosttimer-aanroep: vóór het interval geen I/O. Ook fouten krijgen een interval
    /// voordat de automatische volgende poging mag beginnen. Alleen in rust.
    pub fn tick<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        now: Time,
    ) -> Result<Option<Synced>> {
        self.clock(now)?;
        if !self.due(now) {
            return Ok(None);
        }
        self.sync(b, store, now).map(Some)
    }
    fn due(&self, now: Time) -> bool {
        self.status
            .attempted
            .is_none_or(|last| now.seconds() - last.seconds() >= i64::from(self.config.interval))
    }
    /// Is een volgende [`Replica::tick`] of [`Replica::begin`] aan de beurt?
    /// Een host die voor de capture eerst rust moet vragen, vraagt dit eerst.
    pub fn is_due(&self, now: Time) -> bool {
        self.resumable.is_some() || (!self.pending && !self.maintaining && self.due(now))
    }
    fn clock(&self, now: Time) -> Result {
        if self.closed.get()
            || now.seconds() < 1_577_836_800
            || self.status.attempted.is_some_and(|last| now < last)
        {
            return Err(Error::State);
        }
        self.writer
            .try_borrow()
            .map_err(|_| Error::Busy)?
            .check(now)
    }
    /// Eén expliciete synchronisatie op de eigenaar: capture, upload en afronding
    /// in één beurt, in rust. Wie SQL wil laten doorlopen tijdens de upload van
    /// de delen gebruikt [`Replica::begin`] en [`Replica::finish`].
    pub fn sync<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        now: Time,
    ) -> Result<Synced> {
        self.clock(now)?;
        if self.pending || self.maintaining {
            return Err(Error::State);
        }
        let state = Rc::clone(&self.state);
        let mut st = rest(&state)?;
        self.status.attempted = Some(now);
        let result = match self.plan(&mut st, b, store, now) {
            Ok(Some(capturing)) => match capturing.run(b) {
                Ok(pending) => {
                    self.disarm(&mut st, b);
                    pending.upload(b, store).and_then(|uploaded| {
                        self.complete(&mut st, b, store, pending, uploaded, now)
                    })
                }
                Err(e) => return self.abandon_in(&mut st, b, e, now),
            },
            Ok(None) => self.maintain(&mut st, b, store, now),
            Err(e) => Err(e),
        };
        self.settle(result, now)
    }
    /// Alles vóór de upload van de delen: interval, onzekere commits, generatie
    /// en de capture. `None` als het interval nog loopt of als niets veranderde;
    /// in dat laatste geval is het onderhoud al gedaan. In rust; daarna mag
    /// SQL via de [`Vfs`] gewoon doorschrijven tot [`Replica::finish`], en is
    /// een tweede `begin` een fout.
    pub fn begin<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        now: Time,
    ) -> Result<Option<Pending>> {
        let state = Rc::clone(&self.state);
        let mut st = rest(&state)?;
        match self.begin_in(&mut st, b, store, now)? {
            None => Ok(None),
            Some(Begun::Upload(pending)) => Ok(Some(pending)),
            Some(Begun::Capture(capturing)) => match capturing.run(b) {
                Ok(pending) => Ok(Some(pending)),
                Err(e) => self.abandon_in(&mut st, b, e, now).map(|_| None),
            },
        }
    }
    /// Als [`Replica::begin`], maar het lezen van de pagina's gebeurt buiten
    /// de eigenaar: [`Capturing::run`] mag op een andere stack draaien terwijl
    /// SQL via de [`Vfs`] doorschrijft; de schaduw houdt het beeld op dit
    /// moment. De uitkomst gaat naar [`Replica::finish`] (na de upload) of
    /// [`Replica::abandon`] (een mislukte capture). Deze stap zelf in rust.
    pub fn begin_capture<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        now: Time,
    ) -> Result<Option<Begun>> {
        let state = Rc::clone(&self.state);
        let mut st = rest(&state)?;
        self.begin_in(&mut st, b, store, now)
    }
    fn begin_in<B: Storage, S: Store>(
        &mut self,
        st: &mut State,
        b: &mut B,
        store: &mut S,
        now: Time,
    ) -> Result<Option<Begun>> {
        self.clock(now)?;
        if self.pending {
            return Err(Error::State);
        }
        // Een onderhoudstaak loopt: de volgende beurt wacht tot die klaar is.
        if self.maintaining {
            return Ok(None);
        }
        // Een onderbroken snapshot gaat voor: de delen die er al staan blijven.
        if let Some(pending) = self.resumable.take() {
            self.status.attempted = Some(now);
            self.pending = true;
            return Ok(Some(Begun::Upload(pending)));
        }
        if !self.due(now) {
            return Ok(None);
        }
        self.status.attempted = Some(now);
        match self.plan(st, b, store, now) {
            Ok(Some(capturing)) => {
                self.pending = true;
                Ok(Some(Begun::Capture(capturing)))
            }
            Ok(None) if self.config.background_maintenance => {
                self.settle(Ok(Synced::default()), now).map(|_| None)
            }
            Ok(None) => {
                let result = self.maintain(st, b, store, now);
                self.settle(result, now).map(|_| None)
            }
            Err(e) => self.settle(Err(e), now).map(|_| None),
        }
    }
    /// Een [`Capturing::run`] die mislukte: de schaduw gaat weg, een beeld dat
    /// niet meer te maken was plant een nieuwe snapshot, en dit is de fout van
    /// deze beurt. In rust.
    pub fn abandon<B: Storage>(&mut self, b: &mut B, error: Error, now: Time) -> Result<Synced> {
        let state = Rc::clone(&self.state);
        let mut st = rest(&state)?;
        self.abandon_in(&mut st, b, error, now)
    }
    fn abandon_in<B: Storage>(
        &mut self,
        st: &mut State,
        b: &mut B,
        error: Error,
        now: Time,
    ) -> Result<Synced> {
        self.pending = false;
        self.disarm(st, b);
        if matches!(error, Error::ForeignWrite | Error::Gap | Error::State) {
            self.invalidate(st, b, false)?;
        }
        self.settle(Err(error), now)
    }
    fn disarm<B: Storage>(&mut self, st: &mut State, b: &mut B) {
        st.tracking.disarm();
        if let Ok(name) = self.config.path.suffix(".replica-shadow") {
            let _ = local::write(b, &name, b"");
        }
    }
    /// Rondt een [`Replica::begin`] af met het resultaat van [`Pending::upload`]:
    /// manifest, marker, bevestiging en onderhoud. Een mislukte upload is de
    /// fout van deze beurt; de volgende capture neemt dezelfde pagina's mee.
    /// In rust: het manifest is één kleine PUT, de rest lokaal.
    pub fn finish<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        pending: Pending,
        uploaded: Result<Uploaded>,
        now: Time,
    ) -> Result<Synced> {
        let state = Rc::clone(&self.state);
        let mut st = rest(&state)?;
        self.pending = false;
        self.disarm(&mut st, b);
        self.clock(now)?;
        let result =
            uploaded.and_then(|uploaded| self.complete(&mut st, b, store, pending, uploaded, now));
        self.settle(result, now)
    }
    /// Het onderhoud als losse taak (met [`Config::background_maintenance`]):
    /// hoogstens eens per minuut, nooit naast een andere taak. Legt de
    /// frontier op `now` duurzaam vast, zodat elk venster dat de taak
    /// samenvoegt binnen de frontier valt en nieuwe commits erbuiten. In rust.
    pub fn maintenance_begin<B: Storage>(
        &mut self,
        b: &mut B,
        now: Time,
    ) -> Result<Option<Maintenance>> {
        self.clock(now)?;
        let state = Rc::clone(&self.state);
        let mut st = rest(&state)?;
        if self.maintaining
            || self.pending
            || !st.marker.value.complete
            || st.marker.value.uncertain != 0
            || self
                .maintenance_at
                .is_some_and(|last| now.seconds() - last.seconds() < 60)
        {
            return Ok(None);
        }
        self.maintenance_at = Some(now);
        let sealed = Time::unix(now.seconds(), 0)?;
        if sealed > st.marker.value.sealed_at {
            self.fence(b)?;
            st.marker.value.sealed_at = sealed;
            st.marker.save(b)?;
        }
        let mut levels = alloc::vec::Vec::new();
        crate::reserve(&mut levels, self.config.schedule.levels().len())?;
        levels.extend_from_slice(self.config.schedule.levels());
        self.maintaining = true;
        Ok(Some(Maintenance {
            namespace: string(&self.config.namespace)?,
            generation: string(&st.marker.value.generation)?,
            sequence: st.marker.value.sequence,
            sealed_at: st.marker.value.sealed_at,
            now,
            levels,
            budget: (self.config.pages, self.config.segment_bytes),
            retention: self.config.retention,
        }))
    }
    /// Rondt een [`Maintenance::run`] af. Bewezen archiefschade plant, net als
    /// het inline onderhoud, een nieuwe snapshot; een transportfout is alleen
    /// de fout van deze beurt. In rust.
    pub fn maintenance_finish<B: Storage>(
        &mut self,
        b: &mut B,
        result: Result<Synced>,
    ) -> Result<Synced> {
        let state = Rc::clone(&self.state);
        let mut st = rest(&state)?;
        self.maintaining = false;
        match result {
            Ok(synced) => Ok(synced),
            Err(e) => {
                let e = self.archive_error(&mut st, b, e);
                self.status.error = Some(e);
                Err(e)
            }
        }
    }
    fn settle(&mut self, result: Result<Synced>, now: Time) -> Result<Synced> {
        self.status.error = result.as_ref().err().copied();
        if result.is_ok() {
            self.status.synced = Some(now);
        }
        result
    }
    fn invalidate<B: Storage>(&mut self, st: &mut State, b: &mut B, damaged: bool) -> Result {
        let old = &st.marker.value;
        let mut marker = Marker::new(&self.config.destination, "", Time::ZERO)?;
        // Behoud herkomstbewijs, ook bij onbetrouwbare lokale tracking. Deze
        // generatie is geen fallback, maar current mag tijdens de nieuwe
        // snapshotpoging nog naar haar wijzen.
        marker.repair_from = string(if old.complete {
            &old.generation
        } else {
            &old.repair_from
        })?;
        let mut local = LocalMarker::new(self.config.path, marker)?;
        // Eerst duurzaam besluiten; pas dan de onbetrouwbare trackingstaat loslaten.
        let tracking = Tracking::new(self.config.path, self.config.pages)?;
        if let Err(e) = local.save(b) {
            // De herstelbeslissing kan half op schijf staan; eerst opnieuw Prepare.
            self.closed.set(true);
            return Err(e);
        }
        st.marker = local;
        st.tracking = tracking;
        self.status.reason = if damaged {
            Reason::Repair
        } else {
            Reason::NewSnapshot
        };
        self.maintenance_at = None;
        Ok(())
    }
    fn archive_error<B: Storage>(&mut self, st: &mut State, b: &mut B, error: Error) -> Error {
        if matches!(error, Error::Corrupt | Error::Gap | Error::Legacy)
            && let Err(e) = self.invalidate(st, b, true)
        {
            return e;
        }
        error
    }
    /// Onzekere commits oplossen, de generatie alleen op leeftijd vernieuwen,
    /// en het plan van de capture met de schaduw gewapend; nog geen pagina's.
    fn plan<B: Storage, S: Store>(
        &mut self,
        st: &mut State,
        b: &mut B,
        store: &mut S,
        now: Time,
    ) -> Result<Option<Capturing>> {
        if st.marker.value.uncertain != 0
            && let Err(e) = replication::resolve(b, store, &self.config.namespace, &mut st.marker)
        {
            return Err(self.archive_error(st, b, e));
        }
        let renew = st.marker.value.generation.is_empty()
            || (st.marker.value.complete
                && maintenance::renewal_due(&st.marker.value, now, self.config.generation)?);
        if renew {
            if st.marker.value.complete {
                self.status.reason = Reason::NewSnapshot;
            }
            replication::renew(b, &mut st.marker, now)?;
            self.maintenance_at = None;
        }
        let local = &st.marker.value;
        if local.generation.is_empty()
            || local.uncertain != 0
            || (!local.complete && local.sequence != 0)
        {
            return Err(Error::State);
        }
        let plan = match Plan::new(
            b,
            self.config.path,
            &st.tracking,
            !local.complete,
            local.size,
            self.config.segment_bytes,
        ) {
            Ok(Some(plan)) => plan,
            Ok(None) => return Ok(None),
            Err(e @ (Error::ForeignWrite | Error::Gap | Error::State)) => {
                self.invalidate(st, b, false)?;
                return Err(e);
            }
            Err(e) => return Err(e),
        };
        // Het beeld is dat van nu: wat vanaf hier geschreven wordt, is ná deze
        // capture geschreven en gaat eerst oud de schaduw in.
        st.tracking.captured();
        st.tracking.arm(plan.shadow());
        Ok(Some(Capturing {
            plan,
            namespace: string(&self.config.namespace)?,
            generation: string(&st.marker.value.generation)?,
            previous_sequence: st.marker.value.sequence,
            path: self.config.path,
            now,
        }))
    }
    /// Manifest en marker na de upload, daarna het onderhoud.
    fn complete<B: Storage, S: Store>(
        &mut self,
        st: &mut State,
        b: &mut B,
        store: &mut S,
        pending: Pending,
        uploaded: Uploaded,
        now: Time,
    ) -> Result<Synced> {
        // De upload kan lang duren; het manifest gaat alleen uit onder een
        // geldige lease, gemeten nu en niet bij het begin van de beurt.
        self.fence(b)?;
        if let Err(e) =
            pending
                .batch
                .finish(b, store, uploaded, &mut st.marker, &mut st.tracking, now)
        {
            return Err(self.archive_error(st, b, e));
        }
        Batch::clear_pending(b, self.config.path)?;
        let mut result = if self.config.background_maintenance {
            Synced::default()
        } else {
            self.maintain(st, b, store, now)?
        };
        result.published = true;
        Ok(result)
    }
    /// Onderhoud hoogstens eens per minuut; ook na een fout geen hotloop bij
    /// iedere SQL-wijziging.
    fn maintain<B: Storage, S: Store>(
        &mut self,
        st: &mut State,
        b: &mut B,
        store: &mut S,
        now: Time,
    ) -> Result<Synced> {
        let mut result = Synced::default();
        if st.marker.value.complete
            && self
                .maintenance_at
                .is_none_or(|last| now.seconds() - last.seconds() >= 60)
        {
            self.maintenance_at = Some(now);
            result.maintenance = match maintenance::run(
                b,
                store,
                &self.config.namespace,
                &mut st.marker,
                &self.config.schedule,
                now,
                Budget {
                    pages: self.config.pages,
                    segment_bytes: self.config.segment_bytes,
                },
            ) {
                Ok(report) => report,
                Err(e) => return Err(self.archive_error(st, b, e)),
            };
            result.expired_objects = match maintenance::generations(
                store,
                &self.config.namespace,
                &st.marker.value.generation,
                now,
                self.config.retention,
            ) {
                Ok(removed) => removed,
                Err(e) => return Err(self.archive_error(st, b, e)),
            };
        }
        Ok(result)
    }
}
/// De vernieuwing in stappen over een gedeelde [`Writer`]: elke lening is
/// kort en geen enkele loopt over de I/O van `lease` heen.
fn renew_shared<L: Backend>(writer: &RefCell<Writer>, lease: &mut L, now: Time) -> Result {
    let borrow = || writer.try_borrow_mut().map_err(|_| Error::Busy);
    let Some(renewal) = borrow()?.renew_request(now)? else {
        return Ok(());
    };
    let written = lease.write(&renewal.prev, &renewal.state);
    match borrow()?.renew_commit(&renewal, written)? {
        Renewed::Done => Ok(()),
        Renewed::Reclaim => {
            let read = lease.read();
            let again = borrow()?.reclaim_request(read, renewal.at)?;
            match again {
                Some(again) => {
                    let written = lease.write(&again.prev, &again.state);
                    borrow()?.reclaim_commit(&again, written)
                }
                None => Ok(()),
            }
        }
    }
}
