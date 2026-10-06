//! De expliciete Replica-levensloop van één app-eigenaar; geen verborgen threads.
use crate::{
    Error, Result,
    local::Name,
    maintenance::{self, Budget, Report, Schedule},
    marker::{LocalMarker, Marker},
    object::Store,
    prepare::{self, Prepared, Reason},
    replication::{self, Batch, Staged, Uploaded},
    string,
    time::Time,
    tracking::{Tracked, Tracking},
    writer::{Backend, Writer},
};
use alloc::rc::Rc;
use alloc::string::String;
use core::cell::RefCell;
use replica_sqlite::Storage;
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
/// Eén eigenaar van marker, dirty logs en cadans. Alle SQL-writes lopen via `vfs`;
/// SQL is gesloten vóór `sync`/`tick`. Voor overlappende SQL en upload kan een app
/// de losse capture/publicatie-API gebruiken met expliciete eigendomsoverdracht.
///
/// Alleen de houder van de schrijverlease maakt een eigenaar. Na de deadline van
/// die lease weigeren VFS, synchronisatie en publicatie hard; de app roept
/// [`Replica::renew`] daarom ook aan terwijl opslag of netwerk parkeert.
/// Deze bibliotheek verstopt geen achtergrondtaak of runtime.
pub struct Replica {
    config: Config,
    /// Gedeeld met de leasetaak van de host: die vernieuwt de lease op haar
    /// eigen stack, hoe lang een capture of upload hier ook duurt.
    writer: Rc<RefCell<Writer>>,
    prepared: Prepared,
    status: Status,
    maintenance_at: Option<Time>,
    closed: bool,
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
        writer.borrow().check(now)?;
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
            prepared,
            status,
            maintenance_at: None,
            closed: false,
            pending: false,
            maintaining: false,
            resumable,
        })
    }
    /// Of de volgende [`Replica::begin`] een onderbroken snapshot hervat.
    pub fn resumes(&self) -> bool {
        self.resumable.is_some()
    }
    /// De getrackte VFS. De lening sluit gelijktijdig synchroniseren of sluiten uit.
    /// Na de leasedeadline weigert de VFS iedere write met `SQLITE_READONLY`.
    pub fn vfs<'a, B: Storage>(&'a mut self, b: &'a mut B) -> Result<Tracked<'a, B, LocalMarker>> {
        if self.closed {
            return Err(Error::State);
        }
        let deadline = self.fence(b)?;
        Ok(self
            .prepared
            .tracking
            .wrap(b, &mut self.prepared.marker)
            .until(deadline))
    }
    /// De deadline op de klok van de opslag, of een harde fout als die voorbij is.
    fn fence<B: Storage>(&self, b: &mut B) -> Result<i64> {
        let deadline = i64::try_from(self.writer.borrow().deadline()).map_err(|_| Error::Limit)?;
        if b.unix_millis()? >= deadline {
            return Err(Error::LeaseLost);
        }
        Ok(deadline)
    }
    /// Vernieuwt de schrijverlease op leeftijd; zie [`Writer::renew`]. Na
    /// verlies is deze eigenaar permanent dicht.
    pub fn renew<L: Backend>(&mut self, lease: &mut L, now: Time) -> Result {
        let result = self.writer.borrow_mut().renew(lease, now);
        if result == Err(Error::LeaseLost) {
            self.closed = true;
        }
        result
    }
    /// De lease van deze eigenaar, met het fencing-token.
    pub fn writer(&self) -> core::cell::Ref<'_, Writer> {
        self.writer.borrow()
    }
    /// Sluit deze eigenaar en geeft de lease vrij, zodat een lezer meteen kan
    /// overnemen. SQLite is dan al gesloten.
    pub fn release<L: Backend>(mut self, lease: &mut L) -> Result {
        self.close();
        match Rc::try_unwrap(self.writer) {
            Ok(writer) => writer.into_inner().release(lease),
            // De leasetaak houdt het handvat nog vast: laat de lease verlopen.
            Err(_) => Ok(()),
        }
    }
    /// Onveranderlijke marker voor status, archiefcatalogus en diagnose.
    pub fn marker(&self) -> &Marker {
        &self.prepared.marker.value
    }
    /// De laatste beurt, ook na sluiten.
    pub fn status(&self) -> Status {
        self.status
    }
    /// Zet de eigenaar permanent dicht. Actieve VFS-leningen moeten eerst eindigen;
    /// de host sluit SQLite en geeft daarna de lease vrij met [`Replica::release`].
    pub fn close(&mut self) {
        self.closed = true;
    }
    /// Hosttimer-aanroep: vóór het interval geen I/O. Ook fouten krijgen een interval
    /// voordat de automatische volgende poging mag beginnen.
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
    fn clock(&self, now: Time) -> Result {
        if self.closed
            || now.seconds() < 1_577_836_800
            || self.status.attempted.is_some_and(|last| now < last)
        {
            return Err(Error::State);
        }
        self.writer.borrow().check(now)
    }
    /// Eén expliciete synchronisatie op de eigenaar: capture, upload en afronding
    /// in één beurt. Wie SQL wil laten doorlopen tijdens de upload van de delen
    /// gebruikt [`Replica::begin`] en [`Replica::finish`].
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
        self.status.attempted = Some(now);
        let result = match self.capture(b, store, now) {
            Ok(Some(pending)) => pending
                .upload(b, store)
                .and_then(|uploaded| self.complete(b, store, pending, uploaded, now)),
            Ok(None) => self.maintain(b, store, now),
            Err(e) => Err(e),
        };
        self.settle(result, now)
    }
    /// Alles vóór de upload van de delen: interval, onzekere commits, generatie
    /// en de capture. `None` als het interval nog loopt of als niets veranderde;
    /// in dat laatste geval is het onderhoud al gedaan. Tot [`Replica::finish`]
    /// schrijft SQL via [`Replica::vfs`] gewoon door, en is een tweede `begin`
    /// een fout.
    pub fn begin<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        now: Time,
    ) -> Result<Option<Pending>> {
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
            return Ok(Some(pending));
        }
        if !self.due(now) {
            return Ok(None);
        }
        self.status.attempted = Some(now);
        match self.capture(b, store, now) {
            Ok(Some(pending)) => {
                self.pending = true;
                Ok(Some(pending))
            }
            Ok(None) if self.config.background_maintenance => {
                self.settle(Ok(Synced::default()), now).map(|_| None)
            }
            Ok(None) => {
                let result = self.maintain(b, store, now);
                self.settle(result, now).map(|_| None)
            }
            Err(e) => self.settle(Err(e), now).map(|_| None),
        }
    }
    /// Rondt een [`Replica::begin`] af met het resultaat van [`Pending::upload`]:
    /// manifest, marker, bevestiging en onderhoud. Een mislukte upload is de
    /// fout van deze beurt; de volgende capture neemt dezelfde pagina's mee.
    pub fn finish<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        pending: Pending,
        uploaded: Result<Uploaded>,
        now: Time,
    ) -> Result<Synced> {
        self.pending = false;
        self.clock(now)?;
        let result = uploaded.and_then(|uploaded| self.complete(b, store, pending, uploaded, now));
        self.settle(result, now)
    }
    /// Het onderhoud als losse taak (met [`Config::background_maintenance`]):
    /// hoogstens eens per minuut, nooit naast een andere taak. Legt de
    /// frontier op `now` duurzaam vast, zodat elk venster dat de taak
    /// samenvoegt binnen de frontier valt en nieuwe commits erbuiten.
    pub fn maintenance_begin<B: Storage>(
        &mut self,
        b: &mut B,
        now: Time,
    ) -> Result<Option<Maintenance>> {
        self.clock(now)?;
        if self.maintaining
            || self.pending
            || !self.prepared.marker.value.complete
            || self.prepared.marker.value.uncertain != 0
            || self
                .maintenance_at
                .is_some_and(|last| now.seconds() - last.seconds() < 60)
        {
            return Ok(None);
        }
        self.maintenance_at = Some(now);
        let sealed = Time::unix(now.seconds(), 0)?;
        if sealed > self.prepared.marker.value.sealed_at {
            self.fence(b)?;
            self.prepared.marker.value.sealed_at = sealed;
            self.prepared.marker.save(b)?;
        }
        let mut levels = alloc::vec::Vec::new();
        crate::reserve(&mut levels, self.config.schedule.levels().len())?;
        levels.extend_from_slice(self.config.schedule.levels());
        self.maintaining = true;
        Ok(Some(Maintenance {
            namespace: string(&self.config.namespace)?,
            generation: string(&self.prepared.marker.value.generation)?,
            sequence: self.prepared.marker.value.sequence,
            sealed_at: self.prepared.marker.value.sealed_at,
            now,
            levels,
            budget: (self.config.pages, self.config.segment_bytes),
            retention: self.config.retention,
        }))
    }
    /// Rondt een [`Maintenance::run`] af. Bewezen archiefschade plant, net als
    /// het inline onderhoud, een nieuwe snapshot; een transportfout is alleen
    /// de fout van deze beurt.
    pub fn maintenance_finish<B: Storage>(
        &mut self,
        b: &mut B,
        result: Result<Synced>,
    ) -> Result<Synced> {
        self.maintaining = false;
        match result {
            Ok(synced) => Ok(synced),
            Err(e) => {
                let e = self.archive_error(b, e);
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
    fn invalidate<B: Storage>(&mut self, b: &mut B, damaged: bool) -> Result {
        let old = &self.prepared.marker.value;
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
            self.closed = true;
            return Err(e);
        }
        self.prepared.marker = local;
        self.prepared.tracking = tracking;
        self.status.reason = if damaged {
            Reason::Repair
        } else {
            Reason::NewSnapshot
        };
        self.maintenance_at = None;
        Ok(())
    }
    fn archive_error<B: Storage>(&mut self, b: &mut B, error: Error) -> Error {
        if matches!(error, Error::Corrupt | Error::Gap | Error::Legacy)
            && let Err(e) = self.invalidate(b, true)
        {
            return e;
        }
        error
    }
    /// Onzekere commits oplossen, de generatie alleen op leeftijd vernieuwen,
    /// en de capture met zijn objectsleutels; nog geen upload.
    fn capture<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        now: Time,
    ) -> Result<Option<Pending>> {
        if self.prepared.marker.value.uncertain != 0
            && let Err(e) =
                replication::resolve(b, store, &self.config.namespace, &mut self.prepared.marker)
        {
            return Err(self.archive_error(b, e));
        }
        let renew = self.prepared.marker.value.generation.is_empty()
            || (self.prepared.marker.value.complete
                && maintenance::renewal_due(
                    &self.prepared.marker.value,
                    now,
                    self.config.generation,
                )?);
        if renew {
            if self.prepared.marker.value.complete {
                self.status.reason = Reason::NewSnapshot;
            }
            replication::renew(b, &mut self.prepared.marker, now)?;
            self.maintenance_at = None;
        }
        let batch = match Batch::capture(
            b,
            self.config.path,
            &self.prepared.tracking,
            &self.prepared.marker,
            self.config.segment_bytes,
        ) {
            Ok(Some(batch)) => {
                // De bron was tijdens de capture exclusief: wat nu nog
                // geschreven wordt, is ná deze capture geschreven.
                self.prepared.tracking.captured();
                batch
            }
            Ok(None) => return Ok(None),
            Err(e @ (Error::ForeignWrite | Error::Gap | Error::State)) => {
                self.invalidate(b, false)?;
                return Err(e);
            }
            Err(e) => return Err(e),
        };
        let staged = batch.stage(b, &self.config.namespace, &self.prepared.marker, now)?;
        batch.save_pending(b, self.config.path, &staged)?;
        Ok(Some(Pending { batch, staged }))
    }
    /// Manifest en marker na de upload, daarna het onderhoud.
    fn complete<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        pending: Pending,
        uploaded: Uploaded,
        now: Time,
    ) -> Result<Synced> {
        // De upload kan lang duren; het manifest gaat alleen uit onder een
        // geldige lease, gemeten nu en niet bij het begin van de beurt.
        self.fence(b)?;
        if let Err(e) = pending.batch.finish(
            b,
            store,
            uploaded,
            &mut self.prepared.marker,
            &mut self.prepared.tracking,
            now,
        ) {
            return Err(self.archive_error(b, e));
        }
        Batch::clear_pending(b, self.config.path)?;
        let mut result = if self.config.background_maintenance {
            Synced::default()
        } else {
            self.maintain(b, store, now)?
        };
        result.published = true;
        Ok(result)
    }
    /// Onderhoud hoogstens eens per minuut; ook na een fout geen hotloop bij
    /// iedere SQL-wijziging.
    fn maintain<B: Storage, S: Store>(
        &mut self,
        b: &mut B,
        store: &mut S,
        now: Time,
    ) -> Result<Synced> {
        let mut result = Synced::default();
        if self.prepared.marker.value.complete
            && self
                .maintenance_at
                .is_none_or(|last| now.seconds() - last.seconds() >= 60)
        {
            self.maintenance_at = Some(now);
            result.maintenance = match maintenance::run(
                b,
                store,
                &self.config.namespace,
                &mut self.prepared.marker,
                &self.config.schedule,
                now,
                Budget {
                    pages: self.config.pages,
                    segment_bytes: self.config.segment_bytes,
                },
            ) {
                Ok(report) => report,
                Err(e) => return Err(self.archive_error(b, e)),
            };
            result.expired_objects = match maintenance::generations(
                store,
                &self.config.namespace,
                &self.prepared.marker.value.generation,
                now,
                self.config.retention,
            ) {
                Ok(removed) => removed,
                Err(e) => return Err(self.archive_error(b, e)),
            };
        }
        Ok(result)
    }
}
