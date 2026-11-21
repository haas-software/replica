//! Een groep: meerdere databases onder één schrijverlease, met echte SQLite
//! op een hostmap en de bucket in een map (de toetsen van EasyFlor die met de
//! groep naar replica verhuisden, 21-11-2026).
//!
//! 1. Banen: een capture die onderweg is als een write komt, bevestigt die
//!    write niet; de volgende wel. Zolang de banen niet lopen (S3 hangt), doet
//!    de eigenaar geen store-call en wachten writes na de schrijfdeadline.
//! 2. Een korte storing: vernieuwingen en publicaties worden met backoff
//!    opnieuw geprobeerd, writes wachten voorbij de deadline, de lease komt
//!    terug met dezelfde generatie (`recovered`). Langer dan de lease: verloren.
//! 3. Vrijgeven: na een eerste DELETE die faalt nog eens, bij naam; en bij
//!    naam als een vernieuwing onderweg de Writer nog deelt.
//! 4. Een herstel langer dan de lease vernieuwt hem, ook binnen een ronde van
//!    16 GET's: de volgende database komt nog binnen.
//! 5. Een lezer volgt de schrijver met alleen de incrementen; een schrijver
//!    op de map van de lezer zet die kopie voort.
//!
//! De C-runtime van SQLite is procesglobaal en de klok is een static: de
//! toetsen wachten op elkaar.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use replica_core::{
    Error,
    follow::{self, Follow, Outcome},
    group::{self, Bucket, Config, Group, Report, Standing, Ticking},
    host::{Dir, DirLease},
    lanes::{Job, Lane, LaneSide, Offload},
    local::Name,
    object::{Object, Parallel, Replies, Store, StoreError},
    writer::{self, Backend, Held, LeaseState, Role, Writer, discovery},
};
use replica_sqlite::{
    Connection, Engine, FileId, OpenFlags, Storage, Value,
    memory::{Memory, Slot},
};
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    ffi::{CStr, CString},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        Mutex,
        atomic::{AtomicI64, Ordering::SeqCst},
    },
};

static SQLITE: Mutex<()> = Mutex::new(());
/// Unix-milliseconden, verzet door de toets (en de bucket).
static CLOCK: AtomicI64 = AtomicI64::new(0);
const TTL: u64 = 30_000;

fn millis() -> i64 {
    CLOCK.load(SeqCst)
}
fn advance(ms: i64) {
    CLOCK.fetch_add(ms, SeqCst);
}

/// Wat de groep meldt, als regels.
#[derive(Clone, Default)]
struct Lines(Rc<RefCell<Vec<String>>>);
impl Lines {
    fn has(&self, needle: &str) -> bool {
        self.0.borrow().iter().any(|l| l.contains(needle))
    }
    fn since(&self, n: usize) -> Vec<String> {
        self.0.borrow()[n..].to_vec()
    }
    fn len(&self) -> usize {
        self.0.borrow().len()
    }
}
impl Report for Lines {
    fn retry(&self, file: &str, step: &str, e: Error, cause: group::Failure, n: u32, next: i64) {
        self.0.borrow_mut().push(format!(
            "retry file={file} step={step} error={e:?} cause={} retry={n} next_ms={next}",
            cause.name()
        ));
    }
    fn recovered(&self, retries: u32, ms: i64) {
        self.0
            .borrow_mut()
            .push(format!("recovered retries={retries} ms={ms}"));
    }
    fn lost(&self, file: &str, step: &str, e: Error, cause: group::Failure, retries: u32) {
        self.0.borrow_mut().push(format!(
            "lost file={file} step={step} error={e:?} cause={} retries={retries}",
            cause.name()
        ));
    }
    fn failed(&self, file: &str, step: &str, e: Error) {
        self.0
            .borrow_mut()
            .push(format!("failed file={file} step={step} error={e:?}"));
    }
    fn published(&self, file: &str) {
        self.0.borrow_mut().push(format!("published file={file}"));
    }
}

/// De storingen van een bucket, gedeeld door alle handvatten erop.
#[derive(Default)]
struct Faults {
    /// Elke call faalt met transport, na een "time-out" van `timeout_ms`.
    down: Cell<bool>,
    timeout_ms: Cell<i64>,
    /// Elke GET kost zoveel ms van de klok (ook binnen een ronde).
    get_ms: Cell<i64>,
    /// Zoveel DELETE's van de lease falen nog met transport (vóór de store).
    lease_deletes: Cell<usize>,
}

/// De bucket van de toets: een map, met storingen op commando en een teller
/// van de calls van dit handvat.
struct Faulty {
    dir: Dir,
    faults: Rc<Faults>,
    calls: Rc<Cell<usize>>,
    parallel: Parallel,
}
impl Faulty {
    fn new(root: &Path, faults: &Rc<Faults>) -> Self {
        Faulty {
            dir: Dir::volatile(root).unwrap(),
            faults: faults.clone(),
            calls: Rc::new(Cell::new(0)),
            parallel: Parallel {
                requests: 1,
                bytes: 0,
            },
        }
    }
    fn call(&self) -> Result<(), StoreError> {
        self.calls.set(self.calls.get() + 1);
        if self.faults.down.get() {
            advance(self.faults.timeout_ms.get());
            return Err(StoreError::Transport);
        }
        Ok(())
    }
    fn gets(&self, n: usize) {
        advance(self.faults.get_ms.get() * n as i64);
    }
}
impl Store for Faulty {
    fn put(&mut self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.call()?;
        self.dir.put(key, bytes)
    }
    fn get(&mut self, key: &str, limit: usize) -> Result<Vec<u8>, StoreError> {
        self.call()?;
        self.gets(1);
        self.dir.get(key, limit)
    }
    fn parallel(&self) -> Parallel {
        self.parallel
    }
    fn get_all(&mut self, keys: &[(&str, usize)]) -> Result<Replies, StoreError> {
        self.call()?;
        self.gets(keys.len());
        Ok(keys.iter().map(|&(k, l)| self.dir.get(k, l)).collect())
    }
    fn delete(&mut self, key: &str) -> Result<(), StoreError> {
        self.call()?;
        self.dir.delete(key)
    }
    fn list(&mut self, prefix: &str, limit: usize) -> Result<Vec<Object>, StoreError> {
        self.call()?;
        self.dir.list(prefix, limit)
    }
}
/// De lease over [`Faulty`], met dezelfde storingen.
struct FaultyLease {
    inner: DirLease,
    faults: Rc<Faults>,
    calls: Rc<Cell<usize>>,
}
impl FaultyLease {
    fn call(&self) -> discovery::Result<()> {
        self.calls.set(self.calls.get() + 1);
        if self.faults.down.get() {
            advance(self.faults.timeout_ms.get());
            return Err(discovery::Error::Unreachable);
        }
        Ok(())
    }
}
impl Backend for FaultyLease {
    fn read(&mut self) -> discovery::Result<(LeaseState, String)> {
        self.call()?;
        self.inner.read()
    }
    fn write(&mut self, prev: &str, state: &LeaseState) -> discovery::Result<String> {
        self.call()?;
        self.inner.write(prev, state)
    }
    fn delete(&mut self, handle: &str) -> discovery::Result {
        self.call()?;
        let left = self.faults.lease_deletes.get();
        if left > 0 {
            self.faults.lease_deletes.set(left - 1);
            return Err(discovery::Error::Unreachable);
        }
        self.inner.delete(handle)
    }
}
impl Bucket for Faulty {
    type Lease<'l> = FaultyLease;
    fn lease<'l>(&'l mut self, key: &'l str, ttl_ms: u64) -> Result<FaultyLease, StoreError> {
        Ok(FaultyLease {
            inner: self.dir.lease(key, ttl_ms)?,
            faults: self.faults.clone(),
            calls: self.calls.clone(),
        })
    }
    fn identity(&self) -> (String, String) {
        self.dir.identity()
    }
}

/// Een hostmap met de toetsklok als wandklok: de VFS meet de schrijfdeadline
/// van de lease op de klok van zijn opslag.
struct Local(replica_sqlite::host::Dir);
impl Local {
    fn volatile(path: impl AsRef<Path>) -> replica_sqlite::Result<Self> {
        replica_sqlite::host::Dir::volatile(path).map(Local)
    }
}
impl Storage for Local {
    fn open(&mut self, name: &CStr, flags: OpenFlags) -> replica_sqlite::Result<FileId> {
        self.0.open(name, flags)
    }
    fn close(&mut self, file: FileId) -> replica_sqlite::Result {
        self.0.close(file)
    }
    fn read(&mut self, file: FileId, offset: u64, dst: &mut [u8]) -> replica_sqlite::Result<usize> {
        self.0.read(file, offset, dst)
    }
    fn write(&mut self, file: FileId, offset: u64, src: &[u8]) -> replica_sqlite::Result {
        self.0.write(file, offset, src)
    }
    fn truncate(&mut self, file: FileId, size: u64) -> replica_sqlite::Result {
        self.0.truncate(file, size)
    }
    fn sync(&mut self, file: FileId, flags: i32) -> replica_sqlite::Result {
        self.0.sync(file, flags)
    }
    fn size(&mut self, file: FileId) -> replica_sqlite::Result<u64> {
        self.0.size(file)
    }
    fn remove(&mut self, name: &CStr, sync_directory: bool) -> replica_sqlite::Result {
        self.0.remove(name, sync_directory)
    }
    fn exists(&mut self, name: &CStr) -> replica_sqlite::Result<bool> {
        self.0.exists(name)
    }
    fn random(&mut self, dst: &mut [u8]) -> replica_sqlite::Result {
        self.0.random(dst)
    }
    fn unix_millis(&mut self) -> replica_sqlite::Result<i64> {
        Ok(millis())
    }
}

/// Een toets: een map, de bucket erin, de klok op nu.
struct Bed {
    root: PathBuf,
    faults: Rc<Faults>,
}
impl Bed {
    fn new(name: &str) -> Self {
        // De VFS meet de schrijfdeadline op de klok van zijn opslag (de wandklok
        // van de host): de toetsklok begint op nu.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        CLOCK.store(i64::try_from(now).unwrap(), SeqCst);
        let root =
            std::env::temp_dir().join(format!("replica-group-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        Bed {
            root,
            faults: Rc::new(Faults::default()),
        }
    }
    fn bucket(&self) -> Faulty {
        Faulty::new(&self.root.join("bucket"), &self.faults)
    }
    fn local(&self, node: &str) -> Local {
        Local::volatile(self.root.join(node).join("demo")).unwrap()
    }
    fn config(&self) -> Config {
        Config {
            prefix: "replica".into(),
            domain: "demo".into(),
            ttl_ms: TTL,
            pages: 1 << 25,
            interval: 15,
            compress: true,
        }
    }
    /// De lease van het domein voor `node`.
    fn claim(&self, node: &str, bucket: &mut Faulty) -> Option<Writer> {
        let key = self.config().lease_key();
        let now = group::time_at(millis()).unwrap();
        let mut lease = bucket.lease(&key, TTL).unwrap();
        match writer::claim(&mut self.local(node), &mut lease, node, TTL, now).unwrap() {
            Role::Writer(w) => Some(w),
            Role::Reader { .. } => None,
        }
    }
}
impl Drop for Bed {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Controleert een herstelde scratchdatabase met echte SQLite.
fn checker<'e, 'a, E: Storage>(
    engine: &'e Engine<'a, E>,
) -> impl Fn(&mut Local, &Name, &dyn Fn()) -> replica_core::Result + 'e {
    move |b: &mut Local, name: &Name, tick: &dyn Fn()| {
        let mut ticking = Ticking { inner: b, tick };
        let mut conn = engine.open_with(name.cstr()?, &mut ticking)?;
        let mut s = conn.prepare(c"PRAGMA quick_check")?;
        let ok = s.step()? && s.column(0)? == Value::Text("ok");
        if ok { Ok(()) } else { Err(Error::Corrupt) }
    }
}

fn exec<B: Storage>(db: &mut Connection<'_, '_, B>, sql: &str) {
    db.execute(&CString::new(sql).unwrap()).unwrap();
}
fn try_exec<B: Storage>(db: &mut Connection<'_, '_, B>, sql: &str) -> replica_sqlite::Result {
    db.execute(&CString::new(sql).unwrap())
}
fn count<B: Storage>(db: &mut Connection<'_, '_, B>, sql: &str) -> i64 {
    let sql = CString::new(sql).unwrap();
    let mut s = db.prepare(&sql).unwrap();
    assert!(s.step().unwrap());
    match s.column(0).unwrap() {
        Value::Integer(n) => n,
        v => panic!("{v:?}"),
    }
}

/// Een engine op een RAM-opslag; elke database opent met zijn eigen opslag.
fn engine<'a>(heap: &'a mut [u64], memory: &'a mut Memory<'a, 1>) -> Engine<'a, Memory<'a, 1>> {
    // SAFETY: Eén engine tegelijk in deze binary (`SQLITE`), niet vanaf een andere thread.
    unsafe { Engine::initialize(heap, memory) }.unwrap()
}

/// De banen op commando van de toets: een job in de rij is een upload onderweg.
struct Lanes {
    lease: RefCell<VecDeque<Job>>,
    data: RefCell<VecDeque<Job>>,
    side: RefCell<Side>,
}
/// De kant van de banen: eigen handvatten op de lokale mappen en de bucket.
struct Side {
    local: PathBuf,
    bucket: Faulty,
}
impl LaneSide for Side {
    fn files(
        &mut self,
        domain: &str,
        f: &mut dyn FnMut(&mut dyn Storage, &mut dyn Store),
    ) -> replica_core::Result {
        let mut dir = Local::volatile(self.local.join(domain))?;
        f(&mut dir, &mut self.bucket);
        Ok(())
    }
    fn lease(
        &mut self,
        key: &str,
        ttl_ms: u64,
        f: &mut dyn FnMut(&mut dyn Backend),
    ) -> Result<(), StoreError> {
        let mut lease = self.bucket.lease(key, ttl_ms)?;
        f(&mut lease);
        Ok(())
    }
}
impl Lanes {
    fn run_all(&self) {
        for queue in [&self.lease, &self.data] {
            loop {
                let job = queue.borrow_mut().pop_front();
                let Some(job) = job else { break };
                job(&mut *self.side.borrow_mut());
            }
        }
    }
}
impl Offload for Lanes {
    fn push(&self, lane: Lane, job: Job) {
        match lane {
            Lane::Lease => self.lease.borrow_mut().push_back(job),
            Lane::Data => self.data.borrow_mut().push_back(job),
        }
    }
    fn settle(&self, _ms: u64) -> bool {
        self.run_all();
        true
    }
}

#[test]
fn a_capture_on_its_way_confirms_no_later_write_and_the_owner_never_waits_for_s3() {
    let _sqlite = SQLITE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let bed = Bed::new("lanes");
    let (mut heap, mut a) = (vec![0_u64; 4 << 20], vec![0_u8; 1 << 16]);
    let mut memory = Memory::new([Slot::new(&mut a)], 0, 1);
    let engine = engine(&mut heap, &mut memory);
    let verify = checker(&engine);
    let lines = Lines::default();
    let lanes = Lanes {
        lease: RefCell::new(VecDeque::new()),
        data: RefCell::new(VecDeque::new()),
        side: RefCell::new(Side {
            local: bed.root.join("a"),
            bucket: bed.bucket(),
        }),
    };
    let mut owner_bucket = bed.bucket();
    let owner_calls = owner_bucket.calls.clone();
    let w = bed.claim("a", &mut owner_bucket).unwrap();
    let generation = w.generation();
    let group = Group::new(
        bed.config(),
        Rc::new(RefCell::new(owner_bucket)),
        Rc::new(RefCell::new(w)),
        millis,
        Box::new(lines.clone()),
    )
    .with_offload(&lanes);
    assert!(!group.prepare("main.db", bed.local("a"), &verify).unwrap());
    let mut handle = bed.local("a");
    let mut vfs = group.vfs("main.db", &mut handle).unwrap();
    let mut db = engine.open_with(c"main.db", &mut vfs).unwrap();
    let write = |db: &mut Connection<'_, '_, _>| {
        exec(
            db,
            "CREATE TABLE IF NOT EXISTS t(v INTEGER); INSERT INTO t VALUES (1)",
        );
        group.publications().begun("main.db")
    };
    // Een rustmoment na het interval, daarna elke job van de banen geland en
    // teruggenomen: niets onderweg.
    let quiet = || {
        advance(15_000);
        group.rest().unwrap();
        for _ in 0..8 {
            if lanes.data.borrow().is_empty() {
                break;
            }
            lanes.run_all();
            group.rest().unwrap();
        }
    };
    write(&mut db);
    quiet();
    assert!(lanes.data.borrow().is_empty());
    assert!(lines.has("published file=main.db"), "{:?}", lines.since(0));

    // Een capture begint (zijn upload wacht op de baan), dan komt een write.
    write(&mut db);
    advance(15_000);
    group.rest().unwrap();
    assert_eq!(lanes.data.borrow().len(), 1, "een capture is onderweg");
    let begun = write(&mut db);
    let before = group.publications().count("main.db");
    let n = lines.len();
    lanes.run_all();
    group.rest().unwrap();
    assert!(lines.since(n).iter().any(|l| l == "published file=main.db"));
    let after = group.publications().count("main.db");
    assert!(after > before, "de publicatie telt ({before} -> {after})");
    assert!(
        after <= begun,
        "een capture van vóór de write bevestigt hem niet: {after}, begonnen bij de write {begun}"
    );
    // De volgende capture bevat de write.
    quiet();
    let last = group.publications().count("main.db");
    assert!(last > begun, "count {last}, begonnen bij de write {begun}");

    // S3 hangt: geen baan loopt. Lezen en schrijven gaan door, de eigenaar
    // roept de store niet aan; na de schrijfdeadline wachten writes.
    let calls = owner_calls.get();
    write(&mut db);
    for _ in 0..12 {
        advance(2_000);
        group.keep_lease();
        group.rest().unwrap();
        assert!(
            count(&mut db, "SELECT count(*) FROM t") > 0,
            "lezen gaat door"
        );
    }
    assert_eq!(owner_calls.get(), calls, "geen store-call op de eigenaar");
    assert!(
        matches!(group.standing(), Standing::Held(_)),
        "{:?}",
        group.standing()
    );
    let e = try_exec(&mut db, "INSERT INTO t VALUES (2)").unwrap_err();
    assert_eq!(e.code & 0xff, replica_sqlite::Error::READONLY.code);
    // S3 antwoordt weer: de lease komt terug met dezelfde generatie.
    lanes.run_all();
    advance(1_000);
    group.keep_lease();
    group.rest().unwrap();
    lanes.run_all();
    advance(1_000);
    group.rest().unwrap();
    assert_eq!(group.standing(), Standing::Valid);
    assert_eq!(group.writer().borrow().generation(), generation);
    exec(&mut db, "INSERT INTO t VALUES (3)");
    drop(db);
    drop(vfs);
    group.flush().unwrap();
    assert!(!group.has_changes());
    assert!(!lines.has("lost"), "{:?}", lines.since(0));
}

#[test]
fn a_short_outage_is_retried_and_the_lease_comes_back_with_the_same_generation() {
    let _sqlite = SQLITE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let bed = Bed::new("hiccup");
    let (mut heap, mut a) = (vec![0_u64; 4 << 20], vec![0_u8; 1 << 16]);
    let mut memory = Memory::new([Slot::new(&mut a)], 0, 1);
    let engine = engine(&mut heap, &mut memory);
    let verify = checker(&engine);
    let lines = Lines::default();
    let mut bucket = bed.bucket();
    let w = bed.claim("a", &mut bucket).unwrap();
    let generation = w.generation();
    let group = Group::new(
        bed.config(),
        Rc::new(RefCell::new(bucket)),
        Rc::new(RefCell::new(w)),
        millis,
        Box::new(lines.clone()),
    );
    group.prepare("main.db", bed.local("a"), &verify).unwrap();
    let mut handle = bed.local("a");
    let mut vfs = group.vfs("main.db", &mut handle).unwrap();
    let mut db = engine.open_with(c"main.db", &mut vfs).unwrap();
    exec(
        &mut db,
        "CREATE TABLE t(v INTEGER); INSERT INTO t VALUES (1)",
    );
    advance(15_000);
    group.rest().unwrap();
    assert!(lines.has("published file=main.db"));
    let published = group.publications().count("main.db");

    // Korter dan de lease onbereikbaar: elke call een "time-out" van 2 s.
    bed.faults.down.set(true);
    bed.faults.timeout_ms.set(500);
    exec(&mut db, "INSERT INTO t VALUES (2)");
    advance(15_000);
    group.rest().unwrap();
    assert!(lines.has("retry=1"), "{:?}", lines.since(0));
    let mut held = false;
    for _ in 0..10 {
        advance(1_000);
        group.rest().unwrap();
        group.keep_lease();
        held = matches!(group.standing(), Standing::Held(_));
        if held {
            break;
        }
    }
    assert!(held, "voorbij de schrijfdeadline wachten writes");
    assert_eq!(
        count(&mut db, "SELECT count(*) FROM t"),
        2,
        "lezen gaat door"
    );
    let e = try_exec(&mut db, "INSERT INTO t VALUES (3)").unwrap_err();
    assert_eq!(e.code & 0xff, replica_sqlite::Error::READONLY.code);
    // De store antwoordt weer: herovering met dezelfde generatie, de
    // publicatie haalt in.
    bed.faults.down.set(false);
    for _ in 0..20 {
        advance(1_000);
        group.rest().unwrap();
        if lines.has("recovered") {
            break;
        }
    }
    assert!(lines.has("recovered retries="), "{:?}", lines.since(0));
    assert_eq!(group.standing(), Standing::Valid);
    assert_eq!(group.writer().borrow().generation(), generation);
    assert!(group.publications().count("main.db") > published);
    assert!(!lines.has("lost"));

    // Langer dan de lease: verloren, eenmaal gemeld.
    bed.faults.down.set(true);
    bed.faults.timeout_ms.set(10_000);
    let mut lost = false;
    for _ in 0..6 {
        advance(5_000);
        if group.rest() == Err(Error::LeaseLost) {
            lost = true;
            break;
        }
    }
    assert!(lost, "{:?}", lines.since(0));
    assert_eq!(group.standing(), Standing::Lost);
    assert_eq!(
        lines
            .0
            .borrow()
            .iter()
            .filter(|l| l.starts_with("lost"))
            .count(),
        1
    );
    assert!(lines.has("cause=expired"), "{:?}", lines.since(0));
}

#[test]
fn release_tries_again_by_name() {
    let _sqlite = SQLITE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let bed = Bed::new("release");
    let (mut heap, mut a) = (vec![0_u64; 4 << 20], vec![0_u8; 1 << 16]);
    let mut memory = Memory::new([Slot::new(&mut a)], 0, 1);
    let engine = engine(&mut heap, &mut memory);
    let verify = checker(&engine);
    let key = bed.config().lease_key();
    for named in [false, true] {
        let mut bucket = bed.bucket();
        let w = bed.claim("a", &mut bucket).unwrap();
        let group = Group::new(
            bed.config(),
            Rc::new(RefCell::new(bed.bucket())),
            Rc::new(RefCell::new(w)),
            millis,
            Box::new(Lines::default()),
        );
        group.prepare("main.db", bed.local("a"), &verify).unwrap();
        // Een vernieuwing onderweg houdt de Writer nog.
        let job = named.then(|| group.writer().clone());
        let held = group.close().unwrap();
        assert_eq!(matches!(held, Held::Named { .. }), named);
        // De eerste DELETE faalt, zoals op een stale verbinding.
        bed.faults.lease_deletes.set(1);
        let (out, attempts) = group::release(&mut bucket, &key, TTL, held);
        out.unwrap();
        assert_eq!(attempts, 2);
        drop(job);
        // Een ander neemt meteen over.
        let b = bed.claim("b", &mut bucket).expect("de lease is vrij");
        let (out, _) = group::release(&mut bucket, &key, TTL, Held::Writer(b));
        out.unwrap();
    }
    // Weg, of van een ander: niets van ons om vrij te geven.
    let mut bucket = bed.bucket();
    let b = bed.claim("b", &mut bucket).unwrap();
    let named = Held::Named {
        owner: "a#x".into(),
        generation: 9,
    };
    assert_eq!(group::release(&mut bucket, &key, TTL, named), (Ok(()), 1));
    assert!(
        bed.claim("c", &mut bucket).is_none(),
        "de lease van b blijft"
    );
    drop(b);
}

#[test]
fn a_restore_longer_than_the_lease_renews_it_and_the_next_database_comes_in() {
    let _sqlite = SQLITE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let bed = Bed::new("restore");
    let (mut heap, mut a) = (vec![0_u64; 4 << 20], vec![0_u8; 1 << 16]);
    let mut memory = Memory::new([Slot::new(&mut a)], 0, 1);
    let engine = engine(&mut heap, &mut memory);
    let verify = checker(&engine);
    let key = bed.config().lease_key();
    // A: een main.db van vele segmenten en een kleine 2026.db, gepubliceerd.
    {
        let mut bucket = bed.bucket();
        let w = bed.claim("a", &mut bucket).unwrap();
        let group = Group::new(
            bed.config(),
            Rc::new(RefCell::new(bed.bucket())),
            Rc::new(RefCell::new(w)),
            millis,
            Box::new(Lines::default()),
        );
        for (file, sql) in [
            (
                "main.db",
                "CREATE TABLE t(b BLOB);
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 512)
                 INSERT INTO t SELECT randomblob(65536) FROM n;",
            ),
            (
                "2026.db",
                "CREATE TABLE t(v INTEGER); INSERT INTO t VALUES (26)",
            ),
        ] {
            group.prepare(file, bed.local("a"), &verify).unwrap();
            let mut handle = bed.local("a");
            let mut vfs = group.vfs(file, &mut handle).unwrap();
            let path = CString::new(file).unwrap();
            let mut db = engine.open_with(&path, &mut vfs).unwrap();
            exec(&mut db, sql);
        }
        group.flush().unwrap();
        let held = group.close().unwrap();
        group::release(&mut bucket, &key, TTL, held).0.unwrap();
    }
    // B: niets lokaal; elke GET kost 4 s, 16 tegelijk (een hele ronde 64 s).
    bed.faults.get_ms.set(4_000);
    let lines = Lines::default();
    let mut bucket = bed.bucket();
    bucket.parallel = Parallel {
        requests: 16,
        bytes: 64 << 20,
    };
    let w = bed.claim("b", &mut bucket).unwrap();
    let group = Group::new(
        bed.config(),
        Rc::new(RefCell::new(bucket)),
        Rc::new(RefCell::new(w)),
        millis,
        Box::new(lines.clone()),
    );
    let started = millis();
    assert!(group.prepare("main.db", bed.local("b"), &verify).unwrap());
    let took = millis() - started;
    assert!(
        took > 30_000,
        "het herstel moet langer duren dan de lease: {took} ms"
    );
    assert!(
        group.prepare("2026.db", bed.local("b"), &verify).unwrap(),
        "{:?}",
        lines.since(0)
    );
    for (file, sql, want) in [
        ("main.db", "SELECT count(*) FROM t", 512),
        ("2026.db", "SELECT sum(v) FROM t", 26),
    ] {
        let mut handle = bed.local("b");
        let mut vfs = group.vfs(file, &mut handle).unwrap();
        let path = CString::new(file).unwrap();
        let mut db = engine.open_with(&path, &mut vfs).unwrap();
        assert_eq!(count(&mut db, sql), want);
    }
    assert!(!lines.has("LeaseLost"), "{:?}", lines.since(0));
}

#[test]
fn a_reader_follows_with_increments_and_a_writer_continues_its_copy() {
    let _sqlite = SQLITE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let bed = Bed::new("follow");
    let (mut heap, mut a) = (vec![0_u64; 4 << 20], vec![0_u8; 1 << 16]);
    let mut memory = Memory::new([Slot::new(&mut a)], 0, 1);
    let engine = engine(&mut heap, &mut memory);
    let verify = checker(&engine);
    let config = bed.config();
    let key = config.lease_key();
    let mut bucket = bed.bucket();
    let w = bed.claim("a", &mut bucket).unwrap();
    let writer = Group::new(
        config.clone(),
        Rc::new(RefCell::new(bed.bucket())),
        Rc::new(RefCell::new(w)),
        millis,
        Box::new(Lines::default()),
    );
    writer.prepare("main.db", bed.local("a"), &verify).unwrap();
    let insert = |sql: &str| {
        let mut handle = bed.local("a");
        let mut vfs = writer.vfs("main.db", &mut handle).unwrap();
        let mut db = engine.open_with(c"main.db", &mut vfs).unwrap();
        exec(&mut db, sql);
        drop(db);
        drop(vfs);
        advance(15_000);
        writer.flush().unwrap();
    };
    insert("CREATE TABLE t(v INTEGER); INSERT INTO t VALUES (1)");
    // De lezer: geen lease, hersteld uit de store.
    let (endpoint, name) = bucket.identity();
    let destination = config.destination(&endpoint, &name, "main.db").unwrap();
    let path = Name::new("main.db").unwrap();
    let mut follow =
        Follow::new(&config.namespace("main.db"), &destination, path, 1 << 25).unwrap();
    let mut reader = bed.bucket();
    let mut copy = bed.local("r");
    let check = |b: &mut Local, n: &Name| verify(b, n, &|| {});
    let out = follow.update(&mut copy, &mut reader, check).unwrap();
    assert!(matches!(out, Outcome::Restored { .. }), "{out:?}");
    let rows = |copy: &mut Local| {
        let mut db = engine.open_read_only_with(c"main.db", copy).unwrap();
        count(&mut db, "SELECT count(*) FROM t")
    };
    assert_eq!(rows(&mut copy), 1);
    assert!(!follow.poll(&mut reader).unwrap());
    // De schrijver publiceert meer: alleen de incrementen komen binnen.
    insert("INSERT INTO t VALUES (2)");
    assert!(follow.poll(&mut reader).unwrap());
    let out = follow.update(&mut copy, &mut reader, check).unwrap();
    assert!(matches!(out, Outcome::Updated { .. }), "{out:?}");
    assert_eq!(rows(&mut copy), 2);
    assert_eq!(
        follow.update(&mut copy, &mut reader, check).unwrap(),
        Outcome::Current
    );
    // Een schrijver op de map van de lezer zet zijn kopie voort in plaats van
    // er een nieuwe snapshot van te maken.
    let held = writer.close().unwrap();
    group::release(&mut bucket, &key, TTL, held).0.unwrap();
    assert!(follow::is_follower(&mut copy, path).unwrap());
    let w = bed.claim("r", &mut bucket).unwrap();
    follow::release(&mut copy, path).unwrap();
    assert!(!follow::is_follower(&mut copy, path).unwrap());
    let taken = Group::new(
        config,
        Rc::new(RefCell::new(bed.bucket())),
        Rc::new(RefCell::new(w)),
        millis,
        Box::new(Lines::default()),
    );
    assert!(!taken.prepare("main.db", bed.local("r"), &verify).unwrap());
    assert!(!taken.has_changes(), "voortgezet, geen nieuwe snapshot");
}
