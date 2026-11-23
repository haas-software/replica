//! De lease tijdens één lange call van de store van de eigenaar (EasyFlor,
//! 23-11-2026, de flood-ring: na het herstel van een `main.db` van 64 MiB
//! onder last `step=prepare error=LeaseLost` op `2026.db`, zonder één
//! mislukte vernieuwing).
//!
//! Met banen zet de eigenaar een vernieuwing alleen in de rij als hij zelf
//! loopt (vóór elke store- en opslagcall van het herstel). Eén GET die langer
//! wacht dan de lease, liet hem verlopen zonder dat iets het zei; het herstel
//! liep door en pas de volgende database vond hem verloren. Hier duurt één
//! GET van het herstel van `main.db` 40 s van de toetsklok (de lease is 30 s):
//!
//! 1. zonder [`KeepLease`]: het herstel stopt bij die GET, met
//!    [`Report::slow`] en [`Report::lost`] op `main.db` en stap `prepare`
//!    (oorzaak `expired`); de volgende database meldt niets nieuws;
//! 2. met [`KeepLease`] (de store roept `keep` elke seconde dat hij wacht,
//!    zoals `replica_hopos::keep::Keeping`): de leasebaan vernieuwt tijdens
//!    de GET, beide databases komen binnen, niets is verloren, en de gesloten
//!    groep laat niets achter om bij te houden.
//!
//! Eén toets: één SQLite-runtime per toetsbinary.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use replica_core::{
    Error,
    group::{self, Bucket, Config, Failure, Group, Report, Ticking},
    host::{Dir, DirLease},
    lanes::{Job, KeepLease, Lane, LaneSide, LaneStore, Offload},
    local::Name,
    object::{Object, Store, StoreError},
    writer::{self, Backend, Role},
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
    sync::atomic::{AtomicI64, Ordering::SeqCst},
};

/// Unix-milliseconden, verzet door de toets en de trage GET.
static CLOCK: AtomicI64 = AtomicI64::new(0);
const TTL: u64 = 30_000;
/// Zo lang parkeert de trage GET de eigenaar: langer dan de hele lease.
const SLOW_MS: i64 = 40_000;

fn millis() -> i64 {
    CLOCK.load(SeqCst)
}

/// Wat de groep meldt, als regels.
#[derive(Clone, Default)]
struct Lines(Rc<RefCell<Vec<String>>>);
impl Lines {
    fn has(&self, needle: &str) -> bool {
        self.0.borrow().iter().any(|l| l.contains(needle))
    }
    fn all(&self) -> Vec<String> {
        self.0.borrow().clone()
    }
    fn position(&self, needle: &str) -> Option<usize> {
        self.0.borrow().iter().position(|l| l.contains(needle))
    }
}
impl Report for Lines {
    fn retry(&self, file: &str, step: &str, e: Error, cause: Failure, n: u32, next: i64) {
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
    fn lost(&self, file: &str, step: &str, e: Error, cause: Failure, retries: u32) {
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
    fn slow(&self, file: &str, step: &str, call_ms: i64, renewed_ms: i64) {
        self.0.borrow_mut().push(format!(
            "slow file={file} step={step} call_ms={call_ms} renewed_ms={renewed_ms}"
        ));
    }
}

/// De trage GET van een eigenaar, gedeeld met de toets.
#[derive(Default)]
struct Slowness {
    /// GET's sinds `slow` gezet werd.
    gets: Cell<u32>,
    /// De zoveelste GET duurt [`SLOW_MS`]; 0: geen.
    slow: Cell<u32>,
    /// De sleutel van die GET.
    slowed: RefCell<String>,
}

/// De bucket in een map. Die van de eigenaar: zijn trage GET roept tijdens
/// het wachten elke seconde `keep` aan als hij er een heeft (de waiter van
/// de store op HopOS). Die van de banen: elke call meteen.
struct Slow<'k> {
    dir: Dir,
    slowness: Rc<Slowness>,
    keep: Option<&'k KeepLease<'k>>,
}
impl<'k> Slow<'k> {
    fn new(root: &Path, slowness: &Rc<Slowness>, keep: Option<&'k KeepLease<'k>>) -> Self {
        Slow {
            dir: Dir::volatile(root).unwrap(),
            slowness: slowness.clone(),
            keep,
        }
    }
    fn get_starts(&self, key: &str) {
        let s = &self.slowness;
        let n = s.gets.get() + 1;
        s.gets.set(n);
        if s.slow.get() != n {
            return;
        }
        *s.slowed.borrow_mut() = key.into();
        for _ in 0..SLOW_MS / 1000 {
            CLOCK.fetch_add(1000, SeqCst);
            if let Some(keep) = self.keep {
                keep.keep();
            }
        }
    }
}
impl Store for Slow<'_> {
    fn put(&mut self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.dir.put(key, bytes)
    }
    fn put_new(&mut self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.dir.put_new(key, bytes)
    }
    fn get(&mut self, key: &str, limit: usize) -> Result<Vec<u8>, StoreError> {
        self.get_starts(key);
        self.dir.get(key, limit)
    }
    fn delete(&mut self, key: &str) -> Result<(), StoreError> {
        self.dir.delete(key)
    }
    fn list(&mut self, prefix: &str, limit: usize) -> Result<Vec<Object>, StoreError> {
        self.dir.list(prefix, limit)
    }
}
impl Bucket for Slow<'_> {
    type Lease<'l>
        = DirLease
    where
        Self: 'l;
    fn lease<'l>(&'l mut self, key: &'l str, ttl_ms: u64) -> Result<DirLease, StoreError> {
        self.dir.lease(key, ttl_ms)
    }
    fn identity(&self) -> (String, String) {
        self.dir.identity()
    }
}

/// Een hostmap met de toetsklok als wandklok (de VFS meet de schrijfdeadline
/// op de klok van zijn opslag).
struct Local(replica_sqlite::host::Dir);
impl Local {
    fn at(path: impl AsRef<Path>) -> Self {
        Local(replica_sqlite::host::Dir::volatile(path).unwrap())
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

/// De twee banen van het slot. De leasebaan antwoordt zodra de eigenaar de
/// banen een beurt geeft (`breathe`, nadat hij een vernieuwing in de rij
/// zette): S3 antwoordt de lease meteen, alleen de GET van de eigenaar is
/// traag. De databaan loopt bij een settle.
struct Lanes {
    lease: RefCell<VecDeque<Job>>,
    data: RefCell<VecDeque<Job>>,
    side: RefCell<Side>,
    renewals: Cell<u32>,
}
/// De kant van de banen: eigen handvatten op de lokale map en de bucket.
struct Side {
    local: PathBuf,
    bucket: Slow<'static>,
}
impl LaneSide for Side {
    fn files(
        &mut self,
        domain: &str,
        f: &mut dyn FnMut(&mut dyn Storage, &mut dyn LaneStore),
    ) -> replica_core::Result {
        let mut dir = Local::at(self.local.join(domain));
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
    fn run(&self, lane: Lane) {
        let queue = match lane {
            Lane::Lease => &self.lease,
            Lane::Data => &self.data,
        };
        loop {
            let job = queue.borrow_mut().pop_front();
            let Some(job) = job else { return };
            if lane == Lane::Lease {
                self.renewals.set(self.renewals.get() + 1);
            }
            job.run(&mut *self.side.borrow_mut());
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
    fn breathe(&self) {
        self.run(Lane::Lease);
    }
    fn settle(&self, _ms: u64) -> bool {
        self.run(Lane::Lease);
        self.run(Lane::Data);
        true
    }
}

fn config() -> Config {
    Config {
        prefix: "replica".into(),
        domain: "demo".into(),
        ttl_ms: TTL,
        pages: 1 << 25,
        interval: 15,
        compress: false,
    }
}

/// De lease van het domein voor `node`.
fn claim(root: &Path, node: &str, bucket: &mut Slow<'_>) -> writer::Writer {
    let key = config().lease_key();
    let now = group::time_at(millis()).unwrap();
    let mut lease = bucket.lease(&key, TTL).unwrap();
    let mut local = Local::at(root.join(node).join("demo"));
    match writer::claim(&mut local, &mut lease, node, TTL, now).unwrap() {
        Role::Writer(w) => w,
        Role::Reader { leader } => panic!("{node}: lezer, houder {leader:?}"),
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
fn count<B: Storage>(db: &mut Connection<'_, '_, B>, sql: &str) -> i64 {
    let sql = CString::new(sql).unwrap();
    let mut s = db.prepare(&sql).unwrap();
    assert!(s.step().unwrap());
    match s.column(0).unwrap() {
        Value::Integer(n) => n,
        v => panic!("{v:?}"),
    }
}

#[test]
fn the_lease_is_kept_while_the_owner_waits_on_one_long_get_and_lost_there_without() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    CLOCK.store(i64::try_from(now).unwrap(), SeqCst);
    let root = std::env::temp_dir().join(format!("replica-keep-lease-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let bucket_dir = root.join("bucket");
    let (mut heap, mut a) = (vec![0_u64; 4 << 20], vec![0_u8; 1 << 16]);
    let mut memory = Memory::new([Slot::new(&mut a)], 0, 1);
    // SAFETY: De enige SQLite-runtime in deze toetsbinary, niet vanaf een andere thread.
    let engine = unsafe { Engine::initialize(&mut heap, &mut memory) }.unwrap();
    let verify = checker(&engine);
    let key = config().lease_key();
    let quick = Rc::new(Slowness::default());

    // A: een main.db van een paar segmenten en een kleine 2026.db, gepubliceerd.
    {
        let mut bucket = Slow::new(&bucket_dir, &quick, None);
        let w = claim(&root, "a", &mut bucket);
        let group = Group::new(
            config(),
            Rc::new(RefCell::new(Slow::new(&bucket_dir, &quick, None))),
            Rc::new(RefCell::new(w)),
            millis,
            Box::new(Lines::default()),
        );
        for (file, sql) in [
            (
                "main.db",
                "CREATE TABLE t(b BLOB);
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 128)
                 INSERT INTO t SELECT randomblob(65536) FROM n;",
            ),
            (
                "2026.db",
                "CREATE TABLE t(v INTEGER); INSERT INTO t VALUES (26)",
            ),
        ] {
            let local = Local::at(root.join("a").join("demo"));
            group.prepare(file, local, &verify).unwrap();
            let mut handle = Local::at(root.join("a").join("demo"));
            let mut vfs = group.vfs(file, &mut handle).unwrap();
            let path = CString::new(file).unwrap();
            let mut db = engine.open_with(&path, &mut vfs).unwrap();
            exec(&mut db, sql);
        }
        group.flush().unwrap();
        let held = group.close().unwrap();
        group::release(&mut bucket, &key, TTL, held).0.unwrap();
    }

    // Een node zonder iets lokaal, met banen; de tweede GET van zijn eigenaar
    // (in het herstel van main.db) duurt 40 s. `keeping`: zijn store houdt de
    // lease intussen bij.
    let restore = |node: &str, keeping: bool| {
        let keep = KeepLease::default();
        let lanes = Lanes {
            lease: RefCell::new(VecDeque::new()),
            data: RefCell::new(VecDeque::new()),
            side: RefCell::new(Side {
                local: root.join(node),
                bucket: Slow::new(&bucket_dir, &quick, None),
            }),
            renewals: Cell::new(0),
        };
        let slowness = Rc::new(Slowness::default());
        let mut owner = Slow::new(&bucket_dir, &slowness, keeping.then_some(&keep));
        let w = claim(&root, node, &mut owner);
        let lines = Lines::default();
        let mut group = Group::new(
            config(),
            Rc::new(RefCell::new(owner)),
            Rc::new(RefCell::new(w)),
            millis,
            Box::new(lines.clone()),
        )
        .with_offload(&lanes);
        if keeping {
            group = group.with_keep_lease(&keep);
        }
        slowness.slow.set(2);
        let local = || Local::at(root.join(node).join("demo"));
        let main = group.prepare("main.db", local(), &verify);
        let gets = slowness.gets.get();
        let year = group.prepare("2026.db", local(), &verify);
        slowness.slow.set(0);
        let rows = (main.is_ok() && year.is_ok()).then(|| {
            ["main.db", "2026.db"].map(|file| {
                let mut handle = local();
                let mut vfs = group.vfs(file, &mut handle).unwrap();
                let path = CString::new(file).unwrap();
                let mut db = engine.open_with(&path, &mut vfs).unwrap();
                count(&mut db, "SELECT count(*) FROM t")
            })
        });
        let open = keep.is_active();
        let held = group.close();
        let left = keep.is_active();
        if let Some(held) = held {
            let mut bucket = Slow::new(&bucket_dir, &quick, None);
            let _ = group::release(&mut bucket, &key, TTL, held);
        }
        let slowed = slowness.slowed.borrow().clone();
        (
            main,
            year,
            rows,
            lines,
            gets,
            slowed,
            lanes.renewals.get(),
            (open, left),
        )
    };

    // 1. Zonder KeepLease: de lease verloopt tijdens de GET, en het herstel
    //    stopt daar (vroeger kwam main.db binnen en faalde pas 2026.db).
    let (main, year, _, lines, gets, slowed, _, _) = restore("b", false);
    assert!(slowed.starts_with("replica/demo/main.db/"), "{slowed}");
    assert_eq!(main, Err(Error::LeaseLost), "{:?}", lines.all());
    assert_eq!(
        gets,
        2,
        "het herstel stopt bij de trage GET: {:?}",
        lines.all()
    );
    let slow = lines
        .position("slow file=main.db step=prepare call_ms=40000")
        .unwrap_or_else(|| panic!("{:?}", lines.all()));
    let lost = lines
        .position("lost file=main.db step=prepare error=LeaseLost cause=expired")
        .unwrap_or_else(|| panic!("{:?}", lines.all()));
    assert!(slow < lost, "{:?}", lines.all());
    assert_eq!(year, Err(Error::LeaseLost));
    assert!(!lines.has("2026.db"), "eenmaal gemeld: {:?}", lines.all());
    assert!(!lines.has("failed"), "{:?}", lines.all());
    // De verloren lease verloopt vóór de volgende node hem claimt.
    CLOCK.fetch_add(60_000, SeqCst);

    // 2. Met KeepLease: de leasebaan vernieuwt terwijl de GET wacht.
    let (main, year, rows, lines, _, slowed, renewals, (open, left)) = restore("c", true);
    assert!(slowed.starts_with("replica/demo/main.db/"), "{slowed}");
    assert!(main.unwrap(), "{:?}", lines.all());
    assert!(year.unwrap(), "{:?}", lines.all());
    assert_eq!(rows, Some([128, 1]));
    assert!(!lines.has("lost"), "{:?}", lines.all());
    assert!(!lines.has("retry"), "{:?}", lines.all());
    // De lange call wordt nog wel gemeld: de verzoeken van de eigenaar wachtten zo lang.
    assert!(
        lines.has("slow file=main.db step=prepare call_ms=40000"),
        "{:?}",
        lines.all()
    );
    // Minstens één vernieuwing per 5 s van de GET van 40 s (een zesde lease).
    assert!(renewals >= 7, "{renewals} vernieuwingen in {SLOW_MS} ms");
    assert!(open, "de lease van de open groep wordt bijgehouden");
    assert!(!left, "een gesloten groep laat niets achter");
    let _ = std::fs::remove_dir_all(&root);
}
