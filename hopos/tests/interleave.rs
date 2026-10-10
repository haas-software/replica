//! Twee verbindingen op één engine, elk op een eigen stack (`applib::stacktask`,
//! dezelfde code als op HopOS), die bij **elke** VFS-callback parkeren en
//! elkaar dan afwisselen: de toets van het veiligheidsargument in
//! `replica_sqlite` (moduledocs, "N verbindingen op één engine"). Duizend rijen
//! elk, in transacties die over elkaar heen lopen, en `integrity_check` op
//! beide. Eén SQLite-test per binary: de C-runtime is procesglobaal.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use applib::stacktask::{Suspender, Task};
use core::{
    cell::RefCell,
    ffi::CStr,
    future::{Future, poll_fn},
    pin::Pin,
    task::{Context, Poll, Waker},
};
use replica_hopos::Wait;
use replica_sqlite::{
    Connection, Engine, FileId, OpenFlags, Result, Storage, Value,
    asynchronous::{AsyncStorage, Bridge},
    host::Dir,
};
use std::path::PathBuf;

/// Eén keer Pending, dan klaar: de parkeerplek van een echte system-call.
fn park() -> impl Future<Output = ()> {
    let mut parked = false;
    poll_fn(move |cx| {
        if parked {
            Poll::Ready(())
        } else {
            parked = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
}

/// Een host-map als async opslag die bij elke callback parkeert en in het
/// spoor noteert wie er parkeerde.
struct Parking<'a> {
    dir: Dir,
    me: u8,
    trace: &'a RefCell<Vec<u8>>,
}
impl Parking<'_> {
    async fn step(&mut self) {
        self.trace.borrow_mut().push(self.me);
        park().await;
    }
}
impl AsyncStorage for Parking<'_> {
    async fn cooperate(&mut self) -> Result {
        self.step().await;
        Ok(())
    }
    fn invalidate(&mut self) {}
    async fn open(&mut self, name: &CStr, flags: OpenFlags) -> Result<FileId> {
        self.step().await;
        self.dir.open(name, flags)
    }
    async fn close(&mut self, file: FileId) -> Result {
        self.step().await;
        self.dir.close(file)
    }
    async fn read(&mut self, file: FileId, offset: u64, dst: &mut [u8]) -> Result<usize> {
        self.step().await;
        self.dir.read(file, offset, dst)
    }
    async fn write(&mut self, file: FileId, offset: u64, src: &[u8]) -> Result {
        self.step().await;
        self.dir.write(file, offset, src)
    }
    async fn truncate(&mut self, file: FileId, size: u64) -> Result {
        self.step().await;
        self.dir.truncate(file, size)
    }
    async fn sync(&mut self, file: FileId, flags: i32) -> Result {
        self.step().await;
        self.dir.sync(file, flags)
    }
    async fn size(&mut self, file: FileId) -> Result<u64> {
        self.step().await;
        self.dir.size(file)
    }
    async fn remove(&mut self, name: &CStr, sync_directory: bool) -> Result {
        self.step().await;
        self.dir.remove(name, sync_directory)
    }
    async fn exists(&mut self, name: &CStr) -> Result<bool> {
        self.step().await;
        self.dir.exists(name)
    }
    fn random(&mut self, dst: &mut [u8]) -> Result {
        self.dir.random(dst)
    }
    fn unix_millis(&mut self) -> Result<i64> {
        self.dir.unix_millis()
    }
}

fn scratch() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("replica-interleave-{}-{nanos}", std::process::id()))
}

fn integer<B: Storage>(db: &mut Connection<'_, '_, B>, sql: &CStr) -> Result<i64> {
    let mut s = db.prepare(sql)?;
    s.step()?;
    match s.column(0)? {
        Value::Integer(n) => Ok(n),
        _ => Err(replica_sqlite::Error::MISUSE),
    }
}

/// Het werk van één stack: tabel, 1000 rijen in tien transacties met een
/// lees ertussen, een index (sorter), een update, en de integriteit.
fn work(engine: &Engine<'_, Dir>, io: &mut Parking<'_>, s: &Suspender, tag: &str) -> Result<i64> {
    let wait = Wait(s);
    let mut bridge = Bridge::new(io, &wait);
    let mut db = engine.open_with(c"db.sqlite", &mut bridge)?;
    db.execute(c"CREATE TABLE item(id INTEGER PRIMARY KEY, tag TEXT, n INTEGER, pad TEXT)")?;
    for batch in 0..10 {
        db.execute(c"BEGIN")?;
        for i in 0..100 {
            let mut insert = db.prepare(c"INSERT INTO item(tag, n, pad) VALUES(?1, ?2, ?3)")?;
            insert.bind(1, Value::Text(tag))?;
            insert.bind(2, Value::Integer(batch * 100 + i))?;
            insert.bind(3, Value::Text(&"x".repeat(200)))?;
            insert.step()?;
        }
        db.execute(c"COMMIT")?;
        if integer(&mut db, c"SELECT count(*) FROM item")? != (batch + 1) * 100 {
            return Err(replica_sqlite::Error::MISUSE);
        }
    }
    db.execute(c"CREATE INDEX item_n ON item(n); UPDATE item SET n = n * 2 WHERE n % 3 = 0")?;
    let mut check = db.prepare(c"PRAGMA integrity_check")?;
    check.step()?;
    if check.column(0)? != Value::Text("ok") {
        return Err(replica_sqlite::Error::MISUSE);
    }
    drop(check);
    integer(&mut db, c"SELECT count(*) FROM item WHERE tag = ?1 OR 1")
}

#[test]
fn two_connections_on_two_stacks_alternate_at_every_callback() {
    let root = scratch();
    let trace = RefCell::new(Vec::new());
    let mut heap = vec![0_u64; 1024 * 1024];
    let mut engine_dir = Dir::new(root.join("engine")).unwrap();
    // SAFETY: De enige SQLite-runtime in deze testbinary, op deze thread; de
    // stacks hieronder lopen om en om op dezelfde thread.
    let engine = unsafe { Engine::initialize(&mut heap, &mut engine_dir) }.unwrap();
    let mut a = Parking {
        dir: Dir::new(root.join("a")).unwrap(),
        me: b'a',
        trace: &trace,
    };
    let mut b = Parking {
        dir: Dir::new(root.join("b")).unwrap(),
        me: b'b',
        trace: &trace,
    };
    {
        let engine = &engine;
        let (io_a, io_b) = (&mut a, &mut b);
        // SAFETY: SQLite met begrensde SQL (geen recursie, geen extensies)
        // past ruim in 2 MiB; de callbacks keren na Cancelled terug.
        let mut first = unsafe { Task::new(2 << 20, move |s| work(engine, io_a, s, "a")) }.unwrap();
        // SAFETY: Idem, een tweede stack voor de tweede verbinding.
        let mut second =
            unsafe { Task::new(2 << 20, move |s| work(engine, io_b, s, "b")) }.unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        let (mut done_a, mut done_b) = (None, None);
        // De executor in het klein: om en om pollen tot beide klaar zijn.
        while done_a.is_none() || done_b.is_none() {
            if done_a.is_none()
                && let Poll::Ready(r) = Pin::new(&mut first).poll(&mut cx)
            {
                done_a = Some(r);
            }
            if done_b.is_none()
                && let Poll::Ready(r) = Pin::new(&mut second).poll(&mut cx)
            {
                done_b = Some(r);
            }
        }
        assert_eq!(done_a.unwrap(), Ok(1000));
        assert_eq!(done_b.unwrap(), Ok(1000));
    }
    // Ze wisselden elkaar echt af: zolang beide liepen, volgde op elke
    // callback van de één een callback van de ander (a, b, a, b, ...).
    let trace = trace.into_inner();
    let switches = trace.windows(2).filter(|w| w[0] != w[1]).count();
    let a_calls = trace.iter().filter(|&&c| c == b'a').count();
    let b_calls = trace.len() - a_calls;
    assert!(a_calls > 300 && b_calls > 300, "a={a_calls} b={b_calls}");
    assert!(
        switches + 2 >= 2 * a_calls.min(b_calls),
        "switches={switches} a={a_calls} b={b_calls}"
    );
    // Koud heropenen, nu zonder parkeren: elk zijn eigen 1000 rijen, heel.
    for side in ["a", "b"] {
        let mut dir = Dir::new(root.join(side)).unwrap();
        let mut db = engine.open_with(c"db.sqlite", &mut dir).unwrap();
        assert_eq!(
            integer(&mut db, c"SELECT count(*) FROM item").unwrap(),
            1000
        );
        let mut s = db.prepare(c"SELECT count(DISTINCT tag) FROM item").unwrap();
        assert!(s.step().unwrap());
        assert_eq!(s.column(0).unwrap(), Value::Integer(1));
        drop(s);
        let mut check = db.prepare(c"PRAGMA integrity_check").unwrap();
        assert!(check.step().unwrap());
        assert_eq!(check.column(0).unwrap(), Value::Text("ok"));
    }
    drop(engine);
    std::fs::remove_dir_all(&root).unwrap();
}
