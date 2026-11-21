//! Een manifest-PUT van een schrijver die zijn lease kwijt is, mag de keten
//! van de nieuwe schrijver nooit breken (één SQLite-test per binary).
//!
//! De oude schrijver A haalde zijn deadline nog en stuurde het manifest van
//! sequence 3 weg; die PUT bleef onderweg terwijl B de verlopen lease overnam,
//! herstelde en zelf publiceerde. Pas daarna landt A's PUT: een tweede
//! manifest met sequence 3 (de sleutel bevat `at`, dus `put_new` ziet hem
//! niet). Zonder fencing-token weigerde elke restore die generatie als
//! `Corrupt`; met token wint B's manifest en is dat van A afval.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    archive::{self, Fetch},
    local::Name,
    manifest::Manifest,
    object::{self, Object, Store, StoreError},
    owner::{Config, Replica},
    replication,
    time::Time,
    writer::{MemBackend, Role, Writer, claim},
};
use replica_sqlite::{Engine, Storage, Value};
use std::{cell::RefCell, ffi::CStr, rc::Rc};
use support::{Bucket, Fs};
const NS: &str = "replica/test";
const TTL: u64 = 30_000;
fn at(ms: i64) -> Time {
    let ms = 1_790_765_296_000 + ms;
    Time::unix(ms / 1000, (ms % 1000) as u32 * 1_000_000).unwrap()
}
fn config() -> Config {
    let mut config = Config::new(NS, &"1".repeat(64), Name::new("db").unwrap(), 65536).unwrap();
    config.background_maintenance = true;
    config.interval = 1;
    config
}
fn writer(role: Role) -> Writer {
    match role {
        Role::Writer(w) => w,
        Role::Reader { leader } => panic!("lezer, leider {leader:?}"),
    }
}
/// Wat er gebeurt terwijl de PUT van A onderweg is.
type Late = Box<dyn FnOnce(&mut Bucket)>;
/// De bucket, met één PUT van een raw-commit die pas landt nadat `late` liep.
struct Delayed {
    bucket: Bucket,
    late: Option<Late>,
}
impl Delayed {
    fn hold(&mut self, key: &str) {
        if key.contains("/L0/")
            && let Some(late) = self.late.take()
        {
            late(&mut self.bucket);
        }
    }
}
impl Store for Delayed {
    fn put(&mut self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.hold(key);
        self.bucket.put(key, bytes)
    }
    fn put_new(&mut self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.hold(key);
        self.bucket.put_new(key, bytes)
    }
    fn get(&mut self, key: &str, limit: usize) -> Result<Vec<u8>, StoreError> {
        self.bucket.get(key, limit)
    }
    fn delete(&mut self, key: &str) -> Result<(), StoreError> {
        self.bucket.delete(key)
    }
    fn list(&mut self, prefix: &str, limit: usize) -> Result<Vec<Object>, StoreError> {
        self.bucket.list(prefix, limit)
    }
}
fn sql<B: Storage>(r: &Replica, fs: &mut B, heap: &mut [u64], sql: &CStr) {
    let mut vfs = r.vfs(fs).unwrap();
    // SAFETY: Eén SQLite-test in deze binary; iedere engine sluit vóór de volgende.
    let mut engine = unsafe { Engine::initialize(heap, &mut vfs) }.unwrap();
    engine.open(c"db").unwrap().execute(sql).unwrap();
}
fn integrity<B: Storage>(heap: &mut [u64], b: &mut B, name: &Name) -> replica_core::Result {
    // SAFETY: Eén SQLite-test in deze binary; geen andere engine leeft nu.
    let mut engine = unsafe { Engine::initialize(heap, b) }?;
    let mut db = engine.open(name.cstr()?)?;
    let mut q = db.prepare(c"PRAGMA integrity_check")?;
    assert!(q.step()?);
    assert_eq!(q.column(0)?, Value::Text("ok"));
    Ok(())
}
#[test]
fn a_late_manifest_of_the_old_writer_never_wins_and_restore_keeps_working() {
    let heap = Rc::new(RefCell::new(vec![0u64; 512 * 1024]));
    let lease = Rc::new(RefCell::new(MemBackend::new()));
    let mut fa = Fs::default();
    let a = writer(
        claim(
            &mut Fs::default(),
            &mut *lease.borrow_mut(),
            "a",
            TTL,
            at(0),
        )
        .unwrap(),
    );
    let mut store = Delayed {
        bucket: Bucket::default(),
        late: None,
    };
    let mut ra = Replica::prepare(&mut fa, &mut store, a, config(), at(0), |_, _| {
        panic!("lege bootstrap")
    })
    .unwrap();
    for (t, insert) in [
        (1_000, c"CREATE TABLE item(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO item(value) VALUES('a1')"),
        (2_000, c"INSERT INTO item(value) VALUES('a2')"),
    ] {
        sql(&ra, &mut fa, &mut heap.borrow_mut(), insert);
        fa.clock.set(t);
        assert!(ra.sync(&mut fa, &mut store, at(t)).unwrap().published);
    }
    let generation = ra.marker().generation.clone();
    sql(
        &ra,
        &mut fa,
        &mut heap.borrow_mut(),
        c"INSERT INTO item(value) VALUES('a3')",
    );
    fa.clock.set(3_000);
    let pending = ra.begin(&mut fa, &mut store, at(3_000)).unwrap().unwrap();
    let uploaded = pending.upload(&mut fa, &mut store);
    // A haalt zijn deadline (20 s) ruim; zijn manifest-PUT blijft onderweg
    // terwijl B na het verlopen (30 s) overneemt, herstelt en sequence 3 publiceert.
    let node_b: Rc<RefCell<Option<(Replica, Fs)>>> = Rc::default();
    store.late = Some({
        let (heap, lease, node_b) = (heap.clone(), lease.clone(), node_b.clone());
        Box::new(move |bucket: &mut Bucket| {
            let mut fb = Fs::default();
            fb.clock.set(31_000);
            let b = writer(
                claim(
                    &mut Fs::default(),
                    &mut *lease.borrow_mut(),
                    "b",
                    TTL,
                    at(31_000),
                )
                .unwrap(),
            );
            assert_eq!(b.generation(), 2);
            let mut rb = Replica::prepare(&mut fb, bucket, b, config(), at(31_000), |b, name| {
                integrity(&mut heap.borrow_mut(), b, name)
            })
            .unwrap();
            sql(
                &rb,
                &mut fb,
                &mut heap.borrow_mut(),
                c"INSERT INTO item(value) VALUES('b3')",
            );
            fb.clock.set(32_000);
            assert!(rb.sync(&mut fb, bucket, at(32_000)).unwrap().published);
            assert_eq!(rb.marker().sequence, 3);
            *node_b.borrow_mut() = Some((rb, fb));
        })
    });
    fa.clock.set(4_000);
    // Wat A hierna ziet, doet er niet toe; wat in de bucket staat wel.
    let _ = ra.finish(&mut fa, &mut store, pending, uploaded, at(4_000));
    assert!(store.late.is_none(), "de PUT van A liep");
    let mut bucket = store.bucket;
    let prefix = replication::generation_prefix(NS, &generation).unwrap();
    // Beide manifesten van sequence 3 staan er, met hun token.
    let mut tokens: Vec<u64> = bucket
        .list(&format!("{prefix}L0/000000000003-"), 10)
        .unwrap()
        .iter()
        .map(|o| {
            Manifest::decode(&bucket.data[&o.key], &prefix)
                .unwrap()
                .lease
        })
        .collect();
    tokens.sort_unstable();
    assert_eq!(tokens, [1, 2]);
    // B gaat gewoon verder op zijn eigen keten.
    let (mut rb, mut fb) = node_b.borrow_mut().take().unwrap();
    sql(
        &rb,
        &mut fb,
        &mut heap.borrow_mut(),
        c"INSERT INTO item(value) VALUES('b4')",
    );
    fb.clock.set(33_000);
    assert!(rb.sync(&mut fb, &mut bucket, at(33_000)).unwrap().published);
    // De layout laadt (B's sequence 3 wint) en een derde node herstelt B's data.
    let layout = object::layout(&mut bucket, &prefix).unwrap();
    let plan: Vec<(u64, u64)> = layout
        .plan(None)
        .unwrap()
        .iter()
        .map(|m| (m.sequence, m.lease))
        .collect();
    assert_eq!(plan, [(1, 1), (2, 1), (3, 2), (4, 2)]);
    let mut fc = Fs::default();
    archive::fetch(
        &mut fc,
        &mut bucket,
        Fetch {
            namespace: NS,
            generation: &generation,
            at: None,
            live: Name::new("db").unwrap(),
            destination: Name::new("restored").unwrap(),
            page_limit: 65536,
        },
        |b, name| {
            let mut heap = heap.borrow_mut();
            integrity(&mut heap, b, name)?;
            // SAFETY: Eén SQLite-test in deze binary; geen andere engine leeft nu.
            let mut engine = unsafe { Engine::initialize(&mut heap, b) }?;
            let mut db = engine.open(name.cstr()?)?;
            let mut q = db.prepare(c"SELECT group_concat(value, ',') FROM item ORDER BY id")?;
            assert!(q.step()?);
            assert_eq!(q.column(0)?, Value::Text("a1,a2,b3,b4"));
            Ok(())
        },
    )
    .unwrap();
}
