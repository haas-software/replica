//! B1: een oud volume mag het werk van een andere schrijver nooit verdringen
//! (één SQLite-test per binary).
//!
//! Een tenant draait op A (sequence 1 en 2), dan op B (B herstelt uit de
//! store en publiceert sequence 3), dan weer op A met A's oude bestanden en
//! marker. De store is in dezelfde generatie verder dan A's marker. Vóór de
//! fix publiceerde Prepare A's lokale bron als nieuwe snapshot: A's oude stand
//! werd `current` en B's sequence 3 verdween stil uit beeld.
//!
//! Nu: is A lokaal schoon, dan herstelt A uit de store (B's werk blijft). Heeft
//! A eigen ongepubliceerd werk, dan opent A niet ([`Error::ForeignAhead`]) en
//! blijven zijn bestanden onaangeroerd. Ons eigen volume (een nette stop, of
//! een crash midden in een publicatie) gaat door zoals altijd.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    Error,
    local::Name,
    marker::LocalMarker,
    object::{self, StoreError},
    owner::{Config, Replica},
    prepare::Reason,
    replication,
    time::Time,
    writer::{MemBackend, Role, Writer, claim},
};
use replica_sqlite::{Engine, Storage, Value};
use std::ffi::CStr;
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
fn writer(lease: &mut MemBackend, node: &str, t: i64) -> Writer {
    match claim(&mut Fs::default(), lease, node, TTL, at(t)).unwrap() {
        Role::Writer(w) => w,
        Role::Reader { leader } => panic!("lezer, leider {leader:?}"),
    }
}
fn sql<B: Storage>(r: &Replica, fs: &mut B, heap: &mut [u64], sql: &CStr) {
    let mut vfs = r.vfs(fs).unwrap();
    // SAFETY: Eén SQLite-test in deze binary; iedere engine sluit vóór de volgende.
    let mut engine = unsafe { Engine::initialize(heap, &mut vfs) }.unwrap();
    engine.open(c"db").unwrap().execute(sql).unwrap();
}
fn items<B: Storage>(heap: &mut [u64], b: &mut B, name: &Name) -> String {
    // SAFETY: Eén SQLite-test in deze binary; geen andere engine leeft nu.
    let mut engine = unsafe { Engine::initialize(heap, b) }.unwrap();
    let mut db = engine.open(name.cstr().unwrap()).unwrap();
    let mut q = db
        .prepare(c"SELECT group_concat(value, ',') FROM item ORDER BY id")
        .unwrap();
    assert!(q.step().unwrap());
    match q.column(0).unwrap() {
        Value::Text(s) => s.to_string(),
        v => panic!("{v:?}"),
    }
}
fn verify(heap: &mut [u64], b: &mut Fs, name: &Name) -> replica_core::Result {
    // SAFETY: Eén SQLite-test in deze binary; geen andere engine leeft nu.
    let mut engine = unsafe { Engine::initialize(heap, b) }?;
    let mut db = engine.open(name.cstr()?)?;
    let mut q = db.prepare(c"PRAGMA integrity_check")?;
    assert!(q.step()?);
    assert_eq!(q.column(0)?, Value::Text("ok"));
    Ok(())
}
/// De generatie in `current` en de hoogste sequence daarin.
fn tip(bucket: &mut Bucket) -> (String, u64) {
    let current = String::from_utf8(bucket.data[&format!("{NS}/current")].clone()).unwrap();
    let prefix = replication::generation_prefix(NS, &current).unwrap();
    let layout = object::layout(bucket, &prefix).unwrap();
    let last = layout.plan(None).unwrap().last().unwrap().sequence;
    (current, last)
}
/// A publiceert 1 en 2 (onder token 1).
fn a_publishes(heap: &mut [u64], bucket: &mut Bucket, lease: &mut MemBackend) -> (Replica, Fs) {
    let mut fa = Fs::default();
    let mut ra = Replica::prepare(
        &mut fa,
        bucket,
        writer(lease, "a", 0),
        config(),
        at(0),
        |_, _| panic!("lege bootstrap"),
    )
    .unwrap();
    for (t, insert) in [
        (1_000, c"CREATE TABLE item(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO item(value) VALUES('a1')"),
        (2_000, c"INSERT INTO item(value) VALUES('a2')"),
    ] {
        sql(&ra, &mut fa, heap, insert);
        fa.clock.set(t);
        assert!(ra.sync(&mut fa, bucket, at(t)).unwrap().published);
    }
    assert_eq!(ra.marker().sequence, 2);
    assert_eq!(ra.marker().lease, 1);
    (ra, fa)
}
/// A publiceert 1 en 2 en stopt (met `unpublished`: met een write die nooit de
/// store haalde); B neemt over en publiceert 3. Geeft A's koude volume.
fn a_then_b(
    heap: &mut [u64],
    bucket: &mut Bucket,
    lease: &mut MemBackend,
    unpublished: bool,
) -> Fs {
    let (ra, mut fa) = a_publishes(heap, bucket, lease);
    if unpublished {
        // Een crash, of een lease die bleef en verliep: a3 haalde de store nooit.
        sql(&ra, &mut fa, heap, c"INSERT INTO item(value) VALUES('a3')");
        drop(ra);
    } else {
        // Een nette stop: alles gepubliceerd, de lease vrij.
        ra.release(lease).unwrap();
    }
    let mut fb = Fs::default();
    fb.clock.set(40_000);
    let mut rb = Replica::prepare(
        &mut fb,
        bucket,
        writer(lease, "b", 40_000),
        config(),
        at(40_000),
        |b, name| verify(heap, b, name),
    )
    .unwrap();
    assert_eq!(rb.status().reason, Reason::Restored);
    sql(&rb, &mut fb, heap, c"INSERT INTO item(value) VALUES('b3')");
    fb.clock.set(41_000);
    assert!(rb.sync(&mut fb, bucket, at(41_000)).unwrap().published);
    assert_eq!(rb.marker().sequence, 3);
    rb.release(lease).unwrap();
    fa.cold()
}
#[test]
fn an_old_volume_never_replaces_the_work_of_another_writer() {
    let mut heap = vec![0u64; 512 * 1024];
    let mut run = |f: &mut dyn FnMut(&mut [u64])| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&mut heap))).is_ok()
    };
    // Schoon: A herstelt uit de store en gaat door op B's sequence 3.
    let clean = run(&mut |heap| {
        let mut bucket = Bucket::default();
        let mut lease = MemBackend::new();
        let mut fa = a_then_b(heap, &mut bucket, &mut lease, false);
        let before = tip(&mut bucket);
        assert_eq!(before.1, 3);
        fa.clock.set(50_000);
        let ra = Replica::prepare(
            &mut fa,
            &mut bucket,
            writer(&mut lease, "a", 50_000),
            config(),
            at(50_000),
            |b, name| verify(heap, b, name),
        )
        .unwrap();
        assert_eq!(ra.status().reason, Reason::Restored);
        assert_eq!(ra.marker().generation, before.0);
        assert_eq!(ra.marker().sequence, 3);
        assert_eq!(
            items(heap, &mut fa.cold(), &Name::new("db").unwrap()),
            "a1,a2,b3"
        );
        assert_eq!(tip(&mut bucket), before, "B's sequence 3 blijft current");
    });
    // Eigen ongepubliceerd werk: A opent niet en raakt niets aan.
    let dirty = run(&mut |heap| {
        let mut bucket = Bucket::default();
        let mut lease = MemBackend::new();
        let mut fa = a_then_b(heap, &mut bucket, &mut lease, true);
        let before = tip(&mut bucket);
        let files = fa.stable.clone();
        let objects = bucket.data.clone();
        fa.clock.set(70_000);
        let result = Replica::prepare(
            &mut fa,
            &mut bucket,
            writer(&mut lease, "a", 70_000),
            config(),
            at(70_000),
            |_, _| panic!("geen herstel over eigen werk heen"),
        );
        match result {
            Err(e) => assert_eq!(e, Error::ForeignAhead),
            Ok(r) => panic!("A opende met {:?}", r.status().reason),
        }
        assert_eq!(fa.stable, files, "A's bestanden onaangeroerd");
        assert_eq!(bucket.data, objects, "de store onaangeroerd");
        assert_eq!(tip(&mut bucket), before);
        assert_eq!(
            items(heap, &mut fa.cold(), &Name::new("db").unwrap()),
            "a1,a2,a3"
        );
    });
    // Ons eigen volume na een nette stop: gewoon doorgaan, in dezelfde generatie.
    let reopen = run(&mut |heap| {
        let mut bucket = Bucket::default();
        let mut lease = MemBackend::new();
        let (ra, fa) = a_publishes(heap, &mut bucket, &mut lease);
        ra.release(&mut lease).unwrap();
        let before = tip(&mut bucket);
        let mut fa = fa.cold();
        fa.clock.set(50_000);
        let mut ra = Replica::prepare(
            &mut fa,
            &mut bucket,
            writer(&mut lease, "a", 50_000),
            config(),
            at(50_000),
            |_, _| panic!("eigen volume"),
        )
        .unwrap();
        assert_eq!(ra.status().reason, Reason::Continued);
        assert_eq!(tip(&mut bucket), before);
        sql(&ra, &mut fa, heap, c"INSERT INTO item(value) VALUES('a3')");
        fa.clock.set(51_000);
        assert!(ra.sync(&mut fa, &mut bucket, at(51_000)).unwrap().published);
        assert_eq!(tip(&mut bucket), (before.0, 3));
        assert_eq!(ra.marker().lease, ra.writer().generation());
    });
    // Een crash midden in een publicatie: het manifest van a3 landde, het
    // antwoord niet. De marker blijft onzeker met ons token; na de herstart
    // (een nieuwe lease) is dat manifest nog steeds het onze.
    let crash = run(&mut |heap| {
        let mut bucket = Bucket::default();
        let mut lease = MemBackend::new();
        let (mut ra, mut fa) = a_publishes(heap, &mut bucket, &mut lease);
        sql(&ra, &mut fa, heap, c"INSERT INTO item(value) VALUES('a3')");
        bucket.fail_put_match = Some("/L0/".into());
        fa.clock.set(3_000);
        // Ook de controle-GET na de PUT faalt: de uitkomst blijft onbekend.
        bucket.get_error = Some(StoreError::Transport);
        assert_eq!(
            ra.sync(&mut fa, &mut bucket, at(3_000)).err(),
            Some(Error::Object(StoreError::Transport))
        );
        bucket.get_error = None;
        bucket.fail_put_match = None;
        drop(ra);
        let mut fa = fa.cold();
        let marker = LocalMarker::load(&mut fa, Name::new("db").unwrap()).unwrap();
        assert_eq!(
            (marker.value.uncertain, marker.value.uncertain_lease),
            (3, 1)
        );
        assert_eq!(tip(&mut bucket).1, 3, "het manifest landde");
        fa.clock.set(70_000);
        let mut ra = Replica::prepare(
            &mut fa,
            &mut bucket,
            writer(&mut lease, "a", 70_000),
            config(),
            at(70_000),
            |_, _| panic!("eigen werk wordt niet hersteld"),
        )
        .unwrap();
        assert_eq!(ra.status().reason, Reason::NewSnapshot);
        fa.clock.set(71_000);
        assert!(ra.sync(&mut fa, &mut bucket, at(71_000)).unwrap().published);
        let (current, _) = tip(&mut bucket);
        assert_eq!(current, ra.marker().generation);
        assert_eq!(
            items(heap, &mut fa.cold(), &Name::new("db").unwrap()),
            "a1,a2,a3"
        );
    });
    assert!(
        clean && dirty && reopen && crash,
        "schoon: {clean}, eigen werk: {dirty}, eigen volume: {reopen}, crash: {crash}"
    );
}
