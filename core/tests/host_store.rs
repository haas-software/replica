//! De host-vorm: opslag (`replica_sqlite::host::Dir`), objectstore en lease
//! (`replica_core::host`) in lokale mappen. Een volledige ronde, en een
//! tweede node die uit dezelfde store herstelt (één SQLite-test per binary).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use replica_core::{
    host::{Dir as Bucket, DirLease},
    local::Name,
    object::{Store, StoreError},
    owner::{Config, Replica},
    prepare::Reason,
    time::Time,
    writer::{self, Role},
};
use replica_sqlite::{Engine, Storage, Value, host::Dir};
use std::path::PathBuf;

fn scratch() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("replica-hoststore-{}-{nanos}", std::process::id()))
}
fn now() -> Time {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    Time::unix(d.as_secs() as i64, d.subsec_nanos()).unwrap()
}
fn config() -> Config {
    Config::new(
        "scoot/test/tenant",
        &"2".repeat(64),
        Name::new("db.sqlite").unwrap(),
        65536,
    )
    .unwrap()
}
fn count<B: Storage>(db: &mut replica_sqlite::Connection<'_, '_, B>) -> i64 {
    let mut s = db.prepare(c"SELECT count(*) FROM item").unwrap();
    assert!(s.step().unwrap());
    let Value::Integer(n) = s.column(0).unwrap() else {
        panic!("integer")
    };
    n
}

#[test]
fn a_second_node_restores_from_the_directory_store() {
    let root = scratch();
    // De store zelf: atomair vervangen, sleutels en prefixen naast elkaar.
    let mut bucket = Bucket::new(root.join("bucket")).unwrap();
    bucket.put("a/b", b"1").unwrap();
    bucket.put("a/b/c", b"22").unwrap();
    bucket.put("a/b/c", b"333").unwrap();
    bucket.put("a%x", b"4").unwrap();
    assert_eq!(bucket.get("a/b/c", 16).unwrap(), b"333");
    assert_eq!(bucket.get("a/b/c", 2), Err(StoreError::Limit));
    assert_eq!(bucket.get("a/none", 16), Err(StoreError::Missing));
    let keys: Vec<_> = bucket
        .list("a/", 10)
        .unwrap()
        .into_iter()
        .map(|o| o.key)
        .collect();
    assert_eq!(keys, ["a/b", "a/b/c"]);
    assert_eq!(bucket.list("a", 2).err(), Some(StoreError::Limit));
    assert_eq!(bucket.directories("a/", 10).unwrap(), ["a/b/"]);
    bucket.delete("a/b").unwrap();
    bucket.delete("a/b").unwrap();
    assert_eq!(bucket.list("a/", 10).unwrap().len(), 1);
    assert_eq!(bucket.put(&"k".repeat(300), b""), Err(StoreError::Limit));

    let mut heap = vec![0_u64; 512 * 1024];
    let mut engine_dir = Dir::new(root.join("engine")).unwrap();
    // SAFETY: De enige SQLite-runtime in deze testbinary, op deze thread.
    let engine = unsafe { Engine::initialize(&mut heap, &mut engine_dir) }.unwrap();
    let lease_key = "scoot/test/tenant/lease";

    // Node A: claim, prepare, SQL, sync, release.
    let mut local_a = Dir::new(root.join("a")).unwrap();
    let mut lease = DirLease::new(&bucket, lease_key).unwrap();
    let Role::Writer(w) = writer::claim(&mut local_a, &mut lease, "node-a", 30_000, now()).unwrap()
    else {
        panic!("an empty lease makes a writer");
    };
    // Een tweede eigenaar ziet de lease van A en wordt lezer.
    let mut lease_b = DirLease::new(&bucket, lease_key).unwrap();
    let role = writer::claim(
        &mut Dir::new(root.join("b")).unwrap(),
        &mut lease_b,
        "node-b",
        30_000,
        now(),
    )
    .unwrap();
    assert!(matches!(role, Role::Reader { leader: Some(ref l) } if l.starts_with("node-a#")));
    let mut r = Replica::prepare(&mut local_a, &mut bucket, w, config(), now(), |_, _| {
        panic!("an empty bucket needs no restore")
    })
    .unwrap();
    assert_eq!(r.status().reason, Reason::Bootstrap);
    {
        let tracker = r.tracker().unwrap();
        let mut sql_side = Dir::new(root.join("a")).unwrap();
        let mut vfs = tracker.vfs(&mut sql_side).unwrap();
        let mut db = engine.open_with(c"db.sqlite", &mut vfs).unwrap();
        db.execute(
            c"CREATE TABLE item(id INTEGER PRIMARY KEY, v TEXT);
            WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<2500)
            INSERT INTO item(v) SELECT printf('%0100d', i) FROM n",
        )
        .unwrap();
        let synced = r.sync(&mut local_a, &mut bucket, now()).unwrap();
        assert!(synced.published);
        assert_eq!(count(&mut db), 2500);
    }
    r.renew(&mut lease, now()).unwrap();
    r.release(&mut lease).unwrap();

    // Node B: een lege lokale map, dezelfde store: restore, geverifieerd.
    let mut local_b = Dir::new(root.join("b")).unwrap();
    let Role::Writer(w) =
        writer::claim(&mut local_b, &mut lease_b, "node-b", 30_000, now()).unwrap()
    else {
        panic!("a released lease makes a writer");
    };
    let mut verified = false;
    let r = Replica::prepare(&mut local_b, &mut bucket, w, config(), now(), |b, name| {
        let mut db = engine.open_with(name.cstr()?, b)?;
        let mut s = db.prepare(c"PRAGMA integrity_check")?;
        assert!(s.step()?);
        assert_eq!(s.column(0)?, Value::Text("ok"));
        verified = true;
        Ok(())
    })
    .unwrap();
    assert!(verified);
    assert_eq!(r.status().reason, Reason::Restored);
    {
        let tracker = r.tracker().unwrap();
        let mut sql_side = Dir::new(root.join("b")).unwrap();
        let mut vfs = tracker.vfs(&mut sql_side).unwrap();
        let mut db = engine.open_with(c"db.sqlite", &mut vfs).unwrap();
        assert_eq!(count(&mut db), 2500);
    }
    r.release(&mut lease_b).unwrap();
    drop(engine);
    std::fs::remove_dir_all(&root).unwrap();
}
