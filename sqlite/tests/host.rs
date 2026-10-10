//! Echte bestanden op de host (`host::Dir`) en meerdere verbindingen op één
//! engine (`Engine::open_with`), elk met hun eigen map. Eén test per binary:
//! de C-runtime is procesglobaal.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use replica_sqlite::{Connection, Engine, Storage, Value, host::Dir};
use std::{ffi::CStr, path::PathBuf};

fn scratch(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "replica-host-{}-{nanos}-{name}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn integer<B: Storage>(db: &mut Connection<'_, '_, B>, sql: &CStr) -> i64 {
    let mut s = db.prepare(sql).unwrap();
    assert!(s.step().unwrap());
    let Value::Integer(n) = s.column(0).unwrap() else {
        panic!("integer expected")
    };
    n
}

fn integrity<B: Storage>(db: &mut Connection<'_, '_, B>) {
    let mut s = db.prepare(c"PRAGMA integrity_check").unwrap();
    assert!(s.step().unwrap());
    assert_eq!(s.column(0).unwrap(), Value::Text("ok"));
}

#[test]
fn connections_on_their_own_directories_share_one_engine() {
    let root = scratch("engine");
    let mut heap = vec![0_u64; 1024 * 1024];
    let mut engine_dir = Dir::new(root.join("engine")).unwrap();
    let mut a = Dir::new(root.join("a")).unwrap();
    let mut b = Dir::new(root.join("b")).unwrap();
    {
        // SAFETY: De enige SQLite-runtime in deze testbinary, op deze thread.
        let engine = unsafe { Engine::initialize(&mut heap, &mut engine_dir) }.unwrap();
        let mut x = engine.open_with(c"db.sqlite", &mut a).unwrap();
        let mut y = engine.open_with(c"db.sqlite", &mut b).unwrap();
        x.execute(c"CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        y.execute(c"CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        // Om en om, met een open transactie op allebei tegelijk.
        x.execute(c"BEGIN").unwrap();
        y.execute(c"BEGIN").unwrap();
        for i in 0..500 {
            let mut s = x.prepare(c"INSERT INTO t(v) VALUES(?1)").unwrap();
            s.bind(1, Value::Text("a")).unwrap();
            assert!(!s.step().unwrap());
            drop(s);
            let mut s = y.prepare(c"INSERT INTO t(v) VALUES(?1)").unwrap();
            s.bind(1, Value::Text(if i % 2 == 0 { "b" } else { "bb" }))
                .unwrap();
            assert!(!s.step().unwrap());
        }
        x.execute(c"COMMIT").unwrap();
        y.execute(c"ROLLBACK").unwrap();
        y.execute(c"INSERT INTO t(v) VALUES('only')").unwrap();
        // Een sorter die naar een tijdelijk bestand in de eigen map spilt.
        x.execute(c"CREATE INDEX tv ON t(v)").unwrap();
        assert_eq!(integer(&mut x, c"SELECT count(*) FROM t"), 500);
        assert_eq!(integer(&mut y, c"SELECT count(*) FROM t"), 1);
        integrity(&mut x);
        integrity(&mut y);
        // Een vergeten verbinding: haar opslag mag daarna weg zijn; de engine
        // sluit haar bij drop zonder die opslag nog aan te raken.
        let mut c = Dir::new(root.join("c")).unwrap();
        let mut z = engine.open_with(c"db.sqlite", &mut c).unwrap();
        z.execute(c"CREATE TABLE gone(x)").unwrap();
        std::mem::forget(z);
        drop(c);
    }
    // Koud heropenen: elke map heeft precies zijn eigen database.
    assert!(root.join("a/db.sqlite").exists());
    assert!(!root.join("a/db.sqlite-journal").exists());
    assert!(!root.join("engine/db.sqlite").exists());
    // SAFETY: De vorige runtime is gedropt; dezelfde testthread.
    let engine = unsafe { Engine::initialize(&mut heap, &mut engine_dir) }.unwrap();
    let mut x = engine.open_read_only_with(c"db.sqlite", &mut a).unwrap();
    assert_eq!(integer(&mut x, c"SELECT count(*) FROM t"), 500);
    assert!(x.execute(c"INSERT INTO t(v) VALUES('no')").is_err());
    integrity(&mut x);
    let mut y = engine.open_with(c"db.sqlite", &mut b).unwrap();
    assert_eq!(integer(&mut y, c"SELECT count(*) FROM t"), 1);
    // Geen ontsnapping uit de map.
    let mut z = Dir::new(root.join("z")).unwrap();
    assert!(engine.open_with(c"../a/db.sqlite", &mut z).is_err());
    drop(x);
    drop(y);
    drop(engine);
    std::fs::remove_dir_all(&root).unwrap();
}
