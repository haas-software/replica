//! Een capture buiten de eigenaar: terwijl de segmenten gelezen worden, schrijft
//! SQL via de VFS door. De schaduw houdt het beeld op het moment van het begin;
//! het increment erna draagt wat intussen geschreven is.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    archive::{self, Fetch},
    local::Name,
    owner::{Begun, Config, Replica},
    time::Time,
};
use replica_sqlite::{Engine, Value};
use support::{Bucket, Fs};
fn at(n: i64) -> Time {
    Time::unix(1_790_765_296 + n, 0).unwrap()
}
fn config() -> Config {
    let mut config = Config::new(
        "replica/test",
        &"1".repeat(64),
        Name::new("db").unwrap(),
        65536,
    )
    .unwrap();
    config.segment_bytes = 65536;
    config
}
fn sql(r: &mut Replica, fs: &mut Fs, heap: &mut [u64], sql: &std::ffi::CStr) {
    let mut vfs = r.vfs(fs).unwrap();
    // SAFETY: Eén test met C-SQLite in deze testbinary; iedere engine sluit vóór de volgende.
    let mut engine = unsafe { Engine::initialize(heap, &mut vfs) }.unwrap();
    engine.open(c"db").unwrap().execute(sql).unwrap();
}
/// Herstelt de laatste commit van `generation` en leest er één getal uit.
fn restored(
    fs: &mut Fs,
    bucket: &mut Bucket,
    heap: &mut [u64],
    generation: &str,
    query: &std::ffi::CStr,
) -> i64 {
    let mut result = 0;
    archive::fetch(
        fs,
        bucket,
        Fetch {
            namespace: "replica/test",
            generation,
            at: None,
            live: Name::new("db").unwrap(),
            destination: Name::new("restored").unwrap(),
            page_limit: 65536,
        },
        |b, name| {
            // SAFETY: Alle engines hierboven zijn gedropt; dezelfde boot-eigenaar.
            let mut engine = unsafe { Engine::initialize(heap, b) }?;
            let mut db = engine.open(name.cstr()?)?;
            let mut check = db.prepare(c"PRAGMA integrity_check")?;
            assert!(check.step()?);
            assert_eq!(check.column(0)?, Value::Text("ok"));
            drop(check);
            let mut q = db.prepare(query)?;
            assert!(q.step()?);
            let Value::Integer(n) = q.column(0)? else {
                panic!("integer expected");
            };
            result = n;
            Ok(())
        },
    )
    .unwrap();
    result
}
#[test]
fn writes_during_a_capture_stay_out_of_its_image_and_in_the_next_increment() {
    let mut fs = Fs::default();
    let mut bucket = Bucket::default();
    let mut heap = vec![0u64; 512 * 1024];
    let mut r = Replica::prepare(
        &mut fs,
        &mut bucket,
        support::writer(),
        config(),
        at(0),
        |_, _| panic!("empty bootstrap needs no restore"),
    )
    .unwrap();
    sql(
        &mut r,
        &mut fs,
        &mut heap,
        c"CREATE TABLE item(id INTEGER PRIMARY KEY, value TEXT);
          WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<4000)
          INSERT INTO item(value) SELECT printf('%0500d', i) FROM n",
    );
    // Het beeld begint; de eigenaar leest nog niets.
    let Some(Begun::Capture(mut capturing)) = r.begin_capture(&mut fs, &mut bucket, at(1)).unwrap()
    else {
        panic!("a first snapshot");
    };
    // Een paar segmenten gelezen, dan schrijft SQL door: vroege pagina's (al
    // gelezen), late pagina's (nog te lezen) en nieuwe pagina's erachter.
    for _ in 0..3 {
        assert!(capturing.step(&mut fs).unwrap());
    }
    sql(
        &mut r,
        &mut fs,
        &mut heap,
        c"UPDATE item SET value = 'changed' WHERE id IN (1, 2, 3, 2000, 3998, 3999, 4000);
          WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<500)
          INSERT INTO item(value) SELECT printf('new%0500d', i) FROM n",
    );
    // Halverwege nog een keer, zodat ook pagina's net na de frontier geraakt worden.
    for _ in 0..10 {
        assert!(capturing.step(&mut fs).unwrap());
    }
    sql(
        &mut r,
        &mut fs,
        &mut heap,
        c"UPDATE item SET value = 'changed' WHERE id BETWEEN 100 AND 300;
          DELETE FROM item WHERE id BETWEEN 3000 AND 3100",
    );
    let pending = capturing.run(&mut fs).unwrap();
    let uploaded = pending.upload(&mut fs, &mut bucket);
    r.finish(&mut fs, &mut bucket, pending, uploaded, at(2))
        .unwrap();
    assert!(r.marker().complete);
    let generation = r.marker().generation.clone();
    // Het snapshot is het beeld van vóór de writes: 4000 rijen, niets 'changed'.
    r.close();
    assert_eq!(
        restored(
            &mut fs,
            &mut bucket,
            &mut heap,
            &generation,
            c"SELECT count(*) FROM item"
        ),
        4000
    );
    assert_eq!(
        restored(
            &mut fs,
            &mut bucket,
            &mut heap,
            &generation,
            c"SELECT count(*) FROM item WHERE value = 'changed'"
        ),
        0
    );
    // Het increment erna draagt alle writes van tijdens de capture.
    let mut r = Replica::prepare(
        &mut fs,
        &mut bucket,
        support::writer(),
        config(),
        at(30),
        |_, _| panic!("a complete generation needs no restore"),
    )
    .unwrap();
    let pending = r.begin(&mut fs, &mut bucket, at(31)).unwrap().unwrap();
    assert!(pending.pages() > 0);
    let uploaded = pending.upload(&mut fs, &mut bucket);
    r.finish(&mut fs, &mut bucket, pending, uploaded, at(32))
        .unwrap();
    assert_eq!(r.marker().sequence, 2);
    r.close();
    assert_eq!(
        restored(
            &mut fs,
            &mut bucket,
            &mut heap,
            &generation,
            c"SELECT count(*) FROM item"
        ),
        4000 + 500 - 101
    );
    assert_eq!(
        restored(
            &mut fs,
            &mut bucket,
            &mut heap,
            &generation,
            c"SELECT count(*) FROM item WHERE value = 'changed'"
        ),
        7 + 201
    );
    // Een tweede beurt zonder writes heeft niets te doen: de dirty set is leeg.
    let mut r = Replica::prepare(
        &mut fs,
        &mut bucket,
        support::writer(),
        config(),
        at(60),
        |_, _| panic!("no restore"),
    )
    .unwrap();
    assert!(r.begin(&mut fs, &mut bucket, at(61)).unwrap().is_none());
}
