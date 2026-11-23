//! Het onderhoud als losse taak naast de eigenaar: SQL schrijft door, de
//! frontier houdt nieuwe commits buiten de samengevoegde vensters, en het
//! archief blijft exact herstelbaar (één SQLite-test per binary).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    archive::{self, Fetch},
    local::Name,
    owner::{Config, Replica},
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
    config.background_maintenance = true;
    config
}
#[test]
fn maintenance_runs_beside_the_owner_and_the_archive_stays_exact() {
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
    let sql = |r: &mut Replica, fs: &mut Fs, heap: &mut [u64], sql: &std::ffi::CStr| {
        let mut vfs = r.vfs(fs).unwrap();
        // SAFETY: Eén test met C-SQLite in deze testbinary; iedere engine sluit vóór de volgende.
        let mut engine = unsafe { Engine::initialize(heap, &mut vfs) }.unwrap();
        engine.open(c"db").unwrap().execute(sql).unwrap();
    };
    let sync = |r: &mut Replica, fs: &mut Fs, bucket: &mut Bucket, t: i64| {
        let pending = r.begin(fs, bucket, at(t)).unwrap().expect("a capture");
        let uploaded = pending.upload(fs, bucket);
        let synced = r.finish(fs, bucket, pending, uploaded, at(t + 1)).unwrap();
        assert!(synced.published);
        // Met background_maintenance doet de afronding geen onderhoud.
        assert_eq!(synced.maintenance.merged + synced.maintenance.pruned, 0);
    };
    sql(
        &mut r,
        &mut fs,
        &mut heap,
        c"CREATE TABLE item(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO item(value) VALUES('a')",
    );
    sync(&mut r, &mut fs, &mut bucket, 1);
    // Increments in verschillende kwartiervensters.
    for (i, t) in [(2, 1000), (3, 2000), (4, 3000)] {
        let insert =
            std::ffi::CString::new(format!("INSERT INTO item(value) VALUES('{i}')")).unwrap();
        sql(&mut r, &mut fs, &mut heap, &insert);
        sync(&mut r, &mut fs, &mut bucket, t);
    }
    // Er staat geen inline onderhoud meer achter begin.
    assert!(r.begin(&mut fs, &mut bucket, at(3100)).unwrap().is_none());
    // Ook niet achter een sync zonder wijzigingen (een flush): de vensters
    // blijven voor de taak.
    let synced = r.sync(&mut fs, &mut bucket, at(3200)).unwrap();
    assert!(!synced.published);
    assert_eq!(
        synced.maintenance.merged + synced.maintenance.pruned + synced.expired_objects,
        0,
        "{synced:?}"
    );
    let job = r
        .maintenance_begin(&mut fs, at(5000))
        .unwrap()
        .expect("maintenance is due");
    assert!(r.marker().sealed_at >= Time::unix(at(5000).seconds(), 0).unwrap());
    // Een tweede taak start niet, en een capture wacht tot de taak klaar is.
    assert!(r.maintenance_begin(&mut fs, at(5001)).unwrap().is_none());
    sql(
        &mut r,
        &mut fs,
        &mut heap,
        c"INSERT INTO item(value) VALUES('during')",
    );
    assert!(r.begin(&mut fs, &mut bucket, at(5020)).unwrap().is_none());
    // De taak raakt alleen de store; hier draait hij gewoon in de test.
    let result = job.run(&mut fs, &mut bucket, support::lease_of(&r));
    let synced = r.maintenance_finish(&mut fs, result).unwrap();
    assert!(synced.maintenance.merged >= 1, "{synced:?}");
    // Binnen een minuut geen nieuw onderhoud; de write van tijdens de taak gaat mee.
    assert!(r.maintenance_begin(&mut fs, at(5030)).unwrap().is_none());
    sync(&mut r, &mut fs, &mut bucket, 5040);
    let generation = r.marker().generation.clone();
    r.close();
    archive::fetch(
        &mut fs,
        &mut bucket,
        Fetch {
            namespace: "replica/test",
            generation: &generation,
            at: None,
            live: Name::new("db").unwrap(),
            destination: Name::new("restored").unwrap(),
            page_limit: 65536,
        },
        |b, name| {
            // SAFETY: Alle engines hierboven zijn gedropt; dezelfde boot-eigenaar.
            let mut engine = unsafe { Engine::initialize(&mut heap, b) }?;
            let mut db = engine.open(name.cstr()?)?;
            {
                let mut q = db.prepare(c"PRAGMA integrity_check")?;
                assert!(q.step()?);
                assert_eq!(q.column(0)?, Value::Text("ok"));
            }
            let mut q = db.prepare(c"SELECT count(*) FROM item")?;
            assert!(q.step()?);
            assert_eq!(q.column(0)?, Value::Integer(5));
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        fs.cold().data("restored").unwrap(),
        fs.cold().data("db").unwrap()
    );
}
