//! Een onderbroken snapshot-upload gaat na een herstart verder waar hij
//! ophield: de delen die al in de store staan blijven, en wat intussen
//! geschreven werd, komt met het volgende increment mee.
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
    config.segment_bytes = 65536;
    config
}
fn sql(r: &mut Replica, fs: &mut Fs, heap: &mut [u64], sql: &std::ffi::CStr) {
    let mut vfs = r.vfs(fs).unwrap();
    // SAFETY: Eén test met C-SQLite in deze testbinary; iedere engine sluit vóór de volgende.
    let mut engine = unsafe { Engine::initialize(heap, &mut vfs) }.unwrap();
    engine.open(c"db").unwrap().execute(sql).unwrap();
}
fn segments(bucket: &Bucket) -> usize {
    bucket.data.keys().filter(|k| k.ends_with(".seg")).count()
}
#[test]
fn an_interrupted_snapshot_upload_resumes_after_a_restart() {
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
    // De eerste snapshot breekt bij het tiende deel: negen delen staan er.
    bucket.fail_put_match = Some("000010.seg".into());
    bucket.fail_before_put = true;
    let pending = r.begin(&mut fs, &mut bucket, at(1)).unwrap().unwrap();
    let total = pending.pages().div_ceil(16) as usize;
    assert!(total > 12, "enough segments to interrupt: {total}");
    let uploaded = pending.upload(&mut fs, &mut bucket);
    assert!(uploaded.is_err());
    assert!(
        r.finish(&mut fs, &mut bucket, pending, uploaded, at(2))
            .is_err()
    );
    assert_eq!(segments(&bucket), 9);
    assert!(!r.marker().complete);
    let generation = r.marker().generation.clone();
    // Een write na de capture, vóór de herstart.
    sql(
        &mut r,
        &mut fs,
        &mut heap,
        c"INSERT INTO item(value) VALUES('later')",
    );
    r.close();
    bucket.fail_put_match = None;
    bucket.fail_before_put = false;
    let mut r = Replica::prepare(
        &mut fs,
        &mut bucket,
        support::writer(),
        config(),
        at(30),
        |_, _| panic!("a resumable snapshot needs no restore"),
    )
    .unwrap();
    assert!(r.resumes(), "the record and spool survive the restart");
    assert_eq!(r.marker().generation, generation);
    let pending = r.begin(&mut fs, &mut bucket, at(31)).unwrap().unwrap();
    let uploaded = pending.upload(&mut fs, &mut bucket);
    assert!(uploaded.is_ok());
    assert_eq!(segments(&bucket), total, "only the missing parts were sent");
    r.finish(&mut fs, &mut bucket, pending, uploaded, at(32))
        .unwrap();
    assert!(r.marker().complete);
    assert_eq!(r.marker().generation, generation);
    assert!(!r.resumes());
    // Het increment erna draagt de write van vóór de herstart. Welke pagina's
    // na de capture zijn geschreven, is na een herstart niet meer per pagina
    // bekend: alles wat dirty was blijft dirty, een veilige superset.
    let pending = r.begin(&mut fs, &mut bucket, at(50)).unwrap().unwrap();
    assert!(
        pending.pages() >= 1 && pending.pages() <= 503,
        "{} pages",
        pending.pages()
    );
    let uploaded = pending.upload(&mut fs, &mut bucket);
    r.finish(&mut fs, &mut bucket, pending, uploaded, at(51))
        .unwrap();
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
            let mut q = db.prepare(c"SELECT count(*) FROM item WHERE value='later'")?;
            assert!(q.step()?);
            assert_eq!(q.column(0)?, Value::Integer(1));
            Ok(())
        },
    )
    .unwrap();
}
