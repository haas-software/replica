//! Een write tijdens de upload houdt alleen zijn eigen pagina's dirty, niet de
//! hele vorige capture (één SQLite-test per binary).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    local::Name,
    owner::{Config, Replica},
    time::Time,
};
use replica_sqlite::Engine;
use support::{Bucket, Fs};
fn at(n: i64) -> Time {
    Time::unix(1_790_765_296 + n, 0).unwrap()
}
fn config() -> Config {
    Config::new(
        "replica/test",
        &"1".repeat(64),
        Name::new("db").unwrap(),
        65536,
    )
    .unwrap()
}
#[test]
fn a_write_during_the_upload_keeps_only_its_own_pages_dirty() {
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
    // Veel pagina's in de eerste capture.
    sql(
        &mut r,
        &mut fs,
        &mut heap,
        c"CREATE TABLE item(id INTEGER PRIMARY KEY, value TEXT);
          WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<4000)
          INSERT INTO item(value) SELECT printf('%0500d', i) FROM n",
    );
    let pending = r.begin(&mut fs, &mut bucket, at(1)).unwrap().unwrap();
    let first = pending.pages();
    assert!(first > 20, "the bulk insert spans many pages: {first}");
    // Tijdens de upload één kleine write.
    sql(
        &mut r,
        &mut fs,
        &mut heap,
        c"UPDATE item SET value='changed' WHERE id=1",
    );
    let uploaded = pending.upload(&mut fs, &mut bucket);
    assert!(
        r.finish(&mut fs, &mut bucket, pending, uploaded, at(2))
            .unwrap()
            .published
    );
    assert!(!r.marker().clean, "the write during the upload stays dirty");
    // De volgende capture neemt alleen die write mee, niet de eerste capture opnieuw.
    let pending = r.begin(&mut fs, &mut bucket, at(40)).unwrap().unwrap();
    let second = pending.pages();
    assert!(
        second <= 4,
        "only the pages of the late write: {second} of {first}"
    );
    let uploaded = pending.upload(&mut fs, &mut bucket);
    assert!(
        r.finish(&mut fs, &mut bucket, pending, uploaded, at(41))
            .unwrap()
            .published
    );
    assert!(r.marker().clean);
    assert!(r.begin(&mut fs, &mut bucket, at(60)).unwrap().is_none());
}
