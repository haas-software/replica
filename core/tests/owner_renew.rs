//! Een snapshot van een grote database duurt minuten; de lease wordt per
//! segment vernieuwd en een verloren lease breekt de capture af.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    Error,
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
    let mut config = Config::new(
        "replica/test",
        &"1".repeat(64),
        Name::new("db").unwrap(),
        65536,
    )
    .unwrap();
    // Kleine segmenten: een capture van 2 MB telt dan tientallen vernieuwingen.
    config.segment_bytes = 65536;
    config
}
#[test]
fn a_capture_renews_the_lease_before_every_segment_and_stops_when_it_is_lost() {
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
    {
        let mut vfs = r.vfs(&mut fs).unwrap();
        // SAFETY: Eén test met C-SQLite in deze testbinary.
        let mut engine = unsafe { Engine::initialize(&mut heap, &mut vfs) }.unwrap();
        engine
            .open(c"db")
            .unwrap()
            .execute(
                c"CREATE TABLE item(id INTEGER PRIMARY KEY, value TEXT);
                  WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<4000)
                  INSERT INTO item(value) SELECT printf('%0500d', i) FROM n",
            )
            .unwrap();
    }
    // De eerste capture is het snapshot: meer dan één segment van 64 KiB.
    let mut renewals = 0;
    let pending = r
        .begin_with(&mut fs, &mut bucket, at(1), &mut |_writer| {
            renewals += 1;
            Ok(())
        })
        .unwrap()
        .unwrap();
    assert!(
        renewals >= 2,
        "one renewal per segment: {renewals} for {} pages",
        pending.pages()
    );
    let uploaded = pending.upload(&mut fs, &mut bucket);
    r.finish(&mut fs, &mut bucket, pending, uploaded, at(2))
        .unwrap();
    // Een verloren lease tijdens de capture is de fout van deze beurt.
    {
        let mut vfs = r.vfs(&mut fs).unwrap();
        // SAFETY: De vorige engine is gesloten; dezelfde boot-eigenaar.
        let mut engine = unsafe { Engine::initialize(&mut heap, &mut vfs) }.unwrap();
        engine
            .open(c"db")
            .unwrap()
            .execute(c"INSERT INTO item(value) VALUES('later')")
            .unwrap();
    }
    let lost = r.begin_with(&mut fs, &mut bucket, at(20), &mut |_| Err(Error::LeaseLost));
    assert!(matches!(lost, Err(Error::LeaseLost)));
}
