//! Restore haalt segmenten in begrensde rondes tegelijk op en past ze in volgorde toe.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    local::Name,
    object::Parallel,
    owner::{Config, Replica},
    prepare::Reason,
    time::Time,
};
use replica_sqlite::Engine;
use support::{Bucket, Fs};
fn at(n: i64) -> Time {
    Time::unix(1_790_765_296 + n, 0).unwrap()
}
fn config() -> Config {
    let mut c = Config::new(
        "replica/test",
        &"1".repeat(64),
        Name::new("db").unwrap(),
        65536,
    )
    .unwrap();
    c.segment_bytes = 65536;
    c
}
fn database(fs: &Fs) -> Vec<u8> {
    fs.files
        .iter()
        .find(|f| f.name == "db")
        .unwrap()
        .data
        .clone()
}
fn restore(bucket: &mut Bucket) -> replica_core::Result<Fs> {
    let mut fs = Fs::default();
    let r = Replica::prepare(
        &mut fs,
        bucket,
        support::writer(),
        config(),
        at(100),
        |_, _| Ok(()),
    )?;
    assert_eq!(r.status().reason, Reason::Restored);
    Ok(fs)
}
#[test]
fn parallel_restore_is_identical_and_stays_within_its_budget() {
    let mut fs = Fs::default();
    let mut bucket = Bucket::default();
    let mut heap = vec![0u64; 512 * 1024];
    let mut r = Replica::prepare(
        &mut fs,
        &mut bucket,
        support::writer(),
        config(),
        at(0),
        |_, _| panic!("leeg: geen restore"),
    )
    .unwrap();
    let statements = [
        c"CREATE TABLE item(v BLOB); WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<600) INSERT INTO item SELECT randomblob(1000) FROM c",
        c"UPDATE item SET v = randomblob(900) WHERE rowid % 7 = 0",
        c"DELETE FROM item WHERE rowid % 5 = 0",
    ];
    for (n, sql) in statements.into_iter().enumerate() {
        {
            let mut vfs = r.vfs(&mut fs).unwrap();
            // SAFETY: Eén engine in deze toets; hij sluit aan het eind van de scope.
            let mut engine = unsafe { Engine::initialize(&mut heap, &mut vfs) }.unwrap();
            engine.open(c"db").unwrap().execute(sql).unwrap();
        }
        let synced = r.sync(&mut fs, &mut bucket, at(1 + n as i64)).unwrap();
        assert!(synced.published);
    }
    let original = database(&fs);
    // Na elkaar: de referentie.
    let sequential = database(&restore(&mut bucket.clone()).unwrap());
    assert_eq!(sequential, original);
    // Drie tegelijk binnen 200 kB, delen van ~64 KiB: meerdere rondes, nooit erboven.
    let mut parallel = bucket.clone();
    parallel.parallel = Some(Parallel {
        requests: 3,
        bytes: 200_000,
    });
    assert_eq!(database(&restore(&mut parallel).unwrap()), original);
    let segments: Vec<_> = parallel
        .rounds
        .iter()
        .flatten()
        .filter(|(key, _)| key.contains("/data/"))
        .collect();
    assert!(segments.len() > 10, "{} segmenten", segments.len());
    assert!(parallel.rounds.iter().any(|r| r.len() == 3));
    for round in &parallel.rounds {
        let bytes: usize = round.iter().map(|(_, size)| size).sum();
        assert!(round.len() == 1 || bytes <= 200_000, "{round:?}");
    }
    // Een ontbrekend deel midden in het plan blijft bewezen schade.
    let mut damaged = bucket.clone();
    damaged.parallel = parallel.parallel;
    let (key, _) = segments[segments.len() / 2];
    damaged.data.remove(key).unwrap();
    assert_eq!(
        restore(&mut damaged).err(),
        Some(replica_core::Error::Corrupt)
    );
}
