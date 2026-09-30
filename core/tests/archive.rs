//! Catalogue de punten uitsluitend uit committed manifesten; restore blijft offline.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    Error,
    archive::{self, Fetch},
    local::Name,
    manifest::{Manifest, Part},
    marker::Marker,
    object::StoreError,
    replication, segment,
    time::Time,
};
use support::{Bucket, Fs};
const NS: &str = "replica/test";
const GEN: &str = "20260930T120000Z-abcdef";
fn at(n: i64) -> Time {
    Time::unix(1_790_765_296 + n, n as u32).unwrap()
}
fn fixture() -> (Bucket, Marker) {
    let mut store = Bucket::default();
    let prefix = replication::generation_prefix(NS, GEN).unwrap();
    for seq in 1..=3 {
        let bytes = segment::encode(512, 512, &[(1, &[seq as u8; 512])]).unwrap();
        let key = format!("{prefix}data/{seq}");
        let part = Part {
            key: key.clone(),
            size: bytes.len() as u64,
            hash: hop_auth::sha256(&bytes),
        };
        store.data.insert(key, bytes);
        let m = Manifest {
            min_size: 512,
            first: seq,
            sequence: seq,
            at: at(seq as i64),
            level: 0,
            start: Time::ZERO,
            end: Time::ZERO,
            parts: vec![part],
        };
        let key = if seq == 1 {
            format!("{prefix}snapshot")
        } else {
            replication::raw_key(&prefix, seq, m.at).unwrap()
        };
        store.data.insert(key, m.encode(&prefix).unwrap());
    }
    store.data.insert(
        format!("{NS}/generations/20260801T000000Z-incomplete/data/x"),
        vec![1],
    );
    let mut marker = Marker::new(&"1".repeat(64), GEN, at(0)).unwrap();
    marker.complete = true;
    (store, marker)
}
#[test]
fn points_are_newest_first_exact_nanos_bounded_and_ignore_damaged_old_generation() {
    let (mut store, marker) = fixture();
    let points = archive::points(&mut store, NS, &marker, 10).unwrap();
    assert_eq!(points.len(), 3);
    assert_eq!(
        points.iter().map(|p| p.at).collect::<Vec<_>>(),
        [at(3), at(2), at(1)]
    );
    assert_eq!(
        points.iter().map(|p| p.level).collect::<Vec<_>>(),
        [0, 0, -1]
    );
    assert!(points.iter().all(|p| p.current && p.generation == GEN));
    assert!(matches!(
        archive::points(&mut store, NS, &marker, 2),
        Err(Error::Limit)
    ));
    store.get_error = Some(StoreError::Transport);
    assert!(matches!(
        archive::points(&mut store, NS, &marker, 10),
        Err(Error::Object(StoreError::Transport))
    ));
    store.get_error = None;
    let prefix = replication::generation_prefix(NS, GEN).unwrap();
    store
        .data
        .insert(format!("{prefix}snapshot"), b"bad".to_vec());
    assert!(matches!(
        archive::points(&mut store, NS, &marker, 10),
        Err(Error::Corrupt)
    ));
}
#[test]
fn offline_fetch_checks_aliases_and_integrity_before_changing_destination() {
    let (mut store, _) = fixture();
    let mut fs = Fs::default();
    let options = |destination| Fetch {
        namespace: NS,
        generation: GEN,
        at: Some(at(2)),
        live: Name::new("db").unwrap(),
        destination: Name::new(destination).unwrap(),
        page_limit: 100,
    };
    for name in [
        "db",
        "/db",
        "./db",
        "x/../db",
        "db-journal",
        "db.replica",
        "db.lease",
    ] {
        assert!(
            matches!(
                archive::fetch(&mut fs, &mut store, options(name), |_, _| Ok(())),
                Err(Error::State)
            ),
            "{name}"
        );
    }
    let mut reverse = options("copy");
    reverse.live = Name::new("copy.replica-restore-data").unwrap();
    assert!(matches!(
        archive::fetch(&mut fs, &mut store, reverse, |_, _| Ok(())),
        Err(Error::State)
    ));
    assert!(fs.files.is_empty());
    assert!(matches!(
        archive::fetch(&mut fs, &mut store, options("copy"), |_, _| Err(
            Error::Corrupt
        )),
        Err(Error::Corrupt)
    ));
    assert!(fs.data("copy").is_none());
    let result = archive::fetch(&mut fs, &mut store, options("copy"), |b, n| {
        assert_eq!(b.data(n.cstr()?.to_str().unwrap()).unwrap(), [2; 512]);
        Ok(())
    })
    .unwrap();
    assert_eq!(result.sequence, 2);
    assert_eq!(fs.cold().data("copy").unwrap(), [2; 512]);
}
#[test]
fn discovery_omits_a_point_behind_a_sequence_gap() {
    let (mut store, marker) = fixture();
    let prefix = replication::generation_prefix(NS, GEN).unwrap();
    store
        .data
        .remove(&replication::raw_key(&prefix, 2, at(2)).unwrap());
    let points = archive::points(&mut store, NS, &marker, 10).unwrap();
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].at, at(1));
}
