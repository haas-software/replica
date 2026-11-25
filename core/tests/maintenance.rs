//! Retentie behoudt een herstelpad, ook bij afgebroken opruimrondes.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    Error,
    local::Name,
    maintenance::{self, Budget, Level, Schedule},
    manifest::{Layout, Manifest, Part},
    marker::{self, LocalMarker, Marker},
    object, replication, restore, segment,
    time::Time,
};
use support::{Bucket, Fs, Open};
const NS: &str = "replica/test";
const GEN: &str = "20260930T120000Z-abcdef";
fn at(n: i64) -> Time {
    Time::unix(
        Time::parse("2026-09-30T12:00:00Z").unwrap().seconds() + n,
        0,
    )
    .unwrap()
}
fn manifest(store: &mut Bucket, seq: u64, t: Time, size: u64, pages: &[(u32, &[u8])]) -> Manifest {
    let prefix = replication::generation_prefix(NS, GEN).unwrap();
    let bytes = segment::encode(512, size, pages).unwrap();
    let key = format!("{prefix}data/raw{seq}");
    let part = Part {
        key: key.clone(),
        size: bytes.len() as u64,
        hash: leancrypto::sha256::Sha256::digest(&bytes),
    };
    store.data.insert(key, bytes);
    let m = Manifest {
        min_size: size,
        first: seq,
        sequence: seq,
        at: t,
        level: 0,
        start: Time::ZERO,
        end: Time::ZERO,
        parts: vec![part],
        lease: 0,
    };
    let key = if seq == 1 {
        format!("{prefix}snapshot")
    } else {
        replication::raw_key(&prefix, seq, t).unwrap()
    };
    store.data.insert(key, m.encode(&prefix).unwrap());
    m
}
fn schedule() -> Schedule {
    Schedule::new(&[
        Level {
            window: 60,
            keep: 60,
        },
        Level {
            window: 120,
            keep: 120,
        },
    ])
    .unwrap()
}
#[test]
fn schedule_validation_age_and_closed_boundaries() {
    assert_eq!(
        Schedule::parse("").unwrap().levels(),
        Schedule::defaults().unwrap().levels()
    );
    assert_eq!(
        Schedule::parse(" 1m:1m, 2m:2m ").unwrap().levels(),
        schedule().levels()
    );
    for bad in [
        "1s:1h",
        "1m:10s",
        "1m:1h,90s:1h",
        "2m:2h,1m:1h",
        "1m:1h,1m:1h",
        "60.5s:1h",
        "-1m:1h",
    ] {
        assert!(Schedule::parse(bad).is_err(), "{bad}");
    }
    let mut bucket = Bucket::default();
    let snapshot = manifest(&mut bucket, 1, at(0), 512, &[(1, &[1; 512])]);
    manifest(&mut bucket, 2, at(60), 512, &[(1, &[2; 512])]);
    let prefix = replication::generation_prefix(NS, GEN).unwrap();
    let layout = object::layout(&mut bucket, &prefix).unwrap();
    assert!(schedule().elapsed(&layout, 1, at(59)).unwrap().is_empty());
    assert_eq!(
        schedule().elapsed(&layout, 1, at(60)).unwrap(),
        [(at(0), at(60))]
    );
    assert!(schedule().elapsed(&layout, 0, at(60)).is_err());
    // Afronden op een week gebruikt maandag als weekanker, niet de Unix-epoch op donderdag.
    let mut m = manifest(
        &mut bucket,
        3,
        Time::parse("2026-09-30T12:00:00Z").unwrap(),
        512,
        &[(1, &[3; 512])],
    );
    m.first = 2;
    m.sequence = 2;
    let layout = Layout::new(snapshot, vec![m], &prefix).unwrap();
    let week = Schedule::new(&[Level {
        window: 604800,
        keep: 604800,
    }])
    .unwrap();
    assert_eq!(
        week.elapsed(&layout, 1, Time::parse("2026-10-05T00:00:00Z").unwrap())
            .unwrap(),
        [(
            Time::parse("2026-09-28T00:00:00Z").unwrap(),
            Time::parse("2026-10-05T00:00:00Z").unwrap()
        )]
    );
    let mut marker = Marker::new(
        &marker::destination("e", "b", "p", "d").unwrap(),
        GEN,
        at(0),
    )
    .unwrap();
    marker.complete = true;
    marker.bytes = i64::MAX as u64;
    assert!(!maintenance::renewal_due(&marker, at(120), 120).unwrap());
    assert!(maintenance::renewal_due(&marker, at(121), 120).unwrap());
    assert!(maintenance::renewal_due(&marker, Time::ZERO, 120).is_err());
}
fn fixture() -> (Fs, Bucket, LocalMarker) {
    let mut bucket = Bucket::default();
    manifest(
        &mut bucket,
        1,
        at(0),
        1536,
        &[(1, &[1; 512]), (2, &[2; 512]), (3, &[3; 512])],
    );
    manifest(&mut bucket, 2, at(1), 512, &[(1, &[9; 512])]);
    manifest(&mut bucket, 3, at(60), 1536, &[(3, &[7; 512])]);
    manifest(&mut bucket, 4, at(61), 1536, &[(2, &[6; 512])]);
    manifest(&mut bucket, 5, at(120), 1536, &[(1, &[8; 512])]);
    let mut marker = Marker::new(
        &marker::destination("e", "b", "p", "d").unwrap(),
        GEN,
        at(0),
    )
    .unwrap();
    marker.complete = true;
    marker.sequence = 5;
    marker.page_size = 512;
    marker.size = 1536;
    marker.at = at(120);
    (
        Fs::default(),
        bucket,
        LocalMarker::new(Name::new("db").unwrap(), marker).unwrap(),
    )
}
fn assert_restore(bucket: &mut Bucket) {
    let prefix = replication::generation_prefix(NS, GEN).unwrap();
    let layout = object::layout(bucket, &prefix).unwrap();
    let mut fs = Fs::default();
    restore::stage(
        &mut fs,
        bucket,
        &layout,
        None,
        Name::new("restored").unwrap(),
        100,
    )
    .unwrap()
    .verify(&mut fs, |_, _| Ok(()))
    .unwrap()
    .publish(&mut fs, GEN)
    .unwrap();
    assert_eq!(
        fs.cold().data("restored").unwrap(),
        [vec![8; 512], vec![6; 512], vec![7; 512]].concat()
    );
}
#[test]
fn multilevel_retention_and_every_failed_delete_preserve_restore() {
    for fail in 0..=12 {
        let (mut fs, mut bucket, mut marker) = fixture();
        bucket.fail_delete = if fail == 0 { None } else { Some(fail) };
        let report = maintenance::run(
            &mut fs,
            &mut bucket,
            NS,
            &mut marker,
            &schedule(),
            at(600),
            Budget {
                pages: 100,
                segment_bytes: 65536,
            },
            &mut Open(0),
        );
        if fail == 0 {
            assert_eq!(
                report.unwrap(),
                maintenance::Report {
                    merged: 3,
                    pruned: 6
                }
            );
        } else {
            assert!(report.is_err(), "delete {fail}");
        }
        assert_restore(&mut bucket);
        bucket.fail_delete = None;
        maintenance::run(
            &mut fs,
            &mut bucket,
            NS,
            &mut marker,
            &schedule(),
            at(600),
            Budget {
                pages: 100,
                segment_bytes: 65536,
            },
            &mut Open(0),
        )
        .unwrap();
        assert_restore(&mut bucket);
        let layout = object::layout(
            &mut bucket,
            &replication::generation_prefix(NS, GEN).unwrap(),
        )
        .unwrap();
        assert_eq!(layout.commits().len(), 1);
        assert_eq!(layout.commits()[0].level, 2);
    }
}
fn old_objects(bucket: &mut Bucket, id: &str, complete: bool) -> Vec<String> {
    let prefix = replication::generation_prefix(NS, id).unwrap();
    let mut keys = vec![format!("{prefix}L0/manifest"), format!("{prefix}data/part")];
    if complete {
        keys.insert(0, format!("{prefix}snapshot"));
    }
    for key in &keys {
        bucket.data.insert(key.clone(), vec![1]);
    }
    keys
}
#[test]
fn generation_gc_never_deletes_dependencies_after_failed_metadata_delete() {
    for fail in 1..=3 {
        let mut bucket = Bucket::default();
        bucket
            .data
            .insert(format!("{NS}/current"), GEN.as_bytes().to_vec());
        let keep = old_objects(&mut bucket, GEN, true);
        let old = old_objects(&mut bucket, "20260801T000000Z-old", true);
        bucket.fail_delete = Some(fail);
        assert!(
            maintenance::generations(
                &mut Fs::default(),
                &mut bucket,
                NS,
                GEN,
                at(0),
                86400,
                &mut Open(0)
            )
            .is_err()
        );
        assert!(keep.iter().all(|k| bucket.data.contains_key(k)));
        for key in old.iter().skip(fail - 1) {
            assert!(bucket.data.contains_key(key));
        }
        assert_eq!(bucket.deleted, old[..fail]);
        bucket.fail_delete = None;
        maintenance::generations(
            &mut Fs::default(),
            &mut bucket,
            NS,
            GEN,
            at(0),
            86400,
            &mut Open(0),
        )
        .unwrap();
        assert!(old.iter().all(|k| !bucket.data.contains_key(k)));
    }
}
#[test]
fn gc_preserves_current_unknown_recent_and_complete_within_retention() {
    let mut bucket = Bucket::default();
    bucket
        .data
        .insert(format!("{NS}/current"), GEN.as_bytes().to_vec());
    let mut keep = old_objects(&mut bucket, GEN, true);
    keep.extend(old_objects(&mut bucket, "20260928T000000Z-complete", true));
    keep.extend(old_objects(
        &mut bucket,
        "20260930T110000Z-in-progress",
        false,
    ));
    let unknown = format!("{NS}/generations/unknown/data/x");
    bucket.data.insert(unknown.clone(), vec![1]);
    keep.push(unknown);
    let abandoned = old_objects(&mut bucket, "20260928T000000Z-abandoned", false);
    assert_eq!(
        maintenance::generations(
            &mut Fs::default(),
            &mut bucket,
            NS,
            GEN,
            at(0),
            30 * 86400,
            &mut Open(0)
        )
        .unwrap(),
        2
    );
    assert!(abandoned.iter().all(|k| !bucket.data.contains_key(k)));
    assert!(keep.iter().all(|k| bucket.data.contains_key(k)));
    let count = bucket.deleted.len();
    assert!(matches!(
        maintenance::generations(
            &mut Fs::default(),
            &mut bucket,
            NS,
            "20260928T000000Z-complete",
            at(0),
            1,
            &mut Open(0)
        ),
        Err(Error::Unproven)
    ));
    assert_eq!(bucket.deleted.len(), count);
}

#[test]
fn interrupted_generation_batches_finish_metadata_before_deleting_data() {
    for after in [false, true] {
        for fail in [1, 2, 127, 128, 129, 131, 132, 400] {
            let mut bucket = Bucket::default();
            bucket
                .data
                .insert(format!("{NS}/current"), GEN.as_bytes().to_vec());
            let keep = old_objects(&mut bucket, GEN, true);
            let id = "20260801T000000Z-old";
            old_objects(&mut bucket, id, true);
            let prefix = replication::generation_prefix(NS, id).unwrap();
            for n in 0..129 {
                bucket
                    .data
                    .insert(format!("{prefix}L0/extra{n:04}"), vec![1]);
            }
            for n in 0..300 {
                bucket.data.insert(format!("{prefix}data/{n:04}"), vec![1]);
            }
            bucket.fail_delete = Some(fail);
            bucket.fail_after_delete = after;
            let mut failed = false;
            let mut done = false;
            for _ in 0..16 {
                let result = maintenance::generations(
                    &mut Fs::default(),
                    &mut bucket,
                    NS,
                    GEN,
                    at(0),
                    86400,
                    &mut Open(0),
                );
                assert!(keep.iter().all(|key| bucket.data.contains_key(key)));
                if bucket
                    .data
                    .keys()
                    .any(|key| key.starts_with(&format!("{prefix}L")))
                {
                    assert_eq!(
                        bucket
                            .data
                            .keys()
                            .filter(|key| key.starts_with(&format!("{prefix}data/")))
                            .count(),
                        301
                    );
                }
                match result {
                    Ok(0) => {
                        done = true;
                        break;
                    }
                    Ok(count) => assert!(count <= 128),
                    Err(Error::Object(object::StoreError::Transport)) => {
                        assert!(!failed);
                        failed = true;
                        bucket.fail_delete = None;
                    }
                    Err(error) => panic!("unexpected GC error: {error:?}"),
                }
            }
            assert!(failed && done);
            assert!(!bucket.data.keys().any(|key| key.starts_with(&prefix)));
        }
    }
}
