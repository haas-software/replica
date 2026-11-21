//! Zelfde shrink/grow-uitvoer als Go; iedere onderbroken prune blijft herstelbaar.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    compact::{self, Window},
    local::Name,
    manifest::{Manifest, Part},
    marker::{self, LocalMarker, Marker},
    object, replication, restore, segment,
    time::Time,
};
use support::{Bucket, Fs, Open};
const NS: &str = "replica/test";
const GENERATION: &str = "20260930T123456Z-abcdef";
fn at(n: i64) -> Time {
    Time::unix(1_790_765_296 + n, 0).unwrap()
}
fn part(store: &mut Bucket, prefix: &str, seq: u64, size: u64, pages: &[(u32, &[u8])]) -> Manifest {
    let bytes = segment::encode(512, size, pages).unwrap();
    let key = format!("{prefix}data/raw{seq}");
    let p = Part {
        key: key.clone(),
        size: bytes.len() as u64,
        hash: leancrypto::sha256::Sha256::digest(&bytes),
    };
    store.data.insert(key, bytes);
    Manifest {
        min_size: size,
        first: seq,
        sequence: seq,
        at: at(seq as i64 - 1),
        level: 0,
        start: Time::ZERO,
        end: Time::ZERO,
        parts: vec![p],
        lease: 0,
    }
}
fn fixture() -> (Fs, Bucket, LocalMarker, Manifest, Manifest, String, String) {
    let fs = Fs::default();
    let mut store = Bucket::default();
    let prefix = replication::generation_prefix(NS, GENERATION).unwrap();
    let snapshot = part(
        &mut store,
        &prefix,
        1,
        1536,
        &[(1, &[1; 512]), (2, &[2; 512]), (3, &[3; 512])],
    );
    let m2 = part(&mut store, &prefix, 2, 512, &[(1, &[9; 512])]);
    let m3 = part(&mut store, &prefix, 3, 1536, &[(3, &[7; 512])]);
    let key2 = replication::raw_key(&prefix, 2, m2.at).unwrap();
    let key3 = replication::raw_key(&prefix, 3, m3.at).unwrap();
    store.data.insert(
        format!("{prefix}snapshot"),
        snapshot.encode(&prefix).unwrap(),
    );
    store.data.insert(key2.clone(), m2.encode(&prefix).unwrap());
    store.data.insert(key3.clone(), m3.encode(&prefix).unwrap());
    let mut marker = Marker::new(
        &marker::destination("e", "b", "replica", "test").unwrap(),
        GENERATION,
        at(0),
    )
    .unwrap();
    marker.complete = true;
    marker.sequence = 3;
    marker.page_size = 512;
    marker.size = 1536;
    marker.at = at(2);
    let local = LocalMarker::new(Name::new("db").unwrap(), marker).unwrap();
    (fs, store, local, m2, m3, key2, key3)
}
fn window() -> Window {
    Window {
        level: 1,
        start: at(0),
        end: at(10),
        page_limit: 100,
        segment_bytes: 65536,
    }
}
#[test]
fn merged_segment_matches_go_and_retains_truncation_history() {
    let (mut fs, mut store, mut marker, m2, m3, _, _) = fixture();
    let merged = compact::merge(
        &mut fs,
        &mut store,
        NS,
        &mut marker,
        window(),
        &[&m2, &m3],
        &mut Open(0),
    )
    .unwrap();
    assert_eq!(merged.min_size, 512);
    assert_eq!(merged.sequence, 3);
    assert_eq!(merged.parts.len(), 1);
    assert_eq!(
        store.data[&merged.parts[0].key],
        include_bytes!("fixtures/compact.bin")
    );
    assert_eq!(
        LocalMarker::load(&mut fs.cold(), Name::new("db").unwrap())
            .unwrap()
            .value
            .sealed_at,
        at(10)
    );
}
#[test]
fn a_compressed_input_makes_a_compressed_merge_with_the_same_pages() {
    let (mut fs, mut store, mut marker, m2, m3, _, _) = fixture();
    // m3's segment opnieuw als SPINSEG2: zelfde pagina's, andere bytes.
    let key = m3.parts[0].key.clone();
    let packed = segment::encode_with(512, 1536, &[(3, &[7; 512])], true).unwrap();
    assert_eq!(&packed[..8], b"SPINSEG2");
    let mut m3 = m3;
    m3.parts[0].size = packed.len() as u64;
    m3.parts[0].hash = leancrypto::sha256::Sha256::digest(&packed);
    store.data.insert(key, packed);
    let merged = compact::merge(
        &mut fs,
        &mut store,
        NS,
        &mut marker,
        window(),
        &[&m2, &m3],
        &mut Open(0),
    )
    .unwrap();
    let out = &store.data[&merged.parts[0].key];
    assert_eq!(&out[..8], b"SPINSEG2");
    let seg = segment::Segment::decode(out).unwrap();
    let go = segment::Segment::decode(include_bytes!("fixtures/compact.bin")).unwrap();
    assert_eq!(seg.database_size(), go.database_size());
    let (mut a, mut b) = (vec![0u8; 512], vec![0u8; 512]);
    let pages: Vec<_> = seg.pages().collect();
    let want: Vec<_> = go.pages().collect();
    assert_eq!(pages.len(), want.len());
    for ((n, p), (wn, wp)) in pages.iter().zip(&want) {
        assert_eq!(n, wn);
        p.read(&mut a).unwrap();
        wp.read(&mut b).unwrap();
        assert_eq!(a, b);
    }
}
#[test]
fn parallel_downloads_merge_to_the_same_go_segment() {
    let (mut fs, mut store, mut marker, m2, m3, _, _) = fixture();
    store.parallel = Some(replica_core::object::Parallel {
        requests: 8,
        bytes: u64::MAX,
    });
    let merged = compact::merge(
        &mut fs,
        &mut store,
        NS,
        &mut marker,
        window(),
        &[&m2, &m3],
        &mut Open(0),
    )
    .unwrap();
    assert_eq!(
        store.data[&merged.parts[0].key],
        include_bytes!("fixtures/compact.bin")
    );
    // Twee lussen over dezelfde delen: twee rondes, elk met alle delen tegelijk.
    let parts = m2.parts.len() + m3.parts.len();
    assert_eq!(store.rounds.len(), 2);
    assert!(store.rounds.iter().all(|r| r.len() == parts));
}
#[test]
fn gap_or_unpublished_replacement_never_removes_raw_history() {
    let (mut fs, mut store, mut marker, m2, mut m3, key2, key3) = fixture();
    m3.first = 4;
    m3.sequence = 4;
    assert!(
        compact::merge(
            &mut fs,
            &mut store,
            NS,
            &mut marker,
            window(),
            &[&m2, &m3],
            &mut Open(0)
        )
        .is_err()
    );
    assert!(store.data.contains_key(&key2) && store.data.contains_key(&key3));
    m3.first = 3;
    m3.sequence = 3;
    store.fail_put_match = Some("/complete".into());
    store.fail_before_put = true;
    assert!(
        compact::merge(
            &mut fs,
            &mut store,
            NS,
            &mut marker,
            window(),
            &[&m2, &m3],
            &mut Open(0)
        )
        .is_err()
    );
    assert!(store.data.contains_key(&key2) && store.data.contains_key(&key3));
    assert_eq!(marker.value.sealed_at, at(10));
}

#[test]
fn pruning_accepts_large_old_and_replacement_manifests() {
    for (large_old, large_replacement) in [(true, false), (false, true), (true, true)] {
        let (mut fs, mut store, mut marker, mut old, m3, key, _) = fixture();
        let mut replacement = compact::merge(
            &mut fs,
            &mut store,
            NS,
            &mut marker,
            window(),
            &[&old, &m3],
            &mut Open(0),
        )
        .unwrap();
        let prefix = replication::generation_prefix(NS, GENERATION).unwrap();
        for (large, manifest) in [(large_old, &mut old), (large_replacement, &mut replacement)] {
            if large {
                let original = manifest.parts.pop().unwrap();
                let bytes = store.data[&original.key].clone();
                for index in 0..4113 {
                    let key = format!("{}/{}/{index}", original.key, "p".repeat(200));
                    store.data.insert(key.clone(), bytes.clone());
                    manifest.parts.push(Part {
                        key,
                        size: original.size,
                        hash: original.hash,
                    });
                }
                assert!(manifest.encode(&prefix).unwrap().len() > hop_types::json::MAX_INPUT);
            }
        }
        let replacement_key = compact::window_key(
            &prefix,
            replacement.level,
            replacement.start,
            replacement.end,
        )
        .unwrap();
        store.data.insert(key.clone(), old.encode(&prefix).unwrap());
        store.data.insert(
            replacement_key.clone(),
            replacement.encode(&prefix).unwrap(),
        );
        compact::prune(
            &mut fs,
            &mut store,
            &key,
            &old,
            &replacement,
            &[&replacement, &m3],
            &mut Open(0),
        )
        .unwrap();
        assert!(!store.data.contains_key(&key));
        assert!(store.data.contains_key(&replacement_key));
        let layout = object::layout(&mut store, &prefix).unwrap();
        let mut restored = Fs::default();
        restore::stage(
            &mut restored,
            &mut store,
            &layout,
            None,
            Name::new("copy").unwrap(),
            100,
        )
        .unwrap()
        .verify(&mut restored, |_, _| Ok(()))
        .unwrap()
        .publish(&mut restored, GENERATION)
        .unwrap();
        assert_eq!(
            restored.cold().data("copy").unwrap(),
            [&[9; 512][..], &[0; 512][..], &[7; 512][..]].concat()
        );
    }
}
#[test]
fn failed_delete_always_restores_through_confirmed_window() {
    for fail in 1..=4 {
        let (mut fs, mut store, mut marker, m2, m3, key2, key3) = fixture();
        let merged = compact::merge(
            &mut fs,
            &mut store,
            NS,
            &mut marker,
            window(),
            &[&m2, &m3],
            &mut Open(0),
        )
        .unwrap();
        store.fail_delete = Some(fail);
        let _ = compact::prune(
            &mut fs,
            &mut store,
            &key2,
            &m2,
            &merged,
            &[&merged, &m3],
            &mut Open(0),
        );
        let _ = compact::prune(
            &mut fs,
            &mut store,
            &key3,
            &m3,
            &merged,
            &[&merged],
            &mut Open(0),
        );
        let prefix = replication::generation_prefix(NS, GENERATION).unwrap();
        let layout = object::layout(&mut store, &prefix).unwrap();
        let mut target = Fs::default();
        let verified = restore::stage(
            &mut target,
            &mut store,
            &layout,
            None,
            Name::new("restored").unwrap(),
            100,
        )
        .unwrap()
        .verify(&mut target, |_, _| Ok(()))
        .unwrap();
        verified.publish(&mut target, GENERATION).unwrap();
        assert_eq!(
            target.cold().data("restored").unwrap(),
            [vec![9; 512], vec![0; 512], vec![7; 512]].concat()
        );
    }
}
