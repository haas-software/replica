//! Onafhankelijke Go-fixtures plus beschadigings- en herstelgrenzen.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use replica_core::{
    Error,
    coverage::{Pages, growth_gap},
    dirty,
    manifest::{Layout, Manifest},
    segment::{self, Segment},
    time::Time,
};
const PREFIX: &str = "replica/test/gen/";
const SEG: &[u8] = include_bytes!("fixtures/segment.bin");
const DIRTY: &[u8] = include_bytes!("fixtures/dirty.bin");
const MAN: &[u8] = include_bytes!("fixtures/manifest.json");
const WIN: &[u8] = include_bytes!("fixtures/window.json");
const GEN: &str = "20260930-123456-abcdef0123456789";
#[test]
fn go_segment_exact_and_untrusted_lengths() {
    let seg = Segment::decode(SEG).unwrap();
    assert_eq!((seg.page_size(), seg.database_size()), (512, 1536));
    // SPINSEG1 is rauw: de opgeslagen bytes zijn de pagina.
    let pages: Vec<(u32, &[u8])> = seg.pages().map(|(n, p)| (n, p.stored())).collect();
    assert_eq!(segment::encode(512, 1536, &pages).unwrap(), SEG);
    for end in 0..SEG.len() {
        assert!(Segment::decode(&SEG[..end]).is_err());
    }
    for index in 0..SEG.len() {
        let mut damaged = SEG.to_vec();
        damaged[index] ^= 0x80;
        assert!(Segment::decode(&damaged).is_err());
    }
    assert!(segment::encode(512, 1536, &[pages[0], pages[0]]).is_err());
    assert!(segment::encode(512, 1536, &[(4, pages[0].1)]).is_err());
    assert!(segment::encode(513, 1539, &[]).is_err());
    let mut malformed = SEG.to_vec();
    malformed[20..24].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(Segment::decode(&malformed).is_err());
}
#[test]
fn compressed_segments_roundtrip_and_reject_damage() {
    // Echte vormen: JSON-achtig, nul, onsamendrukbaar, half; en SPINSEG1 ernaast.
    let mut x = 0x2545_F491_4F6C_DD1Du64;
    let mut rnd = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    for page_size in [512u32, 4096, 16384, 65536] {
        let mut images: Vec<Vec<u8>> = Vec::new();
        let mut json = String::new();
        while json.len() < page_size as usize {
            json.push_str(&format!(
                "{{\"Id\":\"orders/{}\",\"Lines\":[{{\"Qty\":{}}}]}},",
                rnd() % 9999,
                rnd() % 50
            ));
        }
        images.push(json.as_bytes()[..page_size as usize].to_vec());
        images.push(vec![0u8; page_size as usize]);
        images.push((0..page_size).map(|_| rnd() as u8).collect());
        let mut half = vec![0u8; page_size as usize];
        half[..page_size as usize / 2].fill(b'a');
        images.push(half);
        let records: Vec<(u32, &[u8])> = images
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u32 + 1, p.as_slice()))
            .collect();
        let size = u64::from(page_size) * 4;
        let packed = segment::encode_with(page_size, size, &records, true).unwrap();
        let plain = segment::encode(page_size, size, &records).unwrap();
        assert_eq!(&packed[..8], b"SPINSEG2");
        assert!(packed.len() < plain.len());
        for bytes in [&packed, &plain] {
            let seg = Segment::decode(bytes).unwrap();
            assert_eq!(seg.is_compressed(), bytes == &packed);
            let mut out = vec![0u8; page_size as usize];
            for ((n, page), (want_n, want)) in seg.pages().zip(&records) {
                assert_eq!(n, *want_n);
                page.read(&mut out).unwrap();
                assert_eq!(&out[..], *want);
            }
        }
        // Elke afgebroken lengte en omgedraaide bit: een fout, geen paniek.
        for end in (0..packed.len()).step_by(7) {
            assert!(Segment::decode(&packed[..end]).is_err());
        }
        for index in (0..packed.len()).step_by(3) {
            let mut damaged = packed.clone();
            damaged[index] ^= 0x10;
            assert!(Segment::decode(&damaged).is_err());
        }
    }
    // Een kloppende hash over een kapot LZ-blok: decode slaagt, het lezen
    // van de pagina geeft een fout of nooit de oorspronkelijke pagina.
    let page = vec![b'x'; 4096];
    let packed = segment::encode_with(4096, 4096, &[(1, &page)], true).unwrap();
    for at in 32..packed.len() - 32 {
        let mut bad = packed[..packed.len() - 32].to_vec();
        bad[at] ^= 0xFF;
        let digest = replica_core::hash(&bad);
        bad.extend_from_slice(&digest);
        if let Ok(seg) = Segment::decode(&bad) {
            let mut out = vec![0u8; 4096];
            let (_, p) = seg.pages().next().unwrap();
            assert!(p.read(&mut out).is_err() || out != page);
        }
    }
}
#[test]
fn dirty_header_and_append_match_go_and_choose_conservatively() {
    let log = dirty::Log::decode(DIRTY).unwrap();
    assert_eq!(log.generation, GEN);
    assert_eq!(log.sequence, 7);
    assert_eq!(log.pages().collect::<Vec<_>>(), [3, 1, 3, 2]);
    let mut data = dirty::header(GEN, 7).unwrap();
    let header = data.len();
    for page in log.pages() {
        data.extend_from_slice(&dirty::record(page));
    }
    assert_eq!(data, DIRTY);
    for n in 0..header {
        assert!(dirty::Log::decode(&DIRTY[..n]).is_err());
    }
    for n in 0..8 {
        assert_eq!(
            dirty::Log::decode(&DIRTY[..header + 24 + n])
                .unwrap()
                .pages()
                .count(),
            3
        );
    }
    for i in 0..DIRTY.len() {
        let mut damaged = DIRTY.to_vec();
        damaged[i] ^= 1;
        assert!(dirty::Log::decode(&damaged).is_err());
    }
    let mut broken = DIRTY.to_vec();
    broken.extend_from_slice(&dirty::record(0));
    assert!(dirty::Log::decode(&broken).is_err());
    let older = dirty::header(GEN, 6).unwrap();
    assert_eq!(
        dirty::select(Some(&older), Some(DIRTY), GEN, 7)
            .unwrap()
            .sequence,
        7
    );
    assert!(dirty::select(Some(&older), Some(DIRTY), GEN, 6).is_err());
    assert!(dirty::select(Some(DIRTY), Some(&broken), GEN, 7).is_err());
    assert!(dirty::select(None, None, GEN, 7).is_err());
}

#[test]
fn equal_sequence_logs_require_a_proven_append_superset() {
    let mut short = dirty::header(GEN, 7).unwrap();
    short.extend_from_slice(&dirty::record(2));
    let mut long = short.clone();
    long.extend_from_slice(&dirty::record(3));
    for (a, b) in [(&short, &long), (&long, &short)] {
        assert_eq!(
            dirty::select(Some(a), Some(b), GEN, 7)
                .unwrap()
                .pages()
                .collect::<Vec<_>>(),
            [2, 3]
        );
    }
    // Een afgebroken laatste append telt niet als een complete dirty pagina.
    let mut partial = short.clone();
    partial.extend_from_slice(&dirty::record(3)[..4]);
    assert_eq!(
        dirty::select(Some(&partial), Some(&long), GEN, 7)
            .unwrap()
            .pages()
            .collect::<Vec<_>>(),
        [2, 3]
    );
    let mut conflicting = dirty::header(GEN, 7).unwrap();
    conflicting.extend_from_slice(&dirty::record(4));
    for (a, b) in [(&short, &conflicting), (&conflicting, &short)] {
        assert!(matches!(
            dirty::select(Some(a), Some(b), GEN, 7),
            Err(Error::Corrupt)
        ));
    }
}
#[test]
fn go_manifests_exact_and_parts_not_visible_without_valid_commit() {
    for fixture in [MAN, WIN] {
        let m = Manifest::decode(fixture, PREFIX).unwrap();
        assert_eq!(m.encode(PREFIX).unwrap(), fixture);
        m.parts[0].read(SEG).unwrap();
    }
    let mut m = Manifest::decode(MAN, PREFIX).unwrap();
    m.parts[0].key = "replica/other/gen/data/one".into();
    assert!(m.validate(PREFIX).is_err());
    let mut m = Manifest::decode(WIN, PREFIX).unwrap();
    m.at = m.start;
    assert!(m.validate(PREFIX).is_err());
    let mut m = Manifest::decode(MAN, PREFIX).unwrap();
    m.parts[0].hash[0] ^= 1;
    assert!(m.parts[0].read(SEG).is_err());
    let duplicate = String::from_utf8(MAN.to_vec())
        .unwrap()
        .replacen("{", "{\"version\":2,", 1);
    assert!(Manifest::decode(duplicate.as_bytes(), PREFIX).is_err());
}
fn raw(seq: u64, seconds: i64) -> Manifest {
    let mut m = Manifest::decode(MAN, PREFIX).unwrap();
    m.first = seq;
    m.sequence = seq;
    m.at = Time::unix(seconds, 0).unwrap();
    m
}
#[test]
fn planner_uses_complete_windows_and_refuses_gaps() {
    let start = Manifest::decode(MAN, PREFIX).unwrap().at;
    let mut window = raw(2, start.seconds() + 9);
    window.sequence = 4;
    window.level = 1;
    window.start = start;
    window.end = Time::unix(start.seconds() + 10, 0).unwrap();
    let l = Layout::new(
        Manifest::decode(MAN, PREFIX).unwrap(),
        vec![
            raw(2, start.seconds() + 1),
            raw(3, start.seconds() + 2),
            raw(4, start.seconds() + 9),
            window,
        ],
        PREFIX,
    )
    .unwrap();
    assert_eq!(
        l.plan(None)
            .unwrap()
            .iter()
            .map(|m| m.sequence)
            .collect::<Vec<_>>(),
        [1, 4]
    );
    assert_eq!(
        l.plan(Some(Time::unix(start.seconds() + 3, 0).unwrap()))
            .unwrap()
            .iter()
            .map(|m| m.sequence)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    let l = Layout::new(
        Manifest::decode(MAN, PREFIX).unwrap(),
        vec![raw(3, start.seconds() + 2)],
        PREFIX,
    )
    .unwrap();
    assert_eq!(l.plan(None).unwrap_err(), Error::Gap);
}
#[test]
fn coverage_growth_and_lock_byte_exception() {
    let mut p = Pages::new(100);
    for page in [1, 3, 4] {
        p.add(page).unwrap();
    }
    assert_eq!(p.shortfall(2048, 512).unwrap(), Some((2, 1)));
    p.add(2).unwrap();
    assert_eq!(p.shortfall(2048, 512).unwrap(), None);
    assert_eq!(
        growth_gap(1024, 2560, 512, &[3, 5], 3).unwrap(),
        Some((4, 1))
    );
    // Een kleine staart ver boven 1 GiB kost alleen die staart, niet de hele DB.
    assert_eq!(
        growth_gap(1 << 30, (1 << 30) + 1024, 512, &[2097154], 2).unwrap(),
        None
    );
    assert_eq!(growth_gap(0, 1 << 40, 512, &[], 1024), Err(Error::Limit));
}
#[test]
fn timestamps_calendar_offsets_and_nanos() {
    for s in [
        "0000-02-29T00:00:00Z",
        "0001-01-01T00:00:00Z",
        "1969-12-31T23:59:59.9Z",
        "1970-01-01T00:00:00Z",
        "2000-02-29T23:59:59.123456789Z",
        "2026-09-30T12:34:56.123456789Z",
        "9999-12-31T23:59:59.999999999Z",
    ] {
        assert_eq!(Time::parse(s).unwrap().encode().unwrap(), s);
    }
    assert_ne!(Time::ZERO, Time::unix(0, 0).unwrap());
    assert_eq!(
        Time::parse("2026-09-30T14:34:56.1+02:00").unwrap(),
        Time::parse("2026-09-30T12:34:56.1Z").unwrap()
    );
    for s in [
        "2025-02-29T00:00:00Z",
        "1900-02-29T00:00:00Z",
        "2026-04-31T00:00:00Z",
        "2026-09-30T24:00:00Z",
        "2026-09-30T00:00:60Z",
        "2026-09-30T00:00:00+02:60",
        "2026-09-30T00:00:00.Z",
    ] {
        assert!(Time::parse(s).is_err(), "{s}");
    }
}

#[test]
fn large_production_snapshot_manifest_keeps_http_json_limit_separate() {
    use replica_core::manifest::{MAX_MANIFEST_BYTES, MAX_PARTS, Part};
    let mut m = Manifest::decode(MAN, PREFIX).unwrap();
    let part = m.parts.pop().unwrap();
    for index in 0..4113 {
        m.parts.push(Part {
            key: format!("{PREFIX}data/{}/{index}", "p".repeat(200)),
            size: part.size,
            hash: part.hash,
        });
    }
    let encoded = m.encode(PREFIX).unwrap();
    assert!(encoded.len() > hop_types::json::MAX_INPUT);
    assert!(hop_types::json::parse(&encoded).is_err());
    assert_eq!(Manifest::decode(&encoded, PREFIX).unwrap(), m);
    assert!(Manifest::decode(&vec![b' '; MAX_MANIFEST_BYTES + 1], PREFIX).is_err());
    while m.parts.len() <= MAX_PARTS {
        let index = m.parts.len();
        m.parts.push(Part {
            key: format!("{PREFIX}data/extra/{index}"),
            size: part.size,
            hash: part.hash,
        });
    }
    assert!(m.encode(PREFIX).is_err());
}

#[test]
fn production_database_pages_are_bounded_by_owner_not_old_four_gib_limit() {
    let page = 17_229_090_816_u64 / 4096;
    assert!(Pages::new(1 << 20).add(page as u32).is_err());
    let mut pages = Pages::new(1 << 24);
    pages.add(page as u32).unwrap();
    assert!(pages.contains(page as u32));
    assert!(pages.add((1 << 24) + 1).is_err());
}
