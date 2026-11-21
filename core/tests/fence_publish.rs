//! Een vaste sleutel (snapshot, venster) en current: een oude schrijver
//! overschrijft nooit wat een nieuwere schreef.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    Error, maintenance,
    manifest::Manifest,
    object::{self, Store},
    time::Time,
};
use support::{Bucket, Fs, Open};
const MAN: &[u8] = include_bytes!("fixtures/manifest.json");
const PREFIX: &str = "replica/test/gen/";
fn manifest(lease: u64, seconds: i64) -> Manifest {
    let mut m = Manifest::decode(MAN, PREFIX).unwrap();
    m.lease = lease;
    m.at = Time::unix(m.at.seconds() + seconds, 0).unwrap();
    m
}
#[test]
fn a_fixed_key_goes_to_the_highest_token() {
    let key = format!("{PREFIX}snapshot");
    let read = |b: &mut Bucket| Manifest::decode(&b.get(&key, 1 << 20).unwrap(), PREFIX).unwrap();
    let mut bucket = Bucket::default();
    object::publish(&mut bucket, &key, &manifest(2, 0), PREFIX).unwrap();
    // Dezelfde bytes: ons eigen verloren antwoord.
    object::publish(&mut bucket, &key, &manifest(2, 0), PREFIX).unwrap();
    // Een oudere schrijver: LeaseLost, het object blijft.
    assert_eq!(
        object::publish(&mut bucket, &key, &manifest(1, 5), PREFIX),
        Err(Error::LeaseLost)
    );
    assert_eq!(read(&mut bucket), manifest(2, 0));
    // Dezelfde schrijver opnieuw (een snapshot die opnieuw gaat) en een nieuwere: vervangen.
    object::publish(&mut bucket, &key, &manifest(2, 7), PREFIX).unwrap();
    assert_eq!(read(&mut bucket), manifest(2, 7));
    object::publish(&mut bucket, &key, &manifest(3, 9), PREFIX).unwrap();
    assert_eq!(read(&mut bucket), manifest(3, 9));
}
#[test]
fn current_set_late_by_an_older_writer_is_put_back() {
    const NS: &str = "replica/test";
    const KEEP: &str = "20260930T120000Z-keep";
    const ZOMBIE: &str = "20260930T130000Z-zombie";
    let now = Time::parse("2026-09-30T14:00:00Z").unwrap();
    let mut bucket = Bucket::default();
    // De zombie (token 1) zette current na zijn generatievernieuwing.
    let prefix = format!("{NS}/generations/{ZOMBIE}/");
    let mut snapshot = Manifest::decode(MAN, PREFIX).unwrap();
    snapshot.parts[0].key = format!("{prefix}data/one");
    snapshot.lease = 1;
    bucket.data.insert(
        format!("{prefix}snapshot"),
        snapshot.encode(&prefix).unwrap(),
    );
    bucket
        .data
        .insert(format!("{NS}/current"), ZOMBIE.as_bytes().to_vec());
    let current = |b: &mut Bucket| b.get(&format!("{NS}/current"), 255).unwrap();
    // Zelfde of lager token: niet bewezen, niets aangeraakt.
    for token in [0, 1] {
        assert_eq!(
            maintenance::generations(
                &mut Fs::default(),
                &mut bucket,
                NS,
                KEEP,
                now,
                86400,
                &mut Open(token)
            ),
            Err(Error::Unproven)
        );
        assert_eq!(current(&mut bucket), ZOMBIE.as_bytes());
    }
    // De houder (token 2) zet hem terug; de jonge zombiegeneratie blijft staan.
    maintenance::generations(
        &mut Fs::default(),
        &mut bucket,
        NS,
        KEEP,
        now,
        86400,
        &mut Open(2),
    )
    .unwrap();
    assert_eq!(current(&mut bucket), KEEP.as_bytes());
    assert!(bucket.data.contains_key(&format!("{prefix}snapshot")));
}
