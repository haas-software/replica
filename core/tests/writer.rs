//! Eén schrijver via de lease in de objectopslag; elke andere node is lezer en schrijft nooit.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    Error,
    local::Name,
    owner::{Config, Replica},
    time::Time,
    writer::{
        Backend, LeaseState, MemBackend, ReadOnly, Renewed, Role, Writer, claim,
        discovery::{self, Error as LeaseError},
    },
};
use replica_sqlite::Engine;
use support::{Bucket, Fs};
const TTL: u64 = 30_000;
/// Eén SQLite-runtime per proces: de SQL-toetsen in dit bestand wachten op elkaar.
static SQLITE: std::sync::Mutex<()> = std::sync::Mutex::new(());
fn at(ms: i64) -> Time {
    let ms = 1_790_765_296_000 + ms;
    Time::unix(ms / 1000, (ms % 1000) as u32 * 1_000_000).unwrap()
}
fn writer(role: Role) -> Writer {
    match role {
        Role::Writer(w) => w,
        Role::Reader { leader } => panic!("lezer, leider {leader:?}"),
    }
}
fn leader(role: Role) -> Option<String> {
    match role {
        Role::Writer(w) => panic!("schrijver {}", w.owner()),
        Role::Reader { leader } => leader,
    }
}
/// MemBackend met een onbereikbare store en een verloren PUT-antwoord.
#[derive(Default)]
struct Faulty {
    inner: MemBackend,
    writes: usize,
    down: bool,
    lose_reply: bool,
}
impl Backend for Faulty {
    fn read(&mut self) -> discovery::Result<(LeaseState, String)> {
        if self.down {
            return Err(LeaseError::Unreachable);
        }
        self.inner.read()
    }
    fn write(&mut self, prev: &str, state: &LeaseState) -> discovery::Result<String> {
        if self.down {
            return Err(LeaseError::Unreachable);
        }
        self.writes += 1;
        let result = self.inner.write(prev, state);
        if result.is_ok() && std::mem::take(&mut self.lose_reply) {
            return Err(LeaseError::Unreachable);
        }
        result
    }
    fn delete(&mut self, handle: &str) -> discovery::Result {
        self.inner.delete(handle)
    }
}
#[test]
fn second_node_is_reader_until_the_lease_expires() {
    let mut fs = Fs::default();
    let mut lease = MemBackend::new();
    let mut a = writer(claim(&mut fs, &mut lease, "10.0.0.1:7000", TTL, at(0)).unwrap());
    assert_eq!(a.generation(), 1);
    assert!(a.owner().starts_with("10.0.0.1:7000#"));
    let seen = leader(claim(&mut fs, &mut lease, "10.0.0.2:7000", TTL, at(1_000)).unwrap());
    assert_eq!(seen.as_deref(), Some(a.owner()));
    // Dezelfde node na een herstart is een nieuwe eigenaar: ook lezer.
    leader(claim(&mut fs, &mut lease, "10.0.0.1:7000", TTL, at(2_000)).unwrap());
    a.renew(&mut lease, at(25_000)).unwrap_err();
    // A hield op met vernieuwen: pas na het verlopen neemt B over, met generatie 2.
    leader(claim(&mut fs, &mut lease, "10.0.0.2:7000", TTL, at(30_000)).unwrap());
    let b = writer(claim(&mut fs, &mut lease, "10.0.0.2:7000", TTL, at(30_001)).unwrap());
    assert_eq!(b.generation(), 2);
    assert_eq!(a.renew(&mut lease, at(30_002)), Err(Error::LeaseLost));
}
#[test]
fn renews_on_age_and_stops_itself_a_third_before_expiry() {
    let mut fs = Fs::default();
    let mut lease = Faulty::default();
    let mut w = writer(claim(&mut fs, &mut lease, "a", TTL, at(0)).unwrap());
    assert_eq!(w.deadline(), 1_790_765_296_000 + 20_000);
    w.renew(&mut lease, at(4_999)).unwrap();
    assert_eq!(lease.writes, 1);
    w.renew(&mut lease, at(5_000)).unwrap();
    assert_eq!(lease.writes, 2);
    assert_eq!(w.deadline(), 1_790_765_296_000 + 25_000);
    // Onbereikbaar is transport; de schrijver blijft geldig tot zijn deadline.
    lease.down = true;
    assert_eq!(
        w.renew(&mut lease, at(11_000)),
        Err(Error::Object(replica_core::object::StoreError::Transport))
    );
    w.check(at(24_999)).unwrap();
    assert_eq!(w.check(at(25_000)), Err(Error::LeaseLost));
    // Na de deadline helpt een bereikbare store niet meer: verlies is blijvend.
    lease.down = false;
    assert_eq!(w.renew(&mut lease, at(25_000)), Err(Error::LeaseLost));
    assert_eq!(w.renew(&mut lease, at(25_001)), Err(Error::LeaseLost));
    // Een klok die iets terugspringt is geen verlies: de eigenaar meet `now` aan
    // het begin van een beurt en een vernieuwing in een capture meet later.
    // Meer dan een zesde TTL terug is wel een klok die terugspringt.
    let mut lease = MemBackend::new();
    let mut w = writer(claim(&mut fs, &mut lease, "a", TTL, at(0)).unwrap());
    w.renew(&mut lease, at(-1)).unwrap();
    assert_eq!(w.renew(&mut lease, at(-5_001)), Err(Error::LeaseLost));
}
#[test]
fn lost_renew_reply_is_adopted_but_a_foreign_write_is_not() {
    let mut fs = Fs::default();
    let mut lease = Faulty::default();
    let mut w = writer(claim(&mut fs, &mut lease, "a", TTL, at(0)).unwrap());
    lease.lose_reply = true;
    assert!(w.renew(&mut lease, at(5_000)).is_err());
    // De PUT landde wel: de volgende renew ziet onze eigen lease en gaat door.
    w.renew(&mut lease, at(10_000)).unwrap();
    assert_eq!(w.generation(), 1);
    // Iemand anders schreef de lease: 412 en geen herkansing.
    let (state, handle) = lease.read().unwrap();
    let foreign = LeaseState {
        owner: "b#x".into(),
        generation: state.generation + 1,
        ..state
    };
    lease.inner.write(&handle, &foreign).unwrap();
    assert_eq!(w.renew(&mut lease, at(15_000)), Err(Error::LeaseLost));
}
#[test]
fn renewal_in_steps_keeps_the_store_io_outside_the_writer() {
    let mut fs = Fs::default();
    let mut lease = Faulty::default();
    let mut w = writer(claim(&mut fs, &mut lease, "a", TTL, at(0)).unwrap());
    assert!(w.renew_request(at(4_999)).unwrap().is_none());
    let renewal = w.renew_request(at(5_000)).unwrap().unwrap();
    let written = lease.write(&renewal.prev, &renewal.state);
    assert_eq!(w.renew_commit(&renewal, written).unwrap(), Renewed::Done);
    assert_eq!(w.deadline(), 1_790_765_296_000 + 25_000);
    // Een verloren antwoord is transport; de PUT landde wel, dus de volgende
    // vernieuwing krijgt 412 en herovert onze eigen lease.
    let renewal = w.renew_request(at(10_000)).unwrap().unwrap();
    lease.lose_reply = true;
    let written = lease.write(&renewal.prev, &renewal.state);
    assert!(w.renew_commit(&renewal, written).is_err());
    let renewal = w.renew_request(at(11_000)).unwrap().unwrap();
    let written = lease.write(&renewal.prev, &renewal.state);
    assert_eq!(w.renew_commit(&renewal, written).unwrap(), Renewed::Reclaim);
    let again = w.reclaim_request(lease.read(), renewal.at).unwrap().unwrap();
    let written = lease.write(&again.prev, &again.state);
    w.reclaim_commit(&again, written).unwrap();
    assert_eq!(w.deadline(), 1_790_765_296_000 + 31_000);
    // Een vreemde lease bij de herovering is blijvend verlies.
    let renewal = w.renew_request(at(16_000)).unwrap().unwrap();
    let (state, handle) = lease.read().unwrap();
    let foreign = LeaseState {
        owner: "b#x".into(),
        generation: state.generation + 1,
        ..state
    };
    lease.inner.write(&handle, &foreign).unwrap();
    let written = lease.write(&renewal.prev, &renewal.state);
    assert_eq!(w.renew_commit(&renewal, written).unwrap(), Renewed::Reclaim);
    assert_eq!(
        w.reclaim_request(lease.read(), renewal.at),
        Err(Error::LeaseLost)
    );
    assert_eq!(w.check(at(16_001)), Err(Error::LeaseLost));
}
#[test]
fn release_lets_a_reader_take_over_at_once() {
    let mut fs = Fs::default();
    let mut lease = MemBackend::new();
    let a = writer(claim(&mut fs, &mut lease, "a", TTL, at(0)).unwrap());
    leader(claim(&mut fs, &mut lease, "b", TTL, at(1_000)).unwrap());
    a.release(&mut lease).unwrap();
    let b = writer(claim(&mut fs, &mut lease, "b", TTL, at(1_001)).unwrap());
    assert_eq!(b.generation(), 1);
}
#[test]
fn rejects_short_leases_and_bad_names() {
    let mut fs = Fs::default();
    let mut lease = MemBackend::new();
    for (node, ttl) in [("a", 14_999), ("", TTL), ("a#b", TTL)] {
        assert_eq!(
            claim(&mut fs, &mut lease, node, ttl, at(0)).err(),
            Some(Error::State)
        );
    }
    assert_eq!(
        claim(&mut fs, &mut lease, "a", TTL, Time::ZERO).err(),
        Some(Error::State)
    );
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
fn owner_refuses_sql_and_publication_after_the_deadline() {
    let _sqlite = SQLITE.lock().unwrap();
    let mut fs = Fs::default();
    let mut bucket = Bucket::default();
    let mut lease = MemBackend::new();
    let mut heap = vec![0u64; 512 * 1024];
    let w = writer(claim(&mut fs, &mut lease, "a", TTL, at(0)).unwrap());
    let mut r = Replica::prepare(&mut fs, &mut bucket, w, config(), at(0), |_, _| {
        panic!("leeg: geen restore")
    })
    .unwrap();
    let clock = fs.clock.clone();
    {
        let mut vfs = r.vfs(&mut fs).unwrap();
        // SAFETY: Eén engine tegelijk in deze toets; hij sluit aan het eind van de scope.
        let mut engine = unsafe { Engine::initialize(&mut heap, &mut vfs) }.unwrap();
        let mut db = engine.open(c"db").unwrap();
        db.execute(c"CREATE TABLE item(v INTEGER); INSERT INTO item VALUES(1)")
            .unwrap();
        // De deadline verstrijkt midden in de sessie: de volgende write is hard fout.
        clock.set(20_000);
        let e = db.execute(c"INSERT INTO item VALUES(2)").unwrap_err();
        assert_eq!(e.code & 0xff, replica_sqlite::Error::READONLY.code);
    }
    assert_eq!(r.vfs(&mut fs).err(), Some(Error::LeaseLost));
    assert_eq!(
        r.sync(&mut fs, &mut bucket, at(20_000)).err(),
        Some(Error::LeaseLost)
    );
    assert!(bucket.data.is_empty(), "niets gepubliceerd na de deadline");
    // Op tijd vernieuwd blijft de eigenaar gewoon werken.
    clock.set(0);
    r.renew(&mut lease, at(5_000)).unwrap();
    r.sync(&mut fs, &mut bucket, at(5_000)).unwrap();
    assert!(!bucket.data.is_empty());
    // Verlies is blijvend: de eigenaar gaat dicht.
    assert_eq!(r.renew(&mut lease, at(30_000)), Err(Error::LeaseLost));
    assert_eq!(r.vfs(&mut fs).err(), Some(Error::State));
}
#[test]
fn reader_storage_refuses_every_write() {
    let _sqlite = SQLITE.lock().unwrap();
    let mut fs = Fs::default();
    let mut heap = vec![0u64; 512 * 1024];
    {
        // SAFETY: Eén engine tegelijk in deze toets.
        let mut engine = unsafe { Engine::initialize(&mut heap, &mut fs) }.unwrap();
        let mut db = engine.open(c"db").unwrap();
        db.execute(c"CREATE TABLE item(v INTEGER); INSERT INTO item VALUES(1)")
            .unwrap();
    }
    let before = fs
        .files
        .iter()
        .find(|f| f.name == "db")
        .unwrap()
        .data
        .clone();
    let mut reader = ReadOnly(&mut fs);
    {
        // SAFETY: De vorige engine is gesloten.
        let mut engine = unsafe { Engine::initialize(&mut heap, &mut reader) }.unwrap();
        let e = engine.open(c"db").err().unwrap();
        assert_eq!(e.code & 0xff, replica_sqlite::Error::READONLY.code);
        let mut db = engine.open_read_only(c"db").unwrap();
        let mut s = db.prepare(c"SELECT count(*) FROM item").unwrap();
        assert!(s.step().unwrap());
        drop(s);
        let e = db.execute(c"INSERT INTO item VALUES(2)").unwrap_err();
        assert_eq!(e.code & 0xff, replica_sqlite::Error::READONLY.code);
    }
    let after = fs.files.iter().find(|f| f.name == "db").unwrap();
    assert_eq!(after.data, before);
}
