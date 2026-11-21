//! Een onderhoudsronde van een schrijver die halverwege zijn lease verliest,
//! gooit vanaf dat moment niets meer weg (één SQLite-test per binary).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    Error,
    local::Name,
    object::{Object, Store, StoreError},
    owner::{Config, Replica},
    time::Time,
    writer::{LeaseState, discovery},
};
use replica_sqlite::Engine;
use std::{cell::Cell, rc::Rc};
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
    config.background_maintenance = true;
    config
}
/// De bucket; na de eerste DELETE zet hij `lost` aan (de lease is dan weg).
struct Losing {
    bucket: Bucket,
    deleted: usize,
    lost: Rc<Cell<bool>>,
}
impl Store for Losing {
    fn put(&mut self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.bucket.put(key, bytes)
    }
    fn put_new(&mut self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.bucket.put_new(key, bytes)
    }
    fn get(&mut self, key: &str, limit: usize) -> Result<Vec<u8>, StoreError> {
        self.bucket.get(key, limit)
    }
    fn delete(&mut self, key: &str) -> Result<(), StoreError> {
        self.deleted += 1;
        self.lost.set(true);
        self.bucket.delete(key)
    }
    fn list(&mut self, prefix: &str, limit: usize) -> Result<Vec<Object>, StoreError> {
        self.bucket.list(prefix, limit)
    }
}
/// Een eigenaar met vier commits in verschillende kwartiervensters: het
/// onderhoud op t=5000 voegt samen en dunt uit.
fn ready(heap: &mut [u64]) -> (Replica, Fs, Bucket) {
    let mut fs = Fs::default();
    let mut bucket = Bucket::default();
    let mut r = Replica::prepare(
        &mut fs,
        &mut bucket,
        support::writer(),
        config(),
        at(0),
        |_, _| panic!("lege bootstrap"),
    )
    .unwrap();
    for (n, t) in [(1, 1), (2, 1000), (3, 2000), (4, 3000)] {
        {
            let mut vfs = r.vfs(&mut fs).unwrap();
            // SAFETY: Eén SQLite-test in deze binary; iedere engine sluit vóór de volgende.
            let mut engine = unsafe { Engine::initialize(heap, &mut vfs) }.unwrap();
            let sql = std::ffi::CString::new(format!(
                "CREATE TABLE IF NOT EXISTS item(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO item(value) VALUES('{n}')"
            ))
            .unwrap();
            engine.open(c"db").unwrap().execute(&sql).unwrap();
        }
        let pending = r.begin(&mut fs, &mut bucket, at(t)).unwrap().unwrap();
        let uploaded = pending.upload(&mut fs, &mut bucket);
        assert!(
            r.finish(&mut fs, &mut bucket, pending, uploaded, at(t + 1))
                .unwrap()
                .published
        );
    }
    (r, fs, bucket)
}
#[test]
fn a_maintenance_round_stops_deleting_once_the_lease_is_gone() {
    let mut heap = vec![0u64; 512 * 1024];
    // Zonder verlies: zoveel DELETEs heeft deze ronde.
    let (mut r, mut fs, bucket) = ready(&mut heap);
    let job = r.maintenance_begin(&mut fs, at(5000)).unwrap().unwrap();
    let lost = Rc::new(Cell::new(false));
    let mut store = Losing {
        bucket,
        deleted: 0,
        lost: lost.clone(),
    };
    let synced = job.run(&mut fs, &mut store, support::lease_of(&r)).unwrap();
    r.maintenance_finish(&mut fs, Ok(synced)).unwrap();
    let all = store.deleted;
    assert!(all >= 4, "{all} deletes");

    // De deadline: na de eerste DELETE springt de klok voorbij het einde van
    // de lease (tien jaar TTL, schrijven tot tweederde). Geen tweede DELETE.
    let (mut r, mut fs, bucket) = ready(&mut heap);
    let job = r.maintenance_begin(&mut fs, at(5000)).unwrap().unwrap();
    let clock = fs.clock.clone();
    let lost = Rc::new(Cell::new(false));
    let mut store = Losing {
        bucket,
        deleted: 0,
        lost: lost.clone(),
    };
    struct Jump<'a>(&'a mut Fs, Rc<Cell<bool>>, Rc<Cell<i64>>);
    impl replica_sqlite::Storage for Jump<'_> {
        fn open(
            &mut self,
            n: &std::ffi::CStr,
            f: replica_sqlite::OpenFlags,
        ) -> replica_sqlite::Result<replica_sqlite::FileId> {
            self.0.open(n, f)
        }
        fn close(&mut self, f: replica_sqlite::FileId) -> replica_sqlite::Result {
            self.0.close(f)
        }
        fn read(
            &mut self,
            f: replica_sqlite::FileId,
            o: u64,
            b: &mut [u8],
        ) -> replica_sqlite::Result<usize> {
            self.0.read(f, o, b)
        }
        fn write(&mut self, f: replica_sqlite::FileId, o: u64, b: &[u8]) -> replica_sqlite::Result {
            self.0.write(f, o, b)
        }
        fn truncate(&mut self, f: replica_sqlite::FileId, n: u64) -> replica_sqlite::Result {
            self.0.truncate(f, n)
        }
        fn sync(&mut self, f: replica_sqlite::FileId, x: i32) -> replica_sqlite::Result {
            self.0.sync(f, x)
        }
        fn size(&mut self, f: replica_sqlite::FileId) -> replica_sqlite::Result<u64> {
            self.0.size(f)
        }
        fn remove(&mut self, n: &std::ffi::CStr, d: bool) -> replica_sqlite::Result {
            self.0.remove(n, d)
        }
        fn exists(&mut self, n: &std::ffi::CStr) -> replica_sqlite::Result<bool> {
            self.0.exists(n)
        }
        fn random(&mut self, b: &mut [u8]) -> replica_sqlite::Result {
            self.0.random(b)
        }
        fn unix_millis(&mut self) -> replica_sqlite::Result<i64> {
            if self.1.get() {
                self.2.set(8 * 365 * 86_400_000);
            }
            self.0.unix_millis()
        }
    }
    let result = job.run(
        &mut Jump(&mut fs, lost.clone(), clock),
        &mut store,
        support::lease_of(&r),
    );
    assert_eq!(result.unwrap_err(), Error::LeaseLost);
    assert_eq!(store.deleted, 1);
    r.maintenance_finish(&mut fs, Err(Error::LeaseLost))
        .unwrap_err();

    // De store: de klok is nog goed, maar na de eerste DELETE staat er een
    // andere houder in de lease. De volgende reeks DELETEs begint niet meer.
    let (mut r, mut fs, bucket) = ready(&mut heap);
    let job = r.maintenance_begin(&mut fs, at(5000)).unwrap().unwrap();
    let lost = Rc::new(Cell::new(false));
    let mut store = Losing {
        bucket,
        deleted: 0,
        lost: lost.clone(),
    };
    let mut ours = support::lease_of::<Losing>(&r);
    let read = move |s: &mut Losing| -> discovery::Result<(LeaseState, String)> {
        if lost.get() {
            return Ok((
                LeaseState {
                    generation: 2,
                    expires_at: u64::MAX,
                    owner: "b#000000000000000000000000".into(),
                },
                "etag-b".into(),
            ));
        }
        ours(s)
    };
    let result = job.run(&mut fs, &mut store, read);
    assert_eq!(result.unwrap_err(), Error::LeaseLost);
    // Eén reeks: het eerste manifest en zijn eigen delen, nooit de hele ronde.
    assert!(
        store.deleted >= 1 && store.deleted < all,
        "{} van {all}",
        store.deleted
    );
    r.maintenance_finish(&mut fs, Err(Error::LeaseLost))
        .unwrap_err();
}
