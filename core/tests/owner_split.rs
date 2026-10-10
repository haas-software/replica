//! De eigenaar los van de VFS: één verbinding blijft de hele toets open, en
//! tussen haar statements door lopen `begin`, de capture buiten de eigenaar,
//! de upload, `finish`, het onderhoud en `renew`. Aan het eind is het archief
//! gelijk aan de levende database: geen pagina verloren (één SQLite-test per
//! binary, de C-runtime is procesglobaal).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod support;
use replica_core::{
    Error,
    archive::{self, Fetch},
    local::Name,
    owner::{Begun, Config, Replica},
    time::Time,
    writer::MemBackend,
};
use replica_sqlite::{Connection, Engine, FileId, OpenFlags, Storage, Value};
use std::{cell::RefCell, collections::BTreeMap, ffi::CStr, rc::Rc};
use support::Bucket;

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
    config.segment_bytes = 65536;
    config.background_maintenance = true;
    config
}

/// Eén volume met twee (of meer) opslag-eigenaars erop, zoals de SQL-taak en
/// de Sync-taak elk hun eigen `Files` op dezelfde map hebben: elk handvat
/// heeft zijn eigen open bestanden, de bytes zijn gedeeld. Elke call leent kort.
#[derive(Clone, Default)]
struct Shared {
    volume: Rc<RefCell<BTreeMap<String, Vec<u8>>>>,
    open: Vec<Option<String>>,
    random: u8,
}
impl Shared {
    fn handle(&self) -> Self {
        Self {
            volume: Rc::clone(&self.volume),
            open: Vec::new(),
            random: self.random.wrapping_add(101),
        }
    }
    fn name(&self, f: FileId) -> replica_sqlite::Result<String> {
        self.open
            .get(f.0 as usize)
            .cloned()
            .flatten()
            .ok_or(replica_sqlite::Error::IO)
    }
}
impl Storage for Shared {
    fn open(&mut self, n: &CStr, f: OpenFlags) -> replica_sqlite::Result<FileId> {
        let name = n.to_str().unwrap().to_string();
        let mut volume = self.volume.borrow_mut();
        if !volume.contains_key(&name) {
            if !f.is_create() {
                return Err(replica_sqlite::Error::CANNOT_OPEN);
            }
            volume.insert(name.clone(), Vec::new());
        }
        self.open.push(Some(name));
        Ok(FileId(self.open.len() as u32 - 1))
    }
    fn close(&mut self, f: FileId) -> replica_sqlite::Result {
        self.name(f)?;
        self.open[f.0 as usize] = None;
        Ok(())
    }
    fn read(&mut self, f: FileId, o: u64, b: &mut [u8]) -> replica_sqlite::Result<usize> {
        let name = self.name(f)?;
        let volume = self.volume.borrow();
        let data = volume.get(&name).ok_or(replica_sqlite::Error::IO)?;
        let off = o as usize;
        let n = data.len().saturating_sub(off).min(b.len());
        if n > 0 {
            b[..n].copy_from_slice(&data[off..off + n]);
        }
        Ok(n)
    }
    fn write(&mut self, f: FileId, o: u64, b: &[u8]) -> replica_sqlite::Result {
        let name = self.name(f)?;
        let mut volume = self.volume.borrow_mut();
        let data = volume.get_mut(&name).ok_or(replica_sqlite::Error::IO)?;
        let end = o as usize + b.len();
        data.resize(data.len().max(end), 0);
        data[o as usize..end].copy_from_slice(b);
        Ok(())
    }
    fn truncate(&mut self, f: FileId, n: u64) -> replica_sqlite::Result {
        let name = self.name(f)?;
        let mut volume = self.volume.borrow_mut();
        volume
            .get_mut(&name)
            .ok_or(replica_sqlite::Error::IO)?
            .resize(n as usize, 0);
        Ok(())
    }
    fn sync(&mut self, f: FileId, _: i32) -> replica_sqlite::Result {
        self.name(f).map(|_| ())
    }
    fn size(&mut self, f: FileId) -> replica_sqlite::Result<u64> {
        let name = self.name(f)?;
        Ok(self
            .volume
            .borrow()
            .get(&name)
            .map_or(0, |d| d.len() as u64))
    }
    fn remove(&mut self, n: &CStr, _: bool) -> replica_sqlite::Result {
        self.volume.borrow_mut().remove(n.to_str().unwrap());
        Ok(())
    }
    fn exists(&mut self, n: &CStr) -> replica_sqlite::Result<bool> {
        Ok(self.volume.borrow().contains_key(n.to_str().unwrap()))
    }
    fn random(&mut self, b: &mut [u8]) -> replica_sqlite::Result {
        for byte in b {
            self.random = self.random.wrapping_mul(13).wrapping_add(7);
            *byte = self.random;
        }
        Ok(())
    }
    fn unix_millis(&mut self) -> replica_sqlite::Result<i64> {
        Ok(1_790_765_296_000)
    }
}

/// Een vingerafdruk van alle rijen, in volgorde: het aantal en een hash.
fn digest<B: Storage>(db: &mut Connection<'_, '_, B>) -> (u64, u64) {
    use std::hash::{DefaultHasher, Hasher};
    let mut s = db
        .prepare(c"SELECT id, value FROM item ORDER BY id")
        .unwrap();
    let mut hash = DefaultHasher::new();
    let mut rows = 0;
    while s.step().unwrap() {
        let Value::Integer(id) = s.column(0).unwrap() else {
            panic!("id")
        };
        let Value::Text(value) = s.column(1).unwrap() else {
            panic!("value")
        };
        hash.write_i64(id);
        hash.write(value.as_bytes());
        rows += 1;
    }
    (rows, hash.finish())
}

fn round(db: &mut Connection<'_, '_, impl Storage>, n: i64) {
    let sql = format!(
        "WITH RECURSIVE k(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM k WHERE i<150)
         INSERT INTO item(value) SELECT printf('r{n}-%0300d', i) FROM k;
         UPDATE item SET value = value || '+' WHERE id % 7 = {m};
         DELETE FROM item WHERE id % 97 = {m}",
        m = n % 7
    );
    let sql = std::ffi::CString::new(sql).unwrap();
    db.execute(&sql).unwrap();
}

#[test]
fn sync_runs_between_statements_while_the_vfs_stays_open() {
    let shared = Shared::default();
    let mut sync_side = shared.handle();
    let mut sql_side = shared.handle();
    let mut engine_side = shared.handle();
    let mut bucket = Bucket::default();
    let mut heap = vec![0u64; 512 * 1024];
    let mut r = Replica::prepare(
        &mut sync_side,
        &mut bucket,
        support::writer(),
        config(),
        at(0),
        |_, _| panic!("empty bootstrap needs no restore"),
    )
    .unwrap();
    // De SQL-kant krijgt alleen de tracker; de eigenaar blijft vrij.
    let tracker = r.tracker().unwrap();
    let live;
    let generation;
    {
        let mut vfs = tracker.vfs(&mut sql_side).unwrap();
        // SAFETY: De enige SQLite-runtime in deze testbinary, op deze thread.
        let engine = unsafe { Engine::initialize(&mut heap, &mut engine_side) }.unwrap();
        let mut db = engine.open_with(c"db", &mut vfs).unwrap();
        db.execute(c"CREATE TABLE item(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        let mut published = 0;
        for n in 1..=8_i64 {
            let t = n * 30;
            round(&mut db, n);
            // In rust (tussen twee statements): de capture naar de spool.
            if n % 3 == 0 {
                // Het beeld buiten de eigenaar: SQL schrijft tussen de segmenten.
                let begun = r.begin_capture(&mut sync_side, &mut bucket, at(t)).unwrap();
                let pending = match begun {
                    Some(Begun::Capture(mut capturing)) => {
                        while capturing.step(&mut sync_side).unwrap() {
                            round(&mut db, n * 100);
                        }
                        capturing.run(&mut sync_side).unwrap()
                    }
                    Some(Begun::Upload(pending)) => pending,
                    None => panic!("round {n}: a capture"),
                };
                round(&mut db, n * 1000);
                let uploaded = pending.upload(&mut sync_side, &mut bucket);
                round(&mut db, n * 10_000);
                r.finish(&mut sync_side, &mut bucket, pending, uploaded, at(t + 1))
                    .unwrap();
            } else {
                let pending = r
                    .begin(&mut sync_side, &mut bucket, at(t))
                    .unwrap()
                    .expect("a capture");
                assert!(pending.pages() > 0);
                // Zonder rust: SQL loopt door terwijl de delen uploaden.
                round(&mut db, n * 1000);
                let uploaded = pending.upload(&mut sync_side, &mut bucket);
                round(&mut db, n * 10_000);
                // Weer in rust: manifest, marker, bevestiging.
                let synced = r
                    .finish(&mut sync_side, &mut bucket, pending, uploaded, at(t + 1))
                    .unwrap();
                assert!(synced.published);
            }
            published += 1;
            // De lease vernieuwen mag altijd, ook terwijl de VFS openstaat.
            r.renew(&mut MemBackend::new(), at(t + 2)).unwrap();
            // Het onderhoud als losse taak, met SQL ertussen.
            if let Some(job) = r.maintenance_begin(&mut sync_side, at(t + 3)).unwrap() {
                round(&mut db, n * 100_000);
                let result = job.run(&mut sync_side, &mut bucket);
                r.maintenance_finish(&mut sync_side, result).unwrap();
            }
        }
        assert_eq!(published, 8);
        // Buiten het protocol: wie de staat leent terwijl SQL schrijft, laat
        // de VFS SQLITE_BUSY geven, zonder schade; daarna gaat alles door.
        {
            let marker = r.marker();
            let error = db
                .execute(c"INSERT INTO item(value) VALUES('busy')")
                .unwrap_err();
            assert_eq!(error.code & 255, 5);
            drop(marker);
        }
        round(&mut db, 99);
        assert!(r.has_changes());
        // De laatste wijzigingen, in rust, en daarna niets meer te doen.
        let pending = r
            .begin(&mut sync_side, &mut bucket, at(1000))
            .unwrap()
            .expect("the last writes");
        let uploaded = pending.upload(&mut sync_side, &mut bucket);
        r.finish(&mut sync_side, &mut bucket, pending, uploaded, at(1001))
            .unwrap();
        // Een host die vóór de rust eerst `has_changes` vraagt, slaat de
        // handdruk nu over.
        assert!(!r.has_changes());
        assert!(
            r.begin(&mut sync_side, &mut bucket, at(1100))
                .unwrap()
                .is_none()
        );
        live = digest(&mut db);
        let mut check = db.prepare(c"PRAGMA integrity_check").unwrap();
        assert!(check.step().unwrap());
        assert_eq!(check.column(0).unwrap(), Value::Text("ok"));
        drop(check);
        generation = r.marker().generation.clone();
        assert!(r.marker().complete);
        assert!(r.marker().sequence >= 9);
    }
    // De verbinding en de VFS zijn dicht; de eigenaar sluit en de lease gaat vrij.
    r.close();
    assert_eq!(r.vfs(&mut sql_side).err(), Some(Error::State));
    r.release(&mut MemBackend::new()).unwrap();
    // Het archief herstelt exact de levende database.
    let mut restored = (0, 0);
    archive::fetch(
        &mut sync_side,
        &mut bucket,
        Fetch {
            namespace: "replica/test",
            generation: &generation,
            at: None,
            live: Name::new("db").unwrap(),
            destination: Name::new("restored").unwrap(),
            page_limit: 65536,
        },
        |b, name| {
            // SAFETY: De vorige engine is gedropt; dezelfde testthread.
            let mut engine = unsafe { Engine::initialize(&mut heap, b) }?;
            let mut db = engine.open(name.cstr()?)?;
            restored = digest(&mut db);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(restored, live);
    assert!(live.0 > 1000, "rows: {}", live.0);
}
