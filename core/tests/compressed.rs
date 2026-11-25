//! Replicatie van gecomprimeerde bestanden, zoals een app het doet: een
//! SQL-kant en een sync-kant, elk een eigen `Compressed<Dir>` op dezelfde map,
//! de store en de lease in lokale mappen. Gecomprimeerd publiceren
//! (`SPINSEG2`), op een lege map herstellen, oude (`SPINSEG1`, ongecomprimeerd
//! lokaal) en nieuwe bestanden en segmenten door elkaar. Eén SQLite-runtime
//! per binary: alles in één toets.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use replica_core::{
    host::{Dir as Bucket, DirLease},
    local::Name,
    owner::{Config, Replica},
    prepare::Reason,
    time::Time,
    writer::{self, Role, Writer},
};
use replica_sqlite::compress::{self, Compressed, Format, Policy};
use replica_sqlite::{Connection, Engine, Storage, Value, host::Dir};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

const NS: &str = "app/test/main.db";

fn scratch() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("replica-compressed-{}-{nanos}", std::process::id()))
}
fn now() -> Time {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    Time::unix(d.as_secs() as i64, d.subsec_nanos()).unwrap()
}
fn config(compress: bool) -> Config {
    let mut c = Config::new(NS, &"3".repeat(64), Name::new("main.db").unwrap(), 1 << 20).unwrap();
    c.compress = compress;
    c
}
fn digest<B: Storage>(db: &mut Connection<'_, '_, B>) -> (i64, u64) {
    let mut h = DefaultHasher::new();
    let mut n = 0;
    let mut s = db.prepare(c"SELECT id, body FROM doc ORDER BY id").unwrap();
    while s.step().unwrap() {
        n += 1;
        format!("{:?}", s.column(0).unwrap()).hash(&mut h);
        format!("{:?}", s.column(1).unwrap()).hash(&mut h);
    }
    (n, h.finish())
}
fn integrity<B: Storage>(db: &mut Connection<'_, '_, B>) {
    let mut s = db.prepare(c"PRAGMA integrity_check").unwrap();
    assert!(s.step().unwrap());
    assert_eq!(s.column(0).unwrap(), Value::Text("ok"));
}
/// JSON-documenten zoals een host-app ze heeft, `n` rijen vanaf `from`.
fn insert<B: Storage>(db: &mut Connection<'_, '_, B>, from: i64, n: i64, tag: &str) {
    let sql = format!(
        "WITH RECURSIVE k(i) AS (SELECT {from} UNION ALL SELECT i+1 FROM k WHERE i<{last})
         INSERT OR REPLACE INTO doc(id, body) SELECT i, json_object('Id', 'orders/' || i,
           'Debtor', 'debtors/' || (i % 97), 'Tag', '{tag}', 'Lines', json_array(
             json_object('Article', 'articles/' || (i % 501), 'Qty', i % 40, 'Price', (i % 300) / 7.0),
             json_object('Article', 'articles/' || (i % 333), 'Qty', i % 13, 'Price', (i % 90) / 3.0)))
         FROM k",
        last = from + n - 1
    );
    db.execute(&std::ffi::CString::new(sql).unwrap()).unwrap();
}
fn claim(local: &mut impl Storage, bucket: &Bucket, node: &str) -> (Writer, DirLease) {
    let mut lease = DirLease::new(bucket, "app/test/lease").unwrap();
    let Role::Writer(w) = writer::claim(local, &mut lease, node, 30_000, now()).unwrap() else {
        panic!("a free lease makes a writer");
    };
    (w, lease)
}
/// Alle segmenten in de store: (aantal SPINSEG1, aantal SPINSEG2, bytes).
fn segments(bucket: &Path) -> (usize, usize, u64) {
    let mut out = (0, 0, 0);
    let mut stack = vec![bucket.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            let bytes = std::fs::read(&p).unwrap();
            match bytes.get(..8) {
                Some(b"SPINSEG1") => out.0 += 1,
                Some(b"SPINSEG2") => out.1 += 1,
                _ => continue,
            }
            out.2 += bytes.len() as u64;
        }
    }
    out
}

/// Een node die een bestaande of lege map opent, schrijft en publiceert.
struct Node<'r> {
    root: &'r Path,
    name: &'static str,
    policy: Policy,
    compress: bool,
}
impl Node<'_> {
    fn dir(&self) -> Compressed<Dir> {
        Compressed::new(Dir::new(self.root.join(self.name)).unwrap(), self.policy)
    }
    /// Prepare (herstel als de map leeg is), dan `work` op een verbinding en sync.
    fn run<B: Storage>(
        &self,
        engine: &Engine<'_, B>,
        bucket: &mut Bucket,
        expect: Reason,
        work: impl FnOnce(&mut Connection<'_, '_, replica_core::owner::Vfs<'_, Compressed<Dir>>>),
    ) -> (i64, u64) {
        let mut sync = self.dir();
        let (w, mut lease) = claim(&mut sync, bucket, self.name);
        let r = Replica::prepare(
            &mut sync,
            bucket,
            w,
            config(self.compress),
            now(),
            |b, name| {
                let mut db = engine.open_with(name.cstr()?, b)?;
                let mut s = db.prepare(c"PRAGMA integrity_check")?;
                assert!(s.step()?);
                assert_eq!(s.column(0)?, Value::Text("ok"));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(r.status().reason, expect, "{}", self.name);
        let mut r = r;
        let out;
        {
            let tracker = r.tracker().unwrap();
            let mut sql_side = self.dir();
            let mut vfs = tracker.vfs(&mut sql_side).unwrap();
            let mut db = engine.open_with(c"main.db", &mut vfs).unwrap();
            work(&mut db);
            integrity(&mut db);
            out = digest(&mut db);
            // Gepubliceerd terwijl de verbinding openstaat (in rust).
            if r.has_changes() {
                assert!(r.sync(&mut sync, bucket, now()).unwrap().published);
            }
        }
        r.renew(&mut lease, now()).unwrap();
        r.release(&mut lease).unwrap();
        out
    }
    fn format(&self) -> Format {
        compress::detect(
            &mut Dir::new(self.root.join(self.name)).unwrap(),
            c"main.db",
        )
        .unwrap()
    }
}

#[test]
fn compressed_and_plain_files_replicate_and_fetch_identically() {
    let root = scratch();
    let mut heap = vec![0_u64; 1 << 20];
    let mut engine_dir = Dir::new(root.join("engine")).unwrap();
    // SAFETY: De enige SQLite-runtime in deze testbinary, op deze thread.
    let engine = unsafe { Engine::initialize(&mut heap, &mut engine_dir) }.unwrap();

    // 1. Een nieuw, gecomprimeerd bestand (16 KiB) publiceert SPINSEG2.
    let mut bucket = Bucket::new(root.join("bucket")).unwrap();
    let a = Node {
        root: &root,
        name: "a",
        policy: Policy::compressed(),
        compress: true,
    };
    let want = a.run(&engine, &mut bucket, Reason::Bootstrap, |db| {
        db.execute(c"PRAGMA page_size=16384").unwrap();
        db.execute(c"CREATE TABLE doc(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        insert(db, 1, 20_000, "first");
    });
    assert_eq!(a.format(), Format::Compressed);
    let logical = 16384 * {
        let mut d = a.dir();
        let id = d.open(c"main.db", replica_sqlite::OpenFlags(2)).unwrap();
        let n = d.size(id).unwrap() / 16384;
        d.close(id).unwrap();
        n
    };
    let (plain, packed, bytes) = segments(&root.join("bucket"));
    eprintln!("snapshot: logical {logical} bytes, store {bytes} bytes in {packed} SPINSEG2");
    assert_eq!(plain, 0);
    assert!(packed > 0);
    assert!(bytes * 3 < logical, "store {bytes} vs logical {logical}");
    // Een increment erbovenop, ook gecomprimeerd.
    let want = {
        let a2 = Node { name: "a", ..a };
        a2.run(&engine, &mut bucket, Reason::Continued, |db| {
            assert_eq!(digest(db), want);
            insert(db, 15_000, 8_000, "second");
            db.execute(c"DELETE FROM doc WHERE id % 7 = 0").unwrap();
        })
    };
    assert_eq!(segments(&root.join("bucket")).0, 0);

    // 2. Herstel op een lege map: dezelfde rijen, lokaal weer gecomprimeerd.
    let b = Node {
        root: &root,
        name: "b",
        policy: Policy::compressed(),
        compress: true,
    };
    let got = b.run(&engine, &mut bucket, Reason::Restored, |_| {});
    assert_eq!(got, want);
    assert_eq!(b.format(), Format::Compressed);
    // Met `Policy::plain` komt hetzelfde terug als gewoon SQLite-bestand.
    let c = Node {
        root: &root,
        name: "c",
        policy: Policy::plain(),
        compress: true,
    };
    assert_eq!(c.run(&engine, &mut bucket, Reason::Restored, |_| {}), want);
    assert_eq!(c.format(), Format::Plain);

    // 3. Een oude replica: ongecomprimeerd lokaal, SPINSEG1 in de store.
    let mut old_bucket = Bucket::new(root.join("old-bucket")).unwrap();
    let old = Node {
        root: &root,
        name: "old",
        policy: Policy::plain(),
        compress: false,
    };
    let first = old.run(&engine, &mut old_bucket, Reason::Bootstrap, |db| {
        db.execute(c"CREATE TABLE doc(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        insert(db, 1, 5_000, "old");
    });
    assert_eq!(old.format(), Format::Plain);
    let (plain, packed, _) = segments(&root.join("old-bucket"));
    assert!(plain > 0 && packed == 0);
    // De nieuwe code haalt hem op een lege map (gecomprimeerd lokaal).
    let fresh = Node {
        root: &root,
        name: "fresh",
        policy: Policy::compressed(),
        compress: true,
    };
    assert_eq!(
        fresh.run(&engine, &mut old_bucket, Reason::Restored, |_| {}),
        first
    );
    assert_eq!(fresh.format(), Format::Compressed);
    // De oude node met de nieuwe code: zijn bestand blijft gewoon, zijn
    // nieuwe incrementen zijn SPINSEG2, door elkaar met de oude SPINSEG1.
    let upgraded = Node {
        root: &root,
        name: "old",
        policy: Policy::compressed(),
        compress: true,
    };
    let mixed = upgraded.run(&engine, &mut old_bucket, Reason::Continued, |db| {
        assert_eq!(digest(db), first);
        insert(db, 4_000, 3_000, "mixed");
    });
    assert_eq!(upgraded.format(), Format::Plain);
    let (plain, packed, _) = segments(&root.join("old-bucket"));
    assert!(plain > 0 && packed > 0, "{plain} {packed}");
    // En een lege map haalt het mengsel op.
    let last = Node {
        root: &root,
        name: "last",
        policy: Policy::compressed(),
        compress: true,
    };
    assert_eq!(
        last.run(&engine, &mut old_bucket, Reason::Restored, |_| {}),
        mixed
    );

    drop(engine);
    std::fs::remove_dir_all(&root).unwrap();
}
