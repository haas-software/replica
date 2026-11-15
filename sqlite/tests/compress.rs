//! Gecomprimeerde opslag onder echte SQLite: lange willekeurige werklasten
//! tegen een ongecomprimeerde referentie, begrensde groei, twee eigenaren op
//! één schijf, conversie, beschadigde containers en stroomuitval tijdens elke
//! fase van een commit. De C-runtime is procesglobaal: de toetsen nemen om
//! de beurt het slot en starten elk hun eigen engine.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod support;
use replica_sqlite::compress::{self, Compressed, Format, Policy};
use replica_sqlite::{Connection, Engine, Storage, Value};
use std::collections::hash_map::DefaultHasher;
use std::ffi::{CStr, CString};
use std::hash::{Hash, Hasher};
use std::sync::Mutex;
use support::disk::{Disk, State, Vol, disk};

static RUNTIME: Mutex<()> = Mutex::new(());

/// Draait `f` met de enige SQLite-engine van het proces.
fn with_engine(f: impl FnOnce(&Engine<'_, Vol>)) {
    let _guard = RUNTIME.lock().unwrap_or_else(|e| e.into_inner());
    let mut heap = vec![0_u64; 2 << 20];
    let mut base = Vol::new(&disk(1));
    // SAFETY: Het slot houdt dit de enige SQLite-runtime van het proces.
    let engine = unsafe { Engine::initialize(&mut heap, &mut base) }.unwrap();
    f(&engine);
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// Een JSON-document zoals EasyFlor ze opslaat, ongeveer `len` bytes.
fn body(rng: &mut Rng, len: usize) -> String {
    let mut s = String::with_capacity(len + 200);
    while s.len() < len {
        let n = rng.below(100_000);
        s.push_str(&format!(
            "{{\"Id\":\"articles/{n}\",\"Name\":\"Rosa Red Naomi {n}\",\"Price\":{}.{:02},\"Stock\":{},\"Tags\":[\"rose\",\"red\",\"{}\"],\"Supplier\":\"suppliers/{}\"}}",
            rng.below(50),
            rng.below(100),
            rng.below(1000),
            rng.below(7),
            rng.below(40)
        ));
    }
    s.truncate(len);
    s
}

/// Willekeurige bytes: onsamendrukbare pagina's.
fn noise(rng: &mut Rng, len: usize) -> Vec<u8> {
    (0..len).map(|_| rng.next() as u8).collect()
}

fn exec<B: Storage>(db: &mut Connection<'_, '_, B>, sql: &str) {
    let sql = CString::new(sql).unwrap();
    db.execute(&sql)
        .unwrap_or_else(|e| panic!("{e} in {sql:?}"));
}

fn try_exec<B: Storage>(db: &mut Connection<'_, '_, B>, sql: &str) -> replica_sqlite::Result {
    db.execute(&CString::new(sql).unwrap())
}

fn run<B: Storage>(db: &mut Connection<'_, '_, B>, sql: &CStr, args: &[Value<'_>]) {
    let mut s = db.prepare(sql).unwrap();
    for (i, a) in args.iter().enumerate() {
        s.bind(i as u32 + 1, dup(a)).unwrap();
    }
    while s.step().unwrap() {}
}

#[expect(clippy::needless_match, reason = "Value heeft geen Clone")]
fn dup<'a>(v: &Value<'a>) -> Value<'a> {
    match *v {
        Value::Null => Value::Null,
        Value::Integer(n) => Value::Integer(n),
        Value::Real(r) => Value::Real(r),
        Value::Text(t) => Value::Text(t),
        Value::Blob(b) => Value::Blob(b),
    }
}

fn integer<B: Storage>(db: &mut Connection<'_, '_, B>, sql: &CStr) -> i64 {
    let mut s = db.prepare(sql).unwrap();
    assert!(s.step().unwrap());
    let Value::Integer(n) = s.column(0).unwrap() else {
        panic!("integer expected")
    };
    n
}

fn integrity<B: Storage>(db: &mut Connection<'_, '_, B>) {
    let mut s = db.prepare(c"PRAGMA integrity_check").unwrap();
    assert!(s.step().unwrap());
    assert_eq!(s.column(0).unwrap(), Value::Text("ok"));
}

/// Een vingerafdruk van alle rijen.
fn digest<B: Storage>(db: &mut Connection<'_, '_, B>) -> u64 {
    let mut h = DefaultHasher::new();
    let mut s = db
        .prepare(c"SELECT id, kind, body FROM docs ORDER BY id")
        .unwrap();
    while s.step().unwrap() {
        match s.column(0).unwrap() {
            Value::Integer(n) => n.hash(&mut h),
            v => panic!("{v:?}"),
        }
        format!("{:?}", s.column(1).unwrap()).hash(&mut h);
        match s.column(2).unwrap() {
            Value::Text(t) => t.hash(&mut h),
            Value::Blob(b) => b.hash(&mut h),
            Value::Null => 0.hash(&mut h),
            v => panic!("{v:?}"),
        }
    }
    h.finish()
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS docs(id INTEGER PRIMARY KEY, kind TEXT, body BLOB); \
                      CREATE INDEX IF NOT EXISTS docs_kind ON docs(kind)";

/// Eén willekeurige transactie op beide verbindingen tegelijk.
fn transaction<A: Storage, B: Storage>(
    rng: &mut Rng,
    dbs: &mut (&mut Connection<'_, '_, A>, &mut Connection<'_, '_, B>),
    big: bool,
) {
    let ops = 1 + rng.below(if big { 200 } else { 12 });
    exec(dbs.0, "BEGIN");
    exec(dbs.1, "BEGIN");
    for _ in 0..ops {
        let id = rng.below(3000) as i64;
        let kind = format!("k{}", rng.below(20));
        match rng.below(10) {
            0..=3 => {
                let len = (rng.below(4) * rng.below(3000)) as usize + 20;
                let b = body(rng, len);
                let args = [Value::Integer(id), Value::Text(&kind), Value::Text(&b)];
                let sql = c"INSERT OR REPLACE INTO docs(id, kind, body) VALUES(?1, ?2, ?3)";
                run(dbs.0, sql, &args);
                run(dbs.1, sql, &args);
            }
            4..=5 => {
                let len = rng.below(9000) as usize + 1;
                let b = body(rng, len);
                let args = [Value::Text(&b), Value::Integer(id)];
                let sql = c"UPDATE docs SET body = ?1 WHERE id = ?2";
                run(dbs.0, sql, &args);
                run(dbs.1, sql, &args);
            }
            6 => {
                let n = rng.below(40) as i64;
                let args = [Value::Integer(id), Value::Integer(id + n)];
                let sql = c"DELETE FROM docs WHERE id BETWEEN ?1 AND ?2";
                run(dbs.0, sql, &args);
                run(dbs.1, sql, &args);
            }
            7 => {
                let n = rng.below(20000) as usize + 1;
                let b = noise(rng, n);
                let args = [Value::Integer(id), Value::Text(&kind), Value::Blob(&b)];
                let sql = c"INSERT OR REPLACE INTO docs(id, kind, body) VALUES(?1, ?2, ?3)";
                run(dbs.0, sql, &args);
                run(dbs.1, sql, &args);
            }
            _ => {
                let args = [Value::Text(&kind), Value::Integer(id)];
                let sql = c"UPDATE docs SET kind = ?1 WHERE id >= ?2 AND id < ?2 + 25";
                run(dbs.0, sql, &args);
                run(dbs.1, sql, &args);
            }
        }
    }
    if rng.below(8) == 0 {
        exec(dbs.0, "ROLLBACK");
        exec(dbs.1, "ROLLBACK");
    } else {
        exec(dbs.0, "COMMIT");
        exec(dbs.1, "COMMIT");
    }
}

fn logical(c: &mut Compressed<Vol>, name: &CStr) -> Vec<u8> {
    let id = c.open(name, replica_sqlite::OpenFlags(2)).unwrap();
    let n = c.size(id).unwrap() as usize;
    let mut v = vec![0u8; n];
    let mut at = 0;
    while at < n {
        let got = c.read(id, at as u64, &mut v[at..]).unwrap();
        assert!(got > 0);
        at += got;
    }
    c.close(id).unwrap();
    v
}

fn physical(d: &Disk, name: &str) -> usize {
    d.borrow().len(name).unwrap()
}

#[test]
fn random_workload_matches_reference() {
    for (seed, page) in [(7_u64, 4096_u32), (11, 16384), (13, 1024)] {
        with_engine(|engine| {
            let cd = disk(seed);
            let pd = disk(seed + 1);
            let mut cs = Compressed::new(Vol::new(&cd), Policy::compressed());
            let mut ps = Vol::new(&pd);
            let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1);
            for round in 0..6 {
                {
                    let mut a = engine.open_with(c"main.db", &mut cs).unwrap();
                    let mut b = engine.open_with(c"main.db", &mut ps).unwrap();
                    if round == 0 {
                        exec(&mut a, &format!("PRAGMA page_size={page}"));
                        exec(&mut b, &format!("PRAGMA page_size={page}"));
                    }
                    exec(&mut a, SCHEMA);
                    exec(&mut b, SCHEMA);
                    for t in 0..150 {
                        transaction(&mut rng, &mut (&mut a, &mut b), t % 37 == 0);
                        if t % 50 == 49 {
                            assert_eq!(
                                digest(&mut a),
                                digest(&mut b),
                                "seed {seed} round {round} t {t}"
                            );
                        }
                    }
                    if round % 2 == 1 {
                        exec(&mut a, "VACUUM");
                        exec(&mut b, "VACUUM");
                    }
                    if round == 4 {
                        // Krimp: bijna alles weg, dan VACUUM.
                        exec(&mut a, "DELETE FROM docs WHERE id > 200; VACUUM");
                        exec(&mut b, "DELETE FROM docs WHERE id > 200; VACUUM");
                    }
                    integrity(&mut a);
                    assert_eq!(digest(&mut a), digest(&mut b));
                    assert_eq!(
                        integer(&mut a, c"PRAGMA page_size"),
                        i64::from(page),
                        "SQLite's paginamaat"
                    );
                }
                // Een paar kleine commits: de schoonmaker ruimt na VACUUM of een
                // grote DELETE op en het bestand krimpt.
                {
                    let mut a = engine.open_with(c"main.db", &mut cs).unwrap();
                    let mut b = engine.open_with(c"main.db", &mut ps).unwrap();
                    for i in 0..30 {
                        let sql = format!(
                            "INSERT OR REPLACE INTO docs VALUES({}, 'tick', 'x')",
                            100_000 + i % 3
                        );
                        exec(&mut a, &sql);
                        exec(&mut b, &sql);
                    }
                }
                // Gesloten: de logische bytes zijn die van de referentie.
                let raw = pd.borrow().bytes("main.db").unwrap();
                assert_eq!(
                    logical(&mut cs, c"main.db"),
                    raw,
                    "seed {seed} round {round}"
                );
                assert_eq!(
                    compress::detect(&mut Vol::new(&cd), c"main.db").unwrap(),
                    Format::Compressed
                );
                // En een verse eigenaar leest dezelfde staat van de schijf.
                let mut fresh = Compressed::new(Vol::new(&cd), Policy::compressed());
                assert_eq!(logical(&mut fresh, c"main.db"), raw);
                let id = cs.open(c"main.db", replica_sqlite::OpenFlags(2)).unwrap();
                let st = cs.stats(id).unwrap().unwrap();
                cs.close(id).unwrap();
                eprintln!("page={page} round={round} logical={} {st:?}", raw.len());
                // Begrensd: levend plus hoogstens een derde, het metadatagebied en
                // een handvol segmenten speling.
                assert!(
                    st.physical <= st.live * 3 / 2 + 24 * 65536,
                    "page={page} round={round} {st:?}"
                );
            }
        });
    }
}

#[test]
fn free_space_reuse_keeps_the_file_bounded() {
    with_engine(|engine| {
        let cd = disk(3);
        let mut cs = Compressed::new(Vol::new(&cd), Policy::compressed());
        let mut rng = Rng(99);
        let mut db = engine.open_with(c"main.db", &mut cs).unwrap();
        exec(&mut db, "PRAGMA page_size=16384");
        exec(&mut db, SCHEMA);
        // Een vaste set van 2000 documenten, steeds opnieuw geschreven.
        exec(&mut db, "BEGIN");
        for id in 0..2000 {
            let b = body(&mut rng, 1500);
            run(
                &mut db,
                c"INSERT INTO docs(id, kind, body) VALUES(?1, 'a', ?2)",
                &[Value::Integer(id), Value::Text(&b)],
            );
        }
        exec(&mut db, "COMMIT");
        let mut peak = 0usize;
        let mut sizes = Vec::new();
        for t in 0..3000 {
            exec(&mut db, "BEGIN");
            for _ in 0..1 + rng.below(6) {
                let id = rng.below(2000) as i64;
                let n = 500 + rng.below(2500) as usize;
                let b = body(&mut rng, n);
                run(
                    &mut db,
                    c"UPDATE docs SET body = ?1 WHERE id = ?2",
                    &[Value::Text(&b), Value::Integer(id)],
                );
            }
            exec(&mut db, "COMMIT");
            let p = physical(&cd, "main.db");
            peak = peak.max(p);
            if t % 500 == 499 {
                sizes.push(p);
            }
        }
        integrity(&mut db);
        let logical = integer(&mut db, c"PRAGMA page_count") as usize * 16384;
        drop(db);
        let id = cs.open(c"main.db", replica_sqlite::OpenFlags(2)).unwrap();
        let st = cs.stats(id).unwrap().unwrap();
        let (phys, live) = (st.physical, st.live);
        cs.close(id).unwrap();
        eprintln!("logical={logical} physical={phys} live={live} peak={peak} sizes={sizes:?}");
        // Dood en vrij samen hoogstens ongeveer een derde boven de levende
        // bytes, plus het metadatagebied en een paar segmenten speling.
        let slack = 16 * 65536;
        assert!(phys <= live * 3 / 2 + slack, "physical {phys} live {live}");
        assert!(peak as u64 <= live * 2 + slack, "peak {peak} live {live}");
        // Na de opwarming groeit het niet meer.
        let (first, last) = (sizes[1], *sizes.last().unwrap());
        assert!(last <= first + first / 5, "{sizes:?}");
        // Alles weg en VACUUM: het bestand krimpt mee.
        let mut db = engine.open_with(c"main.db", &mut cs).unwrap();
        exec(&mut db, "DELETE FROM docs WHERE id >= 100; VACUUM");
        integrity(&mut db);
        for _ in 0..20 {
            // Een paar kleine commits laten de schoonmaker de staart opruimen.
            exec(&mut db, "UPDATE docs SET kind = 'b' WHERE id = 1");
        }
        drop(db);
        let small = physical(&cd, "main.db");
        eprintln!("after vacuum physical={small}");
        assert!((small as u64) < phys / 4, "after vacuum {small} vs {phys}");
    });
}

#[test]
fn plain_files_stay_plain_and_both_formats_mix() {
    with_engine(|engine| {
        let d = disk(5);
        // Een bestaande, ongecomprimeerde database.
        {
            let mut v = Vol::new(&d);
            let mut db = engine.open_with(c"old.db", &mut v).unwrap();
            exec(&mut db, SCHEMA);
            exec(&mut db, "INSERT INTO docs VALUES(1, 'x', 'old')");
        }
        let mut cs = Compressed::new(Vol::new(&d), Policy::compressed());
        {
            let mut db = engine.open_with(c"old.db", &mut cs).unwrap();
            exec(&mut db, "INSERT INTO docs VALUES(2, 'x', 'via compressed')");
            integrity(&mut db);
            assert_eq!(integer(&mut db, c"SELECT count(*) FROM docs"), 2);
        }
        assert_eq!(
            &d.borrow().bytes("old.db").unwrap()[..16],
            b"SQLite format 3\0"
        );
        // Een nieuwe naast de oude, in één verbinding (ATTACH).
        {
            let mut db = engine.open_with(c"new.db", &mut cs).unwrap();
            exec(&mut db, SCHEMA);
            exec(&mut db, "ATTACH 'old.db' AS o");
            exec(&mut db, "INSERT INTO docs SELECT * FROM o.docs");
            exec(&mut db, "INSERT INTO o.docs VALUES(3, 'y', 'both')");
            integrity(&mut db);
        }
        assert_eq!(&d.borrow().bytes("new.db").unwrap()[..8], &compress::MAGIC);
        assert_eq!(
            &d.borrow().bytes("old.db").unwrap()[..16],
            b"SQLite format 3\0"
        );
        // Met `Policy::plain` ontstaat niets nieuws gecomprimeerd, maar een
        // bestaande container blijft leesbaar.
        let mut plain = Compressed::new(Vol::new(&d), Policy::plain());
        {
            let mut db = engine.open_with(c"new.db", &mut plain).unwrap();
            assert_eq!(integer(&mut db, c"SELECT count(*) FROM docs"), 2);
            exec(&mut db, "INSERT INTO docs VALUES(9, 'z', 'later')");
        }
        {
            let mut db = engine.open_with(c"third.db", &mut plain).unwrap();
            exec(&mut db, SCHEMA);
        }
        assert_eq!(
            &d.borrow().bytes("third.db").unwrap()[..16],
            b"SQLite format 3\0"
        );
        // Sidecars zijn nooit containers.
        assert!(
            d.borrow()
                .bytes("new.db-journal")
                .is_none_or(|j| j.get(..8) != Some(&compress::MAGIC[..]))
        );
    });
}

#[test]
fn two_owners_see_each_others_commits() {
    with_engine(|engine| {
        let d = disk(17);
        let mut sql = Compressed::new(Vol::new(&d), Policy::compressed());
        let mut sync = Compressed::new(Vol::new(&d), Policy::compressed());
        let mut rng = Rng(5);
        let mut db = engine.open_with(c"main.db", &mut sql).unwrap();
        exec(&mut db, "PRAGMA page_size=4096");
        exec(&mut db, SCHEMA);
        for round in 0..60 {
            if round == 30 {
                // Zonder sync van SQLite (offline werk, migrate): de afronding van het
                // journal is de commit, en de andere eigenaar ziet hem meteen.
                exec(&mut db, "PRAGMA synchronous=OFF");
            }
            if round == 45 {
                exec(&mut db, "PRAGMA journal_mode=TRUNCATE");
            }
            exec(&mut db, "BEGIN");
            for _ in 0..1 + rng.below(if round % 9 == 0 { 400 } else { 5 }) {
                let n = 100 + rng.below(3000) as usize;
                let b = body(&mut rng, n);
                run(
                    &mut db,
                    c"INSERT OR REPLACE INTO docs(id, kind, body) VALUES(?1, 'a', ?2)",
                    &[Value::Integer(rng.below(5000) as i64), Value::Text(&b)],
                );
            }
            exec(&mut db, "COMMIT");
            // De verbinding blijft open (EXCLUSIVE); de andere eigenaar leest
            // het bestand en ziet precies de gecommitte staat.
            let raw = logical(&mut sync, c"main.db");
            let again = logical(&mut sync, c"main.db");
            assert_eq!(raw, again);
            let mut check = Compressed::new(Vol::new(&d), Policy::compressed());
            assert_eq!(logical(&mut check, c"main.db"), raw, "round {round}");
            // En dat is wat SQLite ziet: dezelfde rijen via de verse eigenaar.
            let mut fresh = Compressed::new(Vol::new(&d), Policy::compressed());
            let mut other = engine.open_with(c"main.db", &mut fresh).unwrap();
            assert_eq!(digest(&mut other), digest(&mut db), "round {round}");
        }
        integrity(&mut db);
    });
}

#[test]
fn vacuum_into_converts_to_compressed_16k() {
    with_engine(|engine| {
        let d = disk(23);
        let mut rng = Rng(77);
        let want;
        {
            let mut v = Vol::new(&d);
            let mut db = engine.open_with(c"old.db", &mut v).unwrap();
            exec(&mut db, "PRAGMA page_size=4096");
            exec(&mut db, SCHEMA);
            exec(&mut db, "BEGIN");
            for id in 0..3000 {
                let b = body(&mut rng, 800);
                run(
                    &mut db,
                    c"INSERT INTO docs(id, kind, body) VALUES(?1, 'k', ?2)",
                    &[Value::Integer(id), Value::Text(&b)],
                );
            }
            exec(&mut db, "COMMIT");
            want = digest(&mut db);
        }
        let mut cs = Compressed::new(Vol::new(&d), Policy::compressed());
        {
            let mut db = engine.open_with(c"old.db", &mut cs).unwrap();
            exec(&mut db, "PRAGMA page_size=16384; VACUUM INTO 'new.db'");
        }
        let old = d.borrow().len("old.db").unwrap();
        let new = d.borrow().len("new.db").unwrap();
        eprintln!("convert: plain 4k {old} -> compressed 16k {new}");
        assert_eq!(&d.borrow().bytes("new.db").unwrap()[..8], &compress::MAGIC);
        assert!(new * 3 < old, "{new} vs {old}");
        let mut fresh = Compressed::new(Vol::new(&d), Policy::compressed());
        let mut db = engine.open_with(c"new.db", &mut fresh).unwrap();
        assert_eq!(integer(&mut db, c"PRAGMA page_size"), 16384);
        integrity(&mut db);
        assert_eq!(digest(&mut db), want);
    });
}

#[test]
fn damaged_containers_give_errors_not_panics() {
    with_engine(|engine| {
        let d = disk(29);
        let mut rng = Rng(31);
        {
            let mut cs = Compressed::new(Vol::new(&d), Policy::compressed());
            let mut db = engine.open_with(c"main.db", &mut cs).unwrap();
            exec(&mut db, SCHEMA);
            for i in 0..300 {
                let b = body(&mut rng, 2000);
                run(
                    &mut db,
                    c"INSERT INTO docs(id, kind, body) VALUES(?1, 'a', ?2)",
                    &[Value::Integer(i), Value::Text(&b)],
                );
            }
        }
        let good = d.borrow().bytes("main.db").unwrap();
        let mut errors = 0;
        for trial in 0..300 {
            let mut bad = good.clone();
            match trial % 3 {
                0 => {
                    for _ in 0..1 + rng.below(8) {
                        let at = rng.below(bad.len() as u64) as usize;
                        bad[at] ^= 1 << rng.below(8);
                    }
                }
                1 => {
                    let at = rng.below(bad.len() as u64) as usize;
                    let n = (rng.below(4096) as usize).min(bad.len() - at);
                    for b in &mut bad[at..at + n] {
                        *b = rng.next() as u8;
                    }
                }
                _ => bad.truncate(rng.below(bad.len() as u64) as usize),
            }
            let d2 = disk(trial);
            d2.borrow_mut().put("main.db", bad);
            let mut cs = Compressed::new(Vol::new(&d2), Policy::compressed());
            let outcome = engine.open_with(c"main.db", &mut cs).and_then(|mut db| {
                db.execute(c"SELECT count(*), sum(length(body)) FROM docs")?;
                let mut s = db.prepare(c"PRAGMA integrity_check")?;
                s.step()?;
                Ok(())
            });
            if outcome.is_err() {
                errors += 1;
            }
        }
        eprintln!("damaged: {errors} of 300 gave an error");
        assert!(errors > 50);
    });
}

/// Eén scenario voor de stroomuitval: een schone begintoestand en één
/// transactie erbovenop.
struct Scenario {
    name: &'static str,
    setup: fn(&mut Connection<'_, '_, Compressed<Vol>>, &mut Rng),
    step: fn(&mut Connection<'_, '_, Compressed<Vol>>, &mut Rng),
}

fn fill(
    db: &mut Connection<'_, '_, Compressed<Vol>>,
    rng: &mut Rng,
    from: i64,
    n: i64,
    len: usize,
) {
    exec(db, "BEGIN");
    for id in from..from + n {
        let b = body(rng, len);
        run(
            db,
            c"INSERT OR REPLACE INTO docs(id, kind, body) VALUES(?1, 'a', ?2)",
            &[Value::Integer(id), Value::Text(&b)],
        );
    }
    exec(db, "COMMIT");
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "first commit of a new file",
            setup: |_, _| {},
            step: |db, rng| {
                exec(db, "PRAGMA page_size=4096");
                exec(db, SCHEMA);
                fill(db, rng, 0, 40, 900);
            },
        },
        Scenario {
            name: "small commit (log record)",
            setup: |db, rng| {
                exec(db, SCHEMA);
                fill(db, rng, 0, 200, 700);
            },
            step: |db, rng| fill(db, rng, 50, 3, 1200),
        },
        Scenario {
            name: "big commit (checkpoint)",
            setup: |db, rng| {
                exec(db, SCHEMA);
                fill(db, rng, 0, 100, 700);
            },
            step: |db, rng| fill(db, rng, 0, 700, 2500),
        },
        Scenario {
            name: "rewrite with cleaning",
            setup: |db, rng| {
                exec(db, SCHEMA);
                fill(db, rng, 0, 400, 2000);
                for _ in 0..6 {
                    fill(db, rng, 0, 400, 2000);
                }
            },
            step: |db, rng| fill(db, rng, 0, 300, 2100),
        },
        Scenario {
            name: "delete and vacuum (shrink)",
            setup: |db, rng| {
                exec(db, SCHEMA);
                fill(db, rng, 0, 500, 1500);
            },
            step: |db, _| exec(db, "DELETE FROM docs WHERE id > 20; VACUUM"),
        },
    ]
}

#[test]
fn power_cut_at_every_write_and_sync_of_a_commit() {
    with_engine(|engine| {
        for sc in scenarios() {
            // De begintoestand, schoon op de schijf.
            let base = disk(41);
            let mut rng = Rng(1234);
            let before = {
                let mut cs = Compressed::new(Vol::new(&base), Policy::compressed());
                let mut db = engine.open_with(c"main.db", &mut cs).unwrap();
                (sc.setup)(&mut db, &mut rng);
                let _ = try_exec(&mut db, SCHEMA);
                digest(&mut db)
            };
            let start = base.borrow().settled();
            let step_rng = rng.0;
            // Een keer zonder knip: hoeveel operaties, en de eindtoestand.
            let after;
            let total = {
                let d = std::rc::Rc::new(std::cell::RefCell::new(start.clone()));
                let mut cs = Compressed::new(Vol::new(&d), Policy::compressed());
                let mut db = engine.open_with(c"main.db", &mut cs).unwrap();
                let ops0 = d.borrow().ops;
                (sc.step)(&mut db, &mut Rng(step_rng));
                let ops = d.borrow().ops - ops0;
                after = digest(&mut db);
                ops
            };
            assert_ne!(before, after, "{}", sc.name);
            let mut outcomes = [0usize; 2];
            for cut in 1..=total + 2 {
                for seed in 0..3u64 {
                    let d: Disk = std::rc::Rc::new(std::cell::RefCell::new(start.clone()));
                    {
                        let mut cs = Compressed::new(Vol::new(&d), Policy::compressed());
                        if let Ok(mut db) = engine.open_with(c"main.db", &mut cs) {
                            let ops0 = d.borrow().ops;
                            d.borrow_mut().cut = Some(ops0 + cut);
                            let hook = std::panic::take_hook();
                            std::panic::set_hook(Box::new(|_| {}));
                            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                (sc.step)(&mut db, &mut Rng(step_rng));
                            }));
                            drop(r);
                            drop(db);
                            std::panic::set_hook(hook);
                        }
                    }
                    let crashed: State = d.borrow().crash(seed * 7919 + cut);
                    let d2: Disk = std::rc::Rc::new(std::cell::RefCell::new(crashed));
                    {
                        let mut probe = Compressed::new(Vol::new(&d2), Policy::compressed());
                        let lg = logical(&mut probe, c"main.db");
                        let jl = d2.borrow().len("main.db-journal");
                        let pre = d.borrow().len("main.db-journal");
                        if cut == 25 {
                            eprintln!(
                                "DEBUG2 logical={} hdr={:?} pages={:?} journal={jl:?} prejournal={pre:?}",
                                lg.len(),
                                &lg.get(..16),
                                lg.get(28..32)
                            );
                        }
                        if let Err(e) = probe.open(c"main.db", replica_sqlite::OpenFlags(2)) {
                            let b = d2.borrow().bytes("main.db").unwrap();
                            let j = d2.borrow().len("main.db-journal");
                            eprintln!(
                                "DEBUG {}: cut {cut} seed {seed}: {e} len={} journal={j:?} a={:?} b={:?}",
                                sc.name,
                                b.len(),
                                &b[..16.min(b.len())],
                                b.get(4096..4112)
                            );
                        }
                    }
                    let mut cs = Compressed::new(Vol::new(&d2), Policy::compressed());
                    let mut db = engine
                        .open_with(c"main.db", &mut cs)
                        .unwrap_or_else(|e| panic!("{}: cut {cut} seed {seed}: open {e}", sc.name));
                    let got = if sc.name.starts_with("first") {
                        // Vóór de eerste commit is er nog geen tabel.
                        match try_exec(&mut db, "SELECT 1 FROM docs LIMIT 1") {
                            Ok(()) => digest(&mut db),
                            Err(_) => before,
                        }
                    } else {
                        digest(&mut db)
                    };
                    let mut s = db.prepare(c"PRAGMA integrity_check").unwrap();
                    assert!(s.step().unwrap());
                    assert_eq!(
                        s.column(0).unwrap(),
                        Value::Text("ok"),
                        "{}: cut {cut} seed {seed}",
                        sc.name
                    );
                    drop(s);
                    assert!(
                        got == before || got == after,
                        "{}: cut {cut}/{total} seed {seed}: neither old nor new",
                        sc.name
                    );
                    outcomes[usize::from(got == after)] += 1;
                    // En de herstelde database blijft bruikbaar.
                    exec(
                        &mut db,
                        "INSERT OR REPLACE INTO docs VALUES(999999, 'z', 'after')",
                    );
                    integrity(&mut db);
                }
            }
            eprintln!(
                "{}: {total} ops, old={} new={}",
                sc.name, outcomes[0], outcomes[1]
            );
            assert!(outcomes[0] > 0 && outcomes[1] > 0, "{}", sc.name);
        }
    });
}
