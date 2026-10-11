//! De statement-cache van een verbinding (`Connection::prepare_cached`):
//! hergebruik op de SQL-tekst, reset en lege bindingen bij teruggeven, LRU
//! bij een volle cache, en een nette close met handvatten in de cache. Eén
//! test per binary: de C-runtime is procesglobaal.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use replica_sqlite::memory::{Memory, Slot};
use replica_sqlite::{CACHE_SIZE, CacheStats, Engine, Value};

#[test]
fn prepared_statements_are_reused_reset_and_evicted() {
    let mut heap = vec![0_u64; 512 * 1024];
    let mut data = vec![0_u8; 1024 * 1024];
    let mut journal = vec![0_u8; 1024 * 1024];
    let mut memory = Memory::new(
        [Slot::new(&mut data), Slot::new(&mut journal)],
        1_790_683_200_000,
        7,
    );
    {
        // SAFETY: De enige SQLite-runtime in deze testbinary, op deze thread.
        let mut engine = unsafe { Engine::initialize(&mut heap, &mut memory) }.unwrap();
        let mut db = engine.open(c"cache.db").unwrap();
        db.execute(c"CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();

        // Dezelfde tekst: één prepare, daarna uit de cache, met verse bindingen.
        for i in 0..10_i64 {
            let mut s = db
                .prepare_cached(c"INSERT INTO t(id, v) VALUES(?1, ?2)")
                .unwrap();
            assert_eq!(s.was_cached(), i > 0);
            s.bind(1, Value::Integer(i)).unwrap();
            s.bind(2, Value::Text("x")).unwrap();
            assert!(!s.step().unwrap());
        }
        assert_eq!(
            db.statement_cache_stats(),
            CacheStats {
                hits: 9,
                misses: 1,
                cached: 1
            }
        );

        // Een half gelezen SELECT gaat gereset terug: hij houdt geen
        // leestransactie open, dus een schrijvende transactie slaagt daarna.
        {
            let mut s = db
                .prepare_cached(c"SELECT id, v FROM t ORDER BY id")
                .unwrap();
            assert_eq!(s.column_count(), 2);
            assert!(s.step().unwrap());
            assert_eq!(s.column(0).unwrap(), Value::Integer(0));
        }
        db.execute(c"BEGIN; DELETE FROM t WHERE id = 9; COMMIT")
            .unwrap();
        {
            // Terug uit de cache: van voren af aan, niet halverwege.
            let mut s = db
                .prepare_cached(c"SELECT id, v FROM t ORDER BY id")
                .unwrap();
            assert!(s.was_cached());
            let mut n = 0;
            while s.step().unwrap() {
                n += 1;
            }
            assert_eq!(n, 9);
        }

        // Bindingen zijn leeg na teruggeven: een ongebonden parameter is NULL.
        {
            let mut s = db.prepare_cached(c"SELECT ?1").unwrap();
            s.bind(1, Value::Integer(42)).unwrap();
            assert!(s.step().unwrap());
            assert_eq!(s.column(0).unwrap(), Value::Integer(42));
        }
        {
            let mut s = db.prepare_cached(c"SELECT ?1").unwrap();
            assert!(s.was_cached());
            assert!(s.step().unwrap());
            assert_eq!(s.column(0).unwrap(), Value::Null);
        }

        // Een andere tekst is een andere sleutel, ook met dezelfde hash-lengte.
        {
            let s = db.prepare_cached(c"SELECT ?2").unwrap();
            assert!(!s.was_cached());
        }

        // Vol: de langst ongebruikte gaat eruit, de cache groeit niet.
        let texts: Vec<std::ffi::CString> = (0..CACHE_SIZE + 8)
            .map(|i| std::ffi::CString::new(format!("SELECT {i}")).unwrap())
            .collect();
        for t in &texts {
            let mut s = db.prepare_cached(t).unwrap();
            assert!(s.step().unwrap());
        }
        assert_eq!(db.statement_cache_stats().cached, CACHE_SIZE);
        // De laatste staan er nog, de eerste niet meer.
        assert!(
            db.prepare_cached(&texts[CACHE_SIZE + 7])
                .unwrap()
                .was_cached()
        );
        assert!(!db.prepare_cached(&texts[0]).unwrap().was_cached());

        // Een fout in de SQL komt niet in de cache.
        let before = db.statement_cache_stats();
        assert!(db.prepare_cached(c"SELEKT 1").is_err());
        assert_eq!(db.statement_cache_stats().cached, before.cached);

        // Leegmaken finaliseert alles.
        db.clear_statement_cache();
        assert_eq!(db.statement_cache_stats().cached, 0);

        // Een vergeten statement verlaat de cache; de verbinding sluit het.
        let s = db.prepare_cached(c"SELECT 1").unwrap();
        std::mem::forget(s);
        let s = db.prepare_cached(c"SELECT 2").unwrap();
        drop(s);
        assert_eq!(db.statement_cache_stats().cached, 1);
        // De verbinding sluit met een handvat in de cache (en een vergeten).
    }
    // De engine is dicht: een tweede open van dezelfde heap werkt weer.
    {
        // SAFETY: De vorige runtime is gesloten; nog steeds deze ene thread.
        let mut engine = unsafe { Engine::initialize(&mut heap, &mut memory) }.unwrap();
        let mut db = engine.open(c"cache.db").unwrap();
        let mut s = db.prepare_cached(c"SELECT count(*) FROM t").unwrap();
        assert!(s.step().unwrap());
        assert_eq!(s.column(0).unwrap(), Value::Integer(9));
    }
}
