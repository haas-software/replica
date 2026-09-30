//! Bewijst native SQLite in een echte HopOS-partitie, met begrensde RAM-opslag.
#![cfg_attr(target_os = "none", no_std, no_main)]
extern crate alloc;

use alloc::vec::Vec;
use applib::{App, log};
use replica_sqlite::{
    Engine, Error, Result, Value,
    memory::{Memory, Slot},
};

const HEAP_WORDS: usize = 512 * 1024;
const FILE_BYTES: usize = 1024 * 1024;
const TEST_MILLIS: i64 = 1_790_683_200_000;

applib::main!(smoke);

#[cfg(not(target_os = "none"))]
fn main() {}

async fn smoke(app: &'static App) {
    let start = applib::clock::now_ns();
    let result = run();
    let elapsed = applib::clock::now_ns().saturating_sub(start) / 1000;
    let exit = match result {
        Ok((bytes, writes, syncs)) => {
            log!(
                "REPLICA_SQLITE_OK version={} bytes={bytes} writes={writes} syncs={syncs} us={elapsed}",
                replica_sqlite::version_number()
            );
            0
        }
        Err(e) => {
            log!("REPLICA_SQLITE_FAIL {e} us={elapsed}");
            1
        }
    };
    app.shutdown(exit).await;
}

/// Een vaste boot-allocatie; resize blijft binnen de vooraf gereserveerde maat.
fn buffer<T: Clone>(n: usize, value: T) -> Result<Vec<T>> {
    let mut v = Vec::new();
    v.try_reserve_exact(n).map_err(|_| Error::MEMORY)?;
    v.resize(n, value);
    Ok(v)
}

fn require(ok: bool) -> Result {
    if ok { Ok(()) } else { Err(Error::MISUSE) }
}

/// Alleen deze boot-schil initialiseert de C-runtime, zonder await of andere cores.
fn run() -> Result<(usize, u64, u64)> {
    let mut heap = buffer(HEAP_WORDS, 0_u64)?;
    let mut data = buffer(FILE_BYTES, 0_u8)?;
    let mut journal = buffer(FILE_BYTES, 0_u8)?;
    let mut memory = Memory::new(
        [Slot::new(&mut data), Slot::new(&mut journal)],
        TEST_MILLIS,
        7,
    );
    {
        // SAFETY: De app start één eigenaar op zijn boot-core; geen andere taak
        // gebruikt SQLite. Engine en verbinding sluiten vóór de buffers verdwijnen.
        let mut engine = unsafe { Engine::initialize(&mut heap, &mut memory) }?;
        let mut db = engine.open(c"smoke.db")?;
        db.execute(c"CREATE TABLE item(id INTEGER PRIMARY KEY, value REAL, label TEXT); BEGIN")?;
        {
            let mut s = db.prepare(c"INSERT INTO item VALUES(?1, ?2, ?3)")?;
            for id in 1..=1000 {
                s.bind(1, Value::Integer(id))?;
                s.bind(2, Value::Real(1.25))?;
                s.bind(3, Value::Text("HopOS hé wereld"))?;
                require(!s.step()?)?;
                s.reset()?;
            }
        }
        db.execute(c"COMMIT; BEGIN; DELETE FROM item; ROLLBACK")?;
    }
    {
        // SAFETY: De vorige runtime is dicht; dezezelfde boot-core bezit de nieuwe.
        let mut engine = unsafe { Engine::initialize(&mut heap, &mut memory) }?;
        let mut db = engine.open(c"smoke.db")?;
        {
            let mut s = db.prepare(c"SELECT count(*),sum(value),max(length(label)) FROM item")?;
            require(s.step()?)?;
            require(s.column(0)? == Value::Integer(1000))?;
            // Toetst ook het C/Rust-softfloat-ABI, inclusief rekenen in SQLite.
            require(s.column(1)? == Value::Real(1250.0))?;
            require(s.column(2)? == Value::Integer(15))?;
        }
        let mut s = db.prepare(c"PRAGMA integrity_check")?;
        require(s.step()?)?;
        require(s.column(0)? == Value::Text("ok"))?;
    }
    let bytes = memory.contents(c"smoke.db")?.len();
    let stats = memory.stats();
    require(stats.writes > 0 && stats.syncs > 0)?;
    Ok((bytes, stats.writes, stats.syncs))
}
