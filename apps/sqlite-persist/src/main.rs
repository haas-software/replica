//! SQLite-C wacht op een eigen stack op echte HopFS-I/O; de persist-proef van Replica.
#![cfg_attr(target_os = "none", no_std, no_main)]
extern crate alloc;
mod pipeline;
use alloc::vec::Vec;
use applib::{App, EXEC, appnet, log, stacktask::Task};
use core::{
    cell::Cell,
    future::{Future, poll_fn},
    pin::{Pin, pin},
    task::Poll,
    time::Duration,
};
use replica_hopos::{Environment, Files, Wait};
use replica_sqlite::{Engine, Error, Result, Value, asynchronous::Bridge};
applib::main!(probe);
#[cfg(not(target_os = "none"))]
fn main() {}
struct TestEnvironment(u64);
impl Environment for TestEnvironment {
    // Reproduceerbare testbron; geen productie-entropie.
    fn random(&mut self, dst: &mut [u8]) -> Result {
        for b in dst {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            *b = self.0 as u8;
        }
        Ok(())
    }
    fn unix_millis(&mut self) -> Result<i64> {
        Ok(1_790_683_200_000)
    }
}
async fn probe(app: &'static App) {
    // De looptijd in de marker (zoals smoke): de maat voor SQLite op HopFS
    // met OP_SYNC, alleen en naast een bulk-app op dezelfde node.
    let start = applib::clock::now_ns();
    let result = run(app).await;
    let us = applib::clock::now_ns().saturating_sub(start) / 1000;
    match result {
        Ok(restored) => log!(
            "REPLICA_SQLITE_PERSIST_{} us={us}",
            if restored { "READ" } else { "WRITE" }
        ),
        Err(e) => log!("REPLICA_SQLITE_PERSIST_FAIL {e} us={us}"),
    }
    loop {
        EXEC.get().after(Duration::from_secs(60)).await;
    }
}
async fn run(app: &'static App) -> Result<bool> {
    let net = appnet::up(app).map_err(|_| Error::IO)?;
    let mut sys = net.system_client();
    let restored = match sys.stat("/data/replica.db").await {
        Ok(_) => true,
        Err(applib::sys::Error::NotFound { .. }) => false,
        Err(_) => return Err(Error::IO),
    };
    let mut heap = Vec::new();
    heap.try_reserve_exact(512 * 1024)
        .map_err(|_| Error::MEMORY)?;
    heap.resize(512 * 1024, 0u64);
    let mut files = Files::new(sys, "/data", TestEnvironment(7))?;
    // SAFETY: Eén SQLite-eigenaar, begrensde SQL zonder recursieve queries of
    // extensies. 8 MiB private stack; de callbacks geven bij annulering IOERR,
    // waarna verbinding en engine op dezelfde stack sluiten. Geen longjmp.
    let task = unsafe {
        Task::new(8 << 20, |s| {
            let wait = Wait(s);
            let mut bridge = Bridge::new(&mut files, &wait);
            // SAFETY: Alleen deze taak gebruikt SQLite, totdat engine hier sluit.
            let mut engine = Engine::initialize(&mut heap, &mut bridge)?;
            let mut db = engine.open(c"replica.db")?;
            if !restored {
                db.execute(
                    c"CREATE TABLE item(id INTEGER PRIMARY KEY,label TEXT,value REAL);BEGIN",
                )?;
                {
                    let mut statement = db.prepare(c"INSERT INTO item VALUES(?1,?2,?3)")?;
                    for n in 1..=1000 {
                        statement.bind(1, Value::Integer(n))?;
                        statement.bind(2, Value::Text("native hé SQLite"))?;
                        statement.bind(3, Value::Real(1.25))?;
                        if statement.step()? {
                            return Err(Error::MISUSE);
                        }
                        statement.reset()?;
                    }
                }
                db.execute(c"COMMIT;BEGIN;DELETE FROM item;ROLLBACK")?;
            }
            {
                let mut statement =
                    db.prepare(c"SELECT count(*),sum(value),min(label) FROM item")?;
                if !statement.step()?
                    || statement.column(0)? != Value::Integer(1000)
                    || statement.column(1)? != Value::Real(1250.0)
                    || statement.column(2)? != Value::Text("native hé SQLite")
                {
                    return Err(Error::MISUSE);
                }
            }
            {
                let mut statement = db.prepare(c"PRAGMA integrity_check")?;
                if !statement.step()? || statement.column(0)? != Value::Text("ok") {
                    return Err(Error::MISUSE);
                }
            }
            drop(db);
            drop(engine);
            // SAFETY: De vorige engine is gesloten; deze private stack blijft
            // de enige SQLite-eigenaar voor capture, verificatie en restore.
            pipeline::run(&mut bridge, &mut heap, restored)?;
            Ok(restored)
        })
    }
    .map_err(|_| Error::MEMORY)?;
    task.await?;
    drop(files);

    // Annuleer tijdens CPU-werk, dus zonder dat bestands-I/O hoeft te gebeuren.
    // De C-progresscallback parkeert de query en moet daarna normaal opruimen.
    let mut files = Files::new(net.system_client(), "/data", TestEnvironment(11))?;
    let entered = Cell::new(false);
    let code = Cell::new(0);
    {
        // SAFETY: Dezelfde unieke eigenaar en stackgrens. Deze recursieve CTE
        // gebruikt SQLite's iteratieve VM, geen recursieve C-functie/extensie.
        let mut task = unsafe {
            Task::new(8 << 20, |s| -> Result {
                let wait = Wait(s);
                let mut bridge = Bridge::new(&mut files, &wait);
                let mut engine = Engine::initialize(&mut heap, &mut bridge)?;
                let mut db = engine.open(c"replica.db")?;
                entered.set(true);
                let result = db.execute(c"WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n");
                code.set(result.as_ref().err().map_or(0, |e| e.code));
                result
            })
        }.map_err(|_| Error::MEMORY)?;
        let mut timer = pin!(EXEC.get().after(Duration::from_millis(5)));
        poll_fn(|cx| {
            if entered.get() && timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Ok(()));
            }
            if let Poll::Ready(result) = Pin::new(&mut task).poll(cx) {
                return Poll::Ready(Err(result.err().unwrap_or(Error::MISUSE)));
            }
            Poll::Pending
        })
        .await?;
        // Drop hervat de private stack met Cancelled en sluit SQLite op die stack.
    }
    if code.get() != 9 {
        // SQLITE_INTERRUPT
        return Err(Error::MISUSE);
    }
    drop(files);

    // Een volgende engine gebruikt dezelfde heap: achtergebleven callbacks,
    // verbindingen of onvolledige cleanup mogen dit niet beschadigen.
    let mut files = Files::new(net.system_client(), "/data", TestEnvironment(13))?;
    // SAFETY: De vorige engine is volledig gesloten; dezelfde begrensde eigenaar.
    unsafe {
        Task::new(8 << 20, |s| -> Result {
            let wait = Wait(s);
            let mut bridge = Bridge::new(&mut files, &wait);
            let mut engine = Engine::initialize(&mut heap, &mut bridge)?;
            let mut db = engine.open(c"replica.db")?;
            let mut statement = db.prepare(c"SELECT count(*) FROM item")?;
            if !statement.step()? || statement.column(0)? != Value::Integer(1000) {
                return Err(Error::MISUSE);
            }
            drop(statement);
            let mut statement = db.prepare(c"PRAGMA integrity_check")?;
            if !statement.step()? || statement.column(0)? != Value::Text("ok") {
                return Err(Error::MISUSE);
            }
            Ok(())
        })
    }
    .map_err(|_| Error::MEMORY)?
    .await?;
    log!("REPLICA_SQLITE_CANCEL_OK");
    Ok(restored)
}
