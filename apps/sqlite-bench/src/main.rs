//! De meetkern van SQLite op HopOS: dezelfde stappen als op de host, met per
//! stap de tijd, het aantal, de bytes over de system-client en het aantal
//! VFS-callbacks tegenover OS-core-rondes. Eén marker per regel.
#![cfg_attr(target_os = "none", no_std, no_main)]
#[cfg(target_os = "none")]
extern crate alloc;
mod steps;
#[cfg(target_os = "none")]
use alloc::vec::Vec;
#[cfg(target_os = "none")]
use applib::{App, EXEC, appnet, log, stacktask::Task};
#[cfg(target_os = "none")]
use core::time::Duration;
#[cfg(target_os = "none")]
use replica_hopos::{Environment, Files, Stats, Wait};
#[cfg(target_os = "none")]
use replica_sqlite::{Engine, Error, Result, asynchronous::Bridge};

/// De heap van SQLite: 64 MiB; de bridge neemt er drie achtste van als paginacache.
const HEAP_WORDS: usize = 8 << 20;
/// De datamaat als de job niets zegt.
const DEFAULT_MIB: u64 = 64;

#[cfg(target_os = "none")]
applib::main!(bench);

#[cfg(target_os = "none")]
struct TestEnvironment(u64);
#[cfg(target_os = "none")]
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

/// De verslaggever op HopOS: per stap de tijd en het verschil in de meetlat.
#[cfg(target_os = "none")]
struct Report(Stats);
#[cfg(target_os = "none")]
impl steps::Bench for Report {
    fn now_us(&mut self) -> u64 {
        applib::clock::now_ns() / 1000
    }
    fn report(&mut self, name: &str, us: u64, n: u64) {
        let now = Stats::snapshot();
        let d = now.since(&self.0);
        self.0 = now;
        let secs = us.max(1) as f64 / 1e6;
        let mib = (d.call_read_bytes + d.call_write_bytes) as f64 / 1048576.0;
        // cpm: rondes door de OS-core per MiB verkeer; 256 is één per pagina.
        log!(
            "REPLICA_BENCH_{name} us={us} n={n} per_s={:.0} mib={mib:.1} mibps={:.1} cpm={:.0} vfs_r={} vfs_w={} vfs_s={} calls={} c_r={} c_w={} c_s={} c_o={} r_mib={:.1} w_mib={:.1}",
            n as f64 / secs,
            mib / secs,
            d.calls() as f64 / mib.max(0.001),
            d.reads,
            d.writes,
            d.syncs,
            d.calls(),
            d.call_reads,
            d.call_writes,
            d.call_syncs,
            d.call_other,
            d.call_read_bytes as f64 / 1048576.0,
            d.call_write_bytes as f64 / 1048576.0,
        );
    }
}

#[cfg(target_os = "none")]
async fn bench(app: &'static App) {
    let mib = app
        .env("REPLICA_BENCH_MIB")
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MIB);
    log!(
        "REPLICA_BENCH_START mib={mib} heap_mib={}",
        (HEAP_WORDS * 8) >> 20
    );
    let start = applib::clock::now_ns();
    let result = run(app, mib).await;
    let us = applib::clock::now_ns().saturating_sub(start) / 1000;
    match result {
        Ok((rows, bytes)) => log!(
            "REPLICA_BENCH_DONE us={us} rows={rows} db_mib={:.1}",
            bytes as f64 / 1048576.0
        ),
        Err(e) => log!("REPLICA_BENCH_FAIL {e} us={us}"),
    }
    loop {
        EXEC.get().after(Duration::from_secs(60)).await;
    }
}

#[cfg(target_os = "none")]
async fn run(app: &'static App, mib: u64) -> Result<(u64, u64)> {
    let net = appnet::up(app).map_err(|_| Error::IO)?;
    let mut sys = net.system_client();
    // Elke run begint leeg: de vorige database en zijn journal weg.
    for p in ["/data/bench.db", "/data/bench.db-journal"] {
        match sys.remove(p).await {
            Ok(()) | Err(applib::sys::Error::NotFound { .. }) => {}
            Err(_) => return Err(Error::IO),
        }
    }
    let mut heap = Vec::new();
    heap.try_reserve_exact(HEAP_WORDS)
        .map_err(|_| Error::MEMORY)?;
    heap.resize(HEAP_WORDS, 0u64);
    let mut files = Files::new(sys, "/data", TestEnvironment(7))?;
    let mut report = Report(Stats::snapshot());
    // SAFETY: Eén SQLite-eigenaar op een private stack van 8 MiB; de callbacks
    // geven bij annulering IOERR, waarna engine en verbinding hier sluiten.
    let rows = unsafe {
        Task::new(8 << 20, |s| {
            let wait = Wait(s);
            let mut bridge = Bridge::new(&mut files, &wait);
            // SAFETY: Alleen deze taak gebruikt SQLite, totdat engine hier sluit.
            let mut engine = Engine::initialize(&mut heap, &mut bridge)?;
            steps::run(&mut engine, c"bench.db", mib, &mut report)
        })
    }
    .map_err(|_| Error::MEMORY)?
    .await?;
    drop(files);
    let bytes = net
        .system_client()
        .stat("/data/bench.db")
        .await
        .map_err(|_| Error::IO)?;
    Ok((rows, bytes))
}

/// Op de host: dezelfde stappen op de RAM-opslag van replica-sqlite, de
/// kale CPU-kosten van SQLite zonder enige I/O. `REPLICA_BENCH_MIB` kiest
/// de maat, zoals de job op HopOS.
#[cfg(not(target_os = "none"))]
fn main() {
    use replica_sqlite::memory::{Memory, Slot};
    use steps::Bench;
    struct Report(std::time::Instant);
    impl steps::Bench for Report {
        fn now_us(&mut self) -> u64 {
            self.0.elapsed().as_micros() as u64
        }
        fn report(&mut self, name: &str, us: u64, n: u64) {
            println!(
                "REPLICA_BENCH_{name} us={us} n={n} per_s={:.0}",
                n as f64 / (us.max(1) as f64 / 1e6)
            );
        }
    }
    let mib: u64 = std::env::var("REPLICA_BENCH_MIB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MIB);
    let mut heap = vec![0u64; HEAP_WORDS];
    // Ruim: de tabel met indexen is ongeveer 1,5 MiB per gevraagde MiB.
    let mut data = vec![0u8; (mib as usize * 3) << 20];
    let mut journal = vec![0u8; (mib as usize * 2) << 20];
    // De sorter en een temp-B-tree spillen boven cache_size naar een tijdelijk bestand.
    let mut temp = [
        vec![0u8; (mib as usize * 2) << 20],
        vec![0u8; (mib as usize * 2) << 20],
    ];
    let [t0, t1] = &mut temp;
    let mut memory = Memory::new(
        [
            Slot::new(&mut data),
            Slot::new(&mut journal),
            Slot::new(t0),
            Slot::new(t1),
        ],
        1_790_683_200_000,
        7,
    );
    let mut report = Report(std::time::Instant::now());
    // SAFETY: Eén eigenaar op de hoofdthread; de engine sluit vóór de buffers.
    let result = unsafe { replica_sqlite::Engine::initialize(&mut heap, &mut memory) }
        .and_then(|mut engine| steps::run(&mut engine, c"bench.db", mib, &mut report));
    match result {
        Ok(rows) => {
            let st = memory.stats();
            println!(
                "REPLICA_BENCH_DONE us={} rows={rows} db_mib={:.1} vfs_r={} vfs_w={} vfs_s={}",
                report.now_us(),
                memory.contents(c"bench.db").map_or(0, <[u8]>::len) as f64 / 1048576.0,
                st.reads,
                st.writes,
                st.syncs
            )
        }
        Err(e) => {
            println!("REPLICA_BENCH_FAIL {e}");
            std::process::exit(1);
        }
    }
}
