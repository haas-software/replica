# Replica

Native SQLite with page-level replication to S3, for `no_std` apps on
[HopOS](https://github.com/hop-os/HopOS). SQLite is linked as C and runs on a
Rust VFS, a fixed heap and rollback journals. No WASM, no POSIX file system,
no hidden executor or background thread.

**Licence:** Business Source License 1.1 (see [LICENSE](LICENSE)). Production
use needs a commercial licence. Each version becomes available under the GNU
Affero General Public License v3.0 or later four years after its publication.
The vendored SQLite sources are public domain.

## Status

Version **3.2.7**. The library covers the whole life cycle of a replicated
database: change tracking, prepare/restore, snapshots and increments, recovery
from uncertain publications, compaction, retention, generation cleanup, a
restore-point catalogue, offline fetch, and a single-writer lease in S3 with
fencing. On top of that: several databases under one lease (`group`), store
work on separate lanes next to the owner (`lanes`, on HopOS
`replica_hopos::lanes::Lanes::spawn`), lease keeping during long store calls,
and a lease-less reader that follows the writer (`follow`).

On HopOS, SQLite has real persistent file I/O through a parkable C stack
(`applib::stacktask`) and the confirmed barrier `sys::Client::sync`
(`OP_SYNC`). Replica is used by a HopOS application in production-like tests
against external S3.

Not done yet: parallel uploads of one capture, promoting a following reader
to writer, and a RISC-V runtime test (see [TODO.md](TODO.md)).

## Crates

| Crate | Responsibility |
| --- | --- |
| `replica-sqlite` | SQLite 3.53.4, C shim, safe connections/statements, fixed `memsys5` heap and VFS; transparent page compression (`compress`, `lz`, see [docs/compress.md](docs/compress.md)) |
| `replica-core` | Storage formats, tracking/dirty logs, local markers, capture, publication, prepare, restore, compaction, retention and the writer lease; several databases under one lease (`group`), lanes (`lanes`), capture numbers (`publications`) and a following reader (`follow`), see [docs/group.md](docs/group.md) |
| `replica-s3` | Object store adapter on Lean S3 (`leans3`): SigV4, bounded streaming and complete listings; one stream and byte gate per app (`Gate`) |
| `replica-hopos` | The async HopOS system client as SQLite storage via the private stack; `xSync` is `OP_SYNC`; the lanes of an owner (`lanes`, `Lanes::spawn`); lease keeping while waiting (`keep`); S3 from an app slot (`s3`) |
| `replica-sqlite-smoke` | RAM SQLite as a real Hop job, two independent slots |
| `replica-sqlite-persist` | HopFS commit, cold restart, real capture/restore and cancellation of long-running SQL |
| `replica-sqlite-bench` | Benchmark kernel: the same steps on HopOS (`tools/qemu-bench.py`, or as a job on hardware) and on the host (`cargo run -p replica-sqlite-bench`, RAM), next to `tools/bench-host.sh` (SQLite's unix VFS); see [docs/bench.md](docs/bench.md) |

Storage formats: segments `SPINSEG1`, dirty logs `SPINDRT1`, manifest v2,
markers and object names. Compressed segments (`SPINSEG2`) are a per-owner
choice (`owner::Config::compress`); readers accept both.

## SQLite configuration

SQLite runs with one owner, `locking_mode=EXCLUSIVE`, `journal_mode=DELETE`
and **`synchronous=EXTRA`**: with DELETE journals the journal deletion must be
durable as well, and FULL alone was not enough in a cold HopFS test. In
EXCLUSIVE mode the journal stays open between transactions with a zeroed
header; it is deleted on close, with EXTRA's directory barrier. The VFS
reports HopFS' 4 KiB block as sector size and `SAFE_APPEND`, which saves one
journal sync per commit. Temporary files (sorter, temp B-trees above
`cache_size`) go through the VFS.

The heap the app lends is split: three eighths is the page cache
(`SQLITE_CONFIG_PAGECACHE`, each connection opens with `cache_size` on that
pool), the rest is `memsys5` for the sorter, statements and schema, with a
soft heap limit so the sorter spills and the cache gives pages back instead
of returning NOMEM. `replica-hopos` coalesces contiguous writes into one call
and reads ahead on sequential reads (64 KiB to 512 KiB) in sixteen windows,
one per stream, so the sorter merge reads contiguously per stream.

WAL is not supported: replication treats the database file as the single
source of truth (capture, dirty tracking, restore), and WAL would only expose
those pages at a checkpoint.

## Versions and pins

Replica follows the HopOS version numbers; a Replica-only patch may run ahead
of its HopOS pin (3.2.5 and 3.2.6 pinned HopOS v3.2.4; 3.2.7 pins HopOS
v3.2.5). Consumers pin a tag, not a branch revision.

| Dependency | Crates | Pin |
| --- | --- | --- |
| HopOS (`hop-os/HopOS`) | `applib`, `sync` | `v3.2.5` |
| Lean (`haas-software/lean`) | `leans3`, `leans3http`, `leanhttp`, `leanhttps`, `leantls`, `leancrypto` (and `leannet` via applib) | `v3.2.3` |
| Hop (`hop-os/hop`) | `types`, `discovery` | `v3.2.1` |
| SQLite | vendored in `vendor/sqlite` | 3.53.4 |
| Rust | `rust-toolchain.toml` | 1.93.0 |

Lean uses the tag HopOS itself pins (HopOS v3.2.5 pins Lean v3.2.3): applib
brings `leannet`, and `TcpConn` must know the same `Dial` as leanhttp.
`tools/gate.sh` checks this table against the `Cargo.toml` files.

**The contract with Hop.** The writer lease is the lease object of
`hop-discovery` (format and CAS rules of Hop `v3.2.1`, with `hop-types` for
node and generation): one JSON object per namespace in the bucket, created
with `If-None-Match: *` and renewed with `If-Match: <etag>`. A Hop cluster and
Replica read each other's lease without translation; a Hop tag with a
different lease format means a new Replica version.

## Build and test

Rust 1.93.0 is fixed in `rust-toolchain.toml` (with the
`aarch64-unknown-none-softfloat` and `riscv64gc-unknown-none-elf` targets).
You also need clang, llvm-ar and Python 3. The bare-metal ARM target needs
`-mgeneral-regs-only -mabi=aapcs-soft`, which Apple's clang does not support;
tested with Homebrew LLVM on macOS ARM64. The scripts find Homebrew LLVM in
`/opt/homebrew/opt/llvm`; elsewhere set `SQLITE_CC` and `SQLITE_AR`.

```sh
cargo test --workspace                          # host tests only
tools/gate.sh                                   # the full gate
QEMU=1 tools/gate.sh                            # plus the RAM probe under QEMU
python3 tools/qemu-persist.py                   # HopFS persistence under QEMU
REPLICA_BENCH_MIB=64 python3 tools/qemu-bench.py
REPLICA_BENCH_MIB=64 cargo run --release -p replica-sqlite-bench   # host, RAM
tools/bench-host.sh 64                          # host, SQLite's unix VFS
```

`tools/gate.sh` checks the vendored SQLite source hashes and the pins table
above, then runs `cargo fmt --check`, strict clippy, the host tests, and
release builds plus clippy of all crates for ARM64 and RISC-V. With `QEMU=1`
it also runs `tools/qemu-smoke.py`: the RAM probe in two slots under the
pinned HopOS. The apps build for RISC-V; they have only been run under
ARM64 QEMU.

The QEMU scripts build against the pinned HopOS tag and take the QEMU runner
and the hard-stop test (`tools/qemu-test-sync.py`) from that same SDK
checkout. They need Hop as a checkout (`HOP_DIR`, default `../hop/hop`) and
expect the default `target/` directory. Evidence of `tools/qemu-persist.py`
lands in `out/persist/`:

- `boot-0.log` and `boot-1.log`: commit/rollback, 1,000 rows and
  `integrity_check`; QEMU is killed hard between the two boots.
- Both boots: `REPLICA_PIPELINE_OK` and `REPLICA_OWNER_OK` (the complete
  `owner::Replica` chain on HopFS: prepare, dirty logs, sync, maintenance,
  restore-point catalogue and verified fetch into a second database) and
  `REPLICA_SQLITE_CANCEL_OK` (cancelling a long SQL query via the progress
  callback, then reopening on the same heap).
- Second boot: `REPLICA_PIPELINE_REPAIR_OK` (the deliberately volatile test
  bucket is empty; the owner rebuilds it from the local database) and
  `REPLICA_RESTORED_COLD_OK` (the restored database survives the restart).
- `checks.json` records these checks. The bucket of this test is in RAM; it
  does not prove external S3 persistence.

## Integration

The caller owns storage, object store, clock/entropy and one SQLite runtime.
The app must provide a reliable UTC clock and entropy source; the
deterministic test environment is not a production entropy provider.
`owner::Replica` ties together prepare, VFS, sync, repair, cadence and
maintenance. The host calls `tick` from its own executor; `sync` forces one
round. A failure also gets an interval before the next timed attempt. Proven
archive damage schedules a new snapshot that keeps the repair lineage;
transport errors keep the existing generation. `close` refuses new VFS loans
and syncs. `core/tests/owner.rs` tests this with real SQLite.

There is exactly one writer per namespace. It holds the Hop lease
(`discovery`) as an object in the bucket, for example `prefix/domain/lease`.
Every claim and renewal is a conditional PUT (`If-None-Match: *`,
`If-Match: <etag>`); the backend must honour those conditions. The S3 lease
handles quoted ETags and ghost objects of some providers the way Hop does.

1. **Claim.** `writer::claim(b, &mut s3.lease(key, ttl_ms), node, ttl_ms, now)`
   returns `Role::Writer` or `Role::Reader`; an unreachable store is an error,
   not a role. A reader opens SQLite with `Engine::open_read_only` on
   `writer::ReadOnly`; every write is then `SQLITE_READONLY`. Only a `Writer`
   can create a `Replica`. Call `Replica::renew` often (it writes at most once
   per sixth of the TTL), also while storage or network is waiting. From one
   third of the TTL before expiry, VFS, sync and manifest publication refuse
   hard; losing the lease closes the owner. Another node takes over only after
   expiry, with a higher generation.

   **Write deadline and call budget.** A writer writes until two thirds of the
   TTL after its last confirmed renewal; another node takes over only after
   the full TTL. The check sits right before every PUT and DELETE, so the PUT
   itself must fit in the remaining third: **the budget of one store call
   (connect, TLS, upload, response) is less than TTL/3 minus the clock skew
   between nodes**. With a 15 s TTL and 1 s skew that is under 4 s per call,
   including a 4 MiB segment. A call that takes longer anyway (a proxy holding
   the request) can land after a takeover; that is what fencing is for.

   **Fencing in the store.** Every manifest carries its writer's fencing token
   (`lease_generation`; older manifests without it are still read). The raw
   key contains `at`, so a late PUT from an old writer is a second manifest
   with the same sequence: `object::layout` keeps the highest token and leaves
   the other as garbage instead of reporting `Corrupt`. Fixed keys (snapshot,
   windows) use `Store::put_new` (`If-None-Match: *`); if the key already
   exists with a higher token, that is `LeaseLost`. Maintenance checks the
   deadline before every PUT and DELETE and reads the lease in the store
   before every series of DELETEs
   (`Maintenance::run(b, store, |s| s.lease(key, ttl).read())`); if it no
   longer belongs to this writer, the round stops. If an old writer still
   moved `current` to its new generation, the holder's maintenance moves it
   back. Inline maintenance (without `background_maintenance`) has no lease
   reader and only checks the deadline.

2. **Prepare.** Run `prepare::run` **before** SQLite opens. The callback
   verifies a scratch restore with real SQLite `PRAGMA integrity_check`.

   **Someone else's work (`Error::ForeignAhead`, since 3.2.5).** A database
   runs on node A, then on B, then on A again with A's old (or restored)
   volume: the store is ahead of A's marker in the same generation. A store
   that is ahead is only ours if it is exactly the one manifest the marker
   recorded as uncertain, carrying our token (the marker keeps the
   `lease_generation` of its last confirmed manifest and the
   `uncertain_lease_generation` of the manifest in flight). Otherwise:
   - local copy clean: restore from the store (`Reason::Restored`);
   - local unpublished work: prepare fails with `Error::ForeignAhead` and
     touches nothing, locally or in the store.

   Also `ForeignAhead`: our confirmed manifest lost to fencing (another
   manifest with the same sequence and a higher token), or an interrupted
   generation switch would fall back to a generation in which someone else
   has moved on.

   **What the host does with `ForeignAhead`:** keep that database closed, log
   one line naming the database and `ForeignAhead`, do not retry in a loop
   and do not discard anything. An operator chooses: set the local files
   aside and open from the store, or save the local work first. `Error` is
   not `non_exhaustive`.

3. **SQL.** Use only `prepared.tracking.wrap(&mut storage, &mut marker)` for
   SQL, or `Replica::vfs` with the combined owner. Initialise the engine with
   a borrowed, aligned `u64` heap:

   ```rust,ignore
   let mut vfs = prepared.tracking.wrap(&mut storage, &mut prepared.marker);
   // SAFETY: the boot owner guarantees one global SQLite runtime, no re-entry.
   let mut engine = unsafe { Engine::initialize(&mut heap, &mut vfs) }?;
   let mut db = engine.open(c"replica.db")?;
   db.execute(c"CREATE TABLE IF NOT EXISTS item(id INTEGER PRIMARY KEY, title TEXT)")?;
   ```

4. **Capture.** Close SQLite or hold a real read transaction that excludes
   all writers. `Batch::capture` copies into one reusable local spool.
   Network I/O starts only at `Batch::publish`; SQL writes between those calls
   go into the next dirty set. Hold at most one batch at a time.
5. Before republishing, call `replication::resolve` when the marker has an
   uncertain sequence. Never just reuse the same sequence.
6. `maintenance::renewal_due` decides generation renewal by age;
   `replication::renew` keeps the previous complete generation during the
   copy. Schedule `maintenance::run` at most once per minute, also after an
   error. `maintenance::generations` removes expired generations in a safe
   order.
7. **Close.** Close SQL before stopping; the private task handles
   cancellation and cleans up C handles before the stack is freed.
   `Replica::release` then releases the lease so a reader can take over at
   once.

`apps/sqlite-persist/src/pipeline.rs` shows the complete chain on HopOS;
`core/tests/replication.rs` does the same with native SQLite and fault
injection.

### Many databases, one engine

An app with a database per tenant initialises one engine and opens a
connection per database with `Engine::open_with(path, &mut backend)`
(`open_read_only_with` for a reader): the engine shared, the storage of that
connection exclusive, each with its own VFS `replica-<n>` in the C shim. Each
connection may park on its own `applib::stacktask` while others run SQL; the
safety argument is in the module docs of `replica-sqlite`, the test in
`hopos/tests/interleave.rs`.

The owner is separate from the VFS: `Replica::tracker()` gives the SQL side
(`Tracker::vfs(&mut storage)`) and the `Replica` stays with the sync task.
While the connection is open, `begin`/`finish`/`tick`/`maintenance_*` only
run while the SQL side is at rest (no running statement, no open
transaction); the upload, `Capturing::run`, `Maintenance::run` and `renew`
may always run. Violating the protocol gives `Error::Busy` (owner) or
`SQLITE_BUSY` (VFS), never a half change. See the module docs of `owner` and
`core/tests/owner_split.rs`. `Replica::has_changes` says without I/O whether
there is anything to publish; `writer::renew_shared` renews the shared lease
in steps, for example before every PUT of a long upload. `Vfs::into_storage`
returns the storage after the connection, so a host can prepare its files
again under a new lease (for example after the previous one expired while S3
was unreachable) and open the next connection on the same storage.

### Several databases per domain

`group::Group` replicates the databases of one domain, each as its own
namespace, under one shared writer lease: prepared lazily per database, the
lease kept up (and reclaimed past the write deadline as long as nobody took
it), publishing at rest with backoff instead of fencing when the store
briefly does not answer, and store work on lanes (`lanes::Offload`) next to
the owner. With lanes the owner only does local work at rest: the capture and
the two local halves of a publication (`Replica::finish_begin`,
`Replica::finish_end`); uploads, the manifest PUT (`Publishing::put`) and
maintenance run on the data lane, lease renewal on the lease lane.
`Group::standing` tells the host whether writes must wait (`Standing::Held`)
or the lease is lost. The owner numbers its captures (`Replica::begun`,
`Synced::capture`), so a host knows which writes are in the store.
`follow::Follow` is a reader without a lease. See
[docs/group.md](docs/group.md).

On HopOS, `replica_hopos::lanes::Lanes::spawn` starts the two lanes on their
own stacks (`Stacks`: lease lane 2 MiB, data lane sized by the host) and
`Lanes::owner` gives the `Offload` for the group.

S3 from an app slot: `replica_hopos::s3::store(&endpoint, Wait(s))` is a
`replica_s3::S3` over `leans3http::Http` over Lean's `WebDial` over
`applib::tcp::Dialer`, with `MOZILLA_ROOTS`, the app's wall clock and an
`applib::rand::Rng` per connection; `store.lease(key, ttl)` is the Hop lease
on top. The store of an owner with a group waits with
`replica_hopos::keep::Keeping::new(Wait(s), &keep)`: while one call waits, it
keeps the lease of the open group (`Group::with_keep_lease(&keep)`). On the
host (feature `std`): `replica_sqlite::host::Dir` (files, `fsync`, directory
barrier) and `replica_core::host::{Dir, DirLease}` (object store and lease in
a directory).

## Limits

All allocations for buffers and metadata are fallible and explicitly bounded.
Snapshot page numbers are enumerated per segment; they need no vector for the
whole database. The local spool must hold the captured data, and a restore
also needs room for a complete scratch database.

A capture holds the source exclusively during the full local copy. Restore
and compaction fetch segments in concurrent rounds within `Store::parallel`
(count and bytes). `S3::new` takes a function that opens one connection and
the app's `Gate` (`replica_hopos::s3::GATE`): one gate for the streams and
bytes of all stores' rounds together. A store opens up to 16 connections,
only when a round needs them and as many as the gate allows; at roughly
10 MB/s per stream that gave 40 to 69 MB/s. `S3::streams` sets another limit
per store. The extra connections are dropped at the first call that is not a
round (PUT, GET, LIST, DELETE) or at `S3::rest`, not on a timer; the lease
bypasses the gate. Uploads of one capture are sequential. Writes after a
capture conservatively keep the whole dirty superset.

The S3 adapter is tested with, among others, a 5 MiB response, aborted
bodies, 403/404, incomplete listings and cancellation. The repository's own
tests use in-memory and directory stores. Physical power loss and a RISC-V
runtime are not tested. The backend must provide strong GET/LIST consistency
and atomic PUT.

More: [docs/port.md](docs/port.md) (contracts per module and the HopOS
storage path), [docs/group.md](docs/group.md), [docs/compress.md](docs/compress.md),
[docs/bench.md](docs/bench.md). The docs are in Dutch.

## Licence

Replica's own code is licensed under the Business Source License 1.1; see
[LICENSE](LICENSE). There is no Additional Use Grant: non-production use is
free, production use needs a commercial licence from the licensor. Each
version converts to the GNU Affero General Public License v3.0 or later
(AGPL-3.0-or-later) four years after that version is published.

SQLite is public domain and not part of the licensed work; its source
provenance and hashes are in `vendor/sqlite/SOURCE.json`.
