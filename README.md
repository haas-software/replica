# Replica in Rust

Native SQLite en paginareplicatie voor `no_std`-apps op HopOS. SQLite wordt als
C gelinkt en gebruikt een Rust-VFS, een vaste heap en rollback-journals.
De bestaande Go-formaten blijven behouden: `SPINSEG1`, `SPINDRT1`, manifest v2,
markers en objectnamen. Geen WASM, POSIX-bestandssysteem of verborgen executor.

De replicatiebibliotheek bevat tracking, prepare/restore, snapshot/increment,
onzekere-publicatieherstel, compactie, retentie, generatieopruiming, herstelpuntcatalogus, offline Fetch en leases.
Op HopOS (SDK `v3.0.0-alpha.16`) heeft SQLite echte persistente bestands-I/O
via de parkeerbare C-stack (`applib::stacktask`) en de bevestigde barrière
`sys::Client::sync` (OP_SYNC). Dit is nog geen vervanging van een bestaande
Spin-instantie: de hostapp moet zijn database-eigenaar en lease-heartbeat nog
op deze bibliotheek aansluiten.
Zie [contracten en resterende aansluiting](docs/port.md).

## Onderdelen

| Crate | Verantwoordelijkheid |
| --- | --- |
| `replica-sqlite` | SQLite 3.53.4, C-shim, veilige verbindingen/statements, vaste `memsys5`-heap en VFS |
| `replica-core` | Go-formaten, tracking/dirty logs, lokale markers, capture, publicatie, prepare, restore, compactie, retentie en leaseprotocol |
| `replica-s3` | Concrete objectadapter op Lean S3 v3.1.1, SigV4, begrensd streamen en volledige listings |
| `replica-hopos` | Async system-client als SQLite-opslag via de private stack; `xSync` is OP_SYNC |
| `replica-sqlite-smoke` | RAM-SQLite als echte Hop-job, twee onafhankelijke slots |
| `replica-sqlite-persist` | HopFS-commit, koude herstart, echte capture/restore en annulering van langlopende SQL |

De Go-versie gebruikte `ncruces/go-sqlite3` en SQLite in WASM. Beide versies
gebruiken echte SQLite; de Rust-versie verandert de uitvoeringsomgeving.
SQLite draait met één eigenaar, `journal_mode=DELETE` en
**`synchronous=EXTRA`**. Bij DELETE-journals moet ook het verwijderen van het
journal duurzaam worden: FULL alleen was in de koude HopFS-proef onvoldoende.
WAL is niet ingebouwd.

## Bouwen en toetsen

Rust 1.93.0 staat vast in `rust-toolchain.toml`. Nodig: clang, llvm-ar en
Python 3. Getoetst met Homebrew LLVM 23.1.0 op macOS ARM64; voor het bare-metal
ARM-target zijn `-mgeneral-regs-only -mabi=aapcs-soft` nodig. Apple's clang
ondersteunt die combinatie hier niet. `tools/gate.sh` vindt Homebrew LLVM;
elders stel je `SQLITE_CC` en `SQLITE_AR` in.

```sh
tools/gate.sh
QEMU=1 tools/gate.sh
python3 tools/qemu-persist.py
```

De gewone poort controleert de meegeleverde SQLite-bronhashes, formatting,
strikte clippy, hosttests en volledige ARM64- en RISC-V-builds. Met `QEMU=1`
start hij de RAM-probe tweemaal onder de **gepinde SDK alpha.16**. Beide apps
bouwen ook voor RISC-V; uitvoering is alleen onder ARM64 QEMU getoetst.

De laatste opdracht is de persist-proef, zonder kopie of patch: de app bouwt
tegen de gepinde tag, en de kern en `tools/qemu-test-sync.py` (plaatsen via Hop
met een eigen volume, SIGKILL, koude herstart) komen uit dezelfde SDK-checkout.
Hop wordt als bestand gebouwd uit `../hop/hop` (`HOP_DIR`). Er worden geen
hardwarejobs vervangen.

Bewijs:

- `out/qemu-sqlite.log`: twee slots, ieder `REPLICA_SQLITE_OK`, exit 0.
- `out/persist/boot-0.log` en `boot-1.log`: commit/rollback, 1.000
  rijen en `integrity_check`; QEMU wordt tussen beide boots hard gestopt.
- Beide boots: `REPLICA_PIPELINE_OK` en `REPLICA_OWNER_OK`, de complete
  `owner::Replica`-keten op HopFS: Prepare, dirty logs, Sync, onderhoud,
  herstelpuntcatalogus en geverifieerde Fetch naar een tweede database.
- De tweede boot: `REPLICA_PIPELINE_REPAIR_OK`. De bewust vluchtige testbucket
  is leeg; de eigenaar herbouwt die uit de bestaande lokale database.
- De tweede boot: `REPLICA_RESTORED_COLD_OK`, controle van de bestaande
  herstelde database vóór die opnieuw wordt aangemaakt.
- Beide boots: `REPLICA_SQLITE_CANCEL_OK`, afbreken van een lange SQL-query
  via de C-progresscallback en vervolgens heropenen met dezelfde heap.
- `checks.json` legt deze afzonderlijke controles vast. De bucket van deze
  QEMU-proef is RAM; dit bewijst geen externe S3-persistentie of O6N-doorvoer.

## Inbouwen

De aanroeper bezit storage, objectstore, klok/entropie en één SQLite-runtime.
`owner::Replica` verbindt Prepare, VFS, Sync, reparatie, cadans en onderhoud.
De host roept `tick` aan vanuit zijn bestaande executor; `sync` forceert één
beurt. Een fout krijgt ook een interval vóór de volgende timerpoging. Bewezen
archiefschade plant een nieuwe snapshot met behoud van repair-lineage;
transportfouten behouden de bestaande generatie. `close` weigert nieuwe VFS-
leningen en synchronisaties. `core/tests/owner.rs` toetst dit met echte SQLite.

Start geen tweede writer voor dezelfde database of namespace; op HopOS is
`update_policy: recreate` nodig. Een advisory lease is geen atomair proceslock.

1. Maak `Lease`, verkrijg hem met `acquire` en plan `renew` uiterlijk iedere
   vijf seconden. `Acquisition::Wait` geeft aan wanneer opnieuw geprobeerd
   mag worden. Na onzeker leaseverlies stopt de eigenaar met schrijven.
2. Voer `prepare::run` uit **vóór** SQLite opent. De callback controleert een
   eventuele scratch-restore met echte SQLite `PRAGMA integrity_check`.
3. Gebruik uitsluitend `prepared.tracking.wrap(&mut storage, &mut marker)`
   voor SQL, of gebruik `Replica::vfs` bij de samengevoegde eigenaar.
   Initialiseer de engine met een geleende, gealigneerde `u64`-heap:

```rust,ignore
let mut vfs = prepared.tracking.wrap(&mut storage, &mut prepared.marker);
// SAFETY: de boot-eigenaar garandeert één globale SQLite-runtime, geen herintrede.
let mut engine = unsafe { Engine::initialize(&mut heap, &mut vfs) }?;
let mut db = engine.open(c"replica.db")?;
db.execute(c"CREATE TABLE IF NOT EXISTS item(id INTEGER PRIMARY KEY, title TEXT)")?;
```

4. Voor een kopie: sluit SQLite of houd een echte leestransactie die alle
   schrijvers uitsluit. `Batch::capture` kopieert naar één herbruikbare lokale
   spool. Netwerk-I/O begint pas bij `Batch::publish`; SQL-writes tussen die
   calls blijven in de volgende dirty set. Houd maximaal één batch tegelijk.
5. Roep vóór herpublicatie `replication::resolve` aan wanneer de marker een
   onzekere sequence heeft. Nooit zomaar dezelfde sequence opnieuw gebruiken.
6. `maintenance::renewal_due` bepaalt generatievernieuwing op leeftijd;
   `replication::renew` bewaart de vorige complete generatie tijdens de kopie.
   Plan `maintenance::run` hoogstens eens per minuut, ook na een fout.
   `maintenance::generations` verwijdert verlopen generaties in veilige volgorde.
7. Sluit SQL vóór stoppen; de private taak verwerkt annulering en ruimt
   C-handvatten op vóór de stack vrijgegeven wordt. Geef daarna de lease vrij.

`apps/sqlite-persist/src/pipeline.rs` toont de complete opslagketen op HopOS;
`core/tests/replication.rs` doet hetzelfde met echte native SQLite en
foutinjectie. De app moet een betrouwbare UTC-klok en entropiebron leveren.
De deterministische testomgeving is geen productie-entropyprovider.

## Grenzen

Alle allocaties voor buffers/metadata zijn faalbaar en expliciet begrensd.
Snapshotpaginanummers worden per segment opgesomd; ze vragen geen vector voor
de hele database. De lokale spool moet wel de gekopieerde data kunnen bevatten.
Een restore heeft daarnaast ruimte voor een complete scratchdatabase nodig.

De huidige capture houdt de bron tijdens de volledige lokale kopie exclusief.
Uploads en downloads zijn opeenvolgend; de Go-optimalisaties met korte
leestransacties, twee gelijktijdige uploads en download-prefetch zijn nog niet
aangesloten. Writes na capture bewaren conservatief de hele dirty superset.
Daarom is hiermee geen gelijkwaardige grote-databaseprestatie gemeten.

De S3-adapter is getoetst met onder meer een 5 MiB-response, afgebroken bodies,
403/404, onvolledige listings en annulering. Een echte externe S3-endpointrun,
fysieke stroomuitval, O6N-NVMe en RISC-V-runtime zijn niet getoetst.
De backend moet sterke GET/LIST-consistentie en atomaire PUT leveren.

Eigen code: MIT. SQLite: public domain; bronherkomst en hashes staan in
`vendor/sqlite/SOURCE.json`.
