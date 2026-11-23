# Replica in Rust

Native SQLite en paginareplicatie voor `no_std`-apps op HopOS. SQLite wordt als
C gelinkt en gebruikt een Rust-VFS, een vaste heap en rollback-journals.
De bestaande Go-formaten blijven behouden: `SPINSEG1`, `SPINDRT1`, manifest v2,
markers en objectnamen; gecomprimeerde segmenten (`SPINSEG2`) zijn een keuze
per eigenaar (`owner::Config::compress`). Geen WASM, POSIX-bestandssysteem of verborgen executor.

De replicatiebibliotheek bevat tracking, prepare/restore, snapshot/increment,
onzekere-publicatieherstel, compactie, retentie, generatieopruiming, herstelpuntcatalogus, offline Fetch en de schrijverlease in S3.
Op HopOS heeft SQLite echte persistente bestands-I/O
via de parkeerbare C-stack (`applib::stacktask`) en de bevestigde barrière
`sys::Client::sync` (OP_SYNC). Dit is nog geen vervanging van een bestaande
Spin-instantie: de hostapp moet zijn database-eigenaar en de renew van de
schrijverlease nog op deze bibliotheek aansluiten.
Zie [contracten en resterende aansluiting](docs/port.md).

## Onderdelen

| Crate | Verantwoordelijkheid |
| --- | --- |
| `replica-sqlite` | SQLite 3.53.4, C-shim, veilige verbindingen/statements, vaste `memsys5`-heap en VFS; transparante paginacompressie (`compress`, `lz`, zie [docs/compress.md](docs/compress.md)) |
| `replica-core` | Go-formaten, tracking/dirty logs, lokale markers, capture, publicatie, prepare, restore, compactie, retentie en schrijverlease; meer databases onder één lease (`group`), banen (`lanes`), capturenummers (`publications`) en een volgende lezer (`follow`), zie [docs/group.md](docs/group.md) |
| `replica-s3` | Concrete objectadapter op Lean S3 (`leans3`), SigV4, begrensd streamen en volledige listings; één stroom- en bytepoort per app (`Gate`) |
| `replica-hopos` | Async system-client als SQLite-opslag via de private stack; `xSync` is OP_SYNC; de banen van een eigenaarslot (`lanes`) |
| `replica-sqlite-smoke` | RAM-SQLite als echte Hop-job, twee onafhankelijke slots |
| `replica-sqlite-persist` | HopFS-commit, koude herstart, echte capture/restore en annulering van langlopende SQL |
| `replica-sqlite-bench` | De meetkern: dezelfde stappen op HopOS (`tools/qemu-bench.py`, of als job op ijzer) en op de host (`cargo run -p replica-sqlite-bench`, RAM) naast `tools/bench-host.sh` (SQLite's unix-VFS) |

De Go-versie gebruikte `ncruces/go-sqlite3` en SQLite in WASM. Beide versies
gebruiken echte SQLite; de Rust-versie verandert de uitvoeringsomgeving.
SQLite draait met één eigenaar, `locking_mode=EXCLUSIVE`, `journal_mode=DELETE`
en **`synchronous=EXTRA`**. Bij DELETE-journals moet ook het verwijderen van het
journal duurzaam worden: FULL alleen was in de koude HopFS-proef onvoldoende.
In EXCLUSIVE blijft het journal tussen transacties open met een gewiste kop en
kijkt SQLite niet per transactie opnieuw naar een heet journal, pagina 1 en de
bestandsmaat; verwijderd wordt het pas bij sluiten, met de mapbarrière van
EXTRA. De VFS meldt HopFS' blok van 4 KiB als sector en `SAFE_APPEND` (de
boom en dus de maat worden pas na een flush van de data vastgelegd), wat per
commit één journalsync spaart. Tijdelijke bestanden (de sorter en temp-B-trees
boven `cache_size`) gaan via de VFS, zoals op Linux. De heap die de app leent
wordt gesplitst: drie achtste is de paginacache (`SQLITE_CONFIG_PAGECACHE`, een
pagina kost daar wat hij is; in `memsys5` kostte hij het dubbele) en elke
verbinding opent met `cache_size` op die pool, de rest is `memsys5` voor de
sorter, statements en schema, met een soft heap limit waardoor de sorter spilt
en de paginacache pagina's teruggeeft in plaats van NOMEM. `replica-hopos`
bundelt aaneengesloten writes tot één call en leest bij een sequentiële lees
vooruit (64 KiB tot 512 KiB) in zestien vensters, één per stroom, zodat ook de
merge van de sorter (tot zestien PMA's door elkaar uit één tempbestand) per
stroom aaneengesloten leest; elke call is een ronde door de OS-core. WAL is niet ingebouwd: de replicatie leest het
databasebestand als de enige waarheid (capture, dirty tracking, restore), en
een WAL zou die pagina's pas bij een checkpoint laten zien.

## Versies en pins

Replica zelf: **v3.2.5** (de nummering van HopOS; een patch van Replica
alleen loopt voor op de HopOS-pin, v3.2.5 pint HopOS v3.2.4). Afnemers pinnen
een tag, geen rev op een branch.

| Afhankelijkheid | Crates | Pin |
| --- | --- | --- |
| HopOS (`hop-os/HopOS`) | `applib`, `sync` | `v3.2.4` |
| Lean (`haas-software/lean`) | `leans3`, `leans3http`, `leanhttp`, `leanhttps`, `leantls`, `leancrypto` (en `leannet` via applib) | `v3.2.3` |
| Hop (`hop-os/hop`) | `types`, `discovery` | `v3.2.1` |
| SQLite | gevendord in `vendor/sqlite` | 3.53.4 |
| Rust | `rust-toolchain.toml` | 1.93.0 |

Lean heeft de tag die HopOS zelf pint (HopOS v3.2.4 pint Lean v3.2.3):
applib brengt `leannet` mee en `TcpConn` moet dezelfde `Dial` kennen als
leanhttp. `tools/gate.sh` legt deze tabel
tegen de `Cargo.toml`'s.

**Het contract met Hop.** De schrijverlease is het leaseobject van
`hop-discovery` (formaat en CAS-regels van Hop `v3.2.1`, met `hop-types` voor
node en generatie): één JSON-object per namespace in de bucket, aangemaakt met
`If-None-Match: *` en vernieuwd met `If-Match: <etag>`. Een Hop-cluster en
Replica lezen elkaars lease dus zonder vertaling; een nieuwe Hop-tag met een
ander leaseformaat is een nieuwe Replica-versie. Het call-budget staat onder
"Inbouwen": één store-call < TTL/3 min het klokverschil.

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
REPLICA_BENCH_MIB=64 python3 tools/qemu-bench.py
```

De gewone poort controleert de meegeleverde SQLite-bronhashes, formatting,
strikte clippy, hosttests en volledige ARM64- en RISC-V-builds. Met `QEMU=1`
start hij de RAM-probe tweemaal onder de **gepinde HopOS** (zie de pins hierboven). Beide apps
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

Er is precies één schrijver per namespace. Die houdt de hop-lease
(`discovery`, hetzelfde protocol en formaat als het Hop-cluster) als object in
de bucket, bijvoorbeeld `prefix/domain/lease`. Elke claim en renew is een
voorwaardelijke PUT (`If-None-Match: *`, `If-Match: <etag>`). De backend moet
die voorwaarden honoreren; de S3-lease kent de herkansingen van Hop voor
geciteerde ETags (Hetzner, Ceph) en voor een Bunny-ghost.

1. `writer::claim(b, &mut s3.lease(key, ttl_ms), node, ttl_ms, now)` geeft
   `Role::Writer` of `Role::Reader`. Een lezer opent SQLite met
   `Engine::open_read_only` op `writer::ReadOnly`; iedere write is dan
   `SQLITE_READONLY`. Alleen een `Writer` kan een `Replica` maken.
   Roep `Replica::renew` vaak aan (hij schrijft hoogstens eens per zesde TTL),
   ook terwijl opslag of netwerk wacht. Een derde TTL vóór het verlopen
   weigeren VFS, sync en manifestpublicatie hard; verlies sluit de eigenaar.
   Een andere node neemt pas na het verlopen over, met een hogere generatie.
   **Schrijfdeadline en call-budget.** Een schrijver schrijft tot tweederde
   van de TTL na zijn laatste bevestigde renew; een ander neemt pas na de hele
   TTL over. De check staat vlak vóór elke PUT en DELETE, dus de PUT zelf moet
   in het resterende derde passen: **het budget van één store-call (connect,
   TLS, upload, antwoord) is kleiner dan TTL/3 min het klokverschil tussen de
   nodes**. Bij een TTL van 15 s en 1 s klokverschil is dat minder dan 4 s per
   call; een segment van 4 MiB moet daar ook in passen. Een call die toch
   langer duurt (een proxy die het verzoek vasthoudt) kan landen na een
   overname; daarvoor is het fencing hieronder.

   **Fencing in de store.** Elk manifest draagt het fencing-token van zijn
   schrijver (`lease_generation`, de generatie van de lease; Go en oudere
   versies schrijven het niet en lezen eroverheen). De raw-sleutel bevat
   `at`, dus een late PUT van de oude schrijver is een tweede manifest met
   dezelfde sequence: `object::layout` houdt dan het hoogste token en laat het
   andere als afval staan, in plaats van `Corrupt`. Vaste sleutels (snapshot,
   vensters) gaan met `Store::put_new` (`If-None-Match: *`); bestaat de sleutel
   al met een hoger token, dan is dat `LeaseLost`. Het onderhoud kijkt vóór
   elke PUT en DELETE naar de deadline en leest vóór elke reeks DELETEs de
   lease in de store (`Maintenance::run(b, store, |s| s.lease(key, ttl).read())`);
   is die niet meer van deze schrijver, dan stopt de ronde. Zette een oude
   schrijver current nog op zijn nieuwe generatie, dan zet het onderhoud van
   de houder hem terug (lager token in die snapshot). Het inline onderhoud
   (zonder `background_maintenance`) heeft geen lease-lezer en kijkt alleen
   naar de deadline.

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
   C-handvatten op vóór de stack vrijgegeven wordt. `Replica::release` geeft
   daarna de lease vrij, zodat een lezer meteen kan overnemen.

`apps/sqlite-persist/src/pipeline.rs` toont de complete opslagketen op HopOS;
`core/tests/replication.rs` doet hetzelfde met echte native SQLite en
foutinjectie. De app moet een betrouwbare UTC-klok en entropiebron leveren.
De deterministische testomgeving is geen productie-entropyprovider.

### Veel databases, één engine (v3.0.1)

Een app met een database per tenant initialiseert één engine en opent per
database een verbinding met `Engine::open_with(path, &mut backend)`: de engine
gedeeld geleend, de opslag van die verbinding exclusief, elk met een eigen
VFS `replica-<n>` in de C-shim. Elke verbinding mag op een eigen
`applib::stacktask` parkeren terwijl de andere SQL doen; het
veiligheidsargument staat in de moduledocs van `replica-sqlite`, de toets in
`hopos/tests/interleave.rs` (twee stacks die bij elke callback wisselen).

De eigenaar is los van de VFS: `Replica::tracker()` geeft de SQL-kant
(`Tracker::vfs(&mut storage)`) en de `Replica` blijft bij de Sync-taak. Zolang
de verbinding openstaat, lopen `begin`/`finish`/`tick`/`maintenance_*` alleen
terwijl de SQL-kant in rust is (geen lopend statement, geen open transactie);
de upload, `Capturing::run`, `Maintenance::run` en `renew` mogen altijd. Wie
het protocol schendt, krijgt `Error::Busy` (eigenaar) of `SQLITE_BUSY` (VFS),
nooit een halve wijziging. Zie de moduledocs van `owner` en
`core/tests/owner_split.rs`. `Replica::has_changes` zegt zonder I/O of er iets
te publiceren is, zodat een host de rust alleen vraagt als het nodig is;
`writer::renew_shared` vernieuwt de gedeelde lease in stappen, bijvoorbeeld
vóór elke PUT van een lange upload.

### Meer databases per domein

`group::Group` repliceert de databases van één domein elk als eigen
namespace onder één gedeelde schrijverlease: lui voorbereid per database,
de lease bijgehouden (ook voorbij de schrijfdeadline heroverd zolang niemand
hem nam), publiceren in rust met backoff in plaats van fencen bij een store
die even niet antwoordt, en met banen (`lanes::Offload`) het store-werk naast
de eigenaar. De eigenaar nummert zijn captures (`Replica::begun`,
`Synced::capture`), zodat een host weet welke writes in de store staan.
`follow::Follow` is een lezer zonder lease. Zie [docs/group.md](docs/group.md).

S3 vanuit een app-slot: `replica_hopos::s3::store(&endpoint, Wait(s))` is een
`replica_s3::S3` over `leans3http::Http` over Lean's `WebDial` over
`applib::tcp::Dialer`, met `MOZILLA_ROOTS`, de wandklok van de app en een
`applib::rand::Rng` per verbinding; `store.lease(key, ttl)` is de hop-lease
erover. De store van een eigenaar met een groep wacht met
`replica_hopos::keep::Keeping::new(Wait(s), &keep)`: terwijl één call wacht,
houdt hij de lease van de open groep bij (`Group::with_keep_lease(&keep)`,
zie [docs/group.md](docs/group.md)). Op de host (feature `std`): `replica_sqlite::host::Dir` (bestanden,
`fsync`, mapbarrière) en `replica_core::host::{Dir, DirLease}` (objectstore en
lease in een map).

## Grenzen

Alle allocaties voor buffers/metadata zijn faalbaar en expliciet begrensd.
Snapshotpaginanummers worden per segment opgesomd; ze vragen geen vector voor
de hele database. De lokale spool moet wel de gekopieerde data kunnen bevatten.
Een restore heeft daarnaast ruimte voor een complete scratchdatabase nodig.

De huidige capture houdt de bron tijdens de volledige lokale kopie exclusief.
Restore en compactie halen segmenten in rondes tegelijk op, binnen
`Store::parallel` (aantal en bytes). `S3::new` krijgt een functie die één
verbinding opent (bijvoorbeeld `leans3http::Http` over Lean's `WebDial`) en
de `Gate` van de app (`replica_hopos::s3::GATE`): één poort voor de stromen en
bytes van de rondes van alle stores samen. De store opent er zelf tot 16, pas
als een ronde ze nodig heeft en zoveel als de poort vrij heeft; tegen Bunny's
ongeveer 10 MB/s per stroom haalt dat 40 tot 69 MB/s. `S3::streams` stelt een
andere grens per store in. De extra verbindingen vallen bij de eerste call die
geen ronde is (PUT, GET, LIST, DELETE) of bij `S3::rest`, niet op een klok; de
lease gaat buiten de poort om. Toepassen blijft in planvolgorde. Uploads zijn opeenvolgend; de
Go-optimalisaties met korte leestransacties en twee gelijktijdige uploads zijn
nog niet aangesloten. Writes na capture bewaren conservatief de hele dirty superset.
Daarom is hiermee geen gelijkwaardige grote-databaseprestatie gemeten.

De S3-adapter is getoetst met onder meer een 5 MiB-response, afgebroken bodies,
403/404, onvolledige listings en annulering. Een echte externe S3-endpointrun,
fysieke stroomuitval, O6N-NVMe en RISC-V-runtime zijn niet getoetst.
De backend moet sterke GET/LIST-consistentie en atomaire PUT leveren.

Eigen code: MIT. SQLite: public domain; bronherkomst en hashes staan in
`vendor/sqlite/SOURCE.json`.
