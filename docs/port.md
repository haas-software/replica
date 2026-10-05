# Replica: portcontract en aansluiting

Bron: `spin/replica` op `f6dd0322f9b1baa63262bf5015e0889cbad1fca0`, inclusief
PRODUCTION_REVIEW.md, SCALE_REVIEW.md en foutinjectietests. De oudere
`haasdb/replica` is geen semantische bron. Stijl: `rustdoc/README.md` en
`PORT.md`, één eigenaar, `no_std + alloc`, faalbare allocaties en Git-tags over
repo-grenzen. De kern van Replica verbiedt unsafe.

## SQLite op HopOS: één async opslagpad

Er ontbraken twee HopOS-functies, geen SQLite-formaat. Beide zitten sinds SDK
`v3.0.0-alpha.16` in de gewone Git-tag; de apps pinnen nu HopOS `v3.0.10`.

Replica gebruikt Hop `v3.0.9` voor het expliciete JSON-bytebudget en Lean
`v3.1.11` voor delimiterlistings van generatieprefixen. Deze functies staan in
de gepubliceerde libraries; een afnemer hoeft alleen Replica te includen.

- De C-stack moet kunnen wachten op async system-I/O terwijl de app-executor
  het netwerk blijft pompen. `applib::stacktask::Task` bewaart die stack;
  `Suspender::wait` parkeert en hervat hem. Geen geneste executor en geen
  opnieuw uitgevoerde SQL als vervanging voor voortzetten.
- `xSync` en journalverwijdering vragen een bevestigde HopFS-barrière.
  `OP_SYNC` gaat naar de filesystemactor, commit en device-flush.
  `Client::sync` herhaalt een call met een onzekere uitkomst niet.
  SQLite `synchronous=EXTRA` laat ook de journalunlink volgen door een sync
  van de oudermap.

Er is één pad: iedere VFS-callback (open, read, write, truncate, sync, size,
delete, exists) wacht via `replica_sqlite::asynchronous::Bridge` en
`replica_hopos::Wait` met `Suspender::wait` op de async system-client. Geen
blokkerende of gepolde variant ernaast. SQLite-locks vragen geen I/O: de VFS
houdt het lockniveau in C bij, want er is precies één eigenaar van de map, en
SQLite draait in `locking_mode=EXCLUSIVE`. Een tijdelijk bestand (sorter,
temp-B-tree) krijgt van de VFS een naam `etilqs_` plus zestien hexcijfers in
dezelfde map en verdwijnt bij sluiten.

`replica-hopos::Files` bezit zijn system-client, met 16 generatienummers voor
open bestanden, canonieke relatieve namen en maximaal 1 MiB per I/O-stap. Per
schrijfbaar bestand bundelt hij aaneengesloten writes tot 256 KiB in één call
(weggeschreven vóór sync, lees, truncate, stat en close: de duurzaamheid blijft
die van xSync), en bij een sequentiële lees leest hij een venster vooruit (64 KiB
verdubbelend tot 512 KiB) dat een write erin meeneemt. Er zijn zestien vensters,
één per stroom: een lees hoort bij het venster waar hij op aansluit (vooruit of
dalend), anders bij het langst ongebruikte; zo leest de merge van SQLite's
sorter (tot zestien PMA's door elkaar, elke lezer 4 KiB per keer) per PMA
aaneengesloten. `Stats::snapshot` telt callbacks en calls; `apps/sqlite-bench`
drukt het verschil per stap af, met `cpm` (rondes per MiB; 256 is één per
pagina).
Timeout, transportfout of protocolfout maakt die eigenaar onbruikbaar.
`AsyncStorage::invalidate` en de bridge doen hetzelfde bij annulering.
De C-progresscallback laat ook CPU-SQL periodiek parkeren; annuleren geeft
SQLITE_INTERRUPT, waarna engine/statements op hun eigen stack worden gesloten.

`Task::new` blijft unsafe: de aanroeper bewijst voldoende stackruimte en moet
na Cancelled terugkeren. De slot-heap heeft geen guard pages. SQLite's unieke
globale C-initialisatie is eveneens expliciet unsafe. Daarbuiten gebruikt de
app veilige handvatten; engine/verbinding/statement zijn geen Send/Sync.

`tools/qemu-persist.py` bewijst op HopOS/QEMU dat originele én herstelde
databases een harde emulatorstop overleven, met apps met de MMU aan
(`applib::mmu`). Hij test ook annuleren/heropenen. Dezelfde app linkt voor
ARM64 en RISC-V. Details/logs staan in de README. Volumeoverdracht zit in
HopOS/Hop; Lumen heeft nog oudere alpha.10-pins en moet die afzonderlijk
bijwerken.

## Geporteerde contracten

| Pad | Gedrag en bewijs |
| --- | --- |
| `segment`, `dirty`, `manifest`, `time` | Bytegelijke Go-fixtures, SHA-256, nanoseconden, lengte-/versiecontrole vóór allocatie, aaneengesloten herstelplan |
| `tracking` | Marker ongeldig vóór write; dirty-logrecords duurzaam vóór DB-sync; twee slots, header als laatste; foutinjectie op iedere rewritefase |
| `marker`, `replication` | Previous/repair_from/uncertain, onderdelen vóór manifest, exacte readback na onbekende PUT, geen sequencehergebruik vóór resolve |
| `capture` | Consistente lokale spool, per-segment snapshotpaginabereik, gebundelde reads; detectie van externe wijzigingen en ongedekte groei |
| `prepare` | Lineage vóór open, bootstrappen alleen bij bewijs, volledige snapshot als de remote sequence voorloopt, beschadiging onderscheiden van transportproblemen |
| `restore` | Scratch + verplichte verificatie, duurzame intent, oud journal weg, kopie/barrière, intent weg; koude foutinjectie vóór/na publicatiefasen |
| `compact` | Vensters `(start,end]`, contiguous sequences, Go-gelijke shrink/grow-uitvoer en veilige verwijdervolgorde |
| `maintenance` | Go-bewaarschema en jaar-0001-tijdanker, multilevel samenvoegen; alle twaalf afgebroken verwijderstappen blijven herstelbaar |
| `owner` | Prepare/VFS/Sync/Tick/Close samen, interval ook na fouten, herstelbeslissing duurzaam vóór nieuwe tracking, automatische reparatie met lineage |
| `archive` | Begrensde catalogus met nanoseconden; offline Fetch weigert levende DB en sidecars, verificatie vóór publicatie |
| `writer` | Hop-lease met CAS: tweede node is lezer, renew op leeftijd, deadline een derde TTL vóór verlopen, verloren PUT-antwoord alleen voor de eigen eigenaar, overname met hogere generatie, VFS en publicatie hard dicht na de deadline, read-only opslag |
| `replica-s3` | Lean SigV4, begrensde streaming-GET en complete listings; annulering vergiftigt het transport |

Bij generatieopruiming gaat eerst het snapshotmanifest weg, daarna de overige
metadata, daarna data. Een fout stopt de ronde. De door `/current` aangewezen
generatie wordt nooit opgeruimd. Oude onvolledige pogingen verdwijnen na een dag;
complete generaties volgen de ingestelde retentie.

De bron is één eigenaar en één namespace. Geen padaliassen of externe schrijver,
geen WAL en geen onbeheerste SQL-wijzigingen aan de journal-/syncinstellingen.
De lokale filesystembarrières en remote atomic-PUT/strong-GET/LIST zijn voorwaarden,
geen gedrag dat Replica bovenop een zwakkere backend kan verzinnen.

## Resterende integratie vóór bestaande gebruikers overgaan

Dit is een bibliotheek, zoals Go. `owner::Replica` verbindt de levensloop:
Prepare vóór open, een geleende tracking-VFS voor SQL, Sync/Tick en permanente
Close. Eén eigenaar sluit gelijktijdige publicatie uit; Tick en onderhoud
onthouden ook mislukte pogingen, zodat er geen hot retrylus ontstaat. Bewezen
archiefschade en ongetrackte writes leiden naar een nieuwe snapshot; transport
blijft een retry. Een onzekere persistente herstelbeslissing sluit de eigenaar.
De host sluit SQLite vóór Sync, roept Tick vanuit zijn executor aan en roept
`Replica::renew` aan. Er is geen verborgen thread of automatisch gestarte
achtergrondtaak. Na de leasedeadline weigert de VFS iedere write en gaat er
geen manifest meer uit, ook niet aan het eind van een lange upload. Wie
uploads langer dan een paar seconden heeft, gebruikt `begin`/`finish` en
vernieuwt de lease daartussen.

`archive::points` somt behouden herstelpunten op met nanoseconden, current en
niveau, nieuwste eerst. Het resultaat is begrensd op maximaal 4.096 punten;
een overschrijding geeft een fout in plaats van een onvolledige lijst.
Beschadigde oude generaties blijven buiten de lijst, terwijl corrupt current
en transportfouten zichtbaar blijven. `archive::fetch` weigert de levende
DB, padaliassen en zijn sidecars; het publiceert pas na de integriteitscallback.
De host waarborgt ook hier exclusiviteit tegenover onderhoud.

Performanceverschillen zijn expliciet: één exclusieve lokale kopie in plaats
van meerdere korte leestransacties; sequentiële uploads (downloads in begrensde rondes); conservatieve
retransmissie van de dirty superset bij writes na capture; een volledige snapshot
als een manifest is gecommit maar de lokale marker na een crash achterloopt.
De snapshotlijst en
herstelplanner zijn wel begrensd en vermijden de eerdere volledige
paginanummervector respectievelijk een volledige metadatascan per commit.
Meet doorvoer en SQL-latency op een representatieve database voordat de Go-host
wordt vervangen. Er is nog geen live S3- of O6N-run uitgevoerd.

De objectstore op HopOS is nog niet aangesloten. `replica-s3` parkeert al op
dezelfde `Suspend`, maar mist een `leans3::Transport` voor een app-slot (Hop's
`SlotTransport` zit in zijn binary) en sleutels in de app. De andere weg, de
store-ops van Hop (`store_push/pull/list/drop`), vraagt een `Store`-adapter in
`replica-hopos`: een objectnaam los van het lokale spoolpad (de ABI kan het via
`data`; de `store_*`-helpers van `applib` niet, `Client::call` met een eigen
`Req` wel). In HopOS/Hop ontbreken nog gepagineerde listings boven 8 KiB
en een limiet vóór download. Hop is dan de eigenaar van S3-sleutels en
TLS.

Lumen gebruikt zelf geen SQLite. Zijn disc-/backup-/SMP-/PWA-aansluiting is
inmiddels in code geschreven; de afzonderlijke compileer- en uitvoerstatus staat in `../hop-app-lumen/docs/RUST-PORT.md`; een geslaagde Replica-proef maakt
die mediaketen niet automatisch af.
