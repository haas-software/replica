# Paginacompressie

`replica_sqlite::compress::Compressed<S>` legt transparante compressie om elke
`Storage`: de host (`host::Dir`), HopFS (`replica-hopos`, of de opslag van een
app) en RAM. SQLite, de tracking-VFS en replica-core zien gewone pagina's; op
schijf staan ze gecomprimeerd. In S3 staan ze ook gecomprimeerd (`SPINSEG2`).

## Codec (`replica_sqlite::lz`)

LZ77 in de blokvorm van LZ4 (token, letterlijke bytes, offset u16, lengtes
met 255-verlengingen), zonder LZ4's eindregels. Hashketen van 16 Ki ingangen,
diepte 16, één positie lazy. Invoer per blok hoogstens 64 KiB (een pagina).
Veilige Rust: decoderen controleert elke grens en geeft `Corrupt`, nooit een
paniek. Geen woordenboek (gemeten: voegt weinig toe).

Gemeten op 15-11-2026 (M4, release) op een steekproef van een gemigreerde productiedatabase (`main.db`
en `2024.db`, 2048 pagina's per soort, alleen totalen):

| Steekproef | Ratio | Comprimeren | Decomprimeren |
| --- | ---: | ---: | ---: |
| `main.db`, 4 KiB | 3,98 | 315 MB/s | 2,66 GB/s |
| `2024.db`, 4 KiB | 3,50 | 376 MB/s | 2,52 GB/s |
| `main.db`, 16 KiB (4 aaneengesloten) | 5,39 | 311 MB/s | 2,76 GB/s |
| `2024.db`, 16 KiB (4 aaneengesloten) | 5,98 | 365 MB/s | 2,99 GB/s |

Ter vergelijking: zstd-1 per pagina gaf 4,0-5,1 (4 KiB) en 6,2-6,5 (16 KiB).
Diepte 64 wint 0,5% ratio en kost 20% snelheid; diepte 8 omgekeerd.

## Container

Een gecomprimeerd bestand herken je aan zijn eerste acht bytes (`ReplicaZ`);
de kop beslist, per bestand. Al het andere gaat ongewijzigd door: bestaande
databases blijven gewone SQLite-bestanden. `Policy::create` zegt of nieuwe
databases containers worden; journals, tijdelijke bestanden en replica's
sidecars nooit (`compress::is_sidecar`, de restore-scratch wel).

```text
0       superblok A (128 bytes)          eigen blok van 4 KiB
4096    superblok B
8192    segment 0, 1, 2, ...             64 KiB elk (of de paginamaat als die groter is)
```

- **Pagina's.** De container heeft één paginamaat: die van SQLite's kop in de
  eerste write (bijvoorbeeld 16 KiB), anders `Policy::page_size`. Een write
  die geen hele pagina is, wordt read-modify-write. Een app met pagina's van
  16 KiB start de engine met `Engine::initialize_paged(heap, opslag, 16384)`:
  met de slots van 4 KiB van `initialize` komen grotere pagina's uit memsys5 en
  loopt de heap onder belasting vol (NOMEM, gemeten in een HopOS-app).
- **Slot.** Een gecomprimeerde pagina staat zonder kop aaneengesloten in één
  segment. Onsamendrukbaar: rauw. Een nulpagina heeft geen slot.
- **Kaart.** Per pagina een `u64`: byte-offset (48 bits) en lengtecode (0: nul,
  `0xFFFF`: rauw, anders de lengte). In het geheugen een vector van 8 bytes per
  pagina (een productie-`main.db` op 16 KiB: 2 miljoen pagina's, 16 MB).
- **Segmenten, log-gestructureerd.** Nieuwe slots komen achter elkaar in het
  open segment; een slot van de laatst gecommitte staat wordt nooit
  overschreven. Een segment zonder levende bytes is na de commit vrij en wordt
  als eerste hergebruikt (laagste eerst); een vrije staart wordt afgekapt.
- **Schoonmaker.** Bij elke commit, met een budget van tweemaal wat de
  transactie schreef (minstens 256 KiB, 2 MiB als er veel vrij is): is meer dan
  een vijfde van de geschreven databytes dood, dan gaan de levende slots van
  het segment met de minste levende bytes naar het open segment; staat meer dan
  een tiende onder het einde vrij, dan het hoogste segment. Slots worden rauw
  gekopieerd (geen hercompressie). Zo blijft het bestand begrensd op ongeveer
  levend plus een derde; na een VACUUM krimpt het over de volgende commits.

### Commit

De kaart is een **snapshot** plus een **log** in een metadatagebied (een reeks
vrije segmenten). Het superblok wijst het gebied aan, met generatie, zout,
sequence, logische maat en checksum van het snapshot.

- Elke `sync` van SQLite op een container is één commit: een logrecord
  (gewijzigde kaartingangen met een checksum van elk nieuw slot, de logische
  maat, de laagste maat in de transactie, zout, sequence, checksum) op een eigen
  sector, dan één `sync` van het bestand.
- Past het record niet meer, dan een **checkpoint**: de hele kaart als nieuw
  snapshot in vrije segmenten, `sync`, het andere superblok met een hogere
  generatie en een nieuw zout, `sync`. Het oude gebied is daarna vrij. Het log
  is half zo groot als het snapshot (60 KiB tot 64 MiB): een checkpoint kost
  zo ongeveer 32 bytes per gewijzigde kaartingang extra, plus de sectorvulling
  van kleine records.
- Zonder `sync` van SQLite (`synchronous=OFF`, offline werk) is de afronding
  van het journal de commit: `<db>-journal` verwijderd, op nul gekapt of zijn kop
  gewist. Dan zonder `sync` (zoals SQLite bij `OFF` belooft), maar zichtbaar
  voor een andere eigenaar of een ander proces, zoals bij een gewoon bestand.
  Sluiten met wijzigingen (VACUUM INTO, dat geen journal heeft) commit ook.
- Openen: het geldige superblok met de hoogste generatie, het snapshot, dan de
  records met het juiste zout en een opeenvolgende sequence. Van het **laatste**
  record worden de slots tegen hun checksums gelezen: alleen dat record kan
  duurzaam zijn terwijl een slot van dezelfde commit het niet werd (stroom weg
  tijdens zijn `sync`). Klopt er één niet, dan telt die commit niet.

**Samen met het rollback-journal.** SQLite schrijft en synct het journal vóór
de eerste databasewrite en ruimt het pas op nadat onze `sync` terugkwam.

1. Stroom weg vóór de `sync`: het record staat er niet (of half, en de checksum
   klopt niet), de nieuwe slots stonden in vrije ruimte: de container is de
   vorige commit. Het hete journal zet de pagina's ook nog eens terug.
2. Tijdens de `sync`: het record kan duurzaam zijn en een slot niet. Dat ziet de
   slotcontrole (de commit telt niet, zoals 1); SQLite's journal was nog heet.
3. Na de `sync`: de commit is duurzaam; het journal opruimen maakt hem af.

Na een commit beginnen slots en records op een nieuwe sector van 4 KiB: een
halve write kan nooit een sector raken waarin een duurzame commit iets heeft,
ook zonder "powersafe overwrite" (de VFS meldt dat niet). Een mislukte commit
maakt de container voor de schrijver onbruikbaar tot hij hem sluit (onbekend wat
duurzaam is); daarna leest de volgende toegang het bestand opnieuw.

**Meerdere eigenaren op één map** (de SQL-kant en de sync-kant van replica-core,
leessessies): wie dezelfde `Maps` krijgt (`Compressed::shared`), deelt per
bestand één kaart, 8 B per pagina één keer in plaats van per eigenaar (een
productie-`main.db`: 17,2 MB; een HopOS-app had per bestand 2 eigenaren plus één per
leessessie, meting 21 nov 2026). De ene schrijver werkt hem bij; zolang zijn
transactie loopt, zien de anderen de gecommitte staat (de oude ingang van elke
pagina die hij veranderde, 16 B per pagina tot de commit, bewaard zodra de
`Maps` meer houders heeft, zodat ook wie midden in de transactie opent de ene
kaart deelt), na zijn commit meteen de nieuwe. Een tweede schrijver krijgt
`SQLITE_BUSY`. Sluit het laatste handvat (van welke eigenaar ook), dan gaat de
kaart terug naar de heap; de volgende `open` leest
superblokken, snapshot en log. Een `open` op een kaart die een andere eigenaar
al had, kijkt superblok en log na: een schrijver buiten deze `Maps` laat hem
opnieuw laden. Een lening van het register overleeft nooit een aanroep van de
binnenste opslag (die kan parkeren); de compiler bewijst dat (`with`).

Na een commit gaat een grote gewijzigde-paginalijst terug naar de heap, en een
kaart met meer dan een achtste lege capaciteit (na een herstel of krimp) wordt
passend gemaakt; groeien doet de kaart met een achtste, niet door te
verdubbelen. Na een herstel van die `main.db` stond de kaart op 33,5 MB in
plaats van 17,2 en de lege lijst op 33,5 MB.

## Replicatie (`SPINSEG2`)

De capture leest logische pagina's (via `Compressed`) en comprimeert ze per
pagina opnieuw met dezelfde codec: `nummer u32 | lengte u32 | bytes`, lengte
gelijk aan de paginamaat is rauw, nul een nulpagina; de rest van het segment
(kop, SHA-256) is als `SPINSEG1`. Waarom niet de opgeslagen slots verschepen:

- het S3-formaat blijft los van de lokale opslag: ook een ongecomprimeerd lokaal
  bestand publiceert klein, en een container met een andere paginamaat dan
  SQLite (na een VACUUM met nieuwe paginamaat) ook;
- capture, tracking, schaduw en restore blijven over logische pagina's gaan,
  zonder een tweede leespad door de `Storage`-wrappers van een app;
- de kosten zijn klein: comprimeren gaat met 300+ MB/s, een upload met 10-60 MB/s.

`owner::Config::compress` kiest het formaat van nieuwe segmenten (standaard
uit, zodat ook een lezer zonder `SPINSEG2` ze kan lezen); lezen kan altijd
beide. Een compactie
schrijft `SPINSEG2` zodra één invoersegment het is. Een restore decodeert per
pagina; de scratch en de herstelde database worden containers als de `Policy`
van de opslag dat zegt (anders gewone bestanden: het formaat in S3 staat los
van het lokale).

## Omzetten

Een bestaand bestand wordt een container met `PRAGMA page_size = 16384;
VACUUM INTO '<nieuw>'` op een `Compressed`-opslag (de doelnaam is nieuw, dus een
container met SQLite's paginamaat), controleren, dan hernoemen. Het
omzetcommando van een HopOS-app (`migrate compress`) doet precies dat. Na omzetten krijgt een gerepliceerd
bestand een nieuw snapshot (alle pagina's zijn anders).

## Metingen (15-11-2026, M4, USB-SSD, release)

Klonen van gemigreerde productiedatabases, omgezet met `migrate compress` (16 KiB):

| Bestand | Ongecomprimeerd (4 KiB) | Container (16 KiB) | Ratio | Omzetten | In S3 (`SPINSEG2`) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Productie A `main.db` | 32,55 GB | 6,22 GB | 5,24 | 463 s | 6,04 GB |
| Productie A `2024.db` | 11,71 GB | 2,10 GB | 5,58 | 112 s | 2,05 GB |
| Productie B, 5 bestanden | 720 MB | 115 MB | 6,3 | 17 s | |

Het containerbestand is iets groter dan zijn gecomprimeerde pagina's: het
metadatagebied (8 bytes per pagina plus log), de sectoralignering per commit en
de vrije ruimte die de schoonmaker laat staan. In S3 staan 10.601 segmenten,
8,09 GB voor 44,26 GB logische pagina's; de seed van beide bestanden had een
piekgeheugen van 44 MB.

## Toetsen

| Toets | Wat |
| --- | --- |
| `sqlite/src/lz.rs` (unit) | randgevallen, te grote invoer, overlap |
| `sqlite/tests/lz.rs` | randmaten 0..64 KiB per paginamaat; 3000 gegenereerde invoeren (vaste seed); 32.000 beschadigde blokken in drie uitvoermaten en 2000 ruisblokken: fout of geldige uitvoer, nooit paniek; optioneel echte pagina's (`REPLICA_LZ_SAMPLES`) |
| `sqlite/src/compress.rs` (unit) | superblok met elke omgedraaide bit, streaming-checksum, sidecarnamen, ingangen |
| `sqlite/tests/compress.rs` `random_workload_matches_reference` | 3 paginamaten × 6 rondes × 150 transacties (insert/update/delete, ruis, rollbacks, VACUUM, krimp) tegen een ongecomprimeerde referentie: rijen, `integrity_check`, logische bytes gelijk, een verse eigenaar leest hetzelfde, het bestand begrensd |
| `free_space_reuse_keeps_the_file_bounded` | 3000 commits herschrijven 2000 documenten: fysiek ≤ 1,5 × levend + speling, geen groei na opwarmen; na DELETE en VACUUM krimpt het |
| `power_cut_at_every_write_and_sync_of_a_commit` | vijf soorten commits (eerste, log, checkpoint, schoonmaken, krimp) met een knip na elke write/sync/truncate, drie seeds voor wat landde (weg, heel, half per sector): heropenen, `integrity_check`, oud of nieuw, daarna bruikbaar |
| `plain_files_stay_plain_and_both_formats_mix`, `two_owners_see_each_others_commits`, `vacuum_into_converts_to_compressed_16k`, `damaged_containers_give_errors_not_panics` | gemengde formaten (ATTACH), `Policy::plain`, twee eigenaren (ook met `synchronous=OFF` en `journal_mode=TRUNCATE`: de ander ziet elke afgeronde transactie), omzetten, 300 beschadigde containers |
| `owners_on_one_maps_share_one_map_and_give_it_back`, `readers_see_the_committed_state_while_one_writes`, `a_writer_gone_mid_transaction_leaves_readers_the_file`, `a_restore_leaves_a_tight_map` | vier eigenaren op één `Maps`: één kaart, commits meteen zichtbaar (ook via SQLite), heap leeg na het laatste handvat; tijdens een transactie (overschrijven, groeien, deelwrite, krimp) ziet een lezer de gecommitte staat, een tweede schrijver `SQLITE_BUSY`, een late opener deelt de kaart; een schrijver weg zonder commit: de lezer leest het bestand opnieuw; een herstel van 40.000 pagina's laat een kaart van precies 8 B per pagina |
| `core/tests/formats.rs` | `SPINSEG2` rondgang (nul, ruis, JSON, half) voor 512 B..64 KiB, afgebroken en omgedraaide bytes, kapot LZ-blok onder een kloppende hash |
| `core/tests/compact.rs` | een `SPINSEG2`-invoer geeft een `SPINSEG2`-compactie met dezelfde pagina's als de vaste fixture |
| `core/tests/compressed.rs` | SQL- en sync-kant elk een `Compressed<Dir>`: publiceren (alleen `SPINSEG2`, store < 1/3 van logisch), increment, herstel op een lege map (container of gewoon), een oude replica (`SPINSEG1`, gewoon bestand) door de nieuwe code, en oud en nieuw door elkaar |
