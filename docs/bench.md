# SQLite op HopOS: de maat

`apps/sqlite-bench` (markers `REPLICA_BENCH_<STAP>`, per stap µs, aantallen,
MiB/s en `cpm`: OS-rondes per MiB, 256 is één ronde per pagina van 4 KiB).
Op de host doet `tools/bench-host.sh` dezelfde SQL op de unix-VFS; onder QEMU
`tools/qemu-bench.py`. Eén run per punt.

## M4 (Apple ANS-NVMe, HopOS 3.0.11), 2048 MiB, heap 64 MiB, 04-10-2026

Als Hop-job naast een andere app (core-class big, sharegroup bench, 128 MiB,
eigen volume). Replica 2f3fdb5. Totaal 136 s, db 1950 MiB.

| Stap | M4 | cpm | Host unix-VFS (dezelfde Mac, 256 MiB) | Wat telt |
| --- | ---: | ---: | ---: | --- |
| LOAD rijen/s (MiB/s) | 1.023.013 (167) | 6 | 1.776.761 | 45 pagina's per schrijfronde |
| INDEX rijen/s (MiB/s) | 1.366.615 (281) | 3 | 4.108.252 | de 16-way merge met een venster per stroom |
| COMMITS/s (1 rij per commit) | 64 | 288 | 9.825 (geen barrière op macOS) | 3 OP_SYNC per commit, 5,5 ms per flush van de ANS |
| POINT reads/s | 5.386 | 174 | 834.028 | één ronde plus één NVMe-read per koude pagina |
| SCAN rijen/s (MiB/s) | 2.805.811 (459) | 3 | 17.367.471 | 179 pagina's per ronde |
| GROUP rijen/s (MiB/s) | 1.451.701 (276) | 3 | 4.368.586 | scan plus sorter, PMA's van 24 MiB |
| RANGE queries/s (500 rijen) | 12 | 139 | | losse opzoekingen via de index |
| SORT rijen/s (MiB/s) | 1.259.995 (233) | 3 | 5.693.950 | |
| JOIN rijen/s | 7.698 | 220 | | 320k losse opzoekingen, 42 s |
| UPDATE rijen/s (MiB/s) | 1.005.173 (492) | 4 | 1.419.976 | aaneengesloten kwart |
| DELETE rijen/s | 9.685 | 152 | | 320k losse rijen plus index |
| CHECK MiB/s (integrity_check) | 487 | 3 | | 3712 MiB in 7,6 s |

Lezing: alles wat stroomt (LOAD, SCAN, GROUP, SORT, UPDATE, CHECK) zit op
230 tot 490 MiB/s met 3 tot 6 rondes per MiB; de ANS zelf doet rond 1000.
Alles wat per rij opzoekt (POINT, RANGE, JOIN, DELETE) zit op één OS-ronde
plus één NVMe-read per koude pagina, 5 tot 10 duizend per seconde: de vloer
zonder hints van SQLite. Een duurzame commit kost drie flushes van de ANS.

## Volgende stappen

- Het rollback-journal weg via batch-atomic in hopfs (`SQLITE_IOCAP_BATCH_ATOMIC`,
  zoals F2FS op Linux): hermappen in een atomaire reeks via `pending`, één
  vastlegging bij OP_SYNC. Dat haalt 2 van de 3 syncs en het journal weg;
  een kern-recept van weinig regels, na het weekend.
- De losse opzoekingen: minder rondes per pagina kan alleen met meerdere
  reads in de lucht (SQLite leest synchroon), dus hier telt de ronde van de
  OS-core (M4 55 µs) en de NVMe-latentie.

## QEMU en host

De volledige tabellen (nulmeting, tussenstappen, eindstand op 64 en 512 MiB,
host RAM en host unix-VFS, de Linux-referenties uit de SQLite-documentatie)
staan in de statusverslagen van 04-10-2026; de kern: op QEMU 64 MiB ging het
totaal van 26,0 naar 8,9 s, SCAN van 21 naar 157 MiB/s, INDEX, GROUP en
SORT van 16 tot 19 naar 3 rondes per MiB.
