# Meer databases onder één lease: de groep

Uit EasyFlor overgenomen (21-11-2026): wat elke app met meer databases per
domein anders zelf moet bouwen. EasyFlor heeft per tenant `main.db` en een
bestand per jaar; dat is hier een domein met databases.

## Indeling in de bucket

```text
<prefix>/<domain>/lease          de schrijverlease (hop-lease), één per domein
<prefix>/<domain>/<file>/...     een replica-namespace per database
```

Elke database is een eigen `owner::Replica` (`Replica::prepare_shared`); de
`Writer` uit `writer::claim` wordt gedeeld. Niet atomair over databases heen:
elke database publiceert op zijn eigen moment.

## Onderdelen

| Module | Wat |
| --- | --- |
| `group::Group` | Prepare per database (lui), lease bijhouden, publiceren in rust, backoff, fencing, sluiten |
| `group::release` | Lease terug bij het sluiten: na een transportfout nog twee keer, bij naam |
| `writer::renew_or_reclaim` | Vernieuwen; voorbij de schrijfdeadline heroveren zolang niemand de lease nam |
| `lanes` | `Offload`, `Lane`, `Job`, `LaneSide`: store-werk buiten de stack van de eigenaar |
| `publications::Publications` | Gedeelde kijk op de capturenummers van de eigenaars |
| `follow::Follow` | Een lezer zonder lease die de publicaties van de schrijver volgt |
| `replica_hopos::lanes` | De banen op HopOS: `Lanes::run` per stack, `OwnerLanes` als `Offload` |

## Inbouwen

1. Claim de lease (`writer::claim` op `<prefix>/<domain>/lease`) vóór iets
   wordt opgehaald. Een `Role::Reader` maakt geen groep.
2. `Group::new(config, store, writer, millis, report)`; met banen
   `.with_offload(&lanes)`, en `.with_publications(rc)` als iemand wacht.
3. `Group::prepare(file, sync, &verify)` de eerste keer dat SQLite `file`
   opent. `verify` controleert een herstelde scratchdatabase en roept zijn
   derde argument aan tijdens de controle (`group::Ticking` om de opslag).
4. SQL via `Group::vfs(file, &mut handle)`; roep `Group::keep_lease` uit de
   `cooperate` van de opslag (de progresscallback van een lang statement).
5. Tussen twee verzoeken `Group::rest`; vóór het sluiten `Group::flush`.
   Alleen `Error::LeaseLost` betekent fencen. `Group::standing` zegt of
   writes moeten wachten (`Standing::Held(ms)`, een Retry-After).
6. Sluiten: SQL dicht, `Offload::settle(group::CLOSE_SETTLE_MS)`,
   `Group::close` en `group::release`.

`Report` krijgt elke herkansing (`retry`, met de oorzaak en `next_ms`), het
herstel (`recovered`), het verlies (`lost`, eenmaal), andere fouten en elke
publicatie: de host maakt er zijn logregels van.

## Welke writes de store heeft

De eigenaar nummert zijn captures zelf: `Replica::begun` telt de captures van
dit leven, `Pending::capture` is het nummer van een capture, `Synced::capture`
het nummer dat gepubliceerd is en `Replica::confirmed` het hoogste. Een
hervatte upload van vóór een herstart is nummer 1 en bevat geen write van dit
leven. Wie wacht tot een write in de store staat, onthoudt `begun` op het
moment van de write en wacht tot `confirmed` erboven is. Met banen wordt een
capture in het ene rustmoment genomen en in een later gepubliceerd; een host
die zelf landingen telde, bevestigde zo een write die nog niet in de store
stond (EasyFlor, 21-11-2026). De groep zet de nummers per database in
`Publications`.

## Banen

Een S3-call parkeert de stack die hem doet. Met banen doet de eigenaar in
rust alleen de capture (lokaal) en het manifest na een gelande upload (één
kleine PUT, en alleen als een baan de store de laatste 10 s hoorde). De
leasebaan doet de vernieuwing, nooit achter een upload; de databaan de uploads
en het onderhoud (`owner::Config::background_maintenance` staat dan aan).

## Toetsen

`core/tests/group.rs`: banen en capturenummers, een korte storing (backoff,
wachten voorbij de deadline, herovering met dezelfde generatie), vrijgeven bij
naam, een herstel langer dan de lease, een lezer die volgt.
`core/tests/writer.rs`: `renew_or_reclaim` en de herovering in `renew_shared`.
`hopos/tests/lanes.rs`: de banen op echte stacks.
