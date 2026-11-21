# TODO

## Hot spare en één schrijver

Doel: valt de schrijvende node uit, dan gaat een tweede node binnen een
lease-termijn verder. Asynchroon is goed genoeg: een paar handelingen kwijt
weegt niet op tegen uren downtime.

1. **Hostapp: lease vernieuwen.** De hostapp roept `Replica::renew` aan, ook
   tijdens uploads (via `begin`/`finish`).
2. **Overname en een late PUT.** Gedaan zonder nieuwe generatie (een
   snapshot van gigabytes per overname is te duur): het fencing-token in elk
   manifest, het hoogste wint bij een dubbele sequence; `put_new` voor vaste
   sleutels; het onderhoud bevestigt de lease vóór het weggooien en zet een
   late current van een oude schrijver terug. Wat blijft: tussen de late
   current-PUT en de volgende onderhoudsronde wijst current naar de generatie
   van de oude schrijver (een herstel of herstart in die minuut neemt die).
   Dicht is pas `current` met `If-Match` op de ETag die de schrijver bij het
   begin van zijn snapshot las; dat vraagt een versie-GET en een
   voorwaardelijke PUT in `Store`. En `replication::resolve` ziet twee
   manifesten op zijn onzekere sequence nog als `Corrupt` (dan een
   reparatiesnapshot); het zou het token van de schrijver moeten kennen.
3. **Volgen.** Een lezer past nieuwe manifesten incrementeel toe op zijn
   lokale kopie (de file change counter op pagina 1 maakt SQLite's cache
   ongeldig); een nieuwe generatie in `current` geeft een volledige restore.
4. **Promotie.** Lease overnemen, inhalen tot de tip, nieuwe generatie,
   SQLite read-write heropenen. Eerst handmatig, daarna automatisch.

## Segmenten parallel uploaden

Downloads gaan nu in rondes tegelijk (`Store::get_all`, standaard 16 stromen in `S3`).
Uploads van een capture nog na elkaar; de Go-versie deed er twee tegelijk.

## Restjes via de buren (na het bovenstaande)

De nodes staan naast elkaar. S3 loopt tot een sync-interval (standaard 15 s)
plus de lease-termijn achter; dat gat kan de schrijver rechtstreeks aan de
spare(s) doorgeven.

- De schrijver stuurt na iedere commit de gewijzigde pagina's naar de spare,
  gemerkt met lease-generatie, S3-generatie en de laatst bevestigde
  S3-sequence als basis. Het adres van de schrijver staat al in de lease.
- De spare bewaart dit als staart bovenop de S3-tip. Bij promotie gebruikt hij
  de staart alleen als de basis precies op de S3-tip aansluit en de hashes
  kloppen; anders weggooien. S3 blijft de bron van waarheid; de nieuwe
  generatie (punt 2) publiceert de staart mee.
- Een staart met een oudere lease-generatie wordt geweigerd: een zombie kan zo
  niets meer aanvullen.
- Optioneel semi-synchroon: COMMIT keert pas terug na de ack van één spare.
  Dan geen verlies bij uitval van één node, tegen LAN-latency per commit; valt
  de spare weg, dan terug naar asynchroon.
