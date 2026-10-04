//! De stappen van de meetkern: dezelfde SQL en dezelfde lussen op HopOS en
//! op de host, zodat de getallen naast elkaar gelegd kunnen worden. Elke stap
//! meldt zich met zijn naam, zijn duur en zijn aantal (rijen, commits, reads).
use core::ffi::CStr;
use core::fmt::Write;
use replica_sqlite::{Connection, Engine, Error, Result, Storage, Value};

/// Rijen per MiB gevraagde data: een rij is ongeveer 190 bytes plus de
/// index, dus 5.000 rijen zijn ruim 1 MiB in de tabel en met de indexen
/// zo'n 1,5 MiB in het bestand.
pub(crate) const ROWS_PER_MIB: u64 = 5000;
/// Kleine commits van één rij: de latency van de duurzame commit.
const COMMITS: u64 = 200;
/// Puntreads met een willekeurige sleutel over de hele tabel.
const POINTS: u64 = 2000;
/// Rangequeries over de index.
const RANGES: u64 = 100;
/// Rijen per rangequery: elk een rowid-opzoeking in de tabel.
const RANGE_ROWS: u64 = 500;
/// Soorten gebeurtenissen; `kind` kiest een achtste voor de join en de delete.
const KINDS: u64 = 8;
/// Groepen; de index (grp, ts) deelt de tabel erin op.
const GROUPS: u64 = 1024;

/// Wat de omgeving levert: de klok, en waar een stap zijn uitkomst laat.
pub(crate) trait Bench {
    /// Microseconden, monotoon.
    fn now_us(&mut self) -> u64;
    /// Eén stap is klaar: naam, duur en het aantal eenheden.
    fn report(&mut self, name: &str, us: u64, n: u64);
}

fn require(ok: bool) -> Result {
    if ok { Ok(()) } else { Err(Error::MISUSE) }
}

/// Een deterministische generator; geen productie-entropie.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// Een tekst in een vaste buffer, zonder heap.
struct Label {
    buf: [u8; 32],
    len: usize,
}
impl Label {
    fn new(args: core::fmt::Arguments<'_>) -> Self {
        let mut l = Self {
            buf: [0; 32],
            len: 0,
        };
        let _ = l.write_fmt(args);
        l
    }
    fn text(&self) -> &str {
        core::str::from_utf8(&self.buf[..self.len]).unwrap_or("")
    }
}
impl Write for Label {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let n = s.len().min(self.buf.len() - self.len);
        self.buf[self.len..self.len + n].copy_from_slice(&s.as_bytes()[..n]);
        self.len += n;
        Ok(())
    }
}

/// Een stap: meet `f`, en meld de duur met het aantal dat `f` teruggeeft.
fn step<R: Bench, F: FnOnce() -> Result<u64>>(r: &mut R, name: &str, f: F) -> Result<u64> {
    let start = r.now_us();
    let n = f()?;
    let us = r.now_us().saturating_sub(start);
    r.report(name, us, n);
    Ok(n)
}

fn count<B: Storage>(db: &mut Connection<'_, '_, B>, sql: &CStr) -> Result<i64> {
    let mut s = db.prepare(sql)?;
    require(s.step()?)?;
    match s.column(0)? {
        Value::Integer(n) => Ok(n),
        _ => Err(Error::MISUSE),
    }
}

/// Laadt `rows` rijen in één transactie met een voorbereid statement.
fn load<B: Storage>(db: &mut Connection<'_, '_, B>, rows: u64, rng: &mut Rng) -> Result<u64> {
    db.execute(c"BEGIN")?;
    {
        let mut s = db.prepare(c"INSERT INTO item VALUES(?1,?2,?3,?4,?5,?6)")?;
        let mut payload = [0u8; 128];
        for i in 1..=rows {
            for b in &mut payload {
                *b = rng.next() as u8;
            }
            let label = Label::new(format_args!("item-{i}"));
            s.bind(1, Value::Integer(i as i64))?;
            s.bind(2, Value::Integer((i % GROUPS) as i64))?;
            s.bind(3, Value::Integer((i * 7) as i64))?;
            s.bind(4, Value::Real((i % 1000) as f64 / 4.0))?;
            s.bind(5, Value::Text(label.text()))?;
            s.bind(6, Value::Blob(&payload))?;
            require(!s.step()?)?;
            s.reset()?;
        }
    }
    db.execute(c"COMMIT")?;
    Ok(rows)
}

/// De gebeurtenissen: een kwart zoveel rijen, elk wijst naar een item.
fn events<B: Storage>(db: &mut Connection<'_, '_, B>, rows: u64, rng: &mut Rng) -> Result<u64> {
    let n = rows / 4;
    db.execute(c"BEGIN")?;
    {
        let mut s = db.prepare(c"INSERT INTO event VALUES(?1,?2,?3,?4)")?;
        for i in 1..=n {
            let note = Label::new(format_args!("event {i} of kind {}", i % KINDS));
            s.bind(1, Value::Integer(i as i64))?;
            s.bind(2, Value::Integer((1 + rng.next() % rows) as i64))?;
            s.bind(3, Value::Integer((i % KINDS) as i64))?;
            s.bind(4, Value::Text(note.text()))?;
            require(!s.step()?)?;
            s.reset()?;
        }
    }
    db.execute(c"COMMIT")?;
    db.execute(c"CREATE INDEX event_item ON event(item)")?;
    Ok(n)
}

/// Kleine transacties: één rij per commit, elk een duurzame commit.
fn commits<B: Storage>(db: &mut Connection<'_, '_, B>, rows: u64, first: u64) -> Result<u64> {
    let mut s = db.prepare(c"INSERT INTO event VALUES(?1,?2,?3,'commit')")?;
    for i in 0..COMMITS {
        s.bind(1, Value::Integer((first + i) as i64))?;
        s.bind(2, Value::Integer((1 + i % rows) as i64))?;
        s.bind(3, Value::Integer(KINDS as i64))?;
        require(!s.step()?)?;
        s.reset()?;
    }
    Ok(COMMITS)
}

/// Puntreads met willekeurige sleutels: koud na het heropenen.
fn points<B: Storage>(db: &mut Connection<'_, '_, B>, rows: u64, rng: &mut Rng) -> Result<u64> {
    let mut s = db.prepare(c"SELECT amount,label FROM item WHERE id=?1")?;
    for _ in 0..POINTS {
        s.bind(1, Value::Integer((1 + rng.next() % rows) as i64))?;
        require(s.step()?)?;
        require(matches!(s.column(0)?, Value::Real(_)))?;
        s.reset()?;
    }
    Ok(POINTS)
}

/// Rangequeries over de index (grp, ts).
fn ranges<B: Storage>(db: &mut Connection<'_, '_, B>, rows: u64, rng: &mut Rng) -> Result<u64> {
    let mut s =
        db.prepare(c"SELECT count(*),sum(amount) FROM item WHERE grp=?1 AND ts BETWEEN ?2 AND ?3")?;
    // Binnen een groep liggen de rijen GROUPS uit elkaar en ts is 7 per rij.
    let span = (RANGE_ROWS * GROUPS * 7) as i64;
    for _ in 0..RANGES {
        let from = (rng.next() % (rows * 7)) as i64;
        s.bind(1, Value::Integer((rng.next() % GROUPS) as i64))?;
        s.bind(2, Value::Integer(from))?;
        s.bind(3, Value::Integer(from + span))?;
        require(s.step()?)?;
        s.reset()?;
    }
    Ok(RANGES)
}

/// Een sortering zonder index over de hele tabel: boven cache_size spilt
/// de sorter PMA's naar een tempbestand en merget ze (tot zestien tegelijk).
fn sort<B: Storage>(db: &mut Connection<'_, '_, B>, rows: u64) -> Result<u64> {
    let mut s = db.prepare(c"SELECT amount FROM item ORDER BY amount")?;
    let mut got = 0;
    while s.step()? {
        got += 1;
    }
    require(got == rows)?;
    Ok(rows)
}

/// De stappen, op een geopende engine; de verbinding wordt vóór de koude
/// leesstappen heropend, zodat SQLite's paginacache leeg is.
pub(crate) fn run<B: Storage, R: Bench>(
    engine: &mut Engine<'_, B>,
    path: &CStr,
    mib: u64,
    r: &mut R,
) -> Result<u64> {
    let rows = mib.max(1) * ROWS_PER_MIB;
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut db = engine.open(path)?;
    // De paginacache en de PMA-maat van de sorter zet de bridge: drie achtste van de heap.
    db.execute(
        c"CREATE TABLE item(id INTEGER PRIMARY KEY,grp INTEGER,ts INTEGER,amount REAL,label TEXT,payload BLOB);\
          CREATE TABLE event(id INTEGER PRIMARY KEY,item INTEGER,kind INTEGER,note TEXT)",
    )?;
    step(r, "LOAD", || load(&mut db, rows, &mut rng))?;
    step(r, "INDEX", || {
        db.execute(c"CREATE INDEX item_grp ON item(grp,ts)")?;
        Ok(rows)
    })?;
    step(r, "EVENTS", || events(&mut db, rows, &mut rng))?;
    step(r, "COMMITS", || commits(&mut db, rows, rows / 4 + 1))?;
    drop(db);
    let mut db = engine.open(path)?;
    step(r, "POINT", || points(&mut db, rows, &mut rng))?;
    step(r, "SCAN", || {
        // De kale tabelscan: alle pagina's op volgorde, zonder index.
        let n = count(
            &mut db,
            c"SELECT count(*) FROM item WHERE amount>=0 AND length(label)>0",
        )?;
        require(n as u64 == rows)?;
        Ok(rows)
    })?;
    step(r, "GROUP", || {
        // Een scan met de sorter voor GROUP BY. Zonder NOT INDEXED neemt de
        // planner de index (grp, ts) en zoekt per rij de tabelrij op: tien
        // miljoen losse pagina's door een cache van tientallen MiB, één
        // ronde en één NVMe-opdracht per rij (de M4 op 04-10: 7.000 per
        // seconde, 24 minuten); POINT meet die prijs al.
        let groups = count(
            &mut db,
            c"SELECT count(*) FROM (SELECT grp,count(*),sum(amount) FROM item NOT INDEXED GROUP BY grp)",
        )?;
        require(groups as u64 == GROUPS.min(rows))?;
        Ok(rows)
    })?;
    step(r, "RANGE", || ranges(&mut db, rows, &mut rng))?;
    step(r, "SORT", || sort(&mut db, rows))?;
    step(r, "JOIN", || {
        let n = count(
            &mut db,
            c"SELECT count(*) FROM event e JOIN item i ON i.id=e.item WHERE e.kind=3",
        )?;
        Ok(n as u64)
    })?;
    step(r, "UPDATE", || {
        // Een kwart van de tabel op rowid-volgorde: pagina's en journal
        // aaneengesloten. Via de index (grp<256) zijn het dezelfde rijen in
        // willekeurige volgorde, zie GROUP.
        let mut s = db.prepare(c"UPDATE item SET amount=amount+1 WHERE id BETWEEN 1 AND ?1")?;
        s.bind(1, Value::Integer((rows / 4) as i64))?;
        require(!s.step()?)?;
        drop(s);
        Ok(db.changes() as u64)
    })?;
    step(r, "DELETE", || {
        db.execute(c"DELETE FROM event WHERE kind=0")?;
        Ok(db.changes() as u64)
    })?;
    step(r, "CHECK", || {
        let mut s = db.prepare(c"PRAGMA quick_check")?;
        require(s.step()? && s.column(0)? == Value::Text("ok"))?;
        Ok(1)
    })?;
    Ok(rows)
}
