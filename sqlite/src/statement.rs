//! Een statement leent zijn verbinding; een rij leent weer zijn statement.
//!
//! [`Connection::prepare_cached`](crate::Connection::prepare_cached) leent
//! een statement uit de statement-cache van de verbinding: een LRU van
//! [`CACHE_SIZE`] C-handvatten, gesleuteld op de SQL-tekst. De cache bezit de
//! handvatten; een [`CachedStatement`] neemt er één zolang hij leeft, en
//! geeft hem bij zijn drop terug na `sqlite3_reset` en
//! `sqlite3_clear_bindings` (zo houdt een half gelezen SELECT geen
//! leestransactie open). De verbinding finaliseert de cache vóór ze sluit.
use crate::{Error, Result, check, ffi};
use core::{
    ffi::{CStr, c_void},
    marker::PhantomData,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    ptr, slice, str,
};

/// Zoveel voorbereide statements houdt de cache van één verbinding.
pub const CACHE_SIZE: usize = 32;

/// Een SQLite-waarde; bytes blijven geleend tot de volgende muterende stap.
#[derive(Debug, PartialEq)]
pub enum Value<'a> {
    /// SQL NULL.
    Null,
    /// Signed 64-bit integer.
    Integer(i64),
    /// IEEE-754 double.
    Real(f64),
    /// UTF-8 tekst met expliciete lengte.
    Text(&'a str),
    /// Willekeurige bytes, inclusief een lege blob.
    Blob(&'a [u8]),
}
/// Een voorbereid statement en de exclusieve lening van zijn verbinding.
pub struct Statement<'a> {
    raw: *mut c_void,
    row: bool,
    _connection: PhantomData<&'a mut c_void>,
}
impl Statement<'_> {
    /// Een statement rond een handvat dat de cache teruggaf.
    fn from_raw(raw: *mut c_void) -> Self {
        Self {
            raw,
            row: false,
            _connection: PhantomData,
        }
    }

    pub(crate) fn prepare(raw: *mut c_void, sql: &CStr) -> Result<Self> {
        let mut stmt = ptr::null_mut();
        let mut tail = ptr::null();
        // SAFETY: De eigenaar leent de verbinding; SQLite kopieert de SQL-parserstaat.
        check(unsafe { ffi::sqlite3_prepare_v2(raw, sql.as_ptr(), -1, &mut stmt, &mut tail) })?;
        if stmt.is_null() {
            return Err(Error::MISUSE);
        }
        let value = Self {
            raw: stmt,
            row: false,
            _connection: PhantomData,
        };
        // SAFETY: tail wijst binnen de nul-afgesloten sql naar de ongebruikte suffix.
        if !unsafe { CStr::from_ptr(tail) }
            .to_bytes()
            .iter()
            .all(u8::is_ascii_whitespace)
        {
            return Err(Error::MISUSE);
        }
        Ok(value)
    }
    /// Bindt een waarde aan een één-gebaseerd parameternummer; bytes worden gekopieerd.
    pub fn bind(&mut self, index: u32, value: Value<'_>) -> Result {
        let index = i32::try_from(index).map_err(|_| Error::RANGE)?;
        let len = |n: usize| i32::try_from(n).map_err(|_| Error::FULL);
        // SAFETY: self leent het statement exclusief en TRANSIENT kopieert alle bytes.
        let rc = unsafe {
            match value {
                Value::Null => ffi::sqlite3_bind_null(self.raw, index),
                Value::Integer(n) => ffi::sqlite3_bind_int64(self.raw, index, n),
                Value::Real(n) => ffi::sqlite3_bind_double(self.raw, index, n),
                Value::Text(s) => {
                    ffi::replica_bind_text(self.raw, index, s.as_ptr(), len(s.len())?)
                }
                Value::Blob(b) => {
                    ffi::replica_bind_blob(self.raw, index, b.as_ptr(), len(b.len())?)
                }
            }
        };
        check(rc)
    }
    /// Stapt naar een rij (`true`) of het einde (`false`).
    pub fn step(&mut self) -> Result<bool> {
        self.row = false;
        // SAFETY: Het statement is exclusief geleend en alle oudere rijen zijn weg.
        match unsafe { ffi::sqlite3_step(self.raw) } {
            100 => {
                self.row = true;
                Ok(true)
            }
            101 => Ok(false),
            code => Err(Error { code }),
        }
    }
    /// Zet een statement terug voor hergebruik; bindingen blijven behouden.
    pub fn reset(&mut self) -> Result {
        self.row = false;
        // SAFETY: self is de enige eigenaar en er leeft geen geleende rij naast &mut.
        check(unsafe { ffi::sqlite3_reset(self.raw) })
    }
    /// Het aantal kolommen van een rij van dit statement (0 voor een
    /// statement zonder resultaat).
    pub fn column_count(&self) -> u32 {
        // SAFETY: Het statement leeft; deze call leest alleen zijn metadata.
        let n = unsafe { ffi::sqlite3_column_count(self.raw) };
        u32::try_from(n).unwrap_or(0)
    }
    /// Leest een nul-gebaseerde kolom van de huidige rij.
    pub fn column(&mut self, index: u32) -> Result<Value<'_>> {
        let index = i32::try_from(index).map_err(|_| Error::RANGE)?;
        // SAFETY: De verbinding/het statement leven en deze call wijzigt geen rij.
        if !self.row || index >= unsafe { ffi::sqlite3_column_count(self.raw) } {
            return Err(Error::RANGE);
        }
        // SAFETY: index is hierboven getoetst. De lening eindigt vóór step/reset/drop.
        unsafe {
            match ffi::sqlite3_column_type(self.raw, index) {
                1 => Ok(Value::Integer(ffi::sqlite3_column_int64(self.raw, index))),
                2 => Ok(Value::Real(ffi::sqlite3_column_double(self.raw, index))),
                3 => {
                    let b = self.bytes(index, true)?;
                    Ok(Value::Text(str::from_utf8(b).map_err(|_| Error::TEXT)?))
                }
                4 => Ok(Value::Blob(self.bytes(index, false)?)),
                5 => Ok(Value::Null),
                _ => Err(Error::MISUSE),
            }
        }
    }
    /// Vraagt bij TEXT eerst UTF-8, zodat column_bytes de pointer niet meer omzet.
    fn bytes(&self, index: i32, text: bool) -> Result<&[u8]> {
        // SAFETY: Deze private helper krijgt alleen een getoetste TEXT/BLOB-kolom.
        let (p, n) = unsafe {
            (
                if text {
                    ffi::sqlite3_column_text(self.raw, index)
                } else {
                    ffi::sqlite3_column_blob(self.raw, index)
                },
                ffi::sqlite3_column_bytes(self.raw, index),
            )
        };
        if text && p.is_null() {
            return Err(Error::MEMORY);
        }
        if n == 0 {
            return Ok(&[]);
        }
        if p.is_null() || n < 0 {
            return Err(Error::MEMORY);
        }
        // SAFETY: SQLite garandeert n leesbare bytes tot de volgende muterende call.
        Ok(unsafe { slice::from_raw_parts(p, n as usize) })
    }
}
impl Drop for Statement<'_> {
    fn drop(&mut self) {
        // SAFETY: De statement-eigenaar finalizeert precies eenmaal, vóór zijn verbinding.
        unsafe { ffi::sqlite3_finalize(self.raw) };
    }
}

/// Eén plaats in de cache: een handvat (of null), de hash van zijn SQL en
/// wanneer hij het laatst terugkwam.
#[derive(Clone, Copy)]
struct Slot {
    raw: *mut c_void,
    hash: u64,
    used: u64,
}

const EMPTY: Slot = Slot {
    raw: ptr::null_mut(),
    hash: 0,
    used: 0,
};

/// De statement-cache van één verbinding: een vaste tabel van
/// [`CACHE_SIZE`] handvatten, zonder allocatie. De sleutel is de SQL-tekst
/// zelf: de hash zoekt, `sqlite3_sql` (de tekst die SQLite bij het handvat
/// bewaart) bevestigt.
pub(crate) struct StatementCache {
    slots: [Slot; CACHE_SIZE],
    clock: u64,
    hits: u64,
    misses: u64,
}

impl StatementCache {
    pub(crate) const fn new() -> Self {
        Self {
            slots: [EMPTY; CACHE_SIZE],
            clock: 0,
            hits: 0,
            misses: 0,
        }
    }

    /// Neemt het handvat voor `sql` uit de cache, als het er is.
    fn take(&mut self, hash: u64, sql: &CStr) -> Option<*mut c_void> {
        let found = self.slots.iter_mut().find(|s| {
            !s.raw.is_null() && s.hash == hash && {
                // SAFETY: Een niet-null handvat in de cache leeft tot de cache
                // het finaliseert; sqlite3_sql geeft de bewaarde,
                // nul-afgesloten SQL van prepare_v2.
                let text = unsafe { ffi::sqlite3_sql(s.raw) };
                // SAFETY: Niet-null is een geldige C-string van SQLite.
                !text.is_null() && unsafe { CStr::from_ptr(text) } == sql
            }
        });
        match found {
            Some(slot) => {
                self.hits = self.hits.wrapping_add(1);
                Some(core::mem::replace(slot, EMPTY).raw)
            }
            None => {
                self.misses = self.misses.wrapping_add(1);
                None
            }
        }
    }

    /// Legt een gereset handvat terug; vol verdringt het langst ongebruikte.
    fn put(&mut self, raw: *mut c_void, hash: u64) {
        self.clock = self.clock.wrapping_add(1);
        let free = self.slots.iter().position(|s| s.raw.is_null());
        let i = free.unwrap_or_else(|| {
            self.slots
                .iter()
                .enumerate()
                .min_by_key(|(_, s)| s.used)
                .map_or(0, |(i, _)| i)
        });
        if let Some(s) = self.slots.get_mut(i) {
            if !s.raw.is_null() {
                // SAFETY: Het verdrongen handvat is van de cache en wordt
                // precies hier, eenmaal, gefinaliseerd.
                unsafe { ffi::sqlite3_finalize(s.raw) };
            }
            *s = Slot {
                raw,
                hash,
                used: self.clock,
            };
        }
    }

    /// Finaliseert alle handvatten; de cache is daarna leeg.
    pub(crate) fn clear(&mut self) {
        for s in &mut self.slots {
            if !s.raw.is_null() {
                // SAFETY: Elk niet-null handvat is van de cache; na deze
                // finalize staat er null.
                unsafe { ffi::sqlite3_finalize(s.raw) };
            }
            *s = EMPTY;
        }
    }

    /// Hits, missers en het aantal handvatten in de cache.
    pub(crate) fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits,
            misses: self.misses,
            cached: self.slots.iter().filter(|s| !s.raw.is_null()).count(),
        }
    }
}

/// De tellers van een statement-cache.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Statements die uit de cache kwamen.
    pub hits: u64,
    /// Statements die voorbereid moesten worden.
    pub misses: u64,
    /// Handvatten die nu in de cache staan.
    pub cached: usize,
}

/// FNV-1a over de SQL-tekst: de snelle sleutel van de cache.
fn hash(sql: &[u8]) -> u64 {
    sql.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// Een statement uit de cache van zijn verbinding; werkt als een
/// [`Statement`] en gaat bij zijn drop gereset terug naar de cache.
pub struct CachedStatement<'s> {
    stmt: ManuallyDrop<Statement<'s>>,
    cache: &'s mut StatementCache,
    hash: u64,
    hit: bool,
}

impl<'s> CachedStatement<'s> {
    /// Uit de cache, of nieuw voorbereid op `conn` (een miss).
    pub(crate) fn get(
        conn: *mut c_void,
        cache: &'s mut StatementCache,
        sql: &CStr,
    ) -> Result<Self> {
        let hash = hash(sql.to_bytes());
        let (stmt, hit) = match cache.take(hash, sql) {
            Some(raw) => (Statement::from_raw(raw), true),
            None => match Statement::prepare(conn, sql) {
                // De vaste heap is krap en de cache houdt geheugen vast: leeg
                // hem en probeer het één keer opnieuw.
                Err(e) if e.code & 0xff == Error::MEMORY.code && cache.stats().cached > 0 => {
                    cache.clear();
                    (Statement::prepare(conn, sql)?, false)
                }
                other => (other?, false),
            },
        };
        Ok(Self {
            stmt: ManuallyDrop::new(stmt),
            cache,
            hash,
            hit,
        })
    }

    /// Kwam dit statement uit de cache (zonder prepare)?
    pub fn was_cached(&self) -> bool {
        self.hit
    }
}

impl<'s> Deref for CachedStatement<'s> {
    type Target = Statement<'s>;
    fn deref(&self) -> &Statement<'s> {
        &self.stmt
    }
}

impl<'s> DerefMut for CachedStatement<'s> {
    fn deref_mut(&mut self) -> &mut Statement<'s> {
        &mut self.stmt
    }
}

impl Drop for CachedStatement<'_> {
    fn drop(&mut self) {
        let raw = self.stmt.raw;
        // SAFETY: Het handvat is van deze lening en leeft; reset en
        // clear_bindings maken het klaar voor de volgende gebruiker. De fout
        // van reset is die van de laatste step en hoorde bij die gebruiker.
        unsafe {
            ffi::sqlite3_reset(raw);
            ffi::sqlite3_clear_bindings(raw);
        }
        // De `Statement` zelf finaliseert niet (ManuallyDrop): het handvat
        // gaat terug naar de cache.
        self.cache.put(raw, self.hash);
    }
}
