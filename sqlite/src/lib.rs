//! Native SQLite met een exclusieve eigenaar en een door de aanroeper geleende VFS.
//!
//! De C-engine krijgt een vaste heap, geen libc-bestandsfuncties en geen threads.
//! De veilige verbinding- en statement-API leent die boot-runtime; RAII sluit
//! ook bij vergeten handvatten de resterende C-objecten voordat de heap vrijkomt.
//!
//! # N verbindingen op één engine
//!
//! [`Engine::open`] leent de hele runtime aan één verbinding op de VFS van de
//! engine. [`Engine::open_with`] leent de engine gedeeld en de opslag van die
//! ene verbinding exclusief: de C-shim registreert per verbinding een eigen
//! VFS `replica-<n>` met een eigen kopie van haar callbacks, dus elke
//! verbinding praat alleen met haar eigen opslag-eigenaar (een eigen map, een
//! eigen stack). Een app met een database per tenant heeft zo één engine en N
//! verbindingen, elk op een eigen `applib::stacktask`, die parkeren en elkaar
//! afwisselen terwijl hun I/O loopt.
//!
//! Waarom dat veilig is, met `SQLITE_THREADSAFE=0` en zonder mutex:
//!
//! 1. SQLite belooft in threadsafe=2 ("multi-thread") dat verbindingen op
//!    *echt gelijktijdige* threads veilig zijn, zolang één verbinding (met
//!    haar statements) nooit op twee threads tegelijk loopt. Coroutines op één
//!    core zijn strikt minder gelijktijdig: er loopt altijd precies één stack,
//!    en een wissel gebeurt alleen op een punt dat wij kiezen. Wat threadsafe=2
//!    met mutexen beschermt (de VFS-lijst, de PRNG, de geheugenstatistiek,
//!    memsys5, de gedeelde pcache1-groep), wordt hier nooit halverwege een
//!    wijziging onderbroken, want daar staat geen parkeerpunt.
//! 2. Parkeren gebeurt alleen in een VFS-blad: een callback van één verbinding
//!    naar haar eigen opslag (`Storage::read` en verwanten, en de
//!    progresscallback `cooperate`). Op dat moment staat SQLite midden in een
//!    operatie van *die* verbinding (haar pager, haar btree), maar nooit
//!    midden in een allocatie (memsys5 roept geen VFS aan) of in een mutatie
//!    van pcache1: die roept de pager vóór of ná zijn I/O aan, en
//!    `pagerStress` (de spill) loopt via `sqlite3PcacheFetchStress` buiten de
//!    pcache1-methoden. Met `SQLITE_ENABLE_MEMORY_MANAGEMENT` deelt pcache1
//!    één groep tussen alle verbindingen (`separateCache = 0`, ook in een
//!    threadsafe build); een andere verbinding kan daaruit alleen een
//!    *ongepinde schone* pagina recyclen, en de pagina waarin een geparkeerde
//!    verbinding leest of schrijft, is gepind. Dat is exact het model dat
//!    threadsafe=1/2 met de LRU-mutex toestaat.
//! 3. De C-shim heeft geen globale staat die dat tegenspreekt: de callbacks
//!    staan per verbinding in haar eigen VFS (`pAppData`), de naam van een
//!    tijdelijk bestand komt uit de entropie van die verbinding en staat in
//!    het bestand zelf, de progresshandler krijgt de callbacks van zijn eigen
//!    verbinding mee, en de enige globale tellers (de VFS-lijst, het
//!    volgnummer van een VFS-naam) worden alleen in open en sluiten geraakt,
//!    nooit over een parkeerpunt heen. De PRNG van SQLite zaait één keer via
//!    de standaard-VFS, dus de opslag van de engine parkeert niet in
//!    `random` of `unix_millis` (geen [`asynchronous::Bridge`] doet dat).
//!
//! Wie ervoor zorgt dat een opslag nooit herintreedt in *dezelfde* verbinding
//! en dat een verbinding maar op één stack loopt: het type (`Connection` is
//! `!Send`, `open_with` leent de opslag exclusief) en `applib::stacktask`
//! (één stack per taak). Een opslag mag wél parkeren terwijl een andere
//! verbinding verder SQL doet; dat is de bedoeling.
#![no_std]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

#[cfg(feature = "std")]
extern crate std;

pub mod asynchronous;
mod callbacks;
mod ffi;
#[cfg(feature = "std")]
pub mod host;
pub mod memory;
mod statement;
mod storage;
use core::{
    ffi::{CStr, c_void},
    fmt,
    marker::PhantomData,
    ptr,
};
pub use statement::{Statement, Value};
pub use storage::{FileId, OpenFlags, Storage};

/// Resultaat met SQLite's primaire of uitgebreide foutcode.
pub type Result<T = ()> = core::result::Result<T, Error>;
/// Een SQLite-fout, zonder allocatie of verloren uitgebreide foutcode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error {
    /// De door SQLite of de VFS geretourneerde code.
    pub code: i32,
}
impl Error {
    /// Onvoldoende geheugen in de vaste SQLite-heap.
    pub const MEMORY: Self = Self { code: 7 };
    /// Deze verbinding of node mag niet schrijven.
    pub const READONLY: Self = Self { code: 8 };
    /// De opslag kon de opdracht niet uitvoeren.
    pub const IO: Self = Self { code: 10 };
    /// De opslaglimiet is bereikt.
    pub const FULL: Self = Self { code: 13 };
    /// Het bestand kan niet worden geopend.
    pub const CANNOT_OPEN: Self = Self { code: 14 };
    /// De aanroep past niet bij de levensloop of het opslagcontract.
    pub const MISUSE: Self = Self { code: 21 };
    /// Parameter- of kolomnummer buiten bereik.
    pub const RANGE: Self = Self { code: 25 };
    /// Externe tekst is geen geldige UTF-8.
    pub const TEXT: Self = Self { code: 20 };
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sqlite: code={}", self.code)
    }
}
impl core::error::Error for Error {}
pub(crate) fn check(code: i32) -> Result {
    if code == 0 {
        Ok(())
    } else {
        Err(Error { code })
    }
}

/// De unieke procesruntime; de heap en opslag blijven exclusief geleend.
///
/// # Invariants
/// SQLite is single-threaded; dit type is niet `Send` of `Sync`. C bewaart geen
/// verwijzing naar dit verplaatsbare handvat, alleen naar de geleende buffers.
pub struct Engine<'a, B: Storage> {
    _owner: PhantomData<(&'a mut B, &'a mut [u64], *mut ())>,
}
impl<'a, B: Storage> Engine<'a, B> {
    /// Initialiseert de unieke SQLite-runtime tijdens de bootfase.
    ///
    /// # Safety
    /// Er mag in dit proces geen andere SQLite-runtime actief zijn of tegelijk
    /// worden gestart. Alle SQLite-calls blijven op deze eigenaar-thread; ook
    /// opslagcallbacks mogen niet in SQLite herintreden. Deze voorwaarde hoort
    /// bij de boot-schil, omdat SQLite zelf procesglobale C-staat bevat.
    pub unsafe fn initialize(heap: &'a mut [u64], backend: &'a mut B) -> Result<Self> {
        let bytes = heap
            .len()
            .checked_mul(8)
            .and_then(|n| i32::try_from(n).ok())
            .ok_or(Error::MEMORY)?;
        let callbacks = callbacks::callbacks(backend);
        // SAFETY: De aanroeper bewijst de unieke boot-runtime; heap en B zijn geleend
        // tot Drop. De C-shim kopieert callbacks voordat deze stackwaarde verdwijnt.
        check(unsafe { ffi::replica_sqlite_init(heap.as_mut_ptr().cast(), bytes, &callbacks) })?;
        Ok(Self {
            _owner: PhantomData,
        })
    }
    /// Opent één verbinding en leent daarvoor de hele runtime.
    pub fn open<'e>(&'e mut self, path: &CStr) -> Result<Connection<'e, 'a, B>> {
        let mut raw = ptr::null_mut();
        // SAFETY: self is de exclusieve runtime en path blijft geldig gedurende open.
        check(unsafe { ffi::replica_sqlite_open(path.as_ptr(), &mut raw) })?;
        if raw.is_null() {
            return Err(Error::MEMORY);
        }
        Ok(Connection {
            raw,
            _borrow: PhantomData,
        })
    }
    /// Opent één bestaande database alleen-lezen (`query_only`): iedere
    /// schrijvende SQL faalt met `SQLITE_READONLY`.
    pub fn open_read_only<'e>(&'e mut self, path: &CStr) -> Result<Connection<'e, 'a, B>> {
        let mut raw = ptr::null_mut();
        // SAFETY: self is de exclusieve runtime en path blijft geldig gedurende open.
        check(unsafe { ffi::replica_sqlite_open_read_only(path.as_ptr(), &mut raw) })?;
        if raw.is_null() {
            return Err(Error::MEMORY);
        }
        Ok(Connection {
            raw,
            _borrow: PhantomData,
        })
    }
    /// Opent een verbinding op haar eigen opslag, naast andere verbindingen
    /// van deze engine (zie de moduledocs: één engine, N verbindingen).
    ///
    /// De engine is gedeeld geleend, `backend` exclusief, zolang de verbinding
    /// leeft. De C-shim registreert een eigen VFS `replica-<n>` met de
    /// callbacks van `backend`; sluiten haalt die weer weg. `backend` mag in
    /// zijn callbacks parkeren (een [`asynchronous::Bridge`] op een eigen
    /// stack) terwijl een andere verbinding van deze engine SQL doet.
    pub fn open_with<'e, B2: Storage>(
        &'e self,
        path: &CStr,
        backend: &'e mut B2,
    ) -> Result<Connection<'e, 'a, B2>> {
        self.open_own(path, backend, false)
    }
    /// Als [`Engine::open_with`], alleen-lezen (`query_only`).
    pub fn open_read_only_with<'e, B2: Storage>(
        &'e self,
        path: &CStr,
        backend: &'e mut B2,
    ) -> Result<Connection<'e, 'a, B2>> {
        self.open_own(path, backend, true)
    }
    fn open_own<'e, B2: Storage>(
        &'e self,
        path: &CStr,
        backend: &'e mut B2,
        readonly: bool,
    ) -> Result<Connection<'e, 'a, B2>> {
        let callbacks = callbacks::callbacks(backend);
        let mut raw = ptr::null_mut();
        // SAFETY: De engine leeft (gedeeld geleend voor 'e) en is de unieke
        // runtime; `backend` blijft exclusief geleend en op zijn adres tot de
        // verbinding sluit, want de verbinding draagt 'e. De C-shim kopieert de
        // callbacks naar haar eigen VFS vóór deze stackwaarde verdwijnt. Wordt
        // de verbinding vergeten (mem::forget), dan wijst de shim haar
        // callbacks bij `Engine::drop` naar dode stubs vóór het sluiten.
        check(unsafe {
            ffi::replica_sqlite_open_with(path.as_ptr(), &callbacks, i32::from(readonly), &mut raw)
        })?;
        if raw.is_null() {
            return Err(Error::MEMORY);
        }
        Ok(Connection {
            raw,
            _borrow: PhantomData,
        })
    }
}
impl<B: Storage> Drop for Engine<'_, B> {
    fn drop(&mut self) {
        // SAFETY: De levenslopen sluiten verdere veilige toegang uit. De C-shim ruimt
        // ook met mem::forget gelekte verbindingen/statements op vóór heap/B vrij zijn.
        unsafe { ffi::replica_sqlite_end() };
    }
}
/// Eén verbinding: haar eigenaar serialiseert haar SQL.
///
/// Uit [`Engine::open`] leent ze de hele runtime exclusief; uit
/// [`Engine::open_with`] de engine gedeeld en haar eigen opslag `B`
/// exclusief. `!Send` en `!Sync`: een verbinding loopt op één stack.
pub struct Connection<'e, 'a, B: Storage> {
    pub(crate) raw: *mut c_void,
    _borrow: PhantomData<(&'e mut B, &'a mut [u64], *mut ())>,
}
impl<B: Storage> Connection<'_, '_, B> {
    /// Voert SQL uit; commitfouten van de VFS komen ongewijzigd terug.
    pub fn execute(&mut self, sql: &CStr) -> Result {
        // SAFETY: De verbinding is exclusief; exec behoudt sql noch callbacks.
        check(unsafe {
            ffi::sqlite3_exec(
                self.raw,
                sql.as_ptr(),
                ptr::null(),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        })
    }
    /// Bereidt exact één statement voor; volgende statements worden geweigerd.
    pub fn prepare<'s>(&'s mut self, sql: &CStr) -> Result<Statement<'s>> {
        Statement::prepare(self.raw, sql)
    }
    /// Het aantal gewijzigde rijen van het laatst voltooide statement.
    pub fn changes(&self) -> i64 {
        // SAFETY: Deze lening kan niet naast een actieve statement-mutatie bestaan.
        unsafe { ffi::sqlite3_changes64(self.raw) }
    }
}
impl<B: Storage> Drop for Connection<'_, '_, B> {
    fn drop(&mut self) {
        // SAFETY: Geen statement kan deze eigenaar veilig overleven; de C-shim
        // sluit ook vergeten statements, zodat close geen verbinding laat lekken.
        unsafe { ffi::replica_sqlite_close(self.raw) };
    }
}
/// Het gepinde SQLite-versienummer, bijvoorbeeld 3053004 voor 3.53.4.
pub fn version_number() -> i32 {
    // SAFETY: Dit leest alleen SQLite's constante versienummer, zonder runtime.
    unsafe { ffi::sqlite3_libversion_number() }
}
