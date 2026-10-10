//! Eén schrijver per namespace: de hop-lease (`discovery`) in de objectopslag.
//!
//! Elke claim en vernieuwing is een voorwaardelijke schrijf op de laatst
//! geziene handle (S3: `If-None-Match: *` en `If-Match: <etag>`); de
//! wederzijdse uitsluiting woont in de backend. Wie de lease niet krijgt, is
//! lezer: [`ReadOnly`] weigert dan iedere write met `SQLITE_READONLY`.
//!
//! Een schrijver stopt uit zichzelf een derde lease vóór het verlopen; een
//! andere node neemt pas na het verlopen over. Die marge vangt klokverschil
//! en een trage store op. De generatie stijgt bij elke overname en is het
//! fencing-token van deze schrijver.
use crate::{Error, Result, object::StoreError, string};
use crate::{manifest::hex, time::Time};
use alloc::{format, string::String};
use core::ffi::CStr;
pub use hop_discovery::{self as discovery, Backend, LeaseState, MemBackend};
use hop_discovery::{Claim, Discovery, RenewOp};
use replica_sqlite::{FileId, OpenFlags, Storage};
/// De kortste lease, zoals de HopOS-bewoner: een renew en een trage lees
/// passen er dan nog in.
pub const MIN_TTL_MS: u64 = 15_000;
/// Wat een claim deze node maakt.
pub enum Role {
    /// Deze node houdt de lease en is de enige schrijver.
    Writer(Writer),
    /// Een ander houdt de lease; deze node mag alleen lezen.
    Reader {
        /// De houder volgens de lease, als die gelezen kon worden.
        leader: Option<String>,
    },
}
/// Het bewijs dat deze node de lease houdt; alleen [`claim`] maakt er een.
pub struct Writer {
    discovery: Discovery,
    ttl_ms: u64,
    generation: u64,
    confirmed: u64,
    lost: bool,
}
fn millis(now: Time) -> Result<u64> {
    if now.seconds() < 1_577_836_800 {
        return Err(Error::State);
    }
    u64::try_from(now.seconds())
        .ok()
        .and_then(|s| s.checked_mul(1000))
        .and_then(|ms| ms.checked_add(u64::from(now.nanos() / 1_000_000)))
        .ok_or(Error::Limit)
}
fn unreachable(e: hop_discovery::Error) -> Error {
    match e {
        hop_discovery::Error::LeaseHeld => Error::LeaseLost,
        hop_discovery::Error::NoLease | hop_discovery::Error::Unreachable => {
            Error::Object(StoreError::Transport)
        }
    }
}
/// Claimt de lease voor `node` (bijvoorbeeld het adres van deze node).
///
/// De eigenaar krijgt een vers 96-bit achtervoegsel: een herstarte node is
/// een nieuwe eigenaar en wacht de lease van zijn vorige leven af, zodat
/// twee processen op één node nooit allebei schrijver zijn.
pub fn claim<B: Storage, L: Backend>(
    b: &mut B,
    lease: &mut L,
    node: &str,
    ttl_ms: u64,
    now: Time,
) -> Result<Role> {
    if ttl_ms < MIN_TTL_MS || node.is_empty() || node.len() > 96 || node.contains('#') {
        return Err(Error::State);
    }
    let at = millis(now)?;
    let mut bytes = [0; 32];
    b.random(&mut bytes[..12])?;
    let encoded = hex(&bytes);
    let suffix = core::str::from_utf8(&encoded[..24]).map_err(|_| Error::State)?;
    let mut discovery = Discovery::new(format!("{node}#{suffix}"), ttl_ms);
    let read = lease.read();
    let leader = match &read {
        Ok((state, _)) if !state.owner.is_empty() => Some(string(&state.owner)?),
        _ => None,
    };
    match discovery.plan_claim(read, at) {
        Claim::Write { prev, state } => {
            let written = lease.write(&prev, &state);
            match discovery.on_write(&state, written) {
                Ok(()) => Ok(Role::Writer(Writer {
                    discovery,
                    ttl_ms,
                    generation: state.generation,
                    confirmed: at,
                    lost: false,
                })),
                // Een andere node won de race op dezelfde handle.
                Err(hop_discovery::Error::LeaseHeld) => Ok(Role::Reader { leader }),
                Err(e) => Err(unreachable(e)),
            }
        }
        Claim::Fail(hop_discovery::Error::LeaseHeld) => Ok(Role::Reader { leader }),
        Claim::Fail(e) => Err(unreachable(e)),
    }
}
impl Writer {
    /// Het fencing-token: stijgt bij elke overname, nooit bij een renew.
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// De eigenaar in de lease: `node#achtervoegsel`.
    pub fn owner(&self) -> &str {
        self.discovery.node_addr()
    }
    /// Is de lease blijvend verloren (een andere houder)?
    pub fn is_lost(&self) -> bool {
        self.lost
    }
    /// Het laatste moment (Unix-milliseconden) waarop deze node mag schrijven.
    pub fn deadline(&self) -> u64 {
        self.confirmed
            .saturating_add(self.ttl_ms)
            .saturating_sub(self.ttl_ms / 3)
    }
    /// Een harde fout zodra de lease verloren is of de deadline voorbij. Een
    /// bevestiging mag iets vóór `now` liggen: de eigenaar meet `now` aan het
    /// begin van een beurt, een vernieuwing midden in een capture meet later
    /// (Spin 06-10-2026: LeaseLost direct na een geslaagde capture). Meer dan
    /// een zesde TTL terug is wel een klok die terugspringt.
    pub fn check(&self, now: Time) -> Result {
        let at = millis(now)?;
        if self.lost || self.confirmed.saturating_sub(at) > self.ttl_ms / 6 || at >= self.deadline()
        {
            return Err(Error::LeaseLost);
        }
        Ok(())
    }
    /// Vernieuwt op leeftijd: hoogstens eens per zesde lease een voorwaardelijke
    /// schrijf. Roep dit ook aan terwijl opslag of netwerk wacht.
    ///
    /// Een onbereikbare store is een transportfout; de schrijver blijft geldig
    /// tot [`Writer::deadline`]. Een andere houder is blijvend verlies.
    pub fn renew<L: Backend>(&mut self, lease: &mut L, now: Time) -> Result {
        let Some(renewal) = self.renew_request(now)? else {
            return Ok(());
        };
        let written = lease.write(&renewal.prev, &renewal.state);
        match self.renew_commit(&renewal, written)? {
            Renewed::Done => Ok(()),
            Renewed::Reclaim => {
                let read = lease.read();
                match self.reclaim_request(read, renewal.at)? {
                    Some(again) => {
                        let written = lease.write(&again.prev, &again.state);
                        self.reclaim_commit(&again, written)
                    }
                    None => Ok(()),
                }
            }
        }
    }
    /// De vernieuwing in stappen, voor een host die de store-I/O buiten een
    /// lening op de Writer wil doen (een gedeelde Writer; een stack die in de
    /// PUT parkeert). `None`: nog niet aan de beurt. Daarna [`Writer::renew_commit`].
    pub fn renew_request(&mut self, now: Time) -> Result<Option<Renewal>> {
        if self.check(now).is_err() {
            self.lost = true;
            return Err(Error::LeaseLost);
        }
        let at = millis(now)?;
        if at.saturating_sub(self.confirmed) < self.ttl_ms / 6 {
            return Ok(None);
        }
        match self.discovery.renew_op(at) {
            RenewOp::Write { prev, state } => Ok(Some(Renewal { at, prev, state })),
            RenewOp::Claim => {
                self.lost = true;
                Err(Error::LeaseLost)
            }
        }
    }
    /// De uitkomst van `lease.write(&renewal.prev, &renewal.state)`. Een
    /// vorige PUT kan wel geland zijn zonder dat wij zijn ETag zagen: dan vraagt
    /// [`Renewed::Reclaim`] om een `lease.read()` en [`Writer::reclaim_request`].
    pub fn renew_commit(
        &mut self,
        renewal: &Renewal,
        written: hop_discovery::Result<String>,
    ) -> Result<Renewed> {
        match self.discovery.on_write(&renewal.state, written) {
            Ok(()) => {
                self.confirmed = renewal.at;
                Ok(Renewed::Done)
            }
            Err(hop_discovery::Error::LeaseHeld) => Ok(Renewed::Reclaim),
            Err(e) => Err(unreachable(e)),
        }
    }
    /// Alleen onze eigen eigenaar en generatie mogen verder; `None` als er
    /// niets te schrijven valt. Daarna `lease.write` en [`Writer::reclaim_commit`].
    pub fn reclaim_request(
        &mut self,
        read: hop_discovery::Result<(LeaseState, String)>,
        at: u64,
    ) -> Result<Option<Renewal>> {
        match &read {
            Ok((state, _))
                if state.owner == self.discovery.node_addr()
                    && state.generation == self.generation => {}
            Err(hop_discovery::Error::Unreachable) => {
                return Err(unreachable(hop_discovery::Error::Unreachable));
            }
            _ => {
                self.lost = true;
                return Err(Error::LeaseLost);
            }
        }
        match self.discovery.plan_claim(read, at) {
            Claim::Write { prev, state } => Ok(Some(Renewal { at, prev, state })),
            Claim::Fail(hop_discovery::Error::LeaseHeld) => {
                self.lost = true;
                Err(Error::LeaseLost)
            }
            Claim::Fail(e) => Err(unreachable(e)),
        }
    }
    /// De uitkomst van de herovering.
    pub fn reclaim_commit(
        &mut self,
        renewal: &Renewal,
        written: hop_discovery::Result<String>,
    ) -> Result {
        match self.discovery.on_write(&renewal.state, written) {
            Ok(()) => {
                self.confirmed = renewal.at;
                Ok(())
            }
            Err(hop_discovery::Error::LeaseHeld) => {
                self.lost = true;
                Err(Error::LeaseLost)
            }
            Err(e) => Err(unreachable(e)),
        }
    }
    /// Geeft de lease vrij, zodat een lezer meteen kan overnemen. Sluit SQL en
    /// publicatie eerst. Mislukt het, dan verloopt de lease vanzelf.
    pub fn release<L: Backend>(mut self, lease: &mut L) -> Result {
        let handle = self.discovery.take_handle();
        if handle.is_empty() {
            return Ok(());
        }
        match lease.delete(&handle) {
            // Al weg of al van een ander: niets meer van ons om op te ruimen.
            Ok(()) | Err(hop_discovery::Error::NoLease | hop_discovery::Error::LeaseHeld) => Ok(()),
            Err(e) => Err(unreachable(e)),
        }
    }
}
/// Eén voorwaardelijke schrijf van de lease, buiten de Writer om uit te voeren.
#[derive(Debug, PartialEq, Eq)]
pub struct Renewal {
    /// Het moment (Unix-milliseconden) waarop de vernieuwing is gepland.
    pub at: u64,
    /// De handle waarop de schrijf voorwaardelijk is.
    pub prev: String,
    /// De lease die geschreven wordt.
    pub state: LeaseState,
}
/// Wat een bevestigde vernieuwing nog vraagt.
#[derive(Debug, PartialEq, Eq)]
pub enum Renewed {
    /// De lease is tot de volgende deadline bevestigd.
    Done,
    /// Een eerdere PUT landde zonder gezien ETag: lees de lease en herover hem.
    Reclaim,
}
/// De opslag van een lezer: alleen lezen, iedere write is `SQLITE_READONLY`.
///
/// Open SQLite ook zelf read-only; dit is de grens eronder, voor het geval
/// een verbinding of pragma toch probeert te schrijven.
pub struct ReadOnly<'a, B: Storage>(pub &'a mut B);
impl<B: Storage> Storage for ReadOnly<'_, B> {
    fn cooperate(&mut self) -> replica_sqlite::Result {
        self.0.cooperate()
    }
    fn open(&mut self, name: &CStr, flags: OpenFlags) -> replica_sqlite::Result<FileId> {
        if flags.is_writable() || flags.is_create() {
            return Err(replica_sqlite::Error::READONLY);
        }
        self.0.open(name, flags)
    }
    fn close(&mut self, file: FileId) -> replica_sqlite::Result {
        self.0.close(file)
    }
    fn read(&mut self, file: FileId, offset: u64, dst: &mut [u8]) -> replica_sqlite::Result<usize> {
        self.0.read(file, offset, dst)
    }
    fn write(&mut self, _: FileId, _: u64, _: &[u8]) -> replica_sqlite::Result {
        Err(replica_sqlite::Error::READONLY)
    }
    fn truncate(&mut self, _: FileId, _: u64) -> replica_sqlite::Result {
        Err(replica_sqlite::Error::READONLY)
    }
    fn sync(&mut self, _: FileId, _: i32) -> replica_sqlite::Result {
        Err(replica_sqlite::Error::READONLY)
    }
    fn size(&mut self, file: FileId) -> replica_sqlite::Result<u64> {
        self.0.size(file)
    }
    fn remove(&mut self, _: &CStr, _: bool) -> replica_sqlite::Result {
        Err(replica_sqlite::Error::READONLY)
    }
    fn exists(&mut self, name: &CStr) -> replica_sqlite::Result<bool> {
        self.0.exists(name)
    }
    fn random(&mut self, dst: &mut [u8]) -> replica_sqlite::Result {
        self.0.random(dst)
    }
    fn unix_millis(&mut self) -> replica_sqlite::Result<i64> {
        self.0.unix_millis()
    }
}
