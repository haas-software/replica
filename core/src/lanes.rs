//! Het store-werk van een eigenaar op eigen stacks: de banen.
//!
//! Een S3-call parkeert de stack die hem doet. Een host die zijn verzoeken
//! op dezelfde stack bedient als zijn eigenaar (één verzoek tegelijk), houdt
//! dan elk verzoek op zolang een publicatie, een onderhoudsbeurt of een
//! leasevernieuwing op S3 wacht: tot de time-out van een hangende verbinding
//! (EasyFlor op de M4, 20-11-2026: "na ~30 verzoeken staat alles 30 tot 60 s
//! stil"). Met banen houdt de eigenaar alleen wat rust vraagt (de capture
//! naar de spool, het manifest na een gelande upload); de uploads, het
//! onderhoud en de lease gaan naar de banen, die het op hun eigen stack doen.
//!
//! - [`Lane::Lease`]: de schrijverlease, één kleine voorwaardelijke call per
//!   paar seconden, nooit achter een upload (een hangende segment-PUT liet
//!   de lease anders verlopen).
//! - [`Lane::Data`]: de uploads van de gecapturede pagina's en het onderhoud
//!   (listings, manifesten, compactie).
//!
//! De host levert de banen ([`Offload`]); `replica_hopos::lanes` doet dat op
//! HopOS met twee `applib::stacktask`s per eigenaarslot. Een [`Job`] krijgt de
//! [`LaneSide`] van zijn baan: de lokale opslag en de store van die stack.
//! Wat een job met de eigenaar deelt, zijn `Rc<RefCell<..>>`'s die nooit over
//! een store- of opslagcall heen geleend worden (het rustprotocol van
//! [`crate::owner`]). [`crate::group::Group`] gebruikt dit.
use crate::Result;
use crate::object::{Object, Parallel, Replies, Store, StoreError};
use crate::writer::Backend;
use alloc::{boxed::Box, string::String, vec::Vec};
use core::ffi::CStr;
use replica_sqlite::{FileId, OpenFlags, Storage};
/// Welke baan een [`Job`] draait: elke baan doet zijn jobs één voor één, op volgorde.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    /// De schrijverlease: één kleine voorwaardelijke call per paar seconden,
    /// nooit in de rij achter een upload.
    Lease,
    /// De uploads van de gecapturede pagina's en het onderhoud.
    Data,
}
/// Store-werk buiten de stack van de eigenaar; het krijgt de [`LaneSide`]
/// van zijn baan.
pub type Job = Box<dyn FnOnce(&mut dyn LaneSide)>;
/// Waar het store-werk van een eigenaar loopt terwijl die verzoeken bedient.
pub trait Offload {
    /// Zet `job` in de rij van `lane`; hij loopt na de jobs die er al stonden,
    /// op de stack van die baan.
    fn push(&self, lane: Lane, job: Job);
    /// Laat de banen even lopen (de eigenaar geeft één keer voorrang):
    /// tijdens een lang statement, nadat de lease in de rij ging, zodat de
    /// vernieuwing begint terwijl het statement doorloopt.
    fn breathe(&self) {}
    /// Wacht tot elke job die er nu staat gelopen heeft, hoogstens `ms`
    /// milliseconden (de eigenaar parkeert: alleen vóór een sluiting).
    /// `true`: de banen zijn leeg.
    fn settle(&self, ms: u64) -> bool;
}
/// Wat een [`Job`] op zijn baan krijgt: de lokale opslag en de store van die stack.
pub trait LaneSide {
    /// Roept `f` aan met een opslag op de lokale map van `domain` en de store.
    fn files(
        &mut self,
        domain: &str,
        f: &mut dyn FnMut(&mut dyn Storage, &mut dyn Store),
    ) -> Result;
    /// Roept `f` aan met de schrijverlease op `key` in de store.
    fn lease(
        &mut self,
        key: &str,
        ttl_ms: u64,
        f: &mut dyn FnMut(&mut dyn Backend),
    ) -> core::result::Result<(), StoreError>;
}
/// Een `dyn Storage` als een sized, voor de generieke stappen van deze crate.
pub struct DynStorage<'a>(pub &'a mut dyn Storage);
impl Storage for DynStorage<'_> {
    fn cooperate(&mut self) -> replica_sqlite::Result {
        self.0.cooperate()
    }
    fn open(&mut self, name: &CStr, flags: OpenFlags) -> replica_sqlite::Result<FileId> {
        self.0.open(name, flags)
    }
    fn close(&mut self, file: FileId) -> replica_sqlite::Result {
        self.0.close(file)
    }
    fn read(&mut self, file: FileId, offset: u64, dst: &mut [u8]) -> replica_sqlite::Result<usize> {
        self.0.read(file, offset, dst)
    }
    fn write(&mut self, file: FileId, offset: u64, src: &[u8]) -> replica_sqlite::Result {
        self.0.write(file, offset, src)
    }
    fn truncate(&mut self, file: FileId, size: u64) -> replica_sqlite::Result {
        self.0.truncate(file, size)
    }
    fn sync(&mut self, file: FileId, flags: i32) -> replica_sqlite::Result {
        self.0.sync(file, flags)
    }
    fn size(&mut self, file: FileId) -> replica_sqlite::Result<u64> {
        self.0.size(file)
    }
    fn remove(&mut self, name: &CStr, sync_directory: bool) -> replica_sqlite::Result {
        self.0.remove(name, sync_directory)
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
/// Een `dyn Store` als een sized, voor de generieke stappen van deze crate.
pub struct DynStore<'a>(pub &'a mut dyn Store);
impl Store for DynStore<'_> {
    fn put(&mut self, key: &str, bytes: &[u8]) -> core::result::Result<(), StoreError> {
        self.0.put(key, bytes)
    }
    fn get(&mut self, key: &str, limit: usize) -> core::result::Result<Vec<u8>, StoreError> {
        self.0.get(key, limit)
    }
    fn parallel(&self) -> Parallel {
        self.0.parallel()
    }
    fn get_all(&mut self, keys: &[(&str, usize)]) -> core::result::Result<Replies, StoreError> {
        self.0.get_all(keys)
    }
    fn delete(&mut self, key: &str) -> core::result::Result<(), StoreError> {
        self.0.delete(key)
    }
    fn list(
        &mut self,
        prefix: &str,
        limit: usize,
    ) -> core::result::Result<Vec<Object>, StoreError> {
        self.0.list(prefix, limit)
    }
    fn directories(
        &mut self,
        prefix: &str,
        limit: usize,
    ) -> core::result::Result<Vec<String>, StoreError> {
        self.0.directories(prefix, limit)
    }
    fn list_batch(
        &mut self,
        prefix: &str,
        limit: usize,
    ) -> core::result::Result<Vec<Object>, StoreError> {
        self.0.list_batch(prefix, limit)
    }
}
