//! Replica's Go-compatibele opslagformaat en herstelcontracten.
//!
//! Een host gebruikt één van twee niveaus: [`owner::Replica`] voor één
//! database (met [`writer`] voor de lease), of [`group::Group`] voor meer
//! databases onder één lease, bovenop `owner::Replica` (met [`lanes`],
//! [`publications`] en [`follow`]). De modules eronder (`prepare`,
//! `replication`, `capture`, `restore`, `compact`, `maintenance`, `tracking`,
//! `manifest`, `segment`, ...) zijn de bouwstenen van die twee: publiek voor
//! de toetsen, de Go-formaten en de proefapps, maar intern; hun vorm kan
//! veranderen zonder dat een host die `owner` of `group` gebruikt iets merkt.
#![no_std]
#![forbid(unsafe_code)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]
extern crate alloc;
#[cfg(feature = "std")]
extern crate std;
pub mod archive;
pub mod capture;
pub mod compact;
pub mod coverage;
pub mod dirty;
pub mod follow;
pub mod group;
#[cfg(feature = "std")]
pub mod host;
pub mod lanes;
pub mod local;
pub mod maintenance;
pub mod manifest;
pub mod marker;
pub mod object;
pub mod owner;
pub mod prepare;
pub mod publications;
pub mod replication;
pub mod restore;
pub mod segment;
pub mod shadow;
pub mod time;
pub mod tracking;
pub mod writer;
use alloc::{string::String, vec::Vec};
/// Begrensde fouten; transportfouten worden nooit als bewezen corruptie behandeld.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Ongeldig of beschadigd gepubliceerd formaat.
    Corrupt,
    /// Het expliciete geheugen- of objectbudget is overschreden.
    Limit,
    /// Een fallibele allocatie is geweigerd.
    Memory,
    /// Er ontbreekt een aaneengesloten commit of databasepagina.
    Gap,
    /// Een versie zonder atomair commitmanifest.
    Legacy,
    /// Onjuiste lokale levensloop of configuratie.
    State,
    /// De database veranderde buiten de tracking-VFS; een nieuw snapshot is vereist.
    ForeignWrite,
    /// Lokale en remote gegevens hebben geen bewezen gezamenlijke herkomst.
    Unproven,
    /// De schrijverlease is niet meer bevestigd; de eigenaar moet stoppen met schrijven.
    LeaseLost,
    /// De store is in deze generatie verder door een andere schrijver, terwijl
    /// de lokale database eigen werk heeft dat nooit in de store kwam (of een
    /// bevestigd manifest dat door de fencing afviel). Prepare opent niet en
    /// laat de lokale bestanden en de store onaangeroerd: wie dit ziet, kiest
    /// tussen de twee. Zie [`prepare`] ("Werk van een ander").
    ForeignAhead,
    /// De gedeelde staat van eigenaar en VFS is op dit moment geleend: de
    /// SQL-kant staat midden in een callback. Het rustprotocol van
    /// [`owner`] is geschonden; niets is gewijzigd, probeer het in rust opnieuw.
    Busy,
    /// Een lokale SQLite/VFS-fout.
    Storage(replica_sqlite::Error),
    /// Objectopslag: niet verwarren met beschadigde committed bytes.
    Object(object::StoreError),
}
/// Resultaat van een formaat- of opslagbewerking.
pub type Result<T = ()> = core::result::Result<T, Error>;
impl From<replica_sqlite::Error> for Error {
    fn from(e: replica_sqlite::Error) -> Self {
        Self::Storage(e)
    }
}
pub(crate) fn reserve<T>(v: &mut Vec<T>, n: usize) -> Result {
    v.try_reserve_exact(n).map_err(|_| Error::Memory)
}
pub(crate) fn grow<T>(v: &mut Vec<T>, n: usize, limit: usize) -> Result {
    let needed = v
        .len()
        .checked_add(n)
        .filter(|n| *n <= limit)
        .ok_or(Error::Limit)?;
    if needed > v.capacity() {
        let target = needed
            .checked_next_power_of_two()
            .unwrap_or(limit)
            .max(8)
            .min(limit);
        reserve(v, target - v.len())?;
    }
    Ok(())
}
pub(crate) fn string(s: &str) -> Result<String> {
    let mut out = String::new();
    out.try_reserve_exact(s.len()).map_err(|_| Error::Memory)?;
    out.push_str(s);
    Ok(out)
}
/// SHA-256 van `data`, zoals delen en segmenten gehasht worden.
pub fn hash(data: &[u8]) -> [u8; 32] {
    leancrypto::sha256::Sha256::digest(data)
}
