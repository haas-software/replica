//! Segmenten: paginabeelden met databasegrootte en SHA-256 van de hele body.
//!
//! - `SPINSEG1`: `magic | page_size u32 | database_size u64 | count u32 |
//!   (nummer u32, pagina)* | sha256`, alles big-endian.
//! - `SPINSEG2` (gecomprimeerd): dezelfde kop, per pagina `nummer u32 |
//!   lengte u32 | bytes`; lengte gelijk aan de paginamaat is rauw, nul is een
//!   nulpagina, daartussen een LZ-blok ([`replica_sqlite::lz`]) dat precies één
//!   pagina oplevert. Elke pagina is los te decoderen.
//!
//! Lezen accepteert beide; welke een capture schrijft, kiest de eigenaar
//! ([`Segments::compress`]); een compactie schrijft `SPINSEG2` zodra één
//! invoersegment dat is.
use crate::{Error, Result, hash, reserve};
use alloc::vec::Vec;
use replica_sqlite::lz;
/// Eén segment mag maximaal 32 MiB kosten, vóór enige allocatie gecontroleerd.
pub const MAX_BYTES: usize = 32 << 20;
const HEADER: usize = 24;
const PLAIN: &[u8; 8] = b"SPINSEG1";
const PACKED: &[u8; 8] = b"SPINSEG2";

/// Hoe een capture segmenten schrijft.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Segments {
    /// Paginabytes per segment (vóór compressie): 64 KiB..16 MiB.
    pub bytes: usize,
    /// `SPINSEG2` in plaats van `SPINSEG1`.
    pub compress: bool,
}
impl From<usize> for Segments {
    /// Ongecomprimeerd (`SPINSEG1`).
    fn from(bytes: usize) -> Self {
        Self {
            bytes,
            compress: false,
        }
    }
}

/// Een gevalideerd, geleend segment; de bytes kunnen tijdens gebruik niet wijzigen.
#[derive(Debug)]
pub struct Segment<'a> {
    bytes: &'a [u8],
    page_size: u32,
    database_size: u64,
    count: usize,
    packed: bool,
}
/// Eén pagina uit een segment, nog in zijn opgeslagen vorm.
#[derive(Clone, Copy, Debug)]
pub struct Page<'a> {
    stored: &'a [u8],
    page_size: u32,
}
impl<'a> Page<'a> {
    /// Decodeert de pagina naar `out` (precies `page_size` bytes).
    pub fn read(&self, out: &mut [u8]) -> Result {
        if out.len() != self.page_size as usize {
            return Err(Error::State);
        }
        if self.stored.is_empty() {
            out.fill(0);
        } else if self.stored.len() == out.len() {
            out.copy_from_slice(self.stored);
        } else if lz::decompress(self.stored, out) != Ok(out.len()) {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
    /// De opgeslagen bytes (rauw, LZ of leeg).
    pub fn stored(&self) -> &'a [u8] {
        self.stored
    }
}
/// Geldige SQLite-paginamaten in een segment.
pub const fn valid_page_size(n: u32) -> bool {
    n >= 512 && n <= 65536 && n.is_power_of_two()
}
fn be32(b: &[u8], at: usize) -> Option<u32> {
    let s = b.get(at..at.checked_add(4)?)?;
    Some(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}
impl<'a> Segment<'a> {
    /// Controleert lengtes, hash, pagina-identiteiten en duplicaten vóór gebruik.
    pub fn decode(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() > MAX_BYTES {
            return Err(Error::Limit);
        }
        if bytes.len() < 56 {
            return Err(Error::Corrupt);
        }
        let packed = match &bytes[..8] {
            m if m == PLAIN => false,
            m if m == PACKED => true,
            _ => return Err(Error::Corrupt),
        };
        let page_size = be32(bytes, 8).ok_or(Error::Corrupt)?;
        let database_size =
            u64::from_be_bytes(bytes[12..20].try_into().map_err(|_| Error::Corrupt)?);
        let count = be32(bytes, 20).ok_or(Error::Corrupt)? as usize;
        if !valid_page_size(page_size)
            || database_size > i64::MAX as u64
            || !database_size.is_multiple_of(u64::from(page_size))
        {
            return Err(Error::Corrupt);
        }
        let size = if packed {
            // Per pagina acht bytes kop plus hoogstens een pagina.
            let mut at = HEADER;
            for _ in 0..count {
                let len = be32(bytes, at + 4).ok_or(Error::Corrupt)? as usize;
                if len > page_size as usize {
                    return Err(Error::Corrupt);
                }
                at = at
                    .checked_add(8 + len)
                    .filter(|&n| n <= bytes.len())
                    .ok_or(Error::Corrupt)?;
            }
            at.checked_add(32).ok_or(Error::Corrupt)?
        } else {
            count
                .checked_mul(page_size as usize + 4)
                .and_then(|n| n.checked_add(56))
                .ok_or(Error::Corrupt)?
        };
        if size != bytes.len() || hash(&bytes[..size - 32]) != bytes[size - 32..] {
            return Err(Error::Corrupt);
        }
        let out = Self {
            bytes,
            page_size,
            database_size,
            count,
            packed,
        };
        check_numbers(page_size, database_size, out.pages().map(|(n, _)| n))?;
        Ok(out)
    }
    /// Lengte van elke pagina in bytes.
    pub const fn page_size(&self) -> u32 {
        self.page_size
    }
    /// Bestandsgrootte nadat alle pagina's uit dit segment zijn toegepast.
    pub const fn database_size(&self) -> u64 {
        self.database_size
    }
    /// `SPINSEG2`.
    pub const fn is_compressed(&self) -> bool {
        self.packed
    }
    /// Pagina's in de oorspronkelijke volgorde; nummering begint bij één.
    pub fn pages(&self) -> Pages<'a> {
        Pages {
            bytes: self.bytes,
            at: HEADER,
            left: self.count,
            page_size: self.page_size,
            packed: self.packed,
        }
    }
}
/// De pagina's van een [`Segment`], in volgorde.
pub struct Pages<'a> {
    bytes: &'a [u8],
    at: usize,
    left: usize,
    page_size: u32,
    packed: bool,
}
impl<'a> Iterator for Pages<'a> {
    type Item = (u32, Page<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        let number = be32(self.bytes, self.at)?;
        let (start, len) = if self.packed {
            (self.at + 8, be32(self.bytes, self.at + 4)? as usize)
        } else {
            (self.at + 4, self.page_size as usize)
        };
        let stored = self.bytes.get(start..start.checked_add(len)?)?;
        self.at = start + len;
        self.left -= 1;
        Some((
            number,
            Page {
                stored,
                page_size: self.page_size,
            },
        ))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}
impl ExactSizeIterator for Pages<'_> {}

/// Schrijft exact `SPINSEG1`; ongeldige invoer levert geen segment.
pub fn encode(page_size: u32, database_size: u64, pages: &[(u32, &[u8])]) -> Result<Vec<u8>> {
    encode_with(page_size, database_size, pages, false)
}

/// Als [`encode`]; met `compress` als `SPINSEG2`.
pub fn encode_with(
    page_size: u32,
    database_size: u64,
    pages: &[(u32, &[u8])],
    compress: bool,
) -> Result<Vec<u8>> {
    if !valid_page_size(page_size)
        || database_size > i64::MAX as u64
        || !database_size.is_multiple_of(u64::from(page_size))
    {
        return Err(Error::Corrupt);
    }
    let bound = pages
        .len()
        .checked_mul(page_size as usize + if compress { 8 } else { 4 })
        .and_then(|n| n.checked_add(56))
        .ok_or(Error::Limit)?;
    if bound > MAX_BYTES {
        return Err(Error::Limit);
    }
    let mut out = Vec::new();
    reserve(&mut out, bound)?;
    out.extend_from_slice(if compress { PACKED } else { PLAIN });
    out.extend_from_slice(&page_size.to_be_bytes());
    out.extend_from_slice(&database_size.to_be_bytes());
    out.extend_from_slice(&(pages.len() as u32).to_be_bytes());
    let mut enc = if compress {
        Some(lz::Encoder::new().ok_or(Error::Memory)?)
    } else {
        None
    };
    let mut scratch = Vec::new();
    if compress {
        reserve(&mut scratch, page_size as usize)?;
        scratch.resize(page_size as usize - 1, 0);
    }
    for &(number, data) in pages {
        if data.len() != page_size as usize {
            return Err(Error::Corrupt);
        }
        out.extend_from_slice(&number.to_be_bytes());
        match enc.as_mut() {
            None => out.extend_from_slice(data),
            Some(_) if data.iter().all(|&b| b == 0) => out.extend_from_slice(&0u32.to_be_bytes()),
            Some(e) => match e.compress(data, &mut scratch) {
                Some(n) => {
                    out.extend_from_slice(&(n as u32).to_be_bytes());
                    out.extend_from_slice(scratch.get(..n).ok_or(Error::State)?);
                }
                None => {
                    out.extend_from_slice(&page_size.to_be_bytes());
                    out.extend_from_slice(data);
                }
            },
        }
    }
    // Wat decode controleert, is hier al waar door constructie (maat, nummers,
    // hash); zelf nog eens decoderen zou elk segment een tweede keer hashen
    // (GEMETEN 06-10 op de M4: 112 MB/s per sha256-pass, de capture 33 MB/s).
    check_numbers(page_size, database_size, pages.iter().map(|&(n, _)| n))?;
    out.extend_from_slice(&hash(&out));
    Ok(out)
}
/// Elke pagina nonnul, binnen de database en hoogstens één keer.
fn check_numbers(
    page_size: u32,
    database_size: u64,
    numbers: impl ExactSizeIterator<Item = u32>,
) -> Result {
    let mut seen = Vec::new();
    reserve(&mut seen, numbers.len())?;
    for number in numbers {
        if number == 0 || u64::from(number) * u64::from(page_size) > database_size {
            return Err(Error::Corrupt);
        }
        seen.push(number);
    }
    seen.sort_unstable();
    if seen.windows(2).any(|p| p[0] == p[1]) {
        return Err(Error::Corrupt);
    }
    Ok(())
}
