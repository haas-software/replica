//! LZ: een LZ77-codec voor databasepagina's, in veilige Rust en zonder tabellen van buiten.
//!
//! Het blokformaat is dat van LZ4 (reeksen van letterlijke bytes plus een
//! verwijzing naar eerdere uitvoer), zonder LZ4's eindregels: een blok eindigt
//! na de letterlijke bytes van de laatste reeks.
//!
//! ```text
//! blok   := reeks* slot
//! reeks  := token [lit+] lit-bytes offset:u16le [match+]
//! slot   := token(matchnibble 0) [lit+] lit-bytes          (einde van de invoer)
//! token  := (lit-lengte 0..15) << 4 | (match-lengte - 4: 0..15)
//! lit+   := 255* rest    (alleen bij nibble 15; de bytes tellen op)
//! ```
//!
//! Een verwijzing kopieert `match` bytes vanaf `offset` terug in de uitvoer;
//! `offset` 1..=65535, minimaal 4 bytes. De invoer van één blok is hoogstens
//! [`MAX_INPUT`] bytes (de grootste SQLite-pagina).
//!
//! Decoderen leest alleen met gecontroleerde grenzen: beschadigde of
//! kwaadwillige invoer geeft [`Corrupt`], nooit een paniek of een lees/schrijf
//! buiten de buffers. De encoder zoekt via een hashketen (diepte [`DEPTH`]) en
//! kijkt één positie vooruit of een langere overeenkomst wacht ("lazy").
//! Meetwaarden staan in `docs/compress.md`.
use core::fmt;

/// De grootste invoer van één blok: een SQLite-pagina van 64 KiB.
pub const MAX_INPUT: usize = 1 << 16;
/// Kortste verwijzing.
const MIN_MATCH: usize = 4;
/// Verste terugverwijzing.
const MAX_OFFSET: usize = 65535;
/// Bits van de hashtabel: 16 Ki ingangen.
const HASH_BITS: u32 = 14;
/// Kandidaten per positie langs de hashketen.
pub const DEPTH: usize = 16;
/// Een overeenkomst van deze lengte is goed genoeg: niet verder zoeken.
const GOOD_ENOUGH: usize = 128;

/// De invoer is geen geldig blok voor deze uitvoerbuffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Corrupt;
impl fmt::Display for Corrupt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("lz: corrupt block")
    }
}
impl core::error::Error for Corrupt {}

/// De werktabellen van de encoder; één keer gealloceerd, herbruikbaar.
///
/// # Invariants
/// `head` heeft `1 << HASH_BITS` ingangen, `chain` [`MAX_INPUT`]. Een
/// ingang in `head` is een positie plus één (nul is leeg) uit de lopende
/// aanroep; `chain[i]` is de afstand naar de vorige positie met dezelfde hash
/// (nul: geen), alleen geldig voor posities die deze aanroep invoegde.
pub struct Encoder {
    head: alloc::vec::Vec<u32>,
    chain: alloc::vec::Vec<u16>,
}

impl Encoder {
    /// De tabellen (192 KiB) faalbaar alloceren.
    pub fn new() -> Option<Self> {
        let mut head = alloc::vec::Vec::new();
        head.try_reserve_exact(1 << HASH_BITS).ok()?;
        head.resize(1 << HASH_BITS, 0);
        let mut chain = alloc::vec::Vec::new();
        chain.try_reserve_exact(MAX_INPUT).ok()?;
        chain.resize(MAX_INPUT, 0);
        // INVARIANT: beide tabellen hebben hun vaste maat.
        Some(Self { head, chain })
    }

    /// Comprimeert `src` naar `dst`; `None` als het niet in `dst` past (geef dan
    /// een `dst` kleiner dan `src` om onsamendrukbare invoer vroeg te herkennen)
    /// of als `src` groter is dan [`MAX_INPUT`].
    pub fn compress(&mut self, src: &[u8], dst: &mut [u8]) -> Option<usize> {
        if src.len() > MAX_INPUT {
            return None;
        }
        self.head.fill(0);
        let mut out = Out { dst, at: 0 };
        let n = src.len();
        let mut anchor = 0;
        let mut i = 0;
        // Een overeenkomst moet MIN_MATCH bytes vóór het einde kunnen lezen.
        let limit = n.saturating_sub(MIN_MATCH);
        while i < limit {
            let Some((mut off, mut len)) = self.find(src, i) else {
                self.insert(src, i);
                i += 1;
                continue;
            };
            self.insert(src, i);
            // Lazy: wacht er één positie verder een duidelijk langere?
            let mut start = i;
            while start + 1 < limit {
                match self.find(src, start + 1) {
                    Some((o2, l2)) if l2 > len => {
                        self.insert(src, start + 1);
                        start += 1;
                        off = o2;
                        len = l2;
                    }
                    _ => break,
                }
            }
            out.sequence(src.get(anchor..start)?, off, len)?;
            let end = start + len;
            // De posities binnen de overeenkomst voeden de keten (behalve de
            // laatste paar, die geen vier bytes meer hebben).
            let mut j = start + 1;
            while j < end && j < limit {
                self.insert(src, j);
                j += 1;
            }
            i = end;
            anchor = end;
        }
        out.last(src.get(anchor..)?)?;
        Some(out.at)
    }

    /// De hash van vier bytes op `i`.
    fn hash(src: &[u8], i: usize) -> Option<usize> {
        let b = src.get(i..i + 4)?;
        let v = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
        Some((v.wrapping_mul(2_654_435_761) >> (32 - HASH_BITS)) as usize)
    }

    fn insert(&mut self, src: &[u8], i: usize) {
        let Some(h) = Self::hash(src, i) else {
            return;
        };
        let (Some(head), Some(link)) = (self.head.get_mut(h), self.chain.get_mut(i)) else {
            return;
        };
        let prev = *head;
        // Een positie plus één; nul is leeg. `i` < MAX_INPUT past in u32.
        *link = if prev == 0 {
            0
        } else {
            u16::try_from(i + 1 - prev as usize).unwrap_or(0)
        };
        *head = (i + 1) as u32;
    }

    /// De langste eerdere overeenkomst voor positie `i`: (offset, lengte).
    fn find(&self, src: &[u8], i: usize) -> Option<(usize, usize)> {
        let h = Self::hash(src, i)?;
        let mut cand = (*self.head.get(h)? as usize).checked_sub(1)?;
        let mut best = (0, MIN_MATCH - 1);
        let rest = src.get(i..)?;
        for _ in 0..DEPTH {
            let off = i.checked_sub(cand)?;
            if off == 0 || off > MAX_OFFSET {
                break;
            }
            let from = src.get(cand..)?;
            // Eerst de byte die een betere overeenkomst moet hebben.
            if from.get(best.1) == rest.get(best.1) {
                let len = common(from, rest);
                if len > best.1 {
                    best = (off, len);
                    if len >= GOOD_ENOUGH {
                        break;
                    }
                }
            }
            let step = *self.chain.get(cand)? as usize;
            if step == 0 {
                break;
            }
            cand = cand.checked_sub(step)?;
        }
        (best.1 >= MIN_MATCH).then_some(best)
    }
}

/// Hoeveel bytes `a` en `b` vanaf het begin gelijk zijn, acht tegelijk.
fn common(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i + 8 <= n {
        let (Some(x), Some(y)) = (a.get(i..i + 8), b.get(i..i + 8)) else {
            break;
        };
        let x = u64::from_le_bytes([x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]]);
        let y = u64::from_le_bytes([y[0], y[1], y[2], y[3], y[4], y[5], y[6], y[7]]);
        let diff = x ^ y;
        if diff != 0 {
            return i + (diff.trailing_zeros() / 8) as usize;
        }
        i += 8;
    }
    while i < n && a.get(i) == b.get(i) {
        i += 1;
    }
    i
}

/// De uitvoer van de encoder; elke schrijfactie controleert de ruimte.
struct Out<'a> {
    dst: &'a mut [u8],
    at: usize,
}

impl Out<'_> {
    fn byte(&mut self, b: u8) -> Option<()> {
        *self.dst.get_mut(self.at)? = b;
        self.at += 1;
        Some(())
    }
    fn bytes(&mut self, src: &[u8]) -> Option<()> {
        let end = self.at.checked_add(src.len())?;
        self.dst.get_mut(self.at..end)?.copy_from_slice(src);
        self.at = end;
        Some(())
    }
    /// De verlenging van een nibble van 15: 255-en plus de rest.
    fn length(&mut self, mut n: usize) -> Option<()> {
        while n >= 255 {
            self.byte(255)?;
            n -= 255;
        }
        self.byte(n as u8)
    }
    fn sequence(&mut self, literals: &[u8], offset: usize, len: usize) -> Option<()> {
        let lit = literals.len();
        let m = len - MIN_MATCH;
        self.byte(((lit.min(15) as u8) << 4) | m.min(15) as u8)?;
        if lit >= 15 {
            self.length(lit - 15)?;
        }
        self.bytes(literals)?;
        self.bytes(&u16::try_from(offset).ok()?.to_le_bytes())?;
        if m >= 15 {
            self.length(m - 15)?;
        }
        Some(())
    }
    fn last(&mut self, literals: &[u8]) -> Option<()> {
        let lit = literals.len();
        self.byte((lit.min(15) as u8) << 4)?;
        if lit >= 15 {
            self.length(lit - 15)?;
        }
        self.bytes(literals)
    }
}

/// Leest een verlenging van een nibble van 15 vanaf `*at`.
fn length(src: &[u8], at: &mut usize, cap: usize) -> Result<usize, Corrupt> {
    let mut n = 0usize;
    loop {
        let b = *src.get(*at).ok_or(Corrupt)?;
        *at += 1;
        n = n.checked_add(usize::from(b)).ok_or(Corrupt)?;
        if n > cap {
            return Err(Corrupt);
        }
        if b != 255 {
            return Ok(n);
        }
    }
}

/// Decodeert `src` naar het begin van `dst`; het aantal geschreven bytes.
///
/// Een blok dat meer uitvoer wil dan `dst` heeft, of buiten zijn eigen
/// uitvoer terugverwijst, is [`Corrupt`]. De aanroeper vergelijkt het aantal
/// met de verwachte maat (een pagina).
pub fn decompress(src: &[u8], dst: &mut [u8]) -> Result<usize, Corrupt> {
    let cap = dst.len();
    let mut s = 0usize;
    let mut d = 0usize;
    loop {
        let token = *src.get(s).ok_or(Corrupt)?;
        s += 1;
        let mut lit = usize::from(token >> 4);
        if lit == 15 {
            lit += length(src, &mut s, cap)?;
        }
        let s_end = s.checked_add(lit).ok_or(Corrupt)?;
        let d_end = d.checked_add(lit).ok_or(Corrupt)?;
        dst.get_mut(d..d_end)
            .ok_or(Corrupt)?
            .copy_from_slice(src.get(s..s_end).ok_or(Corrupt)?);
        s = s_end;
        d = d_end;
        if s == src.len() {
            // Het slot heeft geen verwijzing.
            return if token & 15 == 0 { Ok(d) } else { Err(Corrupt) };
        }
        let o = src.get(s..s + 2).ok_or(Corrupt)?;
        let offset = usize::from(u16::from_le_bytes([o[0], o[1]]));
        s += 2;
        let mut len = usize::from(token & 15);
        if len == 15 {
            len += length(src, &mut s, cap)?;
        }
        len += MIN_MATCH;
        if offset == 0 || offset > d {
            return Err(Corrupt);
        }
        let end = d.checked_add(len).ok_or(Corrupt)?;
        if end > cap {
            return Err(Corrupt);
        }
        copy_back(dst, d, offset, len);
        d = end;
    }
}

/// Kopieert `len` bytes van `d - offset` naar `d`; overlap herhaalt het patroon.
///
/// De aanroeper bewees `0 < offset <= d` en `d + len <= dst.len()`.
fn copy_back(dst: &mut [u8], d: usize, offset: usize, len: usize) {
    let from = d - offset;
    if offset >= len {
        dst.copy_within(from..from + len, d);
        return;
    }
    // Overlap: telkens wat er al staat, verdubbelend; bron en doel van één
    // kopie overlappen nooit (n <= offset + done).
    let mut done = 0;
    while done < len {
        let n = (len - done).min(offset + done);
        dst.copy_within(from..from + n, d + done);
        done += n;
    }
}

/// De grootste uitvoer van een blok van `n` bytes (alles letterlijk).
pub const fn bound(n: usize) -> usize {
    n + n / 255 + 16
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use alloc::vec::Vec;

    fn roundtrip(src: &[u8]) -> usize {
        let mut e = Encoder::new().unwrap();
        let mut c = vec![0u8; bound(src.len())];
        let n = e.compress(src, &mut c).unwrap();
        let mut d = vec![0u8; src.len()];
        assert_eq!(decompress(&c[..n], &mut d), Ok(src.len()));
        assert_eq!(d, src);
        n
    }

    #[test]
    fn edges() {
        assert_eq!(roundtrip(&[]), 1);
        roundtrip(&[7]);
        roundtrip(&[1, 2, 3, 4]);
        roundtrip(&[0; 5]);
        assert!(roundtrip(&[0; 4096]) < 40);
        assert!(roundtrip(&vec![0; MAX_INPUT]) < 300);
    }

    #[test]
    fn too_big() {
        let mut e = Encoder::new().unwrap();
        let src = vec![0u8; MAX_INPUT + 1];
        let mut c = vec![0u8; bound(src.len())];
        assert_eq!(e.compress(&src, &mut c), None);
    }

    #[test]
    fn overlap_pattern() {
        let mut v = Vec::new();
        for i in 0..5000 {
            v.push((i % 3) as u8);
        }
        roundtrip(&v);
    }

    #[test]
    fn small_dst_is_none() {
        let mut e = Encoder::new().unwrap();
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        let src: Vec<u8> = (0..4096)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect();
        let mut c = vec![0u8; 4095];
        assert_eq!(e.compress(&src, &mut c), None);
    }
}
