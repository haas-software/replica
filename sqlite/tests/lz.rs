//! De LZ-codec: rondgang over randmaten en duizenden gegenereerde invoeren
//! (vaste seed), en beschadigde blokken die een fout geven in plaats van een
//! paniek of een toegang buiten de buffers.
//!
//! Met `REPLICA_LZ_SAMPLES=<bestand>:<paginamaat>` gaat ook een bestand met
//! echte pagina's (bijvoorbeeld een steekproef uit een tenantdatabase) rond.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
use replica_sqlite::lz::{self, Encoder, MAX_INPUT, bound};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn roundtrip(e: &mut Encoder, src: &[u8]) -> Vec<u8> {
    let mut c = vec![0u8; bound(src.len())];
    let n = e.compress(src, &mut c).expect("bound is enough");
    c.truncate(n);
    let mut d = vec![0u8; src.len()];
    assert_eq!(
        lz::decompress(&c, &mut d),
        Ok(src.len()),
        "len {}",
        src.len()
    );
    assert_eq!(d, src);
    // Een te kleine uitvoerbuffer is een fout, geen afgekapte uitvoer.
    if !src.is_empty() {
        let mut short = vec![0u8; src.len() - 1];
        assert!(lz::decompress(&c, &mut short).is_err());
    }
    c
}

/// Invoer met de vormen die pagina's hebben: runs, herhalingen op afstand,
/// JSON-tekst, ruis en nullen, door elkaar.
fn generate(rng: &mut Rng, len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(len);
    while v.len() < len {
        let n = (1 + rng.below(300) as usize).min(len - v.len());
        match rng.below(6) {
            0 => v.extend(std::iter::repeat_n(rng.next() as u8, n)),
            1 if !v.is_empty() => {
                let from = rng.below(v.len() as u64) as usize;
                for k in 0..n {
                    let b = v[from + k % (v.len() - from)];
                    v.push(b);
                }
            }
            2 => {
                let s = format!(
                    "{{\"Id\":\"debtors/{}\",\"City\":\"Aalsmeer\"}}",
                    rng.below(5000)
                );
                v.extend(s.bytes().take(n));
            }
            3 => v.extend((0..n).map(|_| rng.next() as u8)),
            _ => v.extend(std::iter::repeat_n(0, n)),
        }
    }
    v.truncate(len);
    v
}

#[test]
fn edge_sizes_roundtrip() {
    let mut e = Encoder::new().unwrap();
    let mut rng = Rng(0x1234_5678_9ABC_DEF1);
    for page in [512usize, 1024, 4096, 16384, 32768, MAX_INPUT] {
        for len in [0, 1, 2, 3, 4, 5, 15, 16, 19, 255, 256, 270, page - 1, page] {
            roundtrip(&mut e, &vec![0u8; len]);
            roundtrip(
                &mut e,
                &(0..len).map(|_| rng.next() as u8).collect::<Vec<_>>(),
            );
            roundtrip(&mut e, &generate(&mut rng, len));
        }
        // Nul is bijna niets; ruis past niet in minder dan de invoer.
        assert!(roundtrip(&mut e, &vec![0u8; page]).len() < 16 + page / 200);
        let noise: Vec<u8> = (0..page).map(|_| rng.next() as u8).collect();
        let mut c = vec![0u8; page - 1];
        assert_eq!(e.compress(&noise, &mut c), None);
    }
}

#[test]
fn generated_inputs_roundtrip() {
    let mut e = Encoder::new().unwrap();
    let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
    for _ in 0..3000 {
        let len = match rng.below(4) {
            0 => rng.below(64) as usize,
            1 => rng.below(4097) as usize,
            2 => rng.below(16385) as usize,
            _ => rng.below(MAX_INPUT as u64 + 1) as usize,
        };
        let src = generate(&mut rng, len);
        roundtrip(&mut e, &src);
    }
}

#[test]
fn damaged_blocks_are_errors_not_panics() {
    let mut e = Encoder::new().unwrap();
    let mut rng = Rng(0x0BAD_C0DE_0BAD_C0DE);
    let mut errors = 0usize;
    let mut total = 0usize;
    for _ in 0..400 {
        let len = 1 + rng.below(16384) as usize;
        let src = generate(&mut rng, len);
        let good = roundtrip(&mut e, &src);
        for _ in 0..25 {
            let mut bad = good.clone();
            match rng.below(5) {
                0 => {
                    let at = rng.below(bad.len() as u64) as usize;
                    bad[at] ^= 1 << rng.below(8);
                }
                1 => bad.truncate(rng.below(bad.len() as u64) as usize),
                2 => {
                    for _ in 0..1 + rng.below(16) {
                        bad.push(rng.next() as u8);
                    }
                }
                3 => {
                    let at = rng.below(bad.len() as u64) as usize;
                    for b in &mut bad[at..] {
                        *b = rng.next() as u8;
                    }
                }
                _ => {
                    // Lengtes die naar 255-verlengingen wijzen.
                    let at = rng.below(bad.len() as u64) as usize;
                    bad[at] = 0xFF;
                }
            }
            // Elke uitvoermaat: kleiner, gelijk, groter. Nooit een paniek.
            for cap in [len / 2, len, len + 100] {
                let mut out = vec![0u8; cap];
                total += 1;
                match lz::decompress(&bad, &mut out) {
                    Ok(n) => assert!(n <= cap),
                    Err(_) => errors += 1,
                }
            }
        }
    }
    // Pure ruis als blok.
    for _ in 0..2000 {
        let n = rng.below(2000) as usize;
        let junk: Vec<u8> = (0..n).map(|_| rng.next() as u8).collect();
        let mut out = vec![0u8; 16384];
        total += 1;
        if lz::decompress(&junk, &mut out).is_err() {
            errors += 1;
        }
    }
    eprintln!("damaged: {errors} of {total} errors");
    assert!(errors * 2 > total);
    // Bijzondere vormen.
    let mut out = vec![0u8; 64];
    assert!(lz::decompress(&[], &mut out).is_err());
    assert!(
        lz::decompress(&[0x01], &mut out).is_err(),
        "match nibble zonder offset"
    );
    assert!(
        lz::decompress(&[0x10, b'a', 0x00, 0x00], &mut out).is_err(),
        "offset 0"
    );
    assert!(
        lz::decompress(&[0x10, b'a', 0x02, 0x00], &mut out).is_err(),
        "offset voorbij het begin"
    );
    assert!(
        lz::decompress(&[0xF0, 0xFF, 0xFF, 0xFF], &mut out).is_err(),
        "lange letterlijke run"
    );
    assert_eq!(
        lz::decompress(&[0x10, b'a', 0x01, 0x00, 0x00], &mut out),
        Ok(5)
    );
    assert_eq!(&out[..5], b"aaaaa");
}

#[test]
fn real_page_samples() {
    let Ok(spec) = std::env::var("REPLICA_LZ_SAMPLES") else {
        return;
    };
    let (path, page) = spec.rsplit_once(':').expect("<bestand>:<paginamaat>");
    let page: usize = page.parse().unwrap();
    let data = std::fs::read(path).unwrap();
    let mut e = Encoder::new().unwrap();
    let (mut stored, mut n) = (0usize, 0usize);
    for p in data.chunks_exact(page) {
        stored += roundtrip(&mut e, p).len().min(page);
        n += 1;
    }
    eprintln!(
        "samples: {n} pages, ratio {:.2}",
        (n * page) as f64 / stored as f64
    );
}
