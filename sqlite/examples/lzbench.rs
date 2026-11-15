//! Meet de LZ-codec op een bestand met pagina's: `cargo run --release --example lzbench -- <bestand> <paginamaat>`.
//!
//! Drukt alleen totalen af: pagina's, ratio, onsamendrukbare pagina's en
//! MB/s (logische bytes) voor comprimeren en decomprimeren.
#![expect(clippy::unwrap_used, clippy::expect_used, reason = "meetprogramma")]
use replica_sqlite::lz;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let data = std::fs::read(&args[1]).expect("sample");
    let page: usize = args[2].parse().unwrap();
    let pages: Vec<&[u8]> = data.chunks_exact(page).collect();
    let mut enc = lz::Encoder::new().unwrap();
    let mut out = vec![0u8; page];
    let mut stored = Vec::with_capacity(pages.len());
    let t = Instant::now();
    let mut total = 0usize;
    let mut raw = 0usize;
    for p in &pages {
        match enc.compress(p, &mut out[..page - 1]) {
            Some(n) => {
                total += n;
                stored.push(out[..n].to_vec());
            }
            None => {
                raw += 1;
                total += page;
                stored.push(Vec::new());
            }
        }
    }
    let ct = t.elapsed().as_secs_f64();
    let mut back = vec![0u8; page];
    let t = Instant::now();
    let rounds = 5;
    for _ in 0..rounds {
        for (p, s) in pages.iter().zip(&stored) {
            if s.is_empty() {
                continue;
            }
            let n = lz::decompress(s, &mut back).unwrap();
            assert_eq!(n, page);
            assert_eq!(&back[..], *p);
        }
    }
    let dt = t.elapsed().as_secs_f64() / f64::from(rounds);
    let mb = data.len() as f64 / 1e6;
    println!(
        "pages={} page={} ratio={:.2} raw={} compress={:.0}MB/s decompress={:.0}MB/s",
        pages.len(),
        page,
        data.len() as f64 / total as f64,
        raw,
        mb / ct,
        mb / dt
    );
}
