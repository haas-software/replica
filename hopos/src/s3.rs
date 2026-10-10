//! S3 vanuit een app-slot: `leans3` over `leans3http::Http` over Lean's
//! `WebDial` over `applib::tcp::Dialer`.
//!
//! De keten is die van Lumen (`hop-app-lumen/rust/platform/src/web.rs`): DNS
//! en TCP uit applib, TLS met ketenverificatie tegen `leantls::MOZILLA_ROOTS`
//! op de wandklok van de app ([`applib::App::wall_ns`]), met verse entropie
//! uit [`applib::rand::Rng`] per handshake. Zonder gezette wandklok faalt een
//! `https://`-dial vóór er een verbinding opengaat, en tekent `leans3` niets:
//! een handtekening met een verzonnen tijd weigert S3 toch.
//!
//! [`store`] geeft een `replica_s3::S3` die op de stack van de aanroeper
//! parkeert ([`crate::Wait`]): de objectstore van `replica_core::owner`, en
//! met `S3::lease` de schrijverlease (`writer::Backend`, de hop-lease) over
//! dezelfde client en hetzelfde transport. Dezelfde [`Transport`] dient een
//! app ook voor gewone bestanden in S3.
use crate::Wait;
use alloc::boxed::Box;
use applib::{
    EXEC,
    rand::Rng,
    tcp::{Dialer, TcpConn},
};
use core::{future::Future, time::Duration};
use leanhttps::WebDial;
use leantls::Entropy;
use replica_core::object::StoreError;

/// 2020-01-01: een wandklok daarvoor is niet gezet.
const EPOCH: u64 = 1_577_836_800;
/// De TCP-opbouw en de handshake elk hoogstens zo lang, zoals Lumen.
const CONNECT: Duration = Duration::from_secs(10);

/// De wandklok van de app in Unix-seconden, of `None` zolang de kern hem niet
/// synct. Een functie (geen sluiting): `leans3::Client::now` is een `fn`.
pub fn unix_seconds() -> Option<u64> {
    applib::app()
        .and_then(applib::App::wall_ns)
        .map(|ns| ns / 1_000_000_000)
        .filter(|s| *s >= EPOCH)
}
/// Als [`unix_seconds`], met nul voor "niet gezet" (de vorm van `leans3`).
fn signing_seconds() -> u64 {
    unix_seconds().unwrap_or(0)
}

/// De monotone klok en de slaap van `leans3http`: de teller van de app en
/// het timerwiel van zijn executor.
#[derive(Clone, Copy, Debug, Default)]
pub struct Clock;
impl leans3http::Clock for Clock {
    fn now(&self) -> Duration {
        Duration::from_nanos(applib::clock::now_ns())
    }
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> {
        EXEC.get().after(duration)
    }
}

/// Verse entropie per handshake, uit een eigen DRBG per verbinding.
pub type Entropic = Box<dyn FnMut() -> Option<Entropy>>;
/// De webdialer van een slot.
pub type Web = WebDial<Dialer, fn() -> Option<u64>, Entropic>;
/// Eén keep-alive-verbinding naar S3 als `leans3::Transport`.
pub type Transport = leans3http::Http<Web, Clock>;
/// Een verbinding van [`Transport`]: TCP of TLS.
pub type Conn = leanhttps::Link<TcpConn>;
/// De objectstore van Replica op de stack van de aanroeper.
pub type Store<'s> = replica_s3::S3<Transport, Wait<'s>>;
/// De schrijverlease over dezelfde store.
pub type Lease<'a, 's> = replica_s3::Lease<'a, Transport, Wait<'s>>;

/// Eén transport met een eigen [`Rng`] uit de control-page van de app. Zonder
/// app (een hosttest) geeft de dialer geen entropie: elke `https://`-dial
/// faalt dan vóór er een verbinding opengaat, met `NoEntropy`.
pub fn transport() -> Transport {
    let entropy: Entropic = match applib::app() {
        Some(app) => {
            let mut rng = Rng::open(app);
            Box::new(move || Some(Entropy::new(rng.array())))
        }
        None => Box::new(|| None),
    };
    let mut web: Web = WebDial::new(
        Dialer::new(EXEC.get(), CONNECT),
        leantls::MOZILLA_ROOTS,
        unix_seconds as fn() -> Option<u64>,
        entropy,
    );
    web.handshake = CONNECT;
    leans3http::Http::new(web, Clock)
}

/// Waar een bucket woont en hoe er getekend wordt.
#[derive(Clone, Debug)]
pub struct Endpoint<'a> {
    /// De basis-URL, bijvoorbeeld `https://fsn1.your-objectstorage.com`.
    pub url: &'a str,
    /// De bucket.
    pub bucket: &'a str,
    /// De regio van de credential-scope (`auto`, `eu-central`, ...).
    pub region: &'a str,
    /// De sleutel-id.
    pub access_key: &'a str,
    /// Het geheim; komt in geen logregel (`leans3::Client` laat het weg).
    pub secret_key: &'a str,
}

/// Een `leans3::Client` voor `endpoint`, path-style (Hetzner, MinIO, Bunny),
/// die tekent op de wandklok van de app.
pub fn client(endpoint: &Endpoint<'_>) -> Result<leans3::Client, StoreError> {
    let text = |s: &str| {
        let mut out = alloc::string::String::new();
        out.try_reserve_exact(s.len())
            .map_err(|_| StoreError::Limit)?;
        out.push_str(s);
        Ok::<_, StoreError>(out)
    };
    Ok(leans3::Client {
        endpoint: text(endpoint.url)?,
        bucket: text(endpoint.bucket)?,
        region: text(endpoint.region)?,
        access_key_id: text(endpoint.access_key)?,
        secret_access_key: text(endpoint.secret_key)?,
        session_token: alloc::string::String::new(),
        path_style: true,
        now: Some(signing_seconds),
    })
}

/// De objectstore van Replica voor `endpoint`, die op `wait` parkeert: één
/// verbinding meteen, tot `replica_s3::STREAMS` als een herstel ze vraagt.
/// Zonder app (`applib::app()` leeg) of zonder wandklok is dit
/// [`StoreError::Configuration`].
pub fn store<'s>(endpoint: &Endpoint<'_>, wait: Wait<'s>) -> Result<Store<'s>, StoreError> {
    if applib::app().is_none() {
        return Err(StoreError::Configuration);
    }
    replica_s3::S3::new(client(endpoint)?, transport, wait)
}
