//! Replica over leans3/SigV4, met dezelfde parkeerbare eigenaar als SQLite.
#![no_std]
#![forbid(unsafe_code)]
extern crate alloc;
use alloc::{boxed::Box, string::String, vec::Vec};
use core::{
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
};
use leans3::{AsyncWrite, Client, IoError, Transport};
use replica_core::object::{Object, Parallel, Replies, Store, StoreError};
use replica_core::writer::{
    Backend, LeaseState,
    discovery::{self, wire},
};
use replica_sqlite::asynchronous::{Cancelled, Suspend};
/// Gelijktijdige segment-GETs van een herstel of compactie, per store. Bunny
/// Storage haalt ongeveer 10 MB/s per stroom; gemeten 05-10 vanaf het LAN met
/// delen van 4 MiB: 1 stroom 10,6, 8 stromen 53 en 16 stromen 69 MB/s,
/// daarboven niet sneller. Wat de hele app tegelijk mag, zegt de [`Gate`].
pub const STREAMS: usize = 16;
/// Segmentbytes die één ronde van één store hoogstens vasthoudt: 16 delen van
/// 4 MiB.
pub const STREAM_BYTES: u64 = 64 << 20;
/// De bytes van één stroom in een ronde: één deel van 4 MiB.
pub const PART_BYTES: u64 = 4 << 20;
/// Stromen van de hele app samen, over alle stores ([`Gate`]). Gemeten 21-11
/// (een host-app op QEMU, een herstel van 304 MiB met 40
/// pollers): 4, 8 en 16 stromen elk 41 tot 43 s; met een draad van 10 ms 272,
/// 264 en 301 s; nergens een mislukte leasevernieuwing. Het kleinste aantal
/// binnen 20 % van het snelste.
pub const APP_STREAMS: usize = 4;
/// Bytes van de rondes van de hele app samen: [`APP_STREAMS`] delen.
pub const APP_STREAM_BYTES: u64 = APP_STREAMS as u64 * PART_BYTES;
/// Bits van de bytes in [`Gate::used`]; de stromen staan erboven.
const BYTE_BITS: u32 = 48;
const BYTE_MASK: u64 = (1 << BYTE_BITS) - 1;
/// De poort van een app: hoeveel stromen de rondes van al zijn stores samen
/// hebben, en hoeveel bytes ze samen vasthouden. Elke `S3` van de app krijgt
/// dezelfde mee ([`S3::new`]); per store blijft [`S3::streams`] de grens.
///
/// Wat telt: de verbindingen van een ronde (de vaste plus de extra) en de
/// bytes van die ronde, vanaf de ronde tot de store ze teruggeeft (de eerste
/// call die geen ronde is, [`S3::rest`], of de drop). Wat niet telt: de ene
/// vaste verbinding van elke store buiten een ronde, de lease en een ronde
/// die geen twee stromen kreeg (die haalt na elkaar op over de vaste
/// verbinding). Een store kan dus altijd verder, één deel tegelijk.
///
/// Eén atomic voor beide tellers (stromen boven, bytes onder), zodat ze samen
/// één waarde zijn; zo mag de poort een `static` zijn, ook op een host met
/// threads.
#[derive(Debug)]
pub struct Gate {
    streams: usize,
    bytes: u64,
    used: AtomicU64,
}
impl Gate {
    /// Een poort voor `streams` stromen en `bytes` bytes (hoogstens 2^16 - 1
    /// stromen en 2^48 - 1 bytes); bijvoorbeeld [`APP_STREAMS`] en
    /// [`APP_STREAM_BYTES`].
    #[must_use]
    pub const fn new(streams: usize, bytes: u64) -> Self {
        Self {
            streams: if streams > 0xffff { 0xffff } else { streams },
            bytes: if bytes > BYTE_MASK { BYTE_MASK } else { bytes },
            used: AtomicU64::new(0),
        }
    }
    /// De stromen en bytes die nu vrij zijn.
    pub fn free(&self) -> (usize, u64) {
        let (streams, bytes) = unpack(self.used.load(Ordering::Relaxed));
        (
            self.streams.saturating_sub(streams),
            self.bytes.saturating_sub(bytes),
        )
    }
    /// Neemt hoogstens `streams` en `bytes`, zoveel als er vrij is.
    fn take(&self, streams: usize, bytes: u64) -> (usize, u64) {
        let mut got = (0, 0);
        let _ = self
            .used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let (s, b) = unpack(used);
                got = (
                    streams.min(self.streams.saturating_sub(s)),
                    bytes.min(self.bytes.saturating_sub(b)),
                );
                Some(pack(s + got.0, b + got.1))
            });
        got
    }
    /// Geeft terug wat [`Gate::take`] gaf.
    fn give(&self, streams: usize, bytes: u64) {
        let _ = self
            .used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let (s, b) = unpack(used);
                Some(pack(s.saturating_sub(streams), b.saturating_sub(bytes)))
            });
    }
}
fn pack(streams: usize, bytes: u64) -> u64 {
    ((streams as u64) << BYTE_BITS) | (bytes & BYTE_MASK)
}
fn unpack(used: u64) -> (usize, u64) {
    ((used >> BYTE_BITS) as usize, used & BYTE_MASK)
}
/// Wat één store van de [`Gate`] heeft. De drop geeft het terug, op elk pad.
struct Permit {
    gate: &'static Gate,
    /// De vaste verbinding plus de extra van de ronde, of nul buiten een ronde.
    streams: usize,
    /// De bytes van de laatste ronde.
    bytes: u64,
}
impl Permit {
    fn give_bytes(&mut self) {
        self.gate.give(0, core::mem::take(&mut self.bytes));
    }
    fn give_all(&mut self) {
        self.gate.give(
            core::mem::take(&mut self.streams),
            core::mem::take(&mut self.bytes),
        );
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.give_all();
    }
}
/// Eén client en wachtbrug met hun eigen verbindingen; annulering maakt deze
/// eigenaar onbruikbaar. Een herstel of compactie haalt tot [`STREAMS`]
/// segmenten tegelijk op, ieder over een eigen verbinding, binnen de
/// [`Gate`] van de app.
pub struct S3<T, W> {
    client: Client,
    transport: T,
    /// De extra verbindingen van de rondes (`get_all`); ze vallen bij de
    /// eerste andere call ([`S3::rest`]), niet op een klok.
    extra: Vec<T>,
    open: Box<dyn FnMut() -> T>,
    streams: usize,
    budget: u64,
    permit: Permit,
    wait: W,
    poisoned: bool,
    /// Sleutel, ETag en eerste waarneming (ms) van een lease-ghost.
    ghost: Option<(String, String, u64)>,
}
impl<T: Transport, W: Suspend> S3<T, W> {
    /// `open` maakt één verbinding, bijvoorbeeld `leans3http::Http` over
    /// Lean's `WebDial`; TLS, DNS en termijnen zijn van dat transport. De
    /// eerste gaat meteen open, de rest pas als een herstel of compactie
    /// tegelijk ophaalt: [`STREAMS`] stromen, samen [`STREAM_BYTES`], en
    /// alleen zoveel als `gate` (de poort van de hele app) vrij heeft.
    pub fn new(
        client: Client,
        mut open: impl FnMut() -> T + 'static,
        wait: W,
        gate: &'static Gate,
    ) -> Result<Self, StoreError> {
        let out = Self {
            client,
            transport: open(),
            extra: Vec::new(),
            open: Box::new(open),
            streams: STREAMS,
            budget: STREAM_BYTES,
            permit: Permit {
                gate,
                streams: 0,
                bytes: 0,
            },
            wait,
            poisoned: false,
            ghost: None,
        };
        out.ready()?;
        Ok(out)
    }
    /// Een andere grens dan [`STREAMS`] en [`STREAM_BYTES`] voor deze store;
    /// één stroom haalt na elkaar op. De [`Gate`] blijft erboven gelden.
    #[must_use]
    pub fn streams(mut self, streams: usize, bytes: u64) -> Self {
        self.streams = streams.max(1);
        self.budget = bytes;
        self
    }
    /// Het einde van de rondes: de extra verbindingen vallen (dicht) en wat
    /// de store van de [`Gate`] had, gaat terug. Elke call die geen
    /// `get_all` is (PUT, GET, LIST, DELETE) doet dit zelf, de lease niet;
    /// core's `restore::stage` roept het aan via `Store::rest` zodra een
    /// herstel klaar is, zodat niemand op de volgende call wacht.
    pub fn rest(&mut self) {
        self.extra.clear();
        self.permit.give_all();
    }
    fn ready(&self) -> Result<(), StoreError> {
        if self.poisoned {
            return Err(StoreError::Cancelled);
        }
        if self.client.now.is_none_or(|now| now() < 1_577_836_800)
            || self.client.endpoint.is_empty()
            || self.client.bucket.is_empty()
            || self.client.region.is_empty()
            || self.client.access_key_id.is_empty()
            || self.client.secret_access_key.is_empty()
        {
            return Err(StoreError::Configuration);
        }
        Ok(())
    }
    fn objects(
        &mut self,
        prefix: &str,
        limit: usize,
        complete: bool,
    ) -> Result<Vec<Object>, StoreError> {
        self.rest();
        self.ready()?;
        if limit == 0 || limit > replica_core::manifest::MAX_COMMITS {
            return Err(StoreError::Limit);
        }
        let (keys, truncated) =
            match self
                .wait
                .wait(self.client.list(&mut self.transport, prefix, limit))
            {
                Ok(result) => result.map_err(map)?,
                Err(_) => {
                    self.poisoned = true;
                    return Err(StoreError::Cancelled);
                }
            };
        if complete && truncated {
            return Err(StoreError::Limit);
        }
        let mut out = Vec::new();
        out.try_reserve_exact(keys.len())
            .map_err(|_| StoreError::Limit)?;
        for key in keys {
            // De XML-parser begrenst de sleutellengte; hier bewaken we de namespace.
            if !key.starts_with(prefix) {
                return Err(StoreError::Transport);
            }
            out.push(Object { key, size: None });
        }
        Ok(out)
    }
}
impl<T: Transport, W: Suspend> S3<T, W> {
    /// Hoeveel verbindingen deze ronde krijgt: wat de store al had, aangevuld
    /// uit de [`Gate`] tot het aantal sleutels, met de extra verbindingen die
    /// daarbij horen geopend. De bytes van de vorige ronde zijn verbruikt
    /// (core vraagt een nieuwe ronde pas als de vorige op is) en gaan eerst
    /// terug.
    fn widen(&mut self, keys: &[(&str, usize)]) -> Result<usize, StoreError> {
        self.permit.give_bytes();
        if keys.len() < 2 {
            return Ok(1);
        }
        let held = self.permit.streams;
        if held < keys.len() {
            let (got, _) = self.permit.gate.take(keys.len() - held, 0);
            if held + got < 2 {
                // Geen tweede stroom: na elkaar, buiten de poort.
                self.permit.gate.give(got, 0);
                return Ok(1);
            }
            self.permit.streams = held + got;
        }
        let width = self.permit.streams.min(keys.len());
        let more = width - 1;
        if self.extra.len() < more {
            self.extra
                .try_reserve_exact(more - self.extra.len())
                .map_err(|_| StoreError::Limit)?;
            while self.extra.len() < more {
                self.extra.push((self.open)());
            }
        }
        let bytes = keys.iter().fold(0u64, |sum, &(_, limit)| {
            sum.saturating_add(u64::try_from(limit).unwrap_or(u64::MAX))
        });
        let (_, got) = self.permit.gate.take(0, bytes);
        self.permit.bytes = got;
        Ok(width)
    }
    /// Eén GET over de vaste verbinding; de extra blijven staan.
    fn fetch(&mut self, key: &str, limit: usize) -> Result<Vec<u8>, StoreError> {
        self.ready()?;
        if limit > replica_core::segment::MAX_BYTES {
            return Err(StoreError::Limit);
        }
        let mut sink = Sink {
            bytes: Vec::new(),
            limit,
            limited: false,
        };
        let result = self
            .wait
            .wait(self.client.get_to(&mut self.transport, key, &mut sink));
        match result {
            Err(_) => {
                self.poisoned = true;
                Err(StoreError::Cancelled)
            }
            Ok(result) => {
                if sink.limited {
                    return Err(StoreError::Limit);
                }
                result.map_err(map)?;
                Ok(sink.bytes)
            }
        }
    }
    /// Eén GET per sleutel tegelijk, over de vaste en de extra verbindingen
    /// (er zijn er genoeg: [`S3::widen`]).
    fn fetch_all(&mut self, keys: &[(&str, usize)]) -> Result<Replies, StoreError> {
        if keys.len() > self.extra.len() + 1 {
            return Err(StoreError::Limit);
        }
        let mut sinks = Vec::new();
        sinks
            .try_reserve_exact(keys.len())
            .map_err(|_| StoreError::Limit)?;
        for &(_, limit) in keys {
            sinks.push(Sink {
                bytes: Vec::new(),
                limit,
                limited: false,
            });
        }
        let transports = core::iter::once(&mut self.transport).chain(self.extra.iter_mut());
        let gets = transports
            .zip(sinks.iter_mut())
            .zip(keys)
            .map(|((transport, sink), &(key, _))| (transport, key, sink));
        let replies = match self.wait.wait(self.client.get_to_all(gets)) {
            Ok(replies) => replies.map_err(map)?,
            Err(_) => {
                self.poisoned = true;
                return Err(StoreError::Cancelled);
            }
        };
        let mut out = Vec::new();
        out.try_reserve_exact(keys.len())
            .map_err(|_| StoreError::Limit)?;
        for (sink, reply) in sinks.into_iter().zip(replies) {
            out.push(match reply {
                _ if sink.limited => Err(StoreError::Limit),
                Ok(_) => Ok(sink.bytes),
                Err(e) => Err(map(e)),
            });
        }
        Ok(out)
    }
}
impl<T: Transport, W: Suspend> S3<T, W> {
    /// De schrijverlease op `key` in deze bucket, via dezelfde client en
    /// hetzelfde transport (zie `replica_core::writer`). `ttl_ms` is de
    /// lease-TTL: zo lang moet een ghost onveranderd blijven vóór overname.
    /// De lease gaat buiten de [`Gate`] om en laat de extra verbindingen van
    /// een ronde staan: een vernieuwing midden in een herstel wacht nergens
    /// op en breekt het herstel niet af.
    pub fn lease<'a>(&'a mut self, key: &'a str, ttl_ms: u64) -> Lease<'a, T, W> {
        Lease {
            s3: self,
            key,
            ttl_ms,
        }
    }
}
/// Het CAS-protocol van de hop-lease op S3: GET geeft de lease met zijn ETag,
/// PUT met `If-None-Match: *` maakt aan, PUT of DELETE met `If-Match` vervangt
/// of verwijdert, en 412 (of 409) is "gehouden". Na een 412 op een geciteerde
/// ETag volgt één poging met de kale (Hetzner, Ceph RGW), zoals agentd.
/// Bunny Storage kan een "ghost" geven: GET 404 terwijl HEAD 200 en de
/// aanmaak 412 zegt. Bleef die ETag een TTL onveranderd, dan is hij van
/// niemand en volgt een overname op die ETag (gemeten 08-09-2026 in Hop).
///
/// Elke andere fout is "onbereikbaar": nooit bewijs dat de lease vrij is.
pub struct Lease<'a, T, W> {
    s3: &'a mut S3<T, W>,
    key: &'a str,
    ttl_ms: u64,
}
impl<T: Transport, W: Suspend> Lease<'_, T, W> {
    fn put(&mut self, prev: &str, body: &[u8]) -> discovery::Result<String> {
        self.s3.ready().map_err(|_| discovery::Error::Unreachable)?;
        let options = if prev.is_empty() {
            leans3::PutOptions {
                content_type: "application/json",
                if_none_match: "*",
                ..leans3::PutOptions::default()
            }
        } else {
            leans3::PutOptions {
                content_type: "application/json",
                if_match: prev,
                ..leans3::PutOptions::default()
            }
        };
        let key = self.key;
        let sent = once_more(self.s3, |s3| {
            s3.wait
                .wait(s3.client.put(&mut s3.transport, key, body, &options))
        });
        let s3 = &mut *self.s3;
        match sent {
            // Zonder ETag kan niemand de volgende CAS doen.
            Ok(Ok(Some(etag))) if !etag.is_empty() => Ok(etag),
            Ok(Err(leans3::Error::PreconditionFailed)) => Err(discovery::Error::LeaseHeld),
            Ok(_) => Err(discovery::Error::Unreachable),
            Err(_) => {
                s3.poisoned = true;
                Err(discovery::Error::Unreachable)
            }
        }
    }
    fn put_unquoting(&mut self, prev: &str, body: &[u8]) -> discovery::Result<String> {
        match (self.put(prev, body), wire::strip_quotes(prev)) {
            (Err(discovery::Error::LeaseHeld), Some(bare)) => self.put(bare, body),
            (result, _) => result,
        }
    }
    /// De ETag van een ghost die een TTL lang dezelfde bleef.
    fn ghost(&mut self) -> Option<String> {
        self.s3.ready().ok()?;
        let s3 = &mut *self.s3;
        let etag = match s3.wait.wait(s3.client.head(&mut s3.transport, self.key)) {
            Ok(Ok(etag)) if !etag.is_empty() => etag,
            Ok(_) => return None,
            Err(_) => {
                s3.poisoned = true;
                return None;
            }
        };
        let now = s3.client.now.map(|now| now().saturating_mul(1000))?;
        match &s3.ghost {
            Some((key, seen, since)) if key == self.key && *seen == etag => {
                (now.saturating_sub(*since) >= self.ttl_ms).then_some(etag)
            }
            _ => {
                s3.ghost = Some((String::from(self.key), etag, now));
                None
            }
        }
    }
    fn delete_if(&mut self, handle: &str) -> discovery::Result {
        self.s3.ready().map_err(|_| discovery::Error::Unreachable)?;
        let options = leans3::DeleteOptions { if_match: handle };
        let key = self.key;
        let sent = once_more(self.s3, |s3| {
            s3.wait
                .wait(s3.client.delete(&mut s3.transport, key, &options))
        });
        let s3 = &mut *self.s3;
        match sent {
            Ok(Ok(())) => Ok(()),
            Ok(Err(leans3::Error::NotFound)) => Err(discovery::Error::NoLease),
            Ok(Err(leans3::Error::PreconditionFailed)) => Err(discovery::Error::LeaseHeld),
            Ok(Err(_)) => Err(discovery::Error::Unreachable),
            Err(_) => {
                s3.poisoned = true;
                Err(discovery::Error::Unreachable)
            }
        }
    }
}
impl<T: Transport, W: Suspend> Backend for Lease<'_, T, W> {
    fn read(&mut self) -> discovery::Result<(LeaseState, String)> {
        self.s3.ready().map_err(|_| discovery::Error::Unreachable)?;
        let s3 = &mut *self.s3;
        let (body, etag) = match s3.wait.wait(s3.client.get(&mut s3.transport, self.key)) {
            Ok(Ok(found)) => found,
            Ok(Err(leans3::Error::NotFound)) => return Err(discovery::Error::NoLease),
            Ok(Err(_)) => return Err(discovery::Error::Unreachable),
            Err(_) => {
                s3.poisoned = true;
                return Err(discovery::Error::Unreachable);
            }
        };
        // Een onleesbare lease is geen bewijs dat hij vrij is.
        let state = wire::decode(&body).map_err(|_| discovery::Error::Unreachable)?;
        match etag {
            Some(etag) if !etag.is_empty() => {
                s3.ghost = None;
                Ok((state, etag))
            }
            _ => Err(discovery::Error::Unreachable),
        }
    }
    fn write(&mut self, prev: &str, state: &LeaseState) -> discovery::Result<String> {
        let body = wire::encode(state).map_err(|_| discovery::Error::Unreachable)?;
        let mut result = self.put_unquoting(prev, &body);
        if result == Err(discovery::Error::LeaseHeld)
            && prev.is_empty()
            && let Some(handle) = self.ghost()
        {
            // De lees zei "geen lease", de aanmaak "bestaat", en HEAD toont
            // een object dat een TTL stilstond: van niemand.
            result = self.put_unquoting(&handle, &body);
        }
        if result.is_ok() {
            self.s3.ghost = None;
        }
        result
    }
    fn delete(&mut self, handle: &str) -> discovery::Result {
        if handle.is_empty() {
            return Err(discovery::Error::LeaseHeld);
        }
        match (self.delete_if(handle), wire::strip_quotes(handle)) {
            (Err(discovery::Error::LeaseHeld), Some(bare)) => self.delete_if(bare),
            (result, _) => result,
        }
    }
}
/// Faalde een verzoek op een verbinding die dicht bleek (gesloten, gereset,
/// of de stroom eindigde zonder antwoord)?
fn is_closed(e: &leans3::Error) -> bool {
    matches!(
        e,
        leans3::Error::Transport {
            source: IoError::Closed | IoError::UnexpectedEof,
            ..
        }
    )
}
/// Eén PUT of DELETE met zijn body in het geheugen, en één nieuwe poging als
/// de verbinding dicht bleek binnen dezelfde seconde (op de signeerklok).
///
/// Een keep-alive-verbinding die een tijd stil was, is vaak al dicht aan de
/// kant van S3; het eerste verzoek erop faalt dan meteen zonder de store te
/// bereiken. `leanhttp` speelt zo'n verzoek alleen opnieuw af voor GET en
/// HEAD, dus faalde een lease-vrijgave of een manifest-PUT na een stilte
/// (een host-app: de opvolger wachtte de hele TTL). Opnieuw is
/// hier veilig: een PUT van dezelfde bytes naar dezelfde sleutel is
/// idempotent; een tweede `If-None-Match: *` op een geland eerste geeft 412
/// ([`StoreError::Exists`], `object::publish` leest dan terug); een tweede
/// `If-Match` op een geland eerste geeft 412 (de lease heroverd via
/// `Renewed::Reclaim`); een tweede DELETE vindt niets (succes). Alleen een
/// snelle sluiting: een call die lang liep en dan brak, gaat niet opnieuw,
/// zodat de controle van de schrijfdeadline vlak vóór de call blijft gelden.
fn once_more<T: Transport, W: Suspend, R>(
    s3: &mut S3<T, W>,
    mut call: impl FnMut(&mut S3<T, W>) -> Result<Result<R, leans3::Error>, Cancelled>,
) -> Result<Result<R, leans3::Error>, Cancelled> {
    let clock = s3.client.now;
    let started = clock.map(|now| now());
    let first = call(s3)?;
    let quick = match (clock, started) {
        (Some(now), Some(at)) => now().saturating_sub(at) <= 1,
        _ => false,
    };
    match first {
        Err(ref e) if quick && is_closed(e) && s3.ready().is_ok() => call(s3),
        first => Ok(first),
    }
}
fn map(e: leans3::Error) -> StoreError {
    match e {
        leans3::Error::NotFound => StoreError::Missing,
        leans3::Error::Status(s) if matches!(s.code, 401 | 403) => StoreError::Denied,
        leans3::Error::ObjectTooLarge { .. }
        | leans3::Error::ListPageTooLarge
        | leans3::Error::OutOfMemory => StoreError::Limit,
        leans3::Error::EndpointRequired
        | leans3::Error::BucketRequired
        | leans3::Error::KeyRequired
        | leans3::Error::EndpointIncomplete
        | leans3::Error::EndpointScheme
        | leans3::Error::ClockRequired
        | leans3::Error::ListMaxZero => StoreError::Configuration,
        _ => StoreError::Transport,
    }
}
impl<T: Transport, W: Suspend> Store for S3<T, W> {
    fn directories(
        &mut self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<alloc::string::String>, StoreError> {
        self.rest();
        self.ready()?;
        if limit == 0 || limit > replica_core::manifest::MAX_COMMITS {
            return Err(StoreError::Limit);
        }
        match self.wait.wait(
            self.client
                .list_directories(&mut self.transport, prefix, limit),
        ) {
            Ok(Ok((keys, false))) => {
                if keys.iter().any(|key| {
                    key.len() <= prefix.len()
                        || !key.starts_with(prefix)
                        || !key.ends_with('/')
                        || key[prefix.len()..key.len() - 1].contains('/')
                }) {
                    return Err(StoreError::Transport);
                }
                Ok(keys)
            }
            Ok(Ok((_, true))) => Err(StoreError::Limit),
            Ok(Err(e)) => Err(map(e)),
            Err(_) => {
                self.poisoned = true;
                Err(StoreError::Cancelled)
            }
        }
    }
    fn list_batch(&mut self, prefix: &str, limit: usize) -> Result<Vec<Object>, StoreError> {
        self.objects(prefix, limit, false)
    }
    fn put(&mut self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.rest();
        self.ready()?;
        if bytes.len() > replica_core::segment::MAX_BYTES {
            return Err(StoreError::Limit);
        }
        let options = leans3::PutOptions::default();
        match once_more(self, |s3| {
            s3.wait
                .wait(s3.client.put(&mut s3.transport, key, bytes, &options))
        }) {
            Ok(result) => result.map(|_| ()).map_err(map),
            Err(_) => {
                self.poisoned = true;
                Err(StoreError::Cancelled)
            }
        }
    }
    /// `If-None-Match: *`, zoals de aanmaak van de lease: 412 (of 409) is
    /// [`StoreError::Exists`], het bestaande object blijft staan.
    fn put_new(&mut self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.rest();
        self.ready()?;
        if bytes.len() > replica_core::segment::MAX_BYTES {
            return Err(StoreError::Limit);
        }
        let options = leans3::PutOptions {
            if_none_match: "*",
            ..leans3::PutOptions::default()
        };
        match once_more(self, |s3| {
            s3.wait
                .wait(s3.client.put(&mut s3.transport, key, bytes, &options))
        }) {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(leans3::Error::PreconditionFailed)) => Err(StoreError::Exists),
            Ok(Err(e)) => Err(map(e)),
            Err(_) => {
                self.poisoned = true;
                Err(StoreError::Cancelled)
            }
        }
    }
    /// Deze store's grens, en niet meer dan de [`Gate`] nu geeft: wat de
    /// store al heeft plus wat vrij is (één stroom altijd). De aanroeper
    /// (core's prefetch) vraagt dit vlak vóór zijn `get_all`, zonder wachten
    /// ertussen, dus wat hij plant is dan nog vrij.
    fn parallel(&self) -> Parallel {
        let (streams, bytes) = self.permit.gate.free();
        Parallel {
            requests: self
                .streams
                .min(self.permit.streams.saturating_add(streams))
                .max(1),
            bytes: self.budget.min(self.permit.bytes.saturating_add(bytes)),
        }
    }
    /// Eén GET per verbinding, samen in één wachtbeurt (`leans3`'s
    /// `get_to_all`); ieder met zijn eigen begrensde sink. Ontbrekende
    /// verbindingen gaan nu pas open, zoveel als de [`Gate`] geeft; ze blijven
    /// voor de volgende ronde tot [`S3::rest`]. Geeft de poort er minder dan
    /// sleutels, dan gaat de ronde in stukken; zonder twee stromen na elkaar
    /// over de vaste verbinding. Annulering vergiftigt de hele eigenaar, zoals
    /// bij `get`.
    fn get_all(&mut self, keys: &[(&str, usize)]) -> Result<Replies, StoreError> {
        self.ready()?;
        if keys.len() > self.streams
            || keys
                .iter()
                .any(|&(_, limit)| limit > replica_core::segment::MAX_BYTES)
        {
            return Err(StoreError::Limit);
        }
        let mut out = Vec::new();
        out.try_reserve_exact(keys.len())
            .map_err(|_| StoreError::Limit)?;
        let width = self.widen(keys)?;
        for round in keys.chunks(width) {
            if let [(key, limit)] = round {
                out.push(self.fetch(key, *limit));
                continue;
            }
            for reply in self.fetch_all(round)? {
                out.push(reply);
            }
        }
        Ok(out)
    }
    /// Zie [`S3::rest`].
    fn rest(&mut self) {
        S3::rest(self);
    }
    fn get(&mut self, key: &str, limit: usize) -> Result<Vec<u8>, StoreError> {
        self.rest();
        self.fetch(key, limit)
    }
    fn delete(&mut self, key: &str) -> Result<(), StoreError> {
        self.rest();
        self.ready()?;
        let options = leans3::DeleteOptions::default();
        match once_more(self, |s3| {
            s3.wait
                .wait(s3.client.delete(&mut s3.transport, key, &options))
        }) {
            Ok(Ok(())) | Ok(Err(leans3::Error::NotFound)) => Ok(()),
            Ok(Err(e)) => Err(map(e)),
            Err(_) => {
                self.poisoned = true;
                Err(StoreError::Cancelled)
            }
        }
    }
    fn list(&mut self, prefix: &str, limit: usize) -> Result<Vec<Object>, StoreError> {
        self.objects(prefix, limit, true)
    }
}
struct Sink {
    bytes: Vec<u8>,
    limit: usize,
    limited: bool,
}
impl AsyncWrite for Sink {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<Result<usize, IoError>> {
        let this = self.get_mut();
        let Some(end) = this
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|end| *end <= this.limit)
        else {
            this.limited = true;
            return Poll::Ready(Err(IoError::Other("replica object budget")));
        };
        if end > this.bytes.capacity() {
            let target = end.next_power_of_two().min(this.limit);
            if this
                .bytes
                .try_reserve_exact(target - this.bytes.len())
                .is_err()
            {
                this.limited = true;
                return Poll::Ready(Err(IoError::Other("replica allocation")));
            }
        }
        this.bytes.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
}
