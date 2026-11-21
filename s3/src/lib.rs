//! Replica over leans3/SigV4, met dezelfde parkeerbare eigenaar als SQLite.
#![no_std]
#![forbid(unsafe_code)]
extern crate alloc;
use alloc::{boxed::Box, string::String, vec::Vec};
use core::{
    pin::Pin,
    task::{Context, Poll},
};
use leans3::{AsyncWrite, Client, IoError, Transport};
use replica_core::object::{Object, Parallel, Replies, Store, StoreError};
use replica_core::writer::{
    Backend, LeaseState,
    discovery::{self, wire},
};
use replica_sqlite::asynchronous::Suspend;
/// Gelijktijdige segment-GETs van een herstel of compactie. Bunny Storage
/// haalt ongeveer 10 MB/s per stroom; gemeten 05-10 vanaf het LAN met delen
/// van 4 MiB: 1 stroom 10,6, 8 stromen 53 en 16 stromen 69 MB/s, daarboven
/// niet sneller.
pub const STREAMS: usize = 16;
/// Segmentbytes die één ronde hoogstens vasthoudt: 16 delen van 4 MiB.
pub const STREAM_BYTES: u64 = 64 << 20;
/// Eén client en wachtbrug met hun eigen verbindingen; annulering maakt deze
/// eigenaar onbruikbaar. Een herstel of compactie haalt tot [`STREAMS`]
/// segmenten tegelijk op, ieder over een eigen verbinding.
pub struct S3<T, W> {
    client: Client,
    transport: T,
    extra: Vec<T>,
    open: Box<dyn FnMut() -> T>,
    streams: usize,
    budget: u64,
    wait: W,
    poisoned: bool,
    /// Sleutel, ETag en eerste waarneming (ms) van een lease-ghost.
    ghost: Option<(String, String, u64)>,
}
impl<T: Transport, W: Suspend> S3<T, W> {
    /// `open` maakt één verbinding, bijvoorbeeld `leans3http::Http` over
    /// Lean's `WebDial`; TLS, DNS en termijnen zijn van dat transport. De
    /// eerste gaat meteen open, de rest pas als een herstel of compactie
    /// tegelijk ophaalt: [`STREAMS`] stromen, samen [`STREAM_BYTES`].
    pub fn new(
        client: Client,
        mut open: impl FnMut() -> T + 'static,
        wait: W,
    ) -> Result<Self, StoreError> {
        let out = Self {
            client,
            transport: open(),
            extra: Vec::new(),
            open: Box::new(open),
            streams: STREAMS,
            budget: STREAM_BYTES,
            wait,
            poisoned: false,
            ghost: None,
        };
        out.ready()?;
        Ok(out)
    }
    /// Een andere grens dan [`STREAMS`] en [`STREAM_BYTES`]; één stroom haalt
    /// na elkaar op.
    #[must_use]
    pub fn streams(mut self, streams: usize, bytes: u64) -> Self {
        self.streams = streams.max(1);
        self.budget = bytes;
        self
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
    /// De schrijverlease op `key` in deze bucket, via dezelfde client en
    /// hetzelfde transport (zie `replica_core::writer`). `ttl_ms` is de
    /// lease-TTL: zo lang moet een ghost onveranderd blijven vóór overname.
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
        let s3 = &mut *self.s3;
        match s3
            .wait
            .wait(s3.client.put(&mut s3.transport, self.key, body, &options))
        {
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
        let s3 = &mut *self.s3;
        match s3
            .wait
            .wait(s3.client.delete(&mut s3.transport, self.key, &options))
        {
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
        self.ready()?;
        if bytes.len() > replica_core::segment::MAX_BYTES {
            return Err(StoreError::Limit);
        }
        let options = leans3::PutOptions::default();
        match self
            .wait
            .wait(self.client.put(&mut self.transport, key, bytes, &options))
        {
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
        self.ready()?;
        if bytes.len() > replica_core::segment::MAX_BYTES {
            return Err(StoreError::Limit);
        }
        let options = leans3::PutOptions {
            if_none_match: "*",
            ..leans3::PutOptions::default()
        };
        match self
            .wait
            .wait(self.client.put(&mut self.transport, key, bytes, &options))
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(leans3::Error::PreconditionFailed)) => Err(StoreError::Exists),
            Ok(Err(e)) => Err(map(e)),
            Err(_) => {
                self.poisoned = true;
                Err(StoreError::Cancelled)
            }
        }
    }
    fn parallel(&self) -> Parallel {
        Parallel {
            requests: self.streams,
            bytes: self.budget,
        }
    }
    /// Eén GET per verbinding, samen in één wachtbeurt (`leans3`'s
    /// `get_to_all`); ieder met zijn eigen begrensde sink. Ontbrekende
    /// verbindingen gaan nu pas open. Annulering vergiftigt de hele eigenaar,
    /// zoals bij `get`.
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
        if let [(key, limit)] = keys {
            out.push(self.get(key, *limit));
            return Ok(out);
        }
        let more = keys.len() - 1;
        if self.extra.len() < more {
            self.extra
                .try_reserve_exact(more - self.extra.len())
                .map_err(|_| StoreError::Limit)?;
            while self.extra.len() < more {
                self.extra.push((self.open)());
            }
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
        for (sink, reply) in sinks.into_iter().zip(replies) {
            out.push(match reply {
                _ if sink.limited => Err(StoreError::Limit),
                Ok(_) => Ok(sink.bytes),
                Err(e) => Err(map(e)),
            });
        }
        Ok(out)
    }
    fn get(&mut self, key: &str, limit: usize) -> Result<Vec<u8>, StoreError> {
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
    fn delete(&mut self, key: &str) -> Result<(), StoreError> {
        self.ready()?;
        let options = leans3::DeleteOptions::default();
        match self
            .wait
            .wait(self.client.delete(&mut self.transport, key, &options))
        {
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
