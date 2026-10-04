//! Replica over leans3/SigV4, met dezelfde parkeerbare eigenaar als SQLite.
#![no_std]
#![forbid(unsafe_code)]
extern crate alloc;
use alloc::{boxed::Box, string::String, vec::Vec};
use core::{
    future::{Future, poll_fn},
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
/// Eén client, transport en wachtbrug; annulering maakt deze eigenaar onbruikbaar.
/// Extra transports laten segment-GETs tegelijk lopen ([`S3::with_transports`]).
pub struct S3<T, W> {
    client: Client,
    transport: T,
    extra: Vec<T>,
    budget: u64,
    wait: W,
    poisoned: bool,
    /// Sleutel, ETag en eerste waarneming (ms) van een lease-ghost.
    ghost: Option<(String, String, u64)>,
}
impl<T: Transport, W: Suspend> S3<T, W> {
    /// Leent geen globale sockets; transport verzorgt TLS, DNS en totale deadlines.
    /// Bijvoorbeeld Hop's `SlotTransport`, of de hostadapter voor integratietoetsen.
    pub fn new(client: Client, transport: T, wait: W) -> Result<Self, StoreError> {
        let out = Self {
            client,
            transport,
            extra: Vec::new(),
            budget: 0,
            wait,
            poisoned: false,
            ghost: None,
        };
        out.ready()?;
        Ok(out)
    }
    /// Extra transports voor gelijktijdige segment-GETs: met `n` erbij haalt
    /// een restore of compactie `n + 1` delen tegelijk op, samen hoogstens
    /// `bytes`. Bunny Storage doet ongeveer 10 MB/s per download; 8 transports
    /// van 4 MiB-segmenten vragen tot 32 MiB geheugen per ronde.
    pub fn with_transports(mut self, extra: Vec<T>, bytes: u64) -> Self {
        self.extra = extra;
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
    fn parallel(&self) -> Parallel {
        Parallel {
            requests: 1 + self.extra.len(),
            bytes: self.budget,
        }
    }
    /// Eén GET per transport, samen in één wachtbeurt; ieder met zijn eigen
    /// begrensde sink. Annulering vergiftigt de hele eigenaar, zoals bij `get`.
    fn get_all(&mut self, keys: &[(&str, usize)]) -> Result<Replies, StoreError> {
        self.ready()?;
        if keys.len() > 1 + self.extra.len()
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
        let mut sinks = Vec::new();
        let mut done = Vec::new();
        let mut futures = Vec::new();
        sinks
            .try_reserve_exact(keys.len())
            .map_err(|_| StoreError::Limit)?;
        done.try_reserve_exact(keys.len())
            .map_err(|_| StoreError::Limit)?;
        futures
            .try_reserve_exact(keys.len())
            .map_err(|_| StoreError::Limit)?;
        for &(_, limit) in keys {
            sinks.push(Sink {
                bytes: Vec::new(),
                limit,
                limited: false,
            });
            done.push(None);
        }
        let transports = core::iter::once(&mut self.transport).chain(self.extra.iter_mut());
        for ((transport, sink), &(key, _)) in transports.zip(sinks.iter_mut()).zip(keys) {
            futures.push(Box::pin(self.client.get_to(transport, key, sink)));
        }
        let finished = self.wait.wait(poll_fn(|cx| {
            let mut pending = false;
            for (future, slot) in futures.iter_mut().zip(done.iter_mut()) {
                if slot.is_none() {
                    match future.as_mut().poll(cx) {
                        Poll::Ready(result) => *slot = Some(result),
                        Poll::Pending => pending = true,
                    }
                }
            }
            if pending {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }));
        drop(futures);
        if finished.is_err() {
            self.poisoned = true;
            return Err(StoreError::Cancelled);
        }
        for (sink, result) in sinks.into_iter().zip(done) {
            out.push(match result {
                _ if sink.limited => Err(StoreError::Limit),
                Some(Ok(_)) => Ok(sink.bytes),
                Some(Err(e)) => Err(map(e)),
                None => Err(StoreError::Transport),
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
