//! De echte SigV4-client, met begrensde bodies, fouten en onderbroken requests.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use leans3::{AsyncRead, IoError, Request, Response, Transport};
use replica_core::object::{Store, StoreError};
use replica_sqlite::asynchronous::{Cancelled, Suspend};
use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    pin::{Pin, pin},
    rc::Rc,
    task::{Context, Poll, Waker},
};
struct Reply {
    status: u16,
    bytes: Vec<u8>,
    at: usize,
    length: Option<u64>,
    etag: Option<String>,
}
impl AsyncRead for Reply {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        dst: &mut [u8],
    ) -> Poll<Result<usize, IoError>> {
        let this = self.get_mut();
        let n = dst.len().min(this.bytes.len() - this.at);
        dst[..n].copy_from_slice(&this.bytes[this.at..this.at + n]);
        this.at += n;
        Poll::Ready(Ok(n))
    }
}
impl Response for Reply {
    fn status(&self) -> u16 {
        self.status
    }
    fn reason(&self) -> &str {
        ""
    }
    fn header(&self, name: &str) -> Option<&str> {
        self.etag
            .as_deref()
            .filter(|_| name.eq_ignore_ascii_case("etag"))
    }
    fn content_length(&self) -> Option<u64> {
        self.length
    }
}
#[derive(Default)]
struct World {
    replies: VecDeque<Reply>,
    calls: Vec<String>,
    conditions: Vec<String>,
    pause: bool,
    /// Ieder verzoek wacht één poll op het net, zoals een echte verbinding.
    slow: bool,
}
/// Eén keer Pending, daarna klaar: genoeg om overlap zichtbaar te maken.
struct Once(bool);
impl Future for Once {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if std::mem::replace(&mut self.0, true) {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
/// Pollt tot klaar, zoals de executor van een geparkeerde stack.
struct Spin;
impl Suspend for Spin {
    fn wait<F: Future>(&self, f: F) -> Result<F::Output, Cancelled> {
        let mut f = pin!(f);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(r) = f.as_mut().poll(&mut cx) {
                return Ok(r);
            }
        }
    }
}
struct Net(Rc<RefCell<World>>);
impl Transport for Net {
    type Response = Reply;
    async fn send(&mut self, r: Request<'_, '_>) -> Result<Reply, IoError> {
        assert!(
            r.headers
                .iter()
                .any(|h| h.name.eq_ignore_ascii_case("authorization")
                    && h.value.starts_with("AWS4-HMAC-SHA256 "))
        );
        assert_eq!(r.host, "s3.example.test");
        self.0
            .borrow_mut()
            .calls
            .push(format!("{} {}", r.method, r.target));
        for h in r.headers {
            if h.name.eq_ignore_ascii_case("if-match")
                || h.name.eq_ignore_ascii_case("if-none-match")
            {
                self.0.borrow_mut().conditions.push(format!(
                    "{}: {}",
                    h.name.to_ascii_lowercase(),
                    h.value
                ));
            }
        }
        let pause = self.0.borrow().pause;
        if pause {
            std::future::pending::<()>().await;
        }
        let slow = self.0.borrow().slow;
        if slow {
            Once(false).await;
            self.0.borrow_mut().calls.push(format!("done {}", r.target));
        }
        self.0
            .borrow_mut()
            .replies
            .pop_front()
            .ok_or(IoError::Closed)
    }
}
struct Wait {
    cancel: bool,
}
impl Suspend for Wait {
    fn wait<F: Future>(&self, f: F) -> Result<F::Output, Cancelled> {
        let mut f = pin!(f);
        let mut cx = Context::from_waker(Waker::noop());
        match f.as_mut().poll(&mut cx) {
            Poll::Ready(r) => Ok(r),
            Poll::Pending if self.cancel => Err(Cancelled),
            Poll::Pending => panic!("unexpected pending in fixture"),
        }
    }
}
/// Een eigen poort per toets (de toetsen lopen tegelijk), ruim genoeg om niets
/// te begrenzen.
fn gate() -> &'static replica_s3::Gate {
    Box::leak(Box::new(replica_s3::Gate::new(64, u64::MAX)))
}
fn client() -> leans3::Client {
    leans3::Client {
        endpoint: "https://s3.example.test".into(),
        bucket: "replica".into(),
        region: "auto".into(),
        access_key_id: "test-access".into(),
        secret_access_key: "test-secret".into(),
        session_token: String::new(),
        path_style: true,
        now: Some(|| 1_790_765_296),
    }
}
fn reply(world: &Rc<RefCell<World>>, status: u16, bytes: Vec<u8>, length: Option<u64>) {
    world.borrow_mut().replies.push_back(Reply {
        status,
        bytes,
        at: 0,
        length,
        etag: None,
    });
}
fn tagged(world: &Rc<RefCell<World>>, status: u16, bytes: &[u8], etag: &str) {
    world.borrow_mut().replies.push_back(Reply {
        status,
        bytes: bytes.to_vec(),
        at: 0,
        length: Some(bytes.len() as u64),
        etag: Some(etag.into()),
    });
}
#[test]
fn lease_is_a_conditional_put_and_412_means_held() {
    use replica_core::writer::{Backend, LeaseState, discovery::Error};
    let world = Rc::new(RefCell::new(World::default()));
    let mut s = replica_s3::S3::new(
        client(),
        {
            let w = world.clone();
            move || Net(w.clone())
        },
        Wait { cancel: false },
        gate(),
    )
    .unwrap();
    let mut lease = s.lease("replica/test/lease", 30_000);
    let state = LeaseState {
        generation: 1,
        expires_at: 1_790_765_326_000,
        owner: "a#1".into(),
    };
    reply(&world, 404, Vec::new(), Some(0));
    assert_eq!(lease.read(), Err(Error::NoLease));
    tagged(&world, 200, b"", "\"one\"");
    assert_eq!(lease.write("", &state).unwrap(), "\"one\"");
    let body = br#"{"generation":1,"expires_at":"2026-10-01T11:42:06Z","owner":"a#1"}"#;
    tagged(&world, 200, body, "\"one\"");
    let (read, etag) = lease.read().unwrap();
    assert_eq!(
        (read.owner.as_str(), read.generation, etag.as_str()),
        ("a#1", 1, "\"one\"")
    );
    // Hetzner/Ceph: 412 op de geciteerde ETag, daarna lukt de kale.
    reply(&world, 412, Vec::new(), Some(0));
    tagged(&world, 200, b"", "\"two\"");
    assert_eq!(lease.write("\"one\"", &state).unwrap(), "\"two\"");
    // Ook de kale faalt: gehouden. Een 5xx is nooit "vrij".
    reply(&world, 412, Vec::new(), Some(0));
    reply(&world, 412, Vec::new(), Some(0));
    assert_eq!(lease.write("\"two\"", &state), Err(Error::LeaseHeld));
    // Bunny-ghost bij de eerste waarneming: HEAD wordt onthouden, geen overname.
    reply(&world, 412, Vec::new(), Some(0));
    tagged(&world, 200, b"", "\"ghost\"");
    assert_eq!(lease.write("", &state), Err(Error::LeaseHeld));
    reply(&world, 503, Vec::new(), Some(0));
    assert_eq!(lease.write("\"two\"", &state), Err(Error::Unreachable));
    // Een antwoord zonder ETag kan geen volgende CAS dragen.
    reply(&world, 200, Vec::new(), Some(0));
    assert_eq!(lease.write("", &state), Err(Error::Unreachable));
    reply(&world, 200, b"{".to_vec(), Some(1));
    assert_eq!(lease.read(), Err(Error::Unreachable));
    reply(&world, 204, Vec::new(), Some(0));
    lease.delete("\"two\"").unwrap();
    assert_eq!(
        world.borrow().conditions,
        [
            "if-none-match: *",
            "if-match: \"one\"",
            "if-match: one",
            "if-match: \"two\"",
            "if-match: two",
            "if-none-match: *",
            "if-match: \"two\"",
            "if-none-match: *",
            "if-match: \"two\"",
        ]
    );
    assert!(
        world
            .borrow()
            .calls
            .iter()
            .all(|c| c.contains("replica/test/lease"))
    );
}
#[test]
fn large_segment_streams_with_budget_and_checks_truncation() {
    let world = Rc::new(RefCell::new(World::default()));
    let mut s = replica_s3::S3::new(
        client(),
        {
            let w = world.clone();
            move || Net(w.clone())
        },
        Wait { cancel: false },
        gate(),
    )
    .unwrap();
    let data = vec![37; 5 << 20];
    reply(&world, 200, data.clone(), Some(data.len() as u64));
    assert_eq!(s.get("generation/data/one", data.len()).unwrap(), data);
    reply(&world, 200, vec![1; 8192], None);
    assert!(matches!(
        s.get("generation/data/large", 4096),
        Err(StoreError::Limit)
    ));
    reply(&world, 200, vec![1; 10], Some(20));
    assert!(matches!(
        s.get("generation/data/short", 100),
        Err(StoreError::Transport)
    ));
    reply(&world, 404, Vec::new(), Some(0));
    assert!(matches!(s.get("missing", 100), Err(StoreError::Missing)));
    reply(&world, 403, Vec::new(), Some(0));
    assert!(matches!(s.get("denied", 100), Err(StoreError::Denied)));
}
#[test]
fn complete_list_only_and_delete_absent_is_success() {
    let world = Rc::new(RefCell::new(World::default()));
    let mut s = replica_s3::S3::new(
        client(),
        {
            let w = world.clone();
            move || Net(w.clone())
        },
        Wait { cancel: false },
        gate(),
    )
    .unwrap();
    let body=b"<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>gen/L0/one.json</Key></Contents></ListBucketResult>".to_vec();
    reply(&world, 200, body, None);
    assert!(matches!(s.list("gen/L", 1), Err(StoreError::Limit)));
    let body=b"<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>gen/L0/one.json</Key></Contents></ListBucketResult>".to_vec();
    reply(&world, 200, body, None);
    let list = s.list("gen/L", 2).unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].key, "gen/L0/one.json");
    reply(&world, 404, Vec::new(), Some(0));
    s.delete("missing").unwrap();
}
#[test]
fn cancellation_after_request_disables_owner_and_invalid_clock_sends_nothing() {
    let world = Rc::new(RefCell::new(World {
        pause: true,
        ..World::default()
    }));
    let mut s = replica_s3::S3::new(
        client(),
        {
            let w = world.clone();
            move || Net(w.clone())
        },
        Wait { cancel: true },
        gate(),
    )
    .unwrap();
    assert_eq!(s.put("commit", b"manifest"), Err(StoreError::Cancelled));
    assert!(matches!(s.get("commit", 100), Err(StoreError::Cancelled)));
    assert_eq!(world.borrow().calls.len(), 1);
    let mut c = client();
    c.now = Some(|| 1);
    assert!(matches!(
        replica_s3::S3::new(
            c,
            {
                let w = world.clone();
                move || Net(w.clone())
            },
            Wait { cancel: false },
            gate(),
        ),
        Err(StoreError::Configuration)
    ));
    assert_eq!(world.borrow().calls.len(), 1);
}

#[test]
fn directory_pages_skip_data_and_batches_allow_explicit_truncation() {
    let world = Rc::new(RefCell::new(World::default()));
    let mut s = replica_s3::S3::new(
        client(),
        {
            let w = world.clone();
            move || Net(w.clone())
        },
        Wait { cancel: false },
        gate(),
    )
    .unwrap();
    reply(&world, 200, b"<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>two</NextContinuationToken><Contents><Key>gen/marker</Key></Contents></ListBucketResult>".to_vec(), None);
    reply(&world, 200, b"<ListBucketResult><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>gen/one/</Prefix></CommonPrefixes></ListBucketResult>".to_vec(), None);
    assert_eq!(s.directories("gen/", 10).unwrap(), ["gen/one/"]);
    assert!(world.borrow().calls[0].contains("?delimiter=%2F&list-type=2&prefix=gen%2F"));
    assert!(world.borrow().calls[1].contains("?continuation-token=two&delimiter=%2F&list-type=2"));
    reply(&world, 200, b"<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>later</NextContinuationToken><Contents><Key>gen/one/data/1</Key></Contents><Contents><Key>gen/one/data/2</Key></Contents></ListBucketResult>".to_vec(), None);
    let batch = s.list_batch("gen/one/data/", 1).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].key, "gen/one/data/1");
    for prefix in ["gen/", "gen/one/nested/", "foreign/one/"] {
        reply(&world, 200, format!("<ListBucketResult><CommonPrefixes><Prefix>{prefix}</Prefix></CommonPrefixes></ListBucketResult>").into_bytes(), None);
        assert!(matches!(
            s.directories("gen/", 10),
            Err(StoreError::Transport)
        ));
    }
    reply(&world, 200, b"<ListBucketResult><CommonPrefixes><Prefix>gen/one/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>gen/two/</Prefix></CommonPrefixes></ListBucketResult>".to_vec(), None);
    assert!(matches!(s.directories("gen/", 1), Err(StoreError::Limit)));
    reply(
        &world,
        200,
        b"<ListBucketResult><Contents><Key>foreign/data/1</Key></Contents></ListBucketResult>"
            .to_vec(),
        None,
    );
    assert!(matches!(
        s.list_batch("gen/", 1),
        Err(StoreError::Transport)
    ));
}

#[test]
fn cancellation_of_directory_or_batch_listing_poison_the_store() {
    for directories in [false, true] {
        let world = Rc::new(RefCell::new(World {
            pause: true,
            ..World::default()
        }));
        let mut s = replica_s3::S3::new(
            client(),
            {
                let w = world.clone();
                move || Net(w.clone())
            },
            Wait { cancel: true },
            gate(),
        )
        .unwrap();
        let error = if directories {
            s.directories("gen/", 10).unwrap_err()
        } else {
            s.list_batch("gen/", 10).err().unwrap()
        };
        assert_eq!(error, StoreError::Cancelled);
        assert!(matches!(
            s.list_batch("gen/", 10),
            Err(StoreError::Cancelled)
        ));
        assert!(matches!(
            s.directories("gen/", 10),
            Err(StoreError::Cancelled)
        ));
        assert_eq!(world.borrow().calls.len(), 1);
    }
}
static CLOCK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1_790_765_296);
#[test]
fn lease_ghost_is_taken_over_only_after_a_ttl_unchanged() {
    use replica_core::writer::{Backend, LeaseState, discovery::Error};
    use std::sync::atomic::Ordering;
    let world = Rc::new(RefCell::new(World::default()));
    let mut c = client();
    c.now = Some(|| CLOCK.load(Ordering::Relaxed));
    let mut s = replica_s3::S3::new(
        c,
        {
            let w = world.clone();
            move || Net(w.clone())
        },
        Wait { cancel: false },
        gate(),
    )
    .unwrap();
    let state = LeaseState {
        generation: 1,
        expires_at: 1_790_765_356_000,
        owner: "a#1".into(),
    };
    let mut create = |etag: &str| {
        reply(&world, 412, Vec::new(), Some(0));
        tagged(&world, 200, b"", etag);
        s.lease("replica/test/lease", 30_000).write("", &state)
    };
    assert_eq!(create("\"g1\""), Err(Error::LeaseHeld));
    CLOCK.store(1_790_765_326, Ordering::Relaxed);
    // Een andere ETag is een levende schrijver: de termijn begint opnieuw.
    assert_eq!(create("\"g2\""), Err(Error::LeaseHeld));
    CLOCK.store(1_790_765_355, Ordering::Relaxed);
    assert_eq!(create("\"g2\""), Err(Error::LeaseHeld));
    CLOCK.store(1_790_765_356, Ordering::Relaxed);
    reply(&world, 412, Vec::new(), Some(0));
    tagged(&world, 200, b"", "\"g2\"");
    tagged(&world, 200, b"", "\"mine\"");
    let mut lease = s.lease("replica/test/lease", 30_000);
    assert_eq!(lease.write("", &state).unwrap(), "\"mine\"");
    assert_eq!(
        world.borrow().conditions.last().map(String::as_str),
        Some("if-match: \"g2\"")
    );
}
#[test]
fn segment_gets_overlap_across_transports_and_keep_their_order() {
    let world = Rc::new(RefCell::new(World {
        slow: true,
        ..World::default()
    }));
    let opened = Rc::new(std::cell::Cell::new(0));
    let s = replica_s3::S3::new(
        client(),
        {
            let (w, opened) = (world.clone(), opened.clone());
            move || {
                opened.set(opened.get() + 1);
                Net(w.clone())
            }
        },
        Spin,
        gate(),
    )
    .unwrap();
    assert_eq!(s.parallel().requests, replica_s3::STREAMS);
    let mut s = s.streams(3, 1 << 20);
    assert_eq!(s.parallel().requests, 3);
    // Eén verbinding bij het maken; de andere twee pas voor de ronde.
    assert_eq!(opened.get(), 1);
    reply(&world, 200, vec![1; 10], Some(10));
    reply(&world, 404, Vec::new(), Some(0));
    reply(&world, 200, vec![3; 30], Some(30));
    let replies = s.get_all(&[("g/a", 10), ("g/b", 10), ("g/c", 20)]).unwrap();
    assert_eq!(replies[0], Ok(vec![1; 10]));
    assert_eq!(replies[1], Err(StoreError::Missing));
    assert_eq!(replies[2], Err(StoreError::Limit));
    assert_eq!(opened.get(), 3);
    // Alle drie gestart vóór het eerste antwoord: echt tegelijk.
    let calls = world.borrow().calls.clone();
    assert_eq!(calls.len(), 6);
    assert!(
        calls[..3].iter().all(|c| c.starts_with("GET ")),
        "{calls:?}"
    );
    assert!(
        calls[3..].iter().all(|c| c.starts_with("done ")),
        "{calls:?}"
    );
    // Meer sleutels dan transports is een fout van de aanroeper.
    let four = [("g/a", 1), ("g/b", 1), ("g/c", 1), ("g/d", 1)];
    assert!(matches!(s.get_all(&four), Err(StoreError::Limit)));
}
#[test]
fn cancelled_parallel_round_poisons_the_store() {
    let world = Rc::new(RefCell::new(World {
        pause: true,
        ..World::default()
    }));
    let mut s = replica_s3::S3::new(
        client(),
        {
            let w = world.clone();
            move || Net(w.clone())
        },
        Wait { cancel: true },
        gate(),
    )
    .unwrap()
    .streams(2, 1 << 20);
    assert!(matches!(
        s.get_all(&[("g/a", 10), ("g/b", 10)]),
        Err(StoreError::Cancelled)
    ));
    world.borrow_mut().pause = false;
    assert!(matches!(s.get("g/a", 10), Err(StoreError::Cancelled)));
}
/// Hoeveel transports er nu bestaan (open zijn), het hoogste aantal ooit, en
/// hoeveel er ooit gemaakt zijn; gedeeld door alle stores van een toets.
#[derive(Default)]
struct Count {
    open: std::cell::Cell<usize>,
    peak: std::cell::Cell<usize>,
    made: std::cell::Cell<usize>,
}
/// Een [`Net`] die zich telt zolang hij bestaat.
struct Counted(Net, Rc<Count>);
impl Counted {
    fn new(world: &Rc<RefCell<World>>, count: &Rc<Count>) -> Self {
        count.open.set(count.open.get() + 1);
        count.made.set(count.made.get() + 1);
        count.peak.set(count.peak.get().max(count.open.get()));
        Counted(Net(world.clone()), count.clone())
    }
}
impl Drop for Counted {
    fn drop(&mut self) {
        self.1.open.set(self.1.open.get() - 1);
    }
}
impl Transport for Counted {
    type Response = Reply;
    async fn send(&mut self, r: Request<'_, '_>) -> Result<Reply, IoError> {
        self.0.send(r).await
    }
}
fn counted(
    gate: &'static replica_s3::Gate,
    count: &Rc<Count>,
) -> (Rc<RefCell<World>>, replica_s3::S3<Counted, Spin>) {
    let world = Rc::new(RefCell::new(World {
        slow: true,
        ..World::default()
    }));
    let s = replica_s3::S3::new(
        client(),
        {
            let (w, c) = (world.clone(), count.clone());
            move || Counted::new(&w, &c)
        },
        Spin,
        gate,
    )
    .unwrap();
    (world, s)
}
fn segments(world: &Rc<RefCell<World>>, n: usize) {
    for i in 0..n {
        reply(world, 200, vec![i as u8; 10], Some(10));
    }
}
#[test]
fn one_gate_bounds_the_streams_of_every_store_and_a_plain_call_drops_the_extras() {
    let gate: &'static _ = Box::leak(Box::new(replica_s3::Gate::new(4, 1 << 20)));
    let count = Rc::new(Count::default());
    let (wa, mut a) = counted(gate, &count);
    let (wb, mut b) = counted(gate, &count);
    assert_eq!(count.open.get(), 2);
    assert_eq!(a.parallel().requests, 4);
    assert_eq!(a.parallel().bytes, 1 << 20);
    // A neemt de hele poort: zijn vaste verbinding en drie extra.
    let keys = [("g/a", 10), ("g/b", 10), ("g/c", 10), ("g/d", 10)];
    segments(&wa, 4);
    let replies = a.get_all(&keys).unwrap();
    assert!(replies.iter().all(Result::is_ok), "{replies:?}");
    assert_eq!(count.open.get(), 5);
    assert_eq!(gate.free(), (0, (1 << 20) - 40));
    // B krijgt niets: één stroom, na elkaar over zijn vaste verbinding.
    assert_eq!(b.parallel().requests, 1);
    segments(&wb, 3);
    let replies = b.get_all(&keys[..3]).unwrap();
    assert_eq!(replies[2], Ok(vec![2; 10]));
    assert_eq!(count.open.get(), 5);
    let calls = wb.borrow().calls.clone();
    assert!(
        calls
            .chunks(2)
            .all(|c| c[0].starts_with("GET ") && c[1].starts_with("done ")),
        "na elkaar: {calls:?}"
    );
    // De volgende ronde van A hergebruikt zijn extra verbindingen.
    segments(&wa, 4);
    a.get_all(&keys).unwrap();
    assert_eq!((count.open.get(), count.made.get()), (5, 5));
    // Een call die geen ronde is: de extra vallen, de poort is weer vrij.
    reply(&wa, 200, Vec::new(), Some(0));
    a.put("g/manifest", b"m").unwrap();
    assert_eq!(count.open.get(), 2);
    assert_eq!(gate.free(), (4, 1 << 20));
    // Nu krijgt B ze, tot zijn eigen grens.
    let mut b = b.streams(3, 1 << 20);
    assert_eq!(b.parallel().requests, 3);
    segments(&wb, 3);
    b.get_all(&keys[..3]).unwrap();
    assert_eq!(count.open.get(), 4);
    // Het einde van een herstel zonder volgende call: `rest`.
    b.rest();
    assert_eq!(count.open.get(), 2);
    assert_eq!(gate.free(), (4, 1 << 20));
    // Nooit meer dan de poort in rondes, plus de vaste verbinding van de ander.
    assert_eq!(count.peak.get(), 5);
    // De drop geeft terug wat een store nog had.
    segments(&wb, 2);
    b.get_all(&keys[..2]).unwrap();
    assert_eq!(gate.free().0, 2);
    drop(b);
    assert_eq!(gate.free(), (4, 1 << 20));
    assert_eq!(count.open.get(), 1);
}
#[test]
fn every_plain_call_ends_the_round() {
    let gate: &'static _ = Box::leak(Box::new(replica_s3::Gate::new(4, 1 << 20)));
    let count = Rc::new(Count::default());
    let (w, mut s) = counted(gate, &count);
    let keys = [("g/a", 10), ("g/b", 10)];
    let list = b"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>".to_vec();
    for call in [
        "get",
        "list",
        "list_batch",
        "directories",
        "delete",
        "put",
        "put_new",
        "rest",
    ] {
        segments(&w, 2);
        s.get_all(&keys).unwrap();
        assert_eq!(count.open.get(), 2, "{call}");
        match call {
            "get" => {
                reply(&w, 200, vec![1], Some(1));
                s.get("g/a", 10).unwrap();
            }
            "list" => {
                reply(&w, 200, list.clone(), None);
                s.list("g/", 10).unwrap();
            }
            "list_batch" => {
                reply(&w, 200, list.clone(), None);
                s.list_batch("g/", 10).unwrap();
            }
            "directories" => {
                reply(&w, 200, list.clone(), None);
                s.directories("g/", 10).unwrap();
            }
            "put" | "put_new" => {
                reply(&w, 200, Vec::new(), Some(0));
                if call == "put" {
                    s.put("g/c", b"c").unwrap();
                } else {
                    s.put_new("g/c", b"c").unwrap();
                }
            }
            "rest" => Store::rest(&mut s),
            _ => {
                reply(&w, 204, Vec::new(), Some(0));
                s.delete("g/a").unwrap();
            }
        }
        assert_eq!(count.open.get(), 1, "{call}");
        assert_eq!(gate.free(), (4, 1 << 20), "{call}");
    }
}
#[test]
fn the_lease_passes_while_the_gate_is_full_and_keeps_the_round() {
    use replica_core::writer::{Backend, LeaseState};
    let gate: &'static _ = Box::leak(Box::new(replica_s3::Gate::new(2, u64::MAX)));
    let count = Rc::new(Count::default());
    let (wa, mut a) = counted(gate, &count);
    let (wb, mut b) = counted(gate, &count);
    let keys = [("g/a", 10), ("g/b", 10)];
    segments(&wa, 2);
    a.get_all(&keys).unwrap();
    assert_eq!(gate.free().0, 0);
    // B's lease (en een gewone call) wachten niet op de poort.
    let state = LeaseState {
        generation: 1,
        expires_at: 1_790_765_326_000,
        owner: "b#1".into(),
    };
    tagged(&wb, 200, b"", "\"b\"");
    assert_eq!(
        b.lease("replica/b/lease", 30_000)
            .write("", &state)
            .unwrap(),
        "\"b\""
    );
    reply(&wb, 200, vec![7], Some(1));
    assert_eq!(b.get("g/x", 10).unwrap(), [7]);
    // A's eigen lease midden in zijn herstel laat de extra verbinding staan.
    let body = br#"{"generation":1,"expires_at":"2026-10-01T11:42:06Z","owner":"a#1"}"#;
    tagged(&wa, 200, body, "\"a\"");
    a.lease("replica/a/lease", 30_000).read().unwrap();
    assert_eq!((count.open.get(), gate.free().0), (3, 0));
    segments(&wa, 2);
    a.get_all(&keys).unwrap();
    assert_eq!(count.made.get(), 3);
}
#[test]
fn a_round_wider_than_the_gate_goes_in_pieces_within_the_byte_budget() {
    let gate: &'static _ = Box::leak(Box::new(replica_s3::Gate::new(2, 100)));
    let count = Rc::new(Count::default());
    let (w, s) = counted(gate, &count);
    let mut s = s.streams(3, 1 << 20);
    // De poort begrenst wat de store plant: twee stromen, 100 bytes.
    assert_eq!(s.parallel().requests, 2);
    assert_eq!(s.parallel().bytes, 100);
    // Een aanroeper die toch drie vraagt, krijgt ze in twee stukken, in volgorde.
    segments(&w, 3);
    let replies = s.get_all(&[("g/a", 60), ("g/b", 60), ("g/c", 60)]).unwrap();
    assert_eq!(replies, [Ok(vec![0; 10]), Ok(vec![1; 10]), Ok(vec![2; 10])]);
    assert_eq!((count.open.get(), count.made.get()), (2, 2));
    // De bytes van die ronde zijn genomen (tot wat er was) en gaan bij de
    // volgende ronde of de rust terug.
    assert_eq!(gate.free(), (0, 0));
    assert_eq!(s.parallel().bytes, 100);
    segments(&w, 2);
    s.get_all(&[("g/a", 10), ("g/b", 10)]).unwrap();
    assert_eq!(gate.free(), (0, 80));
    s.rest();
    assert_eq!(gate.free(), (2, 100));
}
