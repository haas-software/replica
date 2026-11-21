//! De banen van een slot op echte stacks (`applib::stacktask`, dezelfde code
//! als op HopOS), gepold door een executor in het klein: jobs lopen per baan
//! op volgorde, de leasebaan wacht niet op een hangende upload, en `settle`
//! wacht tot de banen leeg zijn of tot zijn slaap afloopt.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use applib::stacktask::{Suspender, Task};
use core::{
    cell::{Cell, RefCell},
    future::{Future, pending, poll_fn, ready},
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};
use replica_core::{
    lanes::{Lane, LaneSide, LaneStore, Offload},
    object::StoreError,
    writer::Backend,
};
use replica_hopos::lanes::{Lanes, OwnerLanes};
use replica_sqlite::Storage;

/// De kant van een baan: elke `files` noteert zijn domein, en een domein
/// `hang` parkeert tot de toets `release` zet (S3 die niet antwoordt).
struct Side<'a> {
    sus: &'a Suspender,
    trace: &'a RefCell<Vec<String>>,
    release: &'a Cell<bool>,
}
impl LaneSide for Side<'_> {
    fn files(
        &mut self,
        domain: &str,
        _f: &mut dyn FnMut(&mut dyn Storage, &mut dyn LaneStore),
    ) -> replica_core::Result {
        if domain == "hang" {
            let release = self.release;
            let _ = self.sus.wait(poll_fn(|cx| {
                if release.get() {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }));
        }
        self.trace.borrow_mut().push(format!("data {domain}"));
        Ok(())
    }
    fn lease(
        &mut self,
        key: &str,
        _ttl_ms: u64,
        _f: &mut dyn FnMut(&mut dyn Backend),
    ) -> Result<(), StoreError> {
        self.trace.borrow_mut().push(format!("lease {key}"));
        Ok(())
    }
}

fn data(domain: &'static str) -> replica_core::lanes::Job {
    replica_core::lanes::Job::new(move |side: &mut dyn LaneSide| {
        side.files(domain, &mut |_, _| {}).unwrap();
    })
}
fn lease(key: &'static str) -> replica_core::lanes::Job {
    replica_core::lanes::Job::new(move |side: &mut dyn LaneSide| {
        side.lease(key, 30_000, &mut |_| {}).unwrap();
    })
}

#[test]
fn jobs_run_per_lane_in_order_and_settle_waits_for_idle_lanes() {
    let lanes = Lanes::new();
    let trace = RefCell::new(Vec::new());
    let release = Cell::new(false);
    let settled = RefCell::new(Vec::new());
    let (lanes, trace, release, settled) = (&lanes, &trace, &release, &settled);
    let lane_task = |lane: Lane| {
        let run = move |sus: &Suspender| {
            let mut side = Side {
                sus,
                trace,
                release,
            };
            lanes.run(lane, sus, &mut side);
        };
        // SAFETY: De lus van één baan met een kant die alleen parkeert via
        // `Suspender::wait`; geen verwijzing naar de stack na terugkeer, geen longjmp.
        unsafe { Task::new(1 << 20, run) }.unwrap()
    };
    let owner = move |sus: &Suspender| {
        // Nooit af: alleen lege banen eindigen het wachten.
        let never = |_: Duration| -> Pin<Box<dyn Future<Output = ()>>> { Box::pin(pending()) };
        // Meteen af: de grens van het wachten.
        let now = |_: Duration| -> Pin<Box<dyn Future<Output = ()>>> { Box::pin(ready(())) };
        let off = OwnerLanes::new(lanes, sus, &never);
        off.push(Lane::Data, data("a"));
        off.push(Lane::Data, data("b"));
        off.push(Lane::Lease, lease("l1"));
        off.breathe();
        settled.borrow_mut().push(off.settle(1_000));
        // Een hangende upload houdt de leasebaan niet op.
        off.push(Lane::Data, data("hang"));
        off.push(Lane::Data, data("c"));
        off.push(Lane::Lease, lease("l2"));
        for _ in 0..3 {
            off.breathe();
        }
        let bounded = OwnerLanes::new(lanes, sus, &now);
        settled.borrow_mut().push(bounded.settle(1_000));
        trace.borrow_mut().push("owner waited".into());
        release.set(true);
        settled.borrow_mut().push(off.settle(1_000));
    };
    // SAFETY: Idem; de eigenaar parkeert alleen via `Suspender::wait`.
    let mut owner = unsafe { Task::new(1 << 20, owner) }.unwrap();
    let mut tasks = [lane_task(Lane::Lease), lane_task(Lane::Data)];
    let mut cx = Context::from_waker(Waker::noop());
    let mut done = false;
    for _ in 0..10_000 {
        for t in &mut tasks {
            let _ = Pin::new(t).poll(&mut cx);
        }
        if Pin::new(&mut owner).poll(&mut cx).is_ready() {
            done = true;
            break;
        }
    }
    assert!(done, "de eigenaar kwam niet klaar: {:?}", trace.borrow());
    assert_eq!(*settled.borrow(), [true, false, true]);
    let trace = trace.borrow();
    let pos = |s: &str| trace.iter().position(|l| l == s).unwrap();
    assert!(pos("data a") < pos("data b"));
    assert!(pos("lease l1") < pos("data hang"));
    assert!(pos("lease l2") < pos("owner waited"), "{trace:?}");
    assert!(pos("owner waited") < pos("data hang"));
    assert!(pos("data hang") < pos("data c"));
    // De banen eindigen als de app hun taken dropt.
    drop(tasks);
}
