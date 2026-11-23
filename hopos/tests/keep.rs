//! De waiter van de store van een eigenaar ([`Keeping`]): elke tik die eerder
//! klaar is dan de call, houdt hij de lease bij en wacht hij verder op
//! dezelfde call; zonder open lease is het een gewone wacht zonder tikken.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use core::{
    cell::Cell,
    future::{Future, ready},
    pin::{Pin, pin},
    task::{Context, Poll, Waker},
    time::Duration,
};
use replica_core::lanes::{Keep, KeepLease};
use replica_hopos::keep::{KEEP_EVERY, Keeping};
use replica_sqlite::asynchronous::{Cancelled, Suspend};
use std::rc::{Rc, Weak};

/// Pollt tot klaar, zoals de executor van een geparkeerde stack.
#[derive(Clone, Copy)]
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

/// Een call die pas bij zijn `n`-de poll klaar is; telt zijn polls.
struct Call<'a> {
    left: u32,
    polls: &'a Cell<u32>,
}
impl Future for Call<'_> {
    type Output = &'static str;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<&'static str> {
        self.polls.set(self.polls.get() + 1);
        self.left -= 1;
        if self.left == 0 {
            Poll::Ready("antwoord")
        } else {
            Poll::Pending
        }
    }
}

/// Telt de keren dat de lease bijgehouden werd.
#[derive(Default)]
struct Counter {
    kept: Cell<u32>,
}
impl Keep for Counter {
    fn keep(&self) {
        self.kept.set(self.kept.get() + 1);
    }
}

#[test]
fn every_tick_before_the_call_keeps_the_lease_and_the_call_goes_on() {
    let keep = KeepLease::default();
    let counter = Rc::new(Counter::default());
    keep.hold(Rc::downgrade(&counter) as Weak<dyn Keep>);
    let ticks = Cell::new(0_u32);
    // Een tik is meteen klaar: elke wachtbeurt waarin de call niet klaar is, is een seconde.
    let sleep = |d: Duration| -> Pin<Box<dyn Future<Output = ()> + '_>> {
        assert_eq!(d, KEEP_EVERY);
        ticks.set(ticks.get() + 1);
        Box::pin(ready(()))
    };
    let long = Cell::new(None);
    let report = |n: u32| long.set(Some(n));
    let waiter = Keeping::with_sleep(Spin, &keep, &sleep).with_long(3, &report);
    let polls = Cell::new(0);
    let out = waiter.wait(Call {
        left: 5,
        polls: &polls,
    });
    assert_eq!(out, Ok("antwoord"));
    // Vijf polls van dezelfde call (niet opnieuw begonnen), vier tikken ertussen.
    assert_eq!(polls.get(), 5);
    assert_eq!(counter.kept.get(), 4);
    assert_eq!(long.get(), Some(4), "een wacht van 4 tikken wordt gemeld");
    // Een korte call: geen melding.
    long.set(None);
    let polls = Cell::new(0);
    assert_eq!(
        waiter.wait(Call {
            left: 2,
            polls: &polls
        }),
        Ok("antwoord")
    );
    assert_eq!(counter.kept.get(), 5);
    assert_eq!(long.get(), None);
}

#[test]
fn without_an_open_lease_it_is_a_plain_wait() {
    let keep = KeepLease::default();
    let ticks = Cell::new(0_u32);
    let sleep = |_: Duration| -> Pin<Box<dyn Future<Output = ()> + '_>> {
        ticks.set(ticks.get() + 1);
        Box::pin(ready(()))
    };
    let waiter = Keeping::with_sleep(Spin, &keep, &sleep);
    let polls = Cell::new(0);
    assert_eq!(
        waiter.wait(Call {
            left: 3,
            polls: &polls
        }),
        Ok("antwoord")
    );
    assert_eq!(ticks.get(), 0, "geen open lease: geen tikken");
    // Een gesloten groep (alleen nog een zwakke verwijzing) telt als geen.
    {
        let counter = Rc::new(Counter::default());
        keep.hold(Rc::downgrade(&counter) as Weak<dyn Keep>);
        assert!(keep.is_active());
    }
    assert!(!keep.is_active());
    let polls = Cell::new(0);
    assert_eq!(
        waiter.wait(Call {
            left: 3,
            polls: &polls
        }),
        Ok("antwoord")
    );
    assert_eq!(ticks.get(), 0);
}

/// Houdt de lease bij en wacht daarbij zelf weer op dezelfde haak.
struct Nested<'k> {
    keep: &'k KeepLease<'k>,
    kept: Cell<u32>,
}
impl Keep for Nested<'_> {
    fn keep(&self) {
        self.kept.set(self.kept.get() + 1);
        self.keep.keep();
    }
}

#[test]
fn a_keep_that_waits_on_the_same_hook_does_not_nest() {
    let keep = KeepLease::default();
    let nested = Rc::new(Nested {
        keep: &keep,
        kept: Cell::new(0),
    });
    keep.hold(Rc::downgrade(&nested) as Weak<dyn Keep + '_>);
    keep.keep();
    keep.keep();
    assert_eq!(nested.kept.get(), 2, "één per aanroep, geen nesting");
}
