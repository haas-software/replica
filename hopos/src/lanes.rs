//! De banen van een eigenaarslot op HopOS ([`replica_core::lanes`]): het
//! S3-werk van de replica op twee eigen stacks naast die van de eigenaar.
//!
//! Een S3-call parkeert de stack die hem doet. Op de stack van een eigenaar
//! die één verzoek tegelijk bedient, hield elke publicatie, onderhoudsbeurt en
//! leasevernieuwing alle verzoeken op zolang S3 erover deed (EasyFlor op de
//! M4, 20-11-2026). Met banen:
//!
//! - **Leasebaan**: de schrijverlease, één kleine voorwaardelijke call per
//!   paar seconden; nooit in de rij achter een upload.
//! - **Databaan**: de uploads van de gecapturede pagina's en het onderhoud,
//!   elk met een eigen lokaal handvat en de S3-verbindingen van deze baan.
//! - **De eigenaar** neemt alleen de rustmomenten voor wat rust vraagt (zie
//!   [`replica_core::group::Group::rest`]).
//!
//! De host maakt per baan een `applib::stacktask` (de stack en zijn
//! veiligheidsargument zijn van de host) en laat die [`Lanes::run`] draaien
//! met de [`LaneSide`] van die stack: zijn lokale opslag en zijn store, die
//! op die stack parkeren. De eigenaar ziet de banen als [`OwnerLanes`], een
//! [`Offload`]. De banen van een slot leven zo lang als de app; de domeinen
//! (tenants) van het slot komen en gaan.
use alloc::{boxed::Box, collections::VecDeque, vec::Vec};
use applib::stacktask::Suspender;
use core::{
    cell::{Cell, RefCell},
    future::{Future, poll_fn},
    pin::Pin,
    task::{Poll, Waker},
    time::Duration,
};
use hopsync::{Either, select};
use replica_core::lanes::{Job, Lane, LaneSide, Offload};
/// Eén baan: zijn rij, één job tegelijk op zijn stack.
struct Queue {
    jobs: RefCell<VecDeque<Job>>,
    /// De stack van de baan, wachtend op een job.
    worker: RefCell<Option<Waker>>,
    /// Er loopt een job.
    running: Cell<bool>,
    /// Eigenaars die wachten tot de banen leeg zijn ([`Offload::settle`]).
    idle: RefCell<Vec<Waker>>,
}
impl Queue {
    const fn new() -> Self {
        Queue {
            jobs: RefCell::new(VecDeque::new()),
            worker: RefCell::new(None),
            running: Cell::new(false),
            idle: RefCell::new(Vec::new()),
        }
    }
    fn push(&self, job: Job) {
        self.jobs.borrow_mut().push_back(job);
        if let Some(w) = self.worker.borrow_mut().take() {
            w.wake();
        }
    }
    /// De volgende job, zodra er een is.
    async fn next(&self) -> Job {
        poll_fn(|cx| match self.jobs.borrow_mut().pop_front() {
            Some(job) => Poll::Ready(job),
            None => {
                *self.worker.borrow_mut() = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
    fn is_idle(&self) -> bool {
        !self.running.get() && self.jobs.borrow().is_empty()
    }
    /// Een job is klaar: de eigenaars die op lege banen wachten, kijken opnieuw.
    fn done(&self) {
        self.running.set(false);
        if self.jobs.borrow().is_empty() {
            let waiting = core::mem::take(&mut *self.idle.borrow_mut());
            waiting.into_iter().for_each(Waker::wake);
        }
    }
}
/// De twee banen van één slot.
pub struct Lanes {
    lease: Queue,
    data: Queue,
}
impl Default for Lanes {
    fn default() -> Self {
        Self::new()
    }
}
impl Lanes {
    /// Twee lege banen; de host laat ze lopen met [`Lanes::run`].
    pub const fn new() -> Self {
        Lanes {
            lease: Queue::new(),
            data: Queue::new(),
        }
    }
    fn queue(&self, lane: Lane) -> &Queue {
        match lane {
            Lane::Lease => &self.lease,
            Lane::Data => &self.data,
        }
    }
    fn is_idle(&self) -> bool {
        self.lease.is_idle() && self.data.is_idle()
    }
    /// De lus van baan `lane` op de stack van `sus`: elke job één voor één,
    /// met `side`. Keert terug als de app de taak dropt (de wacht op de
    /// volgende job eindigt met `Cancelled`).
    pub fn run(&self, lane: Lane, sus: &Suspender, side: &mut dyn LaneSide) {
        let queue = self.queue(lane);
        while let Ok(job) = sus.wait(queue.next()) {
            queue.running.set(true);
            job.run(side);
            queue.done();
        }
    }
}
/// Een slaap van `Duration` op de timer van de host (bijvoorbeeld zijn
/// deadlinewiel), voor de grens van [`Offload::settle`].
pub type Sleep<'s> = &'s dyn Fn(Duration) -> Pin<Box<dyn Future<Output = ()> + 's>>;
/// De banen zoals één eigenaar ze ziet: hij parkeert op zijn eigen stack
/// (`sus`) om ze te laten lopen.
pub struct OwnerLanes<'s> {
    lanes: &'s Lanes,
    sus: &'s Suspender,
    sleep: Sleep<'s>,
}
impl<'s> OwnerLanes<'s> {
    /// De eigenaar op `sus` met de `lanes` van zijn slot; `sleep` begrenst
    /// het wachten van [`Offload::settle`].
    pub fn new(lanes: &'s Lanes, sus: &'s Suspender, sleep: Sleep<'s>) -> Self {
        OwnerLanes { lanes, sus, sleep }
    }
}
impl Offload for OwnerLanes<'_> {
    fn push(&self, lane: Lane, job: Job) {
        self.lanes.queue(lane).push(job);
    }
    fn breathe(&self) {
        let _ = self.sus.wait(hopsync::yield_now());
    }
    fn settle(&self, ms: u64) -> bool {
        let lanes = self.lanes;
        let idle = poll_fn(|cx| {
            if lanes.is_idle() {
                return Poll::Ready(());
            }
            for q in [&lanes.lease, &lanes.data] {
                let mut idle = q.idle.borrow_mut();
                if !q.is_idle() && !idle.iter().any(|w| w.will_wake(cx.waker())) {
                    idle.push(cx.waker().clone());
                }
            }
            Poll::Pending
        });
        let waited = self
            .sus
            .wait(select(idle, (self.sleep)(Duration::from_millis(ms))));
        matches!(waited, Ok(Either::Left(()))) || lanes.is_idle()
    }
}
