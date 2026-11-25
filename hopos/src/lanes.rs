//! De banen van een eigenaarslot op HopOS ([`replica_core::lanes`]): het
//! S3-werk van de replica op twee eigen stacks naast die van de eigenaar.
//!
//! Een S3-call parkeert de stack die hem doet. Op de stack van een eigenaar
//! die één verzoek tegelijk bedient, hield elke publicatie, onderhoudsbeurt en
//! leasevernieuwing alle verzoeken op zolang S3 erover deed (een host-app op de
//! M4, 20-11-2026). Met banen:
//!
//! - **Leasebaan**: de schrijverlease, één kleine voorwaardelijke call per
//!   paar seconden; nooit in de rij achter een upload.
//! - **Databaan**: de uploads van de gecapturede pagina's en het onderhoud,
//!   elk met een eigen lokaal handvat en de S3-verbindingen van deze baan.
//! - **De eigenaar** neemt alleen de rustmomenten voor wat rust vraagt (zie
//!   [`replica_core::group::Group::rest`]).
//!
//! [`Lanes::spawn`] maakt de twee `applib::stacktask`s van een slot en laat
//! op elk [`Lanes::run`] lopen; de host geeft alleen de stackmaten
//! ([`Stacks`]) en opent op de stack van een baan zijn [`LaneSide`]: zijn
//! lokale opslag en zijn store, met eigen verbindingen, die op die stack
//! parkeren. De eigenaar ziet de banen als [`OwnerLanes`] ([`Lanes::owner`]),
//! een [`Offload`]. De banen van een slot leven zo lang als de app; de
//! domeinen (tenants) van het slot komen en gaan.
use alloc::{boxed::Box, collections::VecDeque, format, string::String, vec::Vec};
use applib::{
    EXEC,
    stacktask::{self, Suspender, Task},
};
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
    /// Twee banen op eigen stacks (`applib::stacktask`), elk op de executor
    /// van de app, voor zo lang als de app leeft. Op de stack van een baan
    /// roept de taak `open(lane, sus, run)` aan: de host opent daar zijn kant
    /// (lokale opslag en store, die op `sus` parkeren) en geeft hem aan
    /// `run`, dat de jobs van die baan draait tot de app de taak dropt. Kan
    /// de host zijn kant niet openen, dan meldt hij dat en keert terug: die
    /// baan loopt dan niet, en de groep ziet zijn jobs nooit landen (zoals
    /// een store die niet antwoordt).
    ///
    /// # Safety
    ///
    /// Het contract van `applib::stacktask::Task::new`, voor het deel van de
    /// host: zijn kant past met zijn diepste call (een opslag- of storecall,
    /// een TLS-handshake, een compactie van replica-core) in de stack van die
    /// baan ([`Stacks`]); `open` keert terug na `Cancelled` (de wacht op de
    /// volgende job of een call van zijn store eindigt dan), bewaart geen
    /// verwijzing naar de stack, doet geen longjmp en draait er geen
    /// SQLite-verbinding op.
    #[allow(
        unsafe_code,
        reason = "de stacks van de banen; het contract staat hierboven"
    )]
    pub unsafe fn spawn<O>(stacks: Stacks, open: O) -> Result<&'static Lanes, SpawnError>
    where
        O: Fn(Lane, &Suspender, &mut dyn FnMut(&mut dyn LaneSide)) + Clone + 'static,
    {
        let lanes: &'static Lanes = Box::leak(Box::new(Lanes::new()));
        for lane in [Lane::Lease, Lane::Data] {
            let open = open.clone();
            let run = move |sus: &Suspender| {
                // Eindigt als de app de taak dropt (de stop).
                open(lane, sus, &mut |side| lanes.run(lane, sus, side));
            };
            // SAFETY: Het deel van de host staat in het contract van deze
            // functie. Het deel van replica: de lus wacht alleen in
            // `Suspender::wait` en keert terug na `Cancelled`; een job van de
            // groep wisselt alleen binnen een opslag- of storecall van de
            // kant, en eigenaar en banen delen alleen `RefCell`s die nooit
            // over zo'n call geleend worden (het rustprotocol van
            // `replica_core::owner`).
            let task = unsafe { Task::new(stacks.of(lane), run) }
                .map_err(|e| SpawnError::Stack(lane, e))?;
            EXEC.get()
                .spawn(task)
                .map_err(|e| SpawnError::Task(lane, format!("{e:?}")))?;
        }
        Ok(lanes)
    }
    /// De banen zoals de eigenaar op `sus` ze ziet; [`Offload::settle`]
    /// wacht op een timer van de executor van de app.
    pub fn owner<'s>(&'s self, sus: &'s Suspender) -> OwnerLanes<'s> {
        OwnerLanes::new(self, sus, &app_sleep)
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
/// De stackmaten van de banen van een slot, in bytes (64 KiB tot 16 MiB,
/// zoals `applib::stacktask`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stacks {
    /// De leasebaan: één kleine voorwaardelijke call tegelijk en zijn TLS-handshake.
    pub lease: usize,
    /// De databaan: de uploads, de manifesten, het onderhoud (ook een
    /// compactie) en hun TLS-handshakes.
    pub data: usize,
}
impl Stacks {
    /// De leasebaan: 2 MiB.
    pub const LEASE: usize = 2 << 20;
    /// [`Stacks::LEASE`] voor de leasebaan, `data` (minstens zoveel) voor de databaan.
    pub fn new(data: usize) -> Self {
        Stacks {
            lease: Self::LEASE,
            data: data.max(Self::LEASE),
        }
    }
    fn of(self, lane: Lane) -> usize {
        match lane {
            Lane::Lease => self.lease,
            Lane::Data => self.data,
        }
    }
}
/// Een baan kon niet starten ([`Lanes::spawn`]).
#[derive(Debug)]
pub enum SpawnError {
    /// Zijn stack: een maat buiten het bereik, of geen geheugen.
    Stack(Lane, stacktask::Error),
    /// De executor van de app nam de taak niet aan (vol, of te veel taken).
    Task(Lane, String),
}
/// Een timer van de executor van de app, de grens van [`Offload::settle`].
fn app_sleep<'s>(d: Duration) -> Pin<Box<dyn Future<Output = ()> + 's>> {
    Box::pin(EXEC.get().after(d))
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
