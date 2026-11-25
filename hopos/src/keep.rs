//! De waiter van de store van een eigenaar op HopOS: wacht als [`crate::Wait`]
//! en houdt intussen de lease van zijn open groep bij
//! ([`replica_core::lanes::KeepLease`]).
//!
//! Een S3-call van de eigenaar parkeert zijn stack. De eigenaar zet een
//! vernieuwing op de leasebaan alleen als hij zelf loopt, dus één call die
//! langer wacht dan de marge van de lease (een segment-GET die zijn budget
//! vol maakt en opnieuw gaat) liet de lease stil verlopen (een host-app,
//! 23-11-2026: een GET van 39 s bij een lease van 30 s). [`Keeping`] verlaat
//! de wacht elke [`KEEP_EVERY`], roept [`KeepLease::keep`] aan (de groep zet
//! een vernieuwing op de leasebaan als die aan de beurt is) en wacht verder
//! op dezelfde call: de future van de call blijft gepind, er wordt niets
//! dubbel gepolld. Zonder open groep is het een gewone wacht.
//!
//! Alleen de datacalls van de eigenaar: een baan en de leasecalls zelf
//! (claim, vrijgeven) wachten gewoon.
use crate::lanes::Sleep;
use core::{future::Future, pin::pin, time::Duration};
use hopsync::{Either, select};
use replica_core::lanes::KeepLease;
use replica_sqlite::asynchronous::{Cancelled, Suspend};

/// Zo vaak wordt de lease bijgehouden terwijl een call wacht.
pub const KEEP_EVERY: Duration = Duration::from_secs(1);

/// Waar [`Keeping`] zijn seconden van haalt.
#[derive(Clone, Copy)]
enum Tick<'k> {
    /// Het timerwiel van de executor van de app (`applib::EXEC`).
    Exec,
    /// De slaap van de host (een toets, een eigen wiel).
    Sleep(Sleep<'k>),
}

/// Een waiter die op `W` parkeert en intussen de lease van `keep` bijhoudt
/// (zie de moduledocs). Voor de store van de eigenaar:
/// `replica_s3::S3::new(client, open, Keeping::new(Wait(sus), &keep), gate)`,
/// en dezelfde `keep` aan `Group::with_keep_lease`.
#[derive(Clone, Copy)]
pub struct Keeping<'k, W> {
    wait: W,
    keep: &'k KeepLease<'k>,
    tick: Tick<'k>,
    /// Een wacht van minstens zoveel tikken wordt gemeld (met het aantal).
    long: Option<(u32, &'k dyn Fn(u32))>,
}
impl<'k, W: Suspend> Keeping<'k, W> {
    /// Parkeert op `wait` en houdt de lease van `keep` bij, elke
    /// [`KEEP_EVERY`] op het timerwiel van de app.
    pub fn new(wait: W, keep: &'k KeepLease<'k>) -> Self {
        Keeping {
            wait,
            keep,
            tick: Tick::Exec,
            long: None,
        }
    }
    /// Als [`Keeping::new`], met de slaap van de host in plaats van het
    /// timerwiel van de app.
    pub fn with_sleep(wait: W, keep: &'k KeepLease<'k>, sleep: Sleep<'k>) -> Self {
        Keeping {
            wait,
            keep,
            tick: Tick::Sleep(sleep),
            long: None,
        }
    }
    /// Een call die minstens `ticks` keer [`KEEP_EVERY`] wachtte, wordt na
    /// afloop gemeld aan `report` (met het aantal tikken): de verzoeken van de
    /// eigenaar wachtten zo lang. Alleen terwijl er een lease bij te houden is.
    #[must_use]
    pub fn with_long(mut self, ticks: u32, report: &'k dyn Fn(u32)) -> Self {
        self.long = Some((ticks, report));
        self
    }
    /// Wacht op `future` en roept na elke `tick()` die eerder klaar is
    /// [`KeepLease::keep`] aan.
    fn run<F: Future, T: Future<Output = ()>>(
        &self,
        future: F,
        tick: impl Fn() -> T,
    ) -> Result<F::Output, Cancelled> {
        let mut future = pin!(future);
        let mut ticks = 0_u32;
        loop {
            match self.wait.wait(select(future.as_mut(), tick()))? {
                Either::Left(out) => {
                    if let Some((most, report)) = self.long
                        && ticks >= most
                    {
                        report(ticks);
                    }
                    return Ok(out);
                }
                // Tussen twee wachtbeurten, op de stack van de eigenaar.
                Either::Right(()) => {
                    ticks = ticks.saturating_add(1);
                    self.keep.keep();
                }
            }
        }
    }
}
impl<W: Suspend> Suspend for Keeping<'_, W> {
    fn wait<F: Future>(&self, future: F) -> Result<F::Output, Cancelled> {
        if !self.keep.is_active() {
            return self.wait.wait(future);
        }
        match self.tick {
            Tick::Exec => self.run(future, || applib::EXEC.get().after(KEEP_EVERY)),
            Tick::Sleep(sleep) => self.run(future, || sleep(KEEP_EVERY)),
        }
    }
}
