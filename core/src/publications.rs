//! Welke writes de store al heeft: per database de captures die begonnen en
//! de laatste daarvan die gepubliceerd is, in dit leven van de eigenaar.
//!
//! Een host die op "de volgende publicatie" wacht (een verplaatsing die pas
//! de bron mag wissen als de kopie in de store staat; een proef die een
//! write doet en dan hard stopt), moet weten of die publicatie zijn write
//! bevat. Met [`crate::lanes`] wordt een capture in het ene rustmoment
//! genomen en in een later moment gepubliceerd, nadat de upload landde; een
//! write die ertussen kwam, zit er niet in. Een teller die elke landing telde,
//! bevestigde zo een write die nog niet in de store stond (EasyFlor,
//! 21-11-2026: een jaarbestand gepubliceerd zonder de factuur waarop gewacht
//! werd, en een verplaatsing die haar router kon omzetten vóór haar kopie
//! gepubliceerd was).
//!
//! Daarom nummert de eigenaar zelf zijn captures ([`crate::owner::Replica::begun`],
//! [`crate::owner::Pending::capture`], [`crate::owner::Synced::capture`]),
//! ook een hervatte upload van vóór een herstart. Dit is de gedeelde kijk
//! erop per database, die [`crate::group::Group`] na elke stap bijwerkt
//! ([`Publications::record`]): [`Publications::count`] is het nummer van de
//! laatst gepubliceerde capture. Een wachter onthoudt [`Publications::begun`]
//! en wacht tot `count` er voorbij is; zelf tellen hoeft (en kan) niemand.
//! De host deelt dit (een `Rc`) met wie wacht, op dezelfde stack.
//!
//! Een toets kan databases tegenhouden ([`Publications::hold`]): de groep
//! publiceert er dan niets van, ook niet bij het sluiten, alsof het proces
//! ervoor stopte.
use alloc::{string::String, vec::Vec};
use core::cell::RefCell;
/// De publicaties van de databases van één eigenaar.
#[derive(Debug, Default)]
pub struct Publications {
    /// Per database: begonnen captures en het nummer van de laatst gepubliceerde.
    counts: RefCell<Vec<(String, Counts)>>,
    held: RefCell<Vec<String>>,
}
/// De captures van één database.
#[derive(Clone, Copy, Debug, Default)]
struct Counts {
    begun: u64,
    published: u64,
}
impl Publications {
    /// Nog geen publicatie.
    pub fn new() -> Self {
        Self::default()
    }
    fn get(&self, file: &str) -> Counts {
        self.counts
            .borrow()
            .iter()
            .find(|(f, _)| f == file)
            .map_or(Counts::default(), |(_, n)| *n)
    }
    fn with(&self, file: &str, f: impl FnOnce(&mut Counts)) {
        let mut counts = self.counts.borrow_mut();
        match counts.iter_mut().find(|(n, _)| n == file) {
            Some((_, c)) => f(c),
            None => {
                let mut c = Counts::default();
                f(&mut c);
                counts.push((String::from(file), c));
            }
        }
    }
    /// Het nummer van de laatst gepubliceerde capture van `file`: elke write
    /// van vóór het begin van die capture staat in de store. 0: nog geen.
    pub fn count(&self, file: &str) -> u64 {
        self.get(file).published
    }
    /// De captures van `file` die tot nu toe begonnen, gepubliceerd of
    /// onderweg. Een write van nu staat in de store zodra
    /// [`Publications::count`] voorbij dit nummer is.
    pub fn begun(&self, file: &str) -> u64 {
        self.get(file).begun
    }
    /// De nummers van de eigenaar van `file`: [`crate::owner::Replica::begun`]
    /// en [`crate::owner::Replica::confirmed`]. Een gepubliceerd nummer gaat
    /// nooit terug.
    pub fn record(&self, file: &str, begun: u64, confirmed: u64) {
        self.with(file, |c| {
            c.begun = c.begun.max(begun);
            c.published = c.published.max(confirmed);
        });
    }
    /// Houdt `file` tegen (`*`: elke database) of laat hem weer gaan.
    pub fn hold(&self, file: &str, on: bool) {
        let mut held = self.held.borrow_mut();
        held.retain(|f| f != file);
        if on {
            held.push(String::from(file));
        }
    }
    /// Wordt `file` tegengehouden?
    pub fn is_held(&self, file: &str) -> bool {
        self.held.borrow().iter().any(|f| f == file || f == "*")
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn numbers_never_go_back() {
        let p = Publications::new();
        assert_eq!((p.begun("main.db"), p.count("main.db")), (0, 0));
        p.record("main.db", 2, 1);
        assert_eq!((p.begun("main.db"), p.count("main.db")), (2, 1));
        p.record("main.db", 1, 0);
        assert_eq!((p.begun("main.db"), p.count("main.db")), (2, 1));
        assert_eq!(p.count("2026.db"), 0);
    }
    #[test]
    fn held_files_and_the_wildcard() {
        let p = Publications::new();
        assert!(!p.is_held("main.db"));
        p.hold("main.db", true);
        assert!(p.is_held("main.db") && !p.is_held("2026.db"));
        p.hold("*", true);
        assert!(p.is_held("2026.db"));
        p.hold("*", false);
        p.hold("main.db", false);
        assert!(!p.is_held("main.db"));
    }
}
