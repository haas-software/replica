//! Kopie-vóór-schrijven tijdens een capture op een andere stack.
//!
//! De eigenaar schrijft via de VFS gewoon door; van elke pagina die het beeld
//! nog moet lezen, bewaart hij eerst de oude inhoud in de schaduw. De capture
//! legt die over wat hij zelf uit de database las, dus het beeld is dat van het
//! moment van [`crate::owner::Replica::begin_capture`]. Beide draaien op één
//! kern: de lening op deze staat is kort en loopt nooit over opslag-I/O heen.
use crate::{
    Error, Result,
    local::{File, Name},
};
use alloc::{collections::BTreeMap, rc::Rc, vec::Vec};
use core::cell::RefCell;
use replica_sqlite::{FileId, Storage};
/// De staat die eigenaar en capture delen.
pub struct Shadow {
    file: Name,
    page_size: u32,
    /// Pagina's in het beeld: een snapshot `1..=count`, een increment `pages`.
    count: u32,
    pub(crate) pages: Vec<u32>,
    snapshot: bool,
    /// Tot en met deze pagina is het beeld gelezen; wat daarna geschreven wordt,
    /// hoeft geen schaduw meer.
    frontier: u32,
    /// Pagina → slot in het schaduwbestand, pas zichtbaar na het schrijven.
    slots: BTreeMap<u32, u32>,
    next_slot: u32,
    /// De database kromp onder het beeld: dit beeld is niet meer te maken.
    broken: bool,
}
/// Het gedeelde handvat.
pub type Shared = Rc<RefCell<Shadow>>;
impl Shadow {
    pub(crate) fn new(
        file: Name,
        page_size: u32,
        count: u32,
        pages: Vec<u32>,
        snapshot: bool,
    ) -> Shared {
        Rc::new(RefCell::new(Self {
            file,
            page_size,
            count,
            pages,
            snapshot,
            frontier: 0,
            slots: BTreeMap::new(),
            next_slot: 0,
            broken: false,
        }))
    }
    /// De pagina's van segment `index` (`per_segment` per segment), oplopend.
    pub(crate) fn segment(&self, index: usize, per_segment: usize) -> Vec<u32> {
        let total = self.page_count();
        let from = (index * per_segment).min(total);
        let through = (from + per_segment).min(total);
        if self.snapshot {
            ((from as u32 + 1)..=through as u32).collect()
        } else {
            self.pages[from..through].to_vec()
        }
    }
    pub(crate) fn page_count(&self) -> usize {
        if self.snapshot {
            self.count as usize
        } else {
            self.pages.len()
        }
    }
    /// Moet de oude inhoud van deze pagina bewaard blijven vóór een write?
    fn needed(&self, page: u32) -> bool {
        if self.broken || page <= self.frontier || self.slots.contains_key(&page) {
            return false;
        }
        if self.snapshot {
            page <= self.count
        } else {
            self.pages.binary_search(&page).is_ok()
        }
    }
    pub(crate) fn slot(&self, page: u32) -> Option<u32> {
        self.slots.get(&page).copied()
    }
    /// Segment gelezen en overlegd: tot hier geen schaduw meer nodig.
    pub(crate) fn advance(&mut self, through: u32) {
        self.frontier = self.frontier.max(through);
    }
    /// Het beeld is compleet: geen enkele write hoeft nog een schaduw.
    pub(crate) fn finished(&mut self) {
        self.frontier = u32::MAX;
    }
    pub(crate) fn is_broken(&self) -> bool {
        self.broken
    }
    pub(crate) fn file(&self) -> &Name {
        &self.file
    }
    pub(crate) fn page_size(&self) -> u32 {
        self.page_size
    }
    /// De eigenaar krimpt de database: pagina's onder het beeld verdwijnen.
    pub(crate) fn truncated(&mut self, size: u64) {
        if size < u64::from(self.count) * u64::from(self.page_size) {
            self.broken = true;
        }
    }
}
/// Vóór een write van `len` bytes op `offset` in de database: bewaar van elke
/// nog te lezen pagina de oude inhoud. De lening is kort; elke lees of schrijf
/// gebeurt erbuiten. Een slot is pas zichtbaar als de bytes er staan.
pub(crate) fn copy_before_write<B: Storage>(
    b: &mut B,
    shadow: &Shared,
    main: FileId,
    offset: u64,
    len: usize,
) -> Result {
    let (page_size, name) = {
        let s = shadow.borrow();
        if s.is_broken() {
            return Ok(());
        }
        (u64::from(s.page_size()), *s.file())
    };
    let end = offset.checked_add(len as u64).ok_or(Error::Limit)?;
    let first = offset / page_size + 1;
    let last = (end.max(1) - 1) / page_size + 1;
    let mut bytes = Vec::new();
    for page in first..=last {
        let page = u32::try_from(page).map_err(|_| Error::Limit)?;
        let slot = {
            let mut s = shadow.borrow_mut();
            if !s.needed(page) {
                continue;
            }
            let slot = s.next_slot;
            s.next_slot = slot.checked_add(1).ok_or(Error::Limit)?;
            slot
        };
        if bytes.is_empty() {
            crate::reserve(&mut bytes, page_size as usize)?;
            bytes.resize(page_size as usize, 0);
        }
        let mut read = 0;
        while read < bytes.len() {
            let n = b.read(main, u64::from(page - 1) * page_size + read as u64, &mut bytes[read..])?;
            if n == 0 || n > bytes.len() - read {
                // Korter dan het beeld: de database kromp onder de capture.
                shadow.borrow_mut().broken = true;
                return Ok(());
            }
            read += n;
        }
        let mut file = File::open(b, &name, false)?;
        file.write(u64::from(slot) * page_size, &bytes)?;
        file.close()?;
        shadow.borrow_mut().slots.insert(page, slot);
    }
    Ok(())
}
