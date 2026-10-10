//! Test-only bucket in RAM; de database, tracking, spool en restore gebruiken echt HopFS.
use alloc::{string::String, vec::Vec};
use replica_core::{
    archive::{self, Fetch},
    local::Name,
    marker,
    object::{Object, Store, StoreError},
    owner::{Config, Replica},
    prepare::Reason,
    time::Time,
    writer::{self, MemBackend, Role},
};
use replica_sqlite::{Engine, Error, Result, Storage, Value};
fn error(e: replica_core::Error) -> Error {
    applib::log!("REPLICA_PIPELINE_ERROR {e:?}");
    match e {
        replica_core::Error::Storage(e) => e,
        _ => Error::MISUSE,
    }
}
fn check<B: Storage>(db: &mut replica_sqlite::Connection<'_, '_, B>) -> Result {
    let mut s = db.prepare(c"SELECT count(*) FROM item")?;
    if !s.step()? || s.column(0)? != Value::Integer(1000) {
        return Err(Error::MISUSE);
    }
    drop(s);
    let mut s = db.prepare(c"PRAGMA integrity_check")?;
    if !s.step()? || s.column(0)? != Value::Text("ok") {
        return Err(Error::MISUSE);
    }
    Ok(())
}
/// De caller bezit de unieke SQLite-runtime en staat op zijn eigen 8 MiB C-stack.
pub(super) unsafe fn run<B: Storage>(b: &mut B, heap: &mut [u64], restored: bool) -> Result {
    if restored {
        if !b.exists(c"replica-restored.db")? {
            return Err(Error::MISUSE);
        }
        // SAFETY: Caller heeft de vorige engine gesloten; deze scope sluit ook weer.
        let mut engine = unsafe { Engine::initialize(heap, b) }?;
        let mut db = engine.open(c"replica-restored.db")?;
        check(&mut db)?;
        applib::log!("REPLICA_RESTORED_COLD_OK");
    }
    let path = Name::new("replica.db").map_err(error)?;
    let destination =
        marker::destination("test-memory-bucket", "test", "replica", "qemu").map_err(error)?;
    let now = Time::unix(1_790_765_296, 0).map_err(error)?;
    let mut bucket = Bucket {
        objects: Vec::new(),
    };
    let mut config = Config::new("replica/qemu", &destination, path, 65536).map_err(error)?;
    config.segment_bytes = 65536;
    // De bucket is RAM en deze node is alleen: de lease ook.
    let mut lease = MemBackend::new();
    let Role::Writer(writer) = writer::claim(b, &mut lease, "qemu", 30_000, now).map_err(error)?
    else {
        return Err(Error::MISUSE);
    };
    let mut replica = Replica::prepare(b, &mut bucket, writer, config, now, |backend, name| {
        // SAFETY: Deze eigenaar opent de enige C-engine; alle vorige scopes zijn gesloten.
        let mut engine = unsafe { Engine::initialize(heap, backend) }?;
        let mut db = engine.open(name.cstr()?)?;
        check(&mut db)?;
        Ok(())
    })
    .map_err(error)?;
    // De vluchtige bucket is bij reboot leeg. Prepare herkent dat nu al aan
    // het ontbrekende current naast de bewezen lokale marker.
    let repairing = restored && replica.status().reason == Reason::Repair;
    {
        let mut vfs = replica.vfs(b).map_err(error)?;
        // SAFETY: Prepare is klaar; geen engine leeft tussen twee van deze scopes.
        let mut engine = unsafe { Engine::initialize(heap, &mut vfs) }?;
        let mut db = engine.open(c"replica.db")?;
        db.execute(c"CREATE TABLE IF NOT EXISTS replica_probe(v INTEGER);INSERT INTO replica_probe VALUES(7)")?;
        check(&mut db)?;
    }
    let sync = replica.sync(b, &mut bucket, now).map_err(error)?;
    if !sync.published {
        return Err(Error::MISUSE);
    }
    if repairing {
        applib::log!("REPLICA_PIPELINE_REPAIR_OK");
    }
    let points =
        archive::points(&mut bucket, "replica/qemu", &replica.marker(), 32).map_err(error)?;
    if points.is_empty() || !points[0].current {
        return Err(Error::MISUSE);
    }
    replica.close();
    archive::fetch(
        b,
        &mut bucket,
        Fetch {
            namespace: "replica/qemu",
            generation: &replica.marker().generation,
            at: None,
            live: path,
            destination: Name::new("replica-restored.db").map_err(error)?,
            page_limit: 65536,
        },
        |backend, name| {
            // SAFETY: De eigenaar is gesloten; deze verificatie is de enige C-runtimegebruiker.
            let mut engine = unsafe { Engine::initialize(heap, backend) }?;
            let mut db = engine.open(name.cstr()?)?;
            check(&mut db)?;
            Ok(())
        },
    )
    .map_err(error)?;
    applib::log!("REPLICA_OWNER_OK");
    applib::log!("REPLICA_PIPELINE_OK");
    Ok(())
}
struct Entry {
    key: String,
    bytes: Vec<u8>,
}
struct Bucket {
    objects: Vec<Entry>,
}
fn text(s: &str) -> core::result::Result<String, StoreError> {
    let mut out = String::new();
    out.try_reserve_exact(s.len())
        .map_err(|_| StoreError::Limit)?;
    out.push_str(s);
    Ok(out)
}
fn copy(bytes: &[u8]) -> core::result::Result<Vec<u8>, StoreError> {
    let mut out = Vec::new();
    out.try_reserve_exact(bytes.len())
        .map_err(|_| StoreError::Limit)?;
    out.extend_from_slice(bytes);
    Ok(out)
}
impl Store for Bucket {
    fn put(&mut self, key: &str, bytes: &[u8]) -> core::result::Result<(), StoreError> {
        if key.len() > 1024
            || bytes.len() > 1 << 20
            || self.objects.iter().map(|o| o.bytes.len()).sum::<usize>() + bytes.len() > 2 << 20
        {
            return Err(StoreError::Limit);
        }
        let bytes = copy(bytes)?;
        if let Some(o) = self.objects.iter_mut().find(|o| o.key == key) {
            o.bytes = bytes;
            return Ok(());
        }
        if self.objects.len() >= 32 {
            return Err(StoreError::Limit);
        }
        let key = text(key)?;
        self.objects.try_reserve(1).map_err(|_| StoreError::Limit)?;
        self.objects.push(Entry { key, bytes });
        Ok(())
    }
    fn get(&mut self, key: &str, limit: usize) -> core::result::Result<Vec<u8>, StoreError> {
        let o = self
            .objects
            .iter()
            .find(|o| o.key == key)
            .ok_or(StoreError::Missing)?;
        if o.bytes.len() > limit {
            return Err(StoreError::Limit);
        }
        copy(&o.bytes)
    }
    fn delete(&mut self, key: &str) -> core::result::Result<(), StoreError> {
        self.objects.retain(|o| o.key != key);
        Ok(())
    }
    fn list(
        &mut self,
        prefix: &str,
        limit: usize,
    ) -> core::result::Result<Vec<Object>, StoreError> {
        let mut out = Vec::new();
        for o in self.objects.iter().filter(|o| o.key.starts_with(prefix)) {
            if out.len() >= limit {
                return Err(StoreError::Limit);
            }
            out.try_reserve(1).map_err(|_| StoreError::Limit)?;
            out.push(Object {
                key: text(&o.key)?,
                size: Some(o.bytes.len() as u64),
            });
        }
        Ok(out)
    }
}
