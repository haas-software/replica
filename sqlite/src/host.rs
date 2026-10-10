//! De opslag van de host: echte bestanden in één map, voor ontwikkeling en tests.
//!
//! Dezelfde duurzaamheidsregels als HopFS: `sync` is `fsync` van het bestand
//! (`File::sync_all`), en `remove(.., true)` maakt ook het verdwijnen van de
//! naam duurzaam met een `fsync` van de map. Namen zijn canonieke relatieve
//! namen binnen de map: geen `..`, geen absolute paden, geen lege delen; een
//! alias naar hetzelfde bestand bestaat dus niet. Eén eigenaar per map, zoals
//! het [`Storage`]-contract eist: er is geen OS-locking.
//!
//! Entropie komt uit `/dev/urandom`, de klok is `SystemTime`. Deze module is
//! er alleen met de feature `std`; op HopOS is `replica-hopos` de opslag.
use crate::{Error, FileId, OpenFlags, Result, Storage};
use core::ffi::CStr;
use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
    vec::Vec,
};

/// Hoeveel bestanden tegelijk open mogen: de database, het journal, de
/// sorter en de sidecars van Replica passen er ruim in.
pub const FILES: usize = 64;

/// Eén map op de host als opslag van één SQLite-eigenaar.
pub struct Dir {
    root: PathBuf,
    files: Vec<Option<File>>,
    random: Option<File>,
}

impl Dir {
    /// Een eigenaar van `root`; de map wordt gemaakt als hij nog niet bestaat.
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|_| Error::CANNOT_OPEN)?;
        let mut files = Vec::new();
        files.resize_with(FILES, || None);
        Ok(Self {
            root,
            files,
            random: None,
        })
    }
    /// De map van deze eigenaar.
    pub fn root(&self) -> &Path {
        &self.root
    }
    fn path(&self, name: &CStr) -> Result<PathBuf> {
        let name = name.to_str().map_err(|_| Error::CANNOT_OPEN)?;
        if name.is_empty()
            || name.len() > 255
            || name.starts_with('/')
            || name
                .split('/')
                .any(|s| s.is_empty() || s == "." || s == "..")
            || name.bytes().any(|b| b < 32 || b == 127 || b == b'\\')
        {
            return Err(Error::CANNOT_OPEN);
        }
        Ok(self.root.join(name))
    }
    fn file(&self, id: FileId) -> Result<&File> {
        self.files
            .get(id.0 as usize)
            .and_then(Option::as_ref)
            .ok_or(Error::IO)
    }
    fn sync_parent(path: &Path) -> Result {
        let parent = path.parent().ok_or(Error::IO)?;
        File::open(parent)
            .and_then(|d| d.sync_all())
            .map_err(|_| Error {
                code: 10 | (4 << 8),
            })
    }
}

impl Storage for Dir {
    fn open(&mut self, name: &CStr, flags: OpenFlags) -> Result<FileId> {
        let path = self.path(name)?;
        let slot = self
            .files
            .iter()
            .position(Option::is_none)
            .ok_or(Error::FULL)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|_| Error::CANNOT_OPEN)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(flags.is_writable())
            .create(flags.is_create() && flags.is_writable())
            .truncate(false)
            .open(&path)
            .map_err(|_| Error::CANNOT_OPEN)?;
        let id = u32::try_from(slot).map_err(|_| Error::FULL)?;
        *self.files.get_mut(slot).ok_or(Error::FULL)? = Some(file);
        Ok(FileId(id))
    }
    fn close(&mut self, file: FileId) -> Result {
        self.files
            .get_mut(file.0 as usize)
            .and_then(Option::take)
            .map(drop)
            .ok_or(Error::IO)
    }
    fn read(&mut self, file: FileId, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let f = self.file(file)?;
        let mut done = 0;
        while done < dst.len() {
            let at = offset.checked_add(done as u64).ok_or(Error::IO)?;
            match f.read_at(&mut dst[done..], at) {
                Ok(0) => break,
                Ok(n) => done += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => {
                    return Err(Error {
                        code: 10 | (1 << 8),
                    });
                }
            }
        }
        Ok(done)
    }
    fn write(&mut self, file: FileId, offset: u64, src: &[u8]) -> Result {
        self.file(file)?
            .write_all_at(src, offset)
            .map_err(|_| Error {
                code: 10 | (3 << 8),
            })
    }
    fn truncate(&mut self, file: FileId, size: u64) -> Result {
        self.file(file)?.set_len(size).map_err(|_| Error {
            code: 10 | (6 << 8),
        })
    }
    fn sync(&mut self, file: FileId, _flags: i32) -> Result {
        self.file(file)?.sync_all().map_err(|_| Error {
            code: 10 | (4 << 8),
        })
    }
    fn size(&mut self, file: FileId) -> Result<u64> {
        self.file(file)?
            .metadata()
            .map(|m| m.len())
            .map_err(|_| Error {
                code: 10 | (7 << 8),
            })
    }
    fn remove(&mut self, name: &CStr, sync_directory: bool) -> Result {
        let path = self.path(name)?;
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(_) => {
                return Err(Error {
                    code: 10 | (10 << 8),
                });
            }
        }
        if sync_directory {
            Self::sync_parent(&path)?;
        }
        Ok(())
    }
    fn exists(&mut self, name: &CStr) -> Result<bool> {
        Ok(self.path(name)?.exists())
    }
    fn random(&mut self, dst: &mut [u8]) -> Result {
        if self.random.is_none() {
            self.random = Some(File::open("/dev/urandom").map_err(|_| Error::IO)?);
        }
        self.random
            .as_mut()
            .ok_or(Error::IO)?
            .read_exact(dst)
            .map_err(|_| Error::IO)
    }
    fn unix_millis(&mut self) -> Result<i64> {
        let since = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::IO)?;
        i64::try_from(since.as_millis()).map_err(|_| Error::IO)
    }
}
