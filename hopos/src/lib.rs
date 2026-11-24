//! SQLite-opslag op HopOS: de async system-client, via de parkeerbare C-stack.
//!
//! Eén pad: iedere VFS-callback wacht met `applib::stacktask::Suspender::wait`
//! op de system-client, en `xSync` wordt `sys::Client::sync` (OP_SYNC). Er is
//! geen blokkerende of gepolde variant ernaast en geen geneste executor.
//!
//! [`s3`] is de objectstore en de schrijverlease vanuit een app-slot: S3 over
//! HTTPS met applib's TCP, Lean's TLS, de wandklok en de willekeur van de app.
//!
//! [`lanes`] draait het S3-werk van een eigenaar (uploads, manifesten,
//! onderhoud, de lease) op twee eigen stacks naast hem
//! (`replica_core::lanes`). Die stacks maakt [`lanes::Lanes::spawn`]: de
//! enige `unsafe` van deze crate, een `unsafe fn` met het stackcontract van
//! `applib::stacktask` voor de kant van de host.
//!
//! [`keep::Keeping`] is de waiter van de store van een eigenaar: hij houdt de
//! lease van de open groep bij terwijl één call wacht.
#![no_std]
#![deny(unsafe_code)]
extern crate alloc;
pub mod keep;
pub mod lanes;
pub mod s3;
mod storage;
pub use storage::{Environment, Files, Stats, Wait};
