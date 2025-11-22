//! LMDB-backed persistence layers for Tesser runtime state.
//!
//! The journal exposes repositories compatible with the existing portfolio
//! and execution abstractions, allowing the CLI to choose an LMDB backend
//! without touching calling code.

mod lmdb;

pub use lmdb::{LmdbAlgoStateRepository, LmdbJournal, LmdbStateRepository, MAP_SIZE_BYTES};
