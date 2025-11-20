//! Cortex: zero-copy, hardware-agnostic inference primitives for Tesser.

pub mod buffer;
pub mod config;
pub mod engine;

pub use buffer::FeatureBuffer;
pub use config::{CortexConfig, CortexDevice};
pub use engine::CortexEngine;

pub use ort::Error as OrtError;
