use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CortexDevice {
    /// Ultra-low latency CPU path leveraging AVX-512.
    Cpu {
        #[serde(default = "default_cpu_threads")]
        intra_op_threads: usize,
        #[serde(default = "default_optimization_level")]
        optimization_level: u32,
    },
    /// CUDA execution provider for throughput-heavy contexts.
    Cuda { device_id: i32 },
    /// TensorRT acceleration for maximum throughput.
    TensorRT { device_id: i32 },
}

impl Default for CortexDevice {
    fn default() -> Self {
        Self::Cpu {
            intra_op_threads: default_cpu_threads(),
            optimization_level: default_optimization_level(),
        }
    }
}

fn default_cpu_threads() -> usize {
    1
}

fn default_optimization_level() -> u32 {
    3
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CortexConfig {
    pub model_path: PathBuf,
    #[serde(default)]
    pub device: CortexDevice,
    /// Shape of the model input tensor, typically [1, window, features].
    pub input_shape: Vec<usize>,
    pub input_name: String,
}
