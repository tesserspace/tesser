use crate::{
    buffer::FeatureBuffer,
    config::{CortexConfig, CortexDevice},
};
use anyhow::{anyhow, ensure, Context, Result};
use ort::{
    session::{builder::GraphOptimizationLevel, Session},
    value::TensorRef,
};
use tracing::{info, warn};

#[cfg(feature = "cuda")]
use ort::execution_providers::CUDAExecutionProvider;
#[cfg(feature = "tensorrt")]
use ort::execution_providers::TensorRTExecutionProvider;

/// High-level Cortex wrapper around ONNX Runtime sessions.
pub struct CortexEngine {
    session: Session,
    input_name: String,
}

impl CortexEngine {
    pub fn new(config: CortexConfig) -> Result<Self> {
        let mut builder = Session::builder()?;

        match &config.device {
            CortexDevice::Cpu {
                intra_op_threads,
                optimization_level,
            } => {
                info!(
                    target: "tesser.cortex",
                    threads = *intra_op_threads,
                    opt_level = *optimization_level,
                    "initializing Cortex on CPU"
                );
                builder = builder.with_intra_threads(*intra_op_threads)?;
                builder =
                    builder.with_optimization_level(map_optimization_level(*optimization_level))?;
            }
            CortexDevice::Cuda { device_id } => {
                info!(
                    target: "tesser.cortex",
                    device_id = *device_id,
                    "initializing Cortex on CUDA"
                );
                #[cfg(feature = "cuda")]
                {
                    let provider = CUDAExecutionProvider::default()
                        .with_device_id(*device_id)
                        .build();
                    builder = builder.with_execution_providers([provider])?;
                }
                #[cfg(not(feature = "cuda"))]
                {
                    warn!(target: "tesser.cortex", "CUDA feature not enabled, falling back to default CPU execution");
                }
            }
            CortexDevice::TensorRT { device_id } => {
                info!(
                    target: "tesser.cortex",
                    device_id = *device_id,
                    "initializing Cortex on TensorRT"
                );
                #[cfg(feature = "tensorrt")]
                {
                    let provider = TensorRTExecutionProvider::default()
                        .with_device_id(*device_id)
                        .build();
                    builder = builder.with_execution_providers([provider])?;
                }
                #[cfg(not(feature = "tensorrt"))]
                {
                    warn!(
                        target: "tesser.cortex",
                        "TensorRT feature not enabled, falling back to default CPU execution"
                    );
                }
            }
        }

        let mut session = builder
            .commit_from_file(&config.model_path)
            .with_context(|| {
                format!("failed to load model from {}", config.model_path.display())
            })?;

        info!(target: "tesser.cortex", "warming up inference engine");
        if let Err(err) = warmup_session(
            &mut session,
            config.input_name.as_str(),
            &config.input_shape,
        ) {
            warn!(target: "tesser.cortex", "warmup failed: {err}");
        }

        Ok(Self {
            session,
            input_name: config.input_name,
        })
    }

    /// Performs a single inference call using the provided feature buffer.
    pub fn predict(&mut self, buffer: &FeatureBuffer) -> Result<Option<Vec<f32>>> {
        if !buffer.is_ready() {
            return Ok(None);
        }

        let shape = [1usize, buffer.window_size(), buffer.feature_dim()];
        let tensor = TensorRef::from_array_view((shape, buffer.as_slice()))?;
        let outputs = self.session.run(ort::inputs! {
            self.input_name.as_str() => tensor
        })?;

        if outputs.len() != 1 {
            return Err(anyhow!(
                "model expected to emit a single output, got {}",
                outputs.len()
            ));
        }

        let (_, data) = outputs[0].try_extract_tensor::<f32>()?;
        Ok(Some(data.to_vec()))
    }
}

fn map_optimization_level(level: u32) -> GraphOptimizationLevel {
    match level {
        0 => GraphOptimizationLevel::Disable,
        1 => GraphOptimizationLevel::Level1,
        2 => GraphOptimizationLevel::Level2,
        _ => GraphOptimizationLevel::Level3,
    }
}

fn warmup_session(session: &mut Session, input_name: &str, shape: &[usize]) -> Result<()> {
    ensure!(
        !shape.is_empty(),
        "input shape must have at least one dimension for warmup"
    );
    let total: usize = shape.iter().copied().product();
    ensure!(
        total > 0,
        "input shape cannot contain zero-sized dimensions"
    );
    let zeros = vec![0.0f32; total];
    let tensor = TensorRef::from_array_view((shape.to_vec(), zeros.as_slice()))?;
    let _ = session.run(ort::inputs! {
        input_name => tensor
    })?;
    Ok(())
}
