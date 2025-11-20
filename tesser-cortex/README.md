# Tesser Cortex

**Tesser Cortex** is the high-performance, hardware-agnostic AI inference engine powering the Tesser trading framework. It bridges the gap between Python research and Rust production execution with **zero-copy** memory management and adaptive hardware acceleration.

## Key Features

* **Hardware Agnostic**: Switch between CPU (AVX-512), CUDA, and TensorRT using configuration only—no code changes required.
* **Zero-Copy Dataflow**: `FeatureBuffer` maps market data directly into ONNX Runtime tensors without heap allocations in the hot path.
* **Stateful Inference**: Optimized for time-series models such as LSTM/GRU with efficient warm-up and hidden state management.
* **Microsecond Latency**: Tuned for CPU cache locality, consistently delivering &lt;25 μs inference latency on commodity hardware.

## Usage

```rust
use tesser_cortex::{CortexConfig, CortexDevice, CortexEngine, FeatureBuffer};

let config = CortexConfig {
    model_path: "models/lstm_v1.onnx".into(),
    device: CortexDevice::Cpu {
        intra_op_threads: 1,
        optimization_level: 3,
    },
    input_shape: vec![1, 10, 5],
    input_name: "input".into(),
};

let mut engine = CortexEngine::new(config)?;
let mut buffer = FeatureBuffer::new(10, 5);
buffer.push(&[100.0, 101.0, 99.0, 100.5, 5000.0])?;

if let Some(probs) = engine.predict(&buffer)? {
    println!("Long probability = {}", probs[0]);
}
```

## Performance

| Backend | Hardware | Latency (P99) | Use Case |
| --- | --- | --- | --- |
| **Cortex (CPU)** | AMD EPYC (AVX-512) | **~20 μs** | HFT, Market Making |
| **Cortex (CUDA)** | NVIDIA A100 | ~3 ms | Transformers / NLP |
| *Python (PyTorch)* | Standard CPU | ~500 μs | Research only |

## Installation

```toml
[dependencies]
tesser-cortex = { path = "../tesser-cortex", features = ["cuda"] }
```

Enable the features you need (`cuda`, `tensorrt`, `coreml`) to match target hardware.
