use std::path::PathBuf;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tesser_cortex::{CortexConfig, CortexDevice, CortexEngine, FeatureBuffer};

fn model_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../research/models/lstm_v1.onnx")
}

fn build_engine(path: PathBuf) -> CortexEngine {
    let config = CortexConfig {
        model_path: path,
        device: CortexDevice::Cpu {
            intra_op_threads: 1,
            optimization_level: 3,
        },
        input_shape: vec![1, 10, 5],
        input_name: "input".to_string(),
    };
    CortexEngine::new(config).expect("failed to initialize cortex engine")
}

fn prime_buffer() -> FeatureBuffer {
    let mut buffer = FeatureBuffer::new(10, 5);
    for _ in 0..10 {
        buffer.push(&[1.0, 2.0, 3.0, 4.0, 5.0]).unwrap();
    }
    buffer
}

fn criterion_benchmark(c: &mut Criterion) {
    let path = model_path();
    if !path.exists() {
        eprintln!(
            "Skipping benchmark: model not found at {}. Run `uv run scripts/export_dummy_model.py` from the research directory first.",
            path.display()
        );
        return;
    }

    let mut engine = build_engine(path);
    let mut buffer = prime_buffer();

    c.bench_function("cpu_inference_avx512", |b| {
        b.iter(|| {
            buffer.push(black_box(&[1.0, 2.0, 3.0, 4.0, 5.0])).unwrap();
            engine.predict(black_box(&buffer)).unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
