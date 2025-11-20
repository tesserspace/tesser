use anyhow::{anyhow, Result};
use ndarray::{ArrayView, IxDyn};

/// Fixed-size feature window backed by a contiguous ring buffer.
///
/// The buffer never reallocates and exposes zero-copy ndarray views so
/// ONNX Runtime can read the same memory that feeds the indicator pipeline.
#[derive(Debug)]
pub struct FeatureBuffer {
    data: Vec<f32>,
    window_size: usize,
    feature_dim: usize,
    current_len: usize,
}

impl FeatureBuffer {
    /// Creates a new buffer with the given window length and feature width.
    pub fn new(window_size: usize, feature_dim: usize) -> Self {
        assert!(window_size > 0, "window size must be positive");
        assert!(feature_dim > 0, "feature dimension must be positive");
        Self {
            data: vec![0.0; window_size * feature_dim],
            window_size,
            feature_dim,
            current_len: 0,
        }
    }

    /// Pushes a new feature vector into the ring buffer.
    pub fn push(&mut self, features: &[f32]) -> Result<()> {
        if features.len() != self.feature_dim {
            return Err(anyhow!(
                "feature dimension mismatch: expected {}, got {}",
                self.feature_dim,
                features.len()
            ));
        }

        if self.window_size == 1 {
            self.data.copy_from_slice(features);
        } else {
            let drain_start = self.feature_dim;
            let drain_end = self.data.len();
            self.data.copy_within(drain_start..drain_end, 0);
            let write_start = self.data.len() - self.feature_dim;
            self.data[write_start..].copy_from_slice(features);
        }

        self.current_len = (self.current_len + 1).min(self.window_size);
        Ok(())
    }

    /// Returns a zero-copy tensor view shaped as [1, window_size, feature_dim].
    pub fn as_tensor_view(&self) -> Option<ArrayView<'_, f32, IxDyn>> {
        if !self.is_ready() {
            return None;
        }

        let shape = IxDyn(&[1, self.window_size, self.feature_dim]);
        ArrayView::from_shape(shape, &self.data).ok()
    }

    /// Returns true when the buffer contains a full window of data.
    pub fn is_ready(&self) -> bool {
        self.current_len >= self.window_size
    }

    pub fn window_size(&self) -> usize {
        self.window_size
    }

    pub fn feature_dim(&self) -> usize {
        self.feature_dim
    }

    /// Returns the internal contiguous slice (primarily for bridging into ONNX Runtime).
    pub fn as_slice(&self) -> &[f32] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rolling_window_retains_latest_frames() {
        let mut buffer = FeatureBuffer::new(3, 2);
        buffer.push(&[1.0, 1.1]).unwrap();
        assert!(!buffer.is_ready());

        buffer.push(&[2.0, 2.1]).unwrap();
        buffer.push(&[3.0, 3.1]).unwrap();
        assert!(buffer.is_ready());

        let view = buffer.as_tensor_view().unwrap();
        assert_eq!(view.as_slice().unwrap(), &[1.0, 1.1, 2.0, 2.1, 3.0, 3.1]);

        buffer.push(&[4.0, 4.1]).unwrap();
        let view = buffer.as_tensor_view().unwrap();
        assert_eq!(view.as_slice().unwrap(), &[2.0, 2.1, 3.0, 3.1, 4.0, 4.1]);
    }
}
