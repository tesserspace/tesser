pub const STREAM_SEQ_START: u64 = 1;

pub const MAX_ACTIVE_STREAMS: usize = 4;

pub const MAX_BYTES_PER_PULL: u32 = 256 * 1024;
pub const MAX_CHUNK_BYTES: u32 = 256 * 1024;

pub const REPLAY_WINDOW_CHUNKS: u32 = 0;

#[cfg(test)]
pub const STREAM_IDLE_TIMEOUT_MS: u64 = 250;
#[cfg(not(test))]
pub const STREAM_IDLE_TIMEOUT_MS: u64 = 20_000;
