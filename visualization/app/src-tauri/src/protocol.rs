use serde::Serialize;

pub const PROTOCOL_VERSION: &str = "tesser.viz.ipc.v1";

#[derive(Debug, Clone, Serialize)]
pub struct ProtocolInfo {
    pub protocol_version: String,
    pub app_version: String,
    pub platform: String,
    pub capabilities: Capabilities,
    pub limits: Limits,
    pub stream_pull: StreamPull,
}

#[derive(Debug, Clone, Serialize)]
pub struct Capabilities {
    pub stream_transport_kinds: Vec<String>,
    pub default_stream_transport_kind: String,
    pub arrow_ipc_streaming: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Limits {
    pub max_active_streams: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamPull {
    pub seq_start: u64,
    pub max_bytes_per_pull: u32,
    pub max_chunk_bytes: u32,
    pub replay_window_chunks: u32,
    pub stream_idle_timeout_ms: u32,
}

#[tauri::command]
pub fn protocol_get_info() -> ProtocolInfo {
    ProtocolInfo {
        protocol_version: PROTOCOL_VERSION.to_string(),
        app_version: env!("CARGO_PKG_VERSION").to_string(),
        platform: std::env::consts::OS.to_string(),
        capabilities: Capabilities {
            stream_transport_kinds: vec!["loopback_ws".to_string()],
            default_stream_transport_kind: "loopback_ws".to_string(),
            arrow_ipc_streaming: true,
        },
        limits: Limits {
            max_active_streams: 4,
        },
        stream_pull: StreamPull {
            seq_start: 1,
            max_bytes_per_pull: 256 * 1024,
            max_chunk_bytes: 256 * 1024,
            replay_window_chunks: 0,
            stream_idle_timeout_ms: 20_000,
        },
    }
}
