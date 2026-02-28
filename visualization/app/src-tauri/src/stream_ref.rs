use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct StreamRef {
    pub stream_id: String,
    pub format: String,
    pub transport: StreamTransport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind")]
pub enum StreamTransport {
    #[serde(rename = "loopback_ws")]
    LoopbackWs {
        url: String,
        auth_token: String,
        token_in: String,
        expires_at_ms: u64,
    },
}
