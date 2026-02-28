use serde::{Deserialize, Serialize};

use crate::command_error::CommandError;
use crate::protocol::PROTOCOL_VERSION;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestEnvelope {
    pub protocol_version: String,
    pub correlation_id: String,
    pub request_id: String,
}

#[allow(clippy::result_large_err)]
pub fn validate_envelope(envelope: &RequestEnvelope) -> Result<(), CommandError> {
    if envelope.protocol_version != PROTOCOL_VERSION {
        return Err(CommandError::new(
            "PROTOCOL.VERSION_UNSUPPORTED",
            format!(
                "unsupported protocol_version: expected {}, got {}",
                PROTOCOL_VERSION, envelope.protocol_version
            ),
            envelope.correlation_id.clone(),
        ));
    }
    Ok(())
}
