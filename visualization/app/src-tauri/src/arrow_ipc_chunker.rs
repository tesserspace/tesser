#![allow(clippy::result_large_err)]

use crate::command_error::CommandError;

fn pad_to_8(n: usize) -> usize {
    let rem = n % 8;
    if rem == 0 {
        0
    } else {
        8 - rem
    }
}

pub fn chunk_arrow_ipc_stream(
    bytes: &[u8],
    max_chunk_bytes: usize,
    correlation_id: &str,
) -> Result<Vec<Vec<u8>>, CommandError> {
    if max_chunk_bytes == 0 {
        return Err(CommandError::new(
            "ARROW.CHUNK_SIZE_INVALID",
            "max_chunk_bytes must be > 0",
            correlation_id.to_string(),
        ));
    }

    let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
    let mut i = 0usize;
    while i + 4 <= bytes.len() {
        let msg_start = i;
        let mut len = i32::from_le_bytes(bytes[i..i + 4].try_into().expect("len bytes"));
        i += 4;
        if len == -1 {
            if i + 4 > bytes.len() {
                return Err(CommandError::new(
                    "ARROW.IPC_TRUNCATED",
                    "truncated stream (missing message length after continuation marker)",
                    correlation_id.to_string(),
                ));
            }
            len = i32::from_le_bytes(bytes[i..i + 4].try_into().expect("len bytes"));
            i += 4;
        }

        if len == 0 {
            ranges.push(msg_start..i);
            break;
        }
        if len < 0 {
            return Err(CommandError::new(
                "ARROW.IPC_INVALID",
                format!("invalid ipc message length: {len}"),
                correlation_id.to_string(),
            ));
        }
        let len = len as usize;
        if i + len > bytes.len() {
            return Err(CommandError::new(
                "ARROW.IPC_TRUNCATED",
                "truncated stream (message metadata exceeds buffer)",
                correlation_id.to_string(),
            ));
        }
        let msg = arrow_ipc::root_as_message(&bytes[i..i + len]).map_err(|e| {
            CommandError::new(
                "ARROW.IPC_INVALID",
                format!("invalid flatbuffer message: {e}"),
                correlation_id.to_string(),
            )
        })?;
        let body_len = msg.bodyLength();
        if body_len < 0 {
            return Err(CommandError::new(
                "ARROW.IPC_INVALID",
                format!("invalid ipc body length: {body_len}"),
                correlation_id.to_string(),
            ));
        }
        let body_len = body_len as usize;

        i += len;
        let pad = pad_to_8(i);
        if i + pad > bytes.len() {
            return Err(CommandError::new(
                "ARROW.IPC_TRUNCATED",
                "truncated stream (missing metadata padding)",
                correlation_id.to_string(),
            ));
        }
        i += pad;

        if i + body_len > bytes.len() {
            return Err(CommandError::new(
                "ARROW.IPC_TRUNCATED",
                "truncated stream (body exceeds buffer)",
                correlation_id.to_string(),
            ));
        }
        i += body_len;

        let body_pad = pad_to_8(i);
        if i + body_pad > bytes.len() {
            return Err(CommandError::new(
                "ARROW.IPC_TRUNCATED",
                "truncated stream (missing body padding)",
                correlation_id.to_string(),
            ));
        }
        i += body_pad;

        ranges.push(msg_start..i);
    }

    if ranges.is_empty() {
        return Err(CommandError::new(
            "ARROW.IPC_INVALID",
            "no ipc messages found",
            correlation_id.to_string(),
        ));
    }

    let mut chunks: Vec<Vec<u8>> = Vec::with_capacity(ranges.len());
    for r in ranges {
        let seg = &bytes[r];
        if seg.len() > max_chunk_bytes {
            return Err(CommandError::new(
                "ARROW.CHUNK_TOO_LARGE",
                format!(
                    "single ipc message exceeds max_chunk_bytes ({} > {})",
                    seg.len(),
                    max_chunk_bytes
                ),
                correlation_id.to_string(),
            ));
        }
        chunks.push(seg.to_vec());
    }

    Ok(chunks)
}
