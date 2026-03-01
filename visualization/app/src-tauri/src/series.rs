#![allow(clippy::result_large_err)]

use std::io::Cursor;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tauri::State;

use crate::arrow_ipc_chunker::chunk_arrow_ipc_stream;
use crate::command_error::CommandError;
use crate::envelope::{validate_envelope, RequestEnvelope};
use crate::limits;
use crate::stream_ref::StreamRef;
use crate::transport::{TransportErrorPublic, TransportState};

const MAX_TARGET_POINTS: u32 = 16_384;

#[derive(Debug, Clone, Deserialize)]
pub struct RangeMs {
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SeriesQueryRequest {
    pub envelope: RequestEnvelope,
    pub kind: String,
    pub range: RangeMs,
    pub target_points: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct SeriesQueryMeta {
    pub correlation_id: String,
    pub lod_profile: String,
    pub lod_level: u32,
    pub bucket_ms: u64,
    pub points_returned: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct SeriesQueryResponse {
    pub meta: SeriesQueryMeta,
    pub stream_ref: StreamRef,
}

fn build_arrow_stream_bytes_last(ts: &[i64], value: &[f64]) -> Result<Vec<u8>, String> {
    let schema = Schema::new(vec![
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]);

    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            std::sync::Arc::new(Int64Array::from(ts.to_vec())),
            std::sync::Arc::new(Float64Array::from(value.to_vec())),
        ],
    )
    .map_err(|e| e.to_string())?;

    let mut out = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut out, &schema).map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
        writer.finish().map_err(|e| e.to_string())?;
    }

    let mut reader =
        StreamReader::try_new(Cursor::new(out.clone()), None).map_err(|e| e.to_string())?;
    let decoded = reader.next().transpose().map_err(|e| e.to_string())?;
    if decoded.is_none() {
        return Err("failed to decode generated arrow stream".to_string());
    }

    Ok(out)
}

fn build_arrow_stream_bytes_minmax(
    ts: &[i64],
    min_value: &[f64],
    max_value: &[f64],
    last_value: &[f64],
) -> Result<Vec<u8>, String> {
    let schema = Schema::new(vec![
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("min_value", DataType::Float64, false),
        Field::new("max_value", DataType::Float64, false),
        Field::new("last_value", DataType::Float64, false),
    ]);

    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            std::sync::Arc::new(Int64Array::from(ts.to_vec())),
            std::sync::Arc::new(Float64Array::from(min_value.to_vec())),
            std::sync::Arc::new(Float64Array::from(max_value.to_vec())),
            std::sync::Arc::new(Float64Array::from(last_value.to_vec())),
        ],
    )
    .map_err(|e| e.to_string())?;

    let mut out = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut out, &schema).map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
        writer.finish().map_err(|e| e.to_string())?;
    }

    let mut reader =
        StreamReader::try_new(Cursor::new(out.clone()), None).map_err(|e| e.to_string())?;
    let decoded = reader.next().transpose().map_err(|e| e.to_string())?;
    if decoded.is_none() {
        return Err("failed to decode generated arrow stream".to_string());
    }

    Ok(out)
}

fn align_floor_ms(ts_ms: i64, bucket_ms: u64) -> Result<i64, CommandError> {
    if bucket_ms == 0 {
        return Err(CommandError::new(
            "SERIES.BUCKET_INVALID",
            "bucket_ms must be > 0",
            "series".to_string(),
        ));
    }
    let b = bucket_ms as i128;
    let t = ts_ms as i128;
    let q = if t >= 0 { t / b } else { -((-t + b - 1) / b) };
    let aligned = q * b;
    i64::try_from(aligned).map_err(|_| {
        CommandError::new(
            "SERIES.RANGE_OUT_OF_BOUNDS",
            "timestamp out of bounds for bucket alignment",
            "series".to_string(),
        )
    })
}

fn span_u64(end_ms: i64, start_ms: i64) -> Result<u64, CommandError> {
    let span = (end_ms as i128) - (start_ms as i128);
    if span <= 0 {
        return Ok(0);
    }
    u64::try_from(span).map_err(|_| {
        CommandError::new(
            "SERIES.RANGE_OUT_OF_BOUNDS",
            "range span out of bounds",
            "series".to_string(),
        )
    })
}

fn allowed_bucket_ms() -> &'static [u64] {
    &[
        1_000,
        2_000,
        5_000,
        15_000,
        60_000,
        120_000,
        300_000,
        900_000,
        3_600_000,
        21_600_000,
        86_400_000,
        7 * 86_400_000,
        30 * 86_400_000,
        365 * 86_400_000,
        5 * 365 * 86_400_000,
        10 * 365 * 86_400_000,
    ]
}

fn choose_bucket_ms(
    range: &RangeMs,
    target_points: u32,
    points_per_bucket: u32,
) -> Result<(u64, u32), CommandError> {
    if points_per_bucket == 0 {
        return Err(CommandError::new(
            "SERIES.LOD_PROFILE_INVALID",
            "points_per_bucket must be > 0",
            "series".to_string(),
        ));
    }

    let budget = (target_points as u64) / (points_per_bucket as u64);
    let budget = budget.max(1);

    for (idx, bucket_ms) in allowed_bucket_ms().iter().copied().enumerate() {
        let aligned_start = align_floor_ms(range.start_ms, bucket_ms)?;
        let span = span_u64(range.end_ms, aligned_start)?;
        let buckets = span.div_ceil(bucket_ms);
        if buckets <= budget {
            return Ok((bucket_ms, idx.min(u32::MAX as usize) as u32));
        }
    }

    let span = span_u64(range.end_ms, range.start_ms)?;
    let mut bucket_ms = span.div_ceil(budget).max(1);
    for _ in 0..3 {
        let aligned_start = align_floor_ms(range.start_ms, bucket_ms)?;
        let span2 = span_u64(range.end_ms, aligned_start)?;
        let needed = span2.div_ceil(budget).max(1);
        if needed <= bucket_ms {
            break;
        }
        bucket_ms = needed;
    }

    Ok((
        bucket_ms,
        allowed_bucket_ms().len().min(u32::MAX as usize) as u32,
    ))
}

fn pseudo_u64(seed: &[u8]) -> u64 {
    let mut h = Sha256::new();
    h.update(seed);
    let out = h.finalize();
    u64::from_be_bytes(out[0..8].try_into().expect("8 bytes"))
}

async fn series_query_impl(
    transport: &TransportState,
    req: SeriesQueryRequest,
) -> Result<SeriesQueryResponse, CommandError> {
    validate_envelope(&req.envelope)?;
    if req.range.start_ms >= req.range.end_ms {
        return Err(CommandError::new(
            "SERIES.RANGE_INVALID",
            "range.start_ms must be < range.end_ms",
            req.envelope.correlation_id,
        ));
    }
    if req.target_points == 0 {
        return Err(CommandError::new(
            "SERIES.TARGET_POINTS_INVALID",
            "target_points must be > 0",
            req.envelope.correlation_id,
        ));
    }
    if req.target_points > MAX_TARGET_POINTS {
        return Err(CommandError::new(
            "SERIES.TARGET_POINTS_TOO_LARGE",
            format!("target_points must be <= {MAX_TARGET_POINTS}"),
            req.envelope.correlation_id,
        ));
    }

    let kind = req.kind.as_str();
    let (lod_profile, schema_id, points_per_bucket) = match kind {
        "synthetic.v1" | "synthetic.last.v1" => ("series_last_v1", "series.ts_ms_value.f64.v1", 1),
        "synthetic.minmax.v1" => ("series_minmax_v1", "series.ts_ms_minmax_last.f64.v1", 1),
        _ => {
            return Err(CommandError::new(
                "SERIES.KIND_UNSUPPORTED",
                "supported kinds: synthetic.v1, synthetic.last.v1, synthetic.minmax.v1",
                req.envelope.correlation_id,
            ));
        }
    };

    let (bucket_ms, lod_level) = choose_bucket_ms(&req.range, req.target_points, points_per_bucket)
        .map_err(|mut e| {
            e.correlation_id = req.envelope.correlation_id.clone();
            e
        })?;
    let aligned_start = align_floor_ms(req.range.start_ms, bucket_ms).map_err(|mut e| {
        e.correlation_id = req.envelope.correlation_id.clone();
        e
    })?;
    let span = span_u64(req.range.end_ms, aligned_start).map_err(|mut e| {
        e.correlation_id = req.envelope.correlation_id.clone();
        e
    })?;
    let bucket_count = span.div_ceil(bucket_ms);
    let budget = (req.target_points as u64) / (points_per_bucket as u64);
    let bucket_count = bucket_count.min(budget.max(1));

    let mut ts = Vec::with_capacity(bucket_count as usize);
    for i in 0..bucket_count {
        let t = (aligned_start as i128) + (i as i128) * (bucket_ms as i128);
        let Ok(t) = i64::try_from(t) else {
            return Err(CommandError::new(
                "SERIES.RANGE_OUT_OF_BOUNDS",
                "range too large after bucket alignment",
                req.envelope.correlation_id.clone(),
            ));
        };
        if t >= req.range.end_ms {
            break;
        }
        ts.push(t);
    }
    let points_returned = ts.len().min(u32::MAX as usize) as u32;

    let bytes = if lod_profile == "series_last_v1" {
        let mut values = Vec::with_capacity(ts.len());
        for t in &ts {
            let seed = format!("series|{}|{}|{}", kind, bucket_ms, t);
            let r = pseudo_u64(seed.as_bytes());
            values.push((r % 1_000_000) as f64 / 1000.0);
        }
        build_arrow_stream_bytes_last(&ts, &values)
    } else {
        let mut min_value = Vec::with_capacity(ts.len());
        let mut max_value = Vec::with_capacity(ts.len());
        let mut last_value = Vec::with_capacity(ts.len());
        for t in &ts {
            let seed = format!("series|{}|{}|{}", kind, bucket_ms, t);
            let r1 = pseudo_u64(seed.as_bytes());
            let r2 = pseudo_u64(format!("{seed}|x").as_bytes());
            let base = (r1 % 1_000_000) as f64 / 1000.0;
            let delta = ((r2 % 10_000) as f64) / 10_000.0;
            let last = base * (1.0 + (delta - 0.5) * 0.01);
            let min = base.min(last) * 0.995;
            let max = base.max(last) * 1.005;
            min_value.push(min);
            max_value.push(max);
            last_value.push(last);
        }
        build_arrow_stream_bytes_minmax(&ts, &min_value, &max_value, &last_value)
    }
    .map_err(|e| {
        CommandError::new(
            "SERIES.ENCODE_FAILED",
            format!("failed to encode arrow stream: {e}"),
            req.envelope.correlation_id.clone(),
        )
    })?;

    let chunks = chunk_arrow_ipc_stream(
        &bytes,
        limits::MAX_CHUNK_BYTES as usize,
        &req.envelope.correlation_id,
    )?;

    let stream_ref = transport
        .open_pending_arrow_stream(chunks, Some(schema_id.to_string()))
        .await
        .map_err(|e| match e {
            TransportErrorPublic::Unavailable => CommandError::new(
                "TRANSPORT.UNAVAILABLE",
                "loopback_ws transport unavailable",
                req.envelope.correlation_id.clone(),
            ),
        })?;

    Ok(SeriesQueryResponse {
        meta: SeriesQueryMeta {
            correlation_id: req.envelope.correlation_id,
            lod_profile: lod_profile.to_string(),
            lod_level,
            bucket_ms,
            points_returned,
        },
        stream_ref,
    })
}

#[tauri::command]
pub async fn series_query(
    transport: State<'_, TransportState>,
    req: SeriesQueryRequest,
) -> Result<SeriesQueryResponse, CommandError> {
    series_query_impl(&transport, req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures_util::{SinkExt, StreamExt};
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::HeaderValue;
    use tokio_tungstenite::tungstenite::protocol::Message;
    use uuid::Uuid;

    use crate::stream_ref::StreamTransport;

    fn parse_frame(frame: &[u8]) -> (u8, u64, Uuid, Vec<u8>) {
        assert!(frame.len() >= 32);
        assert_eq!(&frame[0..4], b"TSR1");
        let kind = frame[4];
        let header_len = u16::from_be_bytes([frame[6], frame[7]]);
        assert_eq!(header_len, 32);
        let seq = u64::from_be_bytes(frame[8..16].try_into().expect("seq bytes"));
        let stream_id = Uuid::from_slice(&frame[16..32]).expect("uuid");
        let payload = frame[32..].to_vec();
        (kind, seq, stream_id, payload)
    }

    async fn ws_connect(
        url: &str,
        token: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let mut req = url.into_client_request().expect("client request");
        req.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(token).expect("header"),
        );
        let (ws, _) = tokio_tungstenite::connect_async(req)
            .await
            .expect("ws connect");
        ws
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn series_query_validates_inputs() {
        let state = TransportState::default();
        let err = series_query_impl(
            &state,
            SeriesQueryRequest {
                envelope: RequestEnvelope {
                    protocol_version: crate::protocol::PROTOCOL_VERSION.to_string(),
                    correlation_id: "c1".to_string(),
                    request_id: Uuid::new_v4().to_string(),
                },
                kind: "synthetic.v1".to_string(),
                range: RangeMs {
                    start_ms: 10,
                    end_ms: 10,
                },
                target_points: 10,
            },
        )
        .await
        .expect_err("expected error");
        assert_eq!(err.code, "SERIES.RANGE_INVALID");
        assert_eq!(err.correlation_id, "c1");

        let err = series_query_impl(
            &state,
            SeriesQueryRequest {
                envelope: RequestEnvelope {
                    protocol_version: crate::protocol::PROTOCOL_VERSION.to_string(),
                    correlation_id: "c2".to_string(),
                    request_id: Uuid::new_v4().to_string(),
                },
                kind: "synthetic.v1".to_string(),
                range: RangeMs {
                    start_ms: 0,
                    end_ms: 10,
                },
                target_points: 0,
            },
        )
        .await
        .expect_err("expected error");
        assert_eq!(err.code, "SERIES.TARGET_POINTS_INVALID");
        assert_eq!(err.correlation_id, "c2");

        let err = series_query_impl(
            &state,
            SeriesQueryRequest {
                envelope: RequestEnvelope {
                    protocol_version: crate::protocol::PROTOCOL_VERSION.to_string(),
                    correlation_id: "c_tp".to_string(),
                    request_id: Uuid::new_v4().to_string(),
                },
                kind: "synthetic.v1".to_string(),
                range: RangeMs {
                    start_ms: 0,
                    end_ms: 10,
                },
                target_points: MAX_TARGET_POINTS + 1,
            },
        )
        .await
        .expect_err("expected error");
        assert_eq!(err.code, "SERIES.TARGET_POINTS_TOO_LARGE");
        assert_eq!(err.correlation_id, "c_tp");
    }

    #[test]
    fn arrow_chunker_roundtrips_stream() {
        let ts = vec![1_i64, 2, 3];
        let values = vec![1.0_f64, 2.0, 3.0];
        let bytes = build_arrow_stream_bytes_last(&ts, &values).expect("encode");
        let chunks = chunk_arrow_ipc_stream(&bytes, 4096, "c_chunk").expect("chunk");
        assert!(!chunks.is_empty());

        let mut combined = Vec::new();
        for c in chunks {
            combined.extend_from_slice(&c);
        }

        let mut reader = StreamReader::try_new(Cursor::new(combined), None).expect("reader");
        let decoded = reader.next().transpose().expect("read one");
        assert!(decoded.is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn series_query_minmax_stream_decodes_arrow_ipc() {
        let state = TransportState::default();
        let resp = series_query_impl(
            &state,
            SeriesQueryRequest {
                envelope: RequestEnvelope {
                    protocol_version: crate::protocol::PROTOCOL_VERSION.to_string(),
                    correlation_id: "c4".to_string(),
                    request_id: Uuid::new_v4().to_string(),
                },
                kind: "synthetic.minmax.v1".to_string(),
                range: RangeMs {
                    start_ms: 0,
                    end_ms: 10_000,
                },
                target_points: 128,
            },
        )
        .await
        .expect("series query");

        assert_eq!(resp.meta.correlation_id, "c4");
        assert_eq!(resp.stream_ref.format, "arrow_ipc_stream");
        assert_eq!(
            resp.stream_ref.schema_id.as_deref(),
            Some("series.ts_ms_minmax_last.f64.v1")
        );

        let (ws_url, auth_token, stream_id) = match &resp.stream_ref.transport {
            StreamTransport::LoopbackWs {
                url, auth_token, ..
            } => (
                url.clone(),
                auth_token.clone(),
                resp.stream_ref.stream_id.clone(),
            ),
        };

        let mut ws = ws_connect(&ws_url, &auth_token).await;

        let mut combined = Vec::new();
        let mut next_seq = 1u64;
        loop {
            ws.send(Message::Text(
                serde_json::to_string(&serde_json::json!({
                    "type": "streams.pull",
                    "stream_id": stream_id,
                    "next_seq": next_seq,
                    "max_bytes": 256 * 1024,
                    "correlation_id": "c4",
                    "request_id": Uuid::new_v4().to_string()
                }))
                .expect("json"),
            ))
            .await
            .expect("send pull");

            let msg = timeout(std::time::Duration::from_secs(2), ws.next())
                .await
                .expect("timeout")
                .expect("stream ended")
                .expect("ws msg");

            match msg {
                Message::Binary(frame) => {
                    let (kind, seq, _sid, payload) = parse_frame(&frame);
                    assert_eq!(seq, next_seq);
                    if kind == 1 {
                        combined.extend_from_slice(&payload);
                        next_seq += 1;
                        continue;
                    }
                    if kind == 2 {
                        assert!(payload.is_empty());
                        break;
                    }
                    panic!("unexpected frame kind: {kind}");
                }
                other => panic!("unexpected ws msg: {other:?}"),
            }
        }

        let msg = timeout(std::time::Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("ws msg");
        let Message::Text(text) = msg else {
            panic!("expected streams.closed");
        };
        let v: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert_eq!(v["type"], "streams.closed");

        let mut reader = StreamReader::try_new(Cursor::new(combined), None).expect("arrow reader");
        let mut rows = 0usize;
        while let Some(batch) = reader.next().transpose().expect("read batch") {
            rows += batch.num_rows();
            let schema = batch.schema();
            let fields = schema.fields();
            assert_eq!(fields[0].name(), "ts_ms");
            assert_eq!(fields[1].name(), "min_value");
            assert_eq!(fields[2].name(), "max_value");
            assert_eq!(fields[3].name(), "last_value");
        }
        assert_eq!(rows as u32, resp.meta.points_returned);
        assert!(resp.meta.points_returned <= 128);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn series_query_stream_decodes_arrow_ipc() {
        let state = TransportState::default();
        let resp = series_query_impl(
            &state,
            SeriesQueryRequest {
                envelope: RequestEnvelope {
                    protocol_version: crate::protocol::PROTOCOL_VERSION.to_string(),
                    correlation_id: "c3".to_string(),
                    request_id: Uuid::new_v4().to_string(),
                },
                kind: "synthetic.v1".to_string(),
                range: RangeMs {
                    start_ms: 0,
                    end_ms: 10_000,
                },
                target_points: 512,
            },
        )
        .await
        .expect("series query");

        assert_eq!(resp.meta.correlation_id, "c3");
        assert_eq!(resp.stream_ref.format, "arrow_ipc_stream");
        assert_eq!(
            resp.stream_ref.schema_id.as_deref(),
            Some("series.ts_ms_value.f64.v1")
        );

        let (ws_url, auth_token, stream_id) = match &resp.stream_ref.transport {
            StreamTransport::LoopbackWs {
                url, auth_token, ..
            } => (
                url.clone(),
                auth_token.clone(),
                resp.stream_ref.stream_id.clone(),
            ),
        };

        let mut ws = ws_connect(&ws_url, &auth_token).await;

        let mut combined = Vec::new();
        let mut next_seq = 1u64;
        loop {
            ws.send(Message::Text(
                serde_json::to_string(&serde_json::json!({
                    "type": "streams.pull",
                    "stream_id": stream_id,
                    "next_seq": next_seq,
                    "max_bytes": 256 * 1024,
                    "correlation_id": "c3",
                    "request_id": Uuid::new_v4().to_string()
                }))
                .expect("json"),
            ))
            .await
            .expect("send pull");

            let msg = timeout(std::time::Duration::from_secs(2), ws.next())
                .await
                .expect("timeout")
                .expect("stream ended")
                .expect("ws msg");

            match msg {
                Message::Binary(frame) => {
                    let (kind, seq, _sid, payload) = parse_frame(&frame);
                    assert_eq!(seq, next_seq);
                    if kind == 1 {
                        combined.extend_from_slice(&payload);
                        next_seq += 1;
                        continue;
                    }
                    if kind == 2 {
                        assert!(payload.is_empty());
                        break;
                    }
                    panic!("unexpected frame kind: {kind}");
                }
                other => panic!("unexpected ws msg: {other:?}"),
            }
        }

        let msg = timeout(std::time::Duration::from_secs(2), ws.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("ws msg");
        let Message::Text(text) = msg else {
            panic!("expected streams.closed");
        };
        let v: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert_eq!(v["type"], "streams.closed");

        let mut reader = StreamReader::try_new(Cursor::new(combined), None).expect("arrow reader");
        let mut rows = 0usize;
        while let Some(batch) = reader.next().transpose().expect("read batch") {
            rows += batch.num_rows();
            let schema = batch.schema();
            let fields = schema.fields();
            assert_eq!(fields[0].name(), "ts_ms");
            assert_eq!(fields[1].name(), "value");
        }
        assert_eq!(rows as u32, resp.meta.points_returned);
    }
}
