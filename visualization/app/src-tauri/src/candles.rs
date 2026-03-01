#![allow(clippy::result_large_err)]

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tauri::AppHandle;
use tauri::State;

use crate::arrow_ipc_chunker::chunk_arrow_ipc_stream;
use crate::command_error::CommandError;
use crate::datasets::{load_manifest_strict, load_pointer_strict, DatasetManifest};
use crate::envelope::{validate_envelope, RequestEnvelope};
use crate::limits;
use crate::storage::{default_layout, StorageLayout};
use crate::stream_ref::StreamRef;
use crate::transport::{TransportErrorPublic, TransportState};

#[derive(Debug, Clone, Deserialize)]
pub struct RangeMs {
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetManifestRef {
    pub manifest_hash: String,
    #[serde(default)]
    pub uri: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CandlesQueryRequest {
    pub envelope: RequestEnvelope,
    pub dataset_id: String,
    #[serde(default)]
    pub dataset_manifest_ref: Option<DatasetManifestRef>,
    pub range: RangeMs,
    pub target_points: u32,
    #[serde(default)]
    pub prefer_tiles: Option<bool>,
    #[serde(default)]
    pub allow_raw_fallback: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CandlesQueryMeta {
    pub correlation_id: String,
    pub manifest_hash: String,
    pub lod_profile: String,
    pub lod_level: u32,
    pub bucket_ms: u64,
    pub points_returned: u32,
    pub data_source: String,
    pub cache: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CandlesQueryResponse {
    pub meta: CandlesQueryMeta,
    pub stream_ref: StreamRef,
}

const LOD_PROFILE_CANDLES_OHLCV_V1: &str = "candles_ohlcv_v1";
const SCHEMA_ID_CANDLES_OHLCV_F64_V1: &str = "candles.ohlcv.f64.v1";
const MAX_TARGET_POINTS: u32 = 16_384;

fn align_floor_ms(ts_ms: i64, bucket_ms: u64) -> Result<i64, CommandError> {
    if bucket_ms == 0 {
        return Err(CommandError::new(
            "CANDLES.BUCKET_INVALID",
            "bucket_ms must be > 0",
            "candles".to_string(),
        ));
    }

    let b = bucket_ms as i128;
    let t = ts_ms as i128;
    let q = if t >= 0 { t / b } else { -((-t + b - 1) / b) };
    let aligned = q * b;
    i64::try_from(aligned).map_err(|_| {
        CommandError::new(
            "CANDLES.RANGE_OUT_OF_BOUNDS",
            "timestamp out of bounds for bucket alignment",
            "candles".to_string(),
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

fn span_u64(end_ms: i64, start_ms: i64) -> Result<u64, CommandError> {
    let span = (end_ms as i128) - (start_ms as i128);
    if span <= 0 {
        return Ok(0);
    }
    u64::try_from(span).map_err(|_| {
        CommandError::new(
            "CANDLES.RANGE_OUT_OF_BOUNDS",
            "range span out of bounds",
            "candles".to_string(),
        )
    })
}

fn choose_bucket_ms(range: &RangeMs, target_points: u32) -> Result<(u64, u32), CommandError> {
    let target_points_u64 = target_points as u64;
    for (idx, bucket_ms) in allowed_bucket_ms().iter().copied().enumerate() {
        let aligned_start = align_floor_ms(range.start_ms, bucket_ms)?;
        let span = span_u64(range.end_ms, aligned_start)?;
        let count = span.div_ceil(bucket_ms);
        if count <= target_points_u64 {
            return Ok((bucket_ms, idx.min(u32::MAX as usize) as u32));
        }
    }

    let span = span_u64(range.end_ms, range.start_ms)?;
    let mut bucket_ms = span.div_ceil(target_points_u64.max(1)).max(1);
    for _ in 0..3 {
        let aligned_start = align_floor_ms(range.start_ms, bucket_ms)?;
        let span2 = span_u64(range.end_ms, aligned_start)?;
        let needed = span2.div_ceil(target_points_u64.max(1)).max(1);
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

fn resolve_manifest_hash(
    layout: &StorageLayout,
    dataset_id: &str,
    requested: Option<&DatasetManifestRef>,
) -> Result<String, CommandError> {
    if let Some(r) = requested {
        if r.uri
            .as_deref()
            .is_some_and(|u| u.contains("://") || u.starts_with('/'))
        {
            return Err(CommandError::new(
                "CANDLES.MANIFEST_REF_INVALID",
                "dataset_manifest_ref.uri must not be an absolute path or contain a scheme",
                "candles".to_string(),
            ));
        }
        return Ok(r.manifest_hash.clone());
    }
    let pointer = load_pointer_strict(layout, dataset_id)?;
    Ok(pointer.active_manifest_hash)
}

fn load_manifest(
    layout: &StorageLayout,
    dataset_id: &str,
    manifest_hash: &str,
) -> Result<DatasetManifest, CommandError> {
    load_manifest_strict(layout, dataset_id, manifest_hash)
}

fn pseudo_u64(seed: &[u8]) -> u64 {
    let mut h = Sha256::new();
    h.update(seed);
    let out = h.finalize();
    u64::from_be_bytes(out[0..8].try_into().expect("8 bytes"))
}

struct CandlesOhlcv {
    ts_ms: Vec<i64>,
    open: Vec<f64>,
    high: Vec<f64>,
    low: Vec<f64>,
    close: Vec<f64>,
    volume: Vec<f64>,
}

fn build_candles_for_range(
    dataset_id: &str,
    manifest_hash: &str,
    range: &RangeMs,
    bucket_ms: u64,
) -> Result<CandlesOhlcv, CommandError> {
    let aligned_start = align_floor_ms(range.start_ms, bucket_ms)?;
    let span = span_u64(range.end_ms, aligned_start)?;
    let count = span.div_ceil(bucket_ms);
    let mut candles = CandlesOhlcv {
        ts_ms: Vec::with_capacity(count as usize),
        open: Vec::with_capacity(count as usize),
        high: Vec::with_capacity(count as usize),
        low: Vec::with_capacity(count as usize),
        close: Vec::with_capacity(count as usize),
        volume: Vec::with_capacity(count as usize),
    };

    for i in 0..count {
        let t = aligned_start + (i * bucket_ms) as i64;
        if t >= range.end_ms {
            break;
        }
        candles.ts_ms.push(t);
        let seed = format!("{dataset_id}|{manifest_hash}|{t}|{bucket_ms}");
        let r1 = pseudo_u64(seed.as_bytes());
        let r2 = pseudo_u64(format!("{seed}|x").as_bytes());

        let base = 100.0 + ((r1 % 10_000) as f64) / 100.0;
        let delta = ((r2 % 1_000) as f64) / 10_000.0;

        let o = base;
        let c = base * (1.0 + (delta - 0.05));
        let hi = o.max(c) * (1.0 + 0.0025);
        let lo = o.min(c) * (1.0 - 0.0025);
        let v = 1.0 + ((r1 % 1_000_000) as f64) / 100_000.0;

        candles.open.push(o);
        candles.close.push(c);
        candles.high.push(hi);
        candles.low.push(lo);
        candles.volume.push(v);
    }

    Ok(candles)
}

fn build_arrow_stream_bytes_ohlcv(
    ts: &[i64],
    open: &[f64],
    high: &[f64],
    low: &[f64],
    close: &[f64],
    volume: &[f64],
) -> Result<Vec<u8>, String> {
    const MAX_ROWS_PER_BATCH: usize = 2048;

    let schema = Schema::new(vec![
        Field::new("ts_ms", DataType::Int64, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]);

    let schema = Arc::new(schema);

    let ts_arr: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(ts.to_vec()));
    let open_arr: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(open.to_vec()));
    let high_arr: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(high.to_vec()));
    let low_arr: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(low.to_vec()));
    let close_arr: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(close.to_vec()));
    let volume_arr: Arc<dyn arrow_array::Array> = Arc::new(Float64Array::from(volume.to_vec()));

    let mut out = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut out, &schema).map_err(|e| e.to_string())?;
        let mut offset = 0usize;
        while offset < ts.len() {
            let len = (ts.len() - offset).min(MAX_ROWS_PER_BATCH);
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    ts_arr.slice(offset, len),
                    open_arr.slice(offset, len),
                    high_arr.slice(offset, len),
                    low_arr.slice(offset, len),
                    close_arr.slice(offset, len),
                    volume_arr.slice(offset, len),
                ],
            )
            .map_err(|e| e.to_string())?;
            writer.write(&batch).map_err(|e| e.to_string())?;
            offset += len;
        }
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

async fn candles_query_impl(
    transport: &TransportState,
    layout: &StorageLayout,
    req: CandlesQueryRequest,
) -> Result<CandlesQueryResponse, CommandError> {
    validate_envelope(&req.envelope)?;

    if req.range.start_ms >= req.range.end_ms {
        return Err(CommandError::new(
            "CANDLES.RANGE_INVALID",
            "range.start_ms must be < range.end_ms",
            req.envelope.correlation_id.clone(),
        ));
    }
    if req.target_points == 0 {
        return Err(CommandError::new(
            "CANDLES.TARGET_POINTS_INVALID",
            "target_points must be > 0",
            req.envelope.correlation_id.clone(),
        ));
    }
    if req.target_points > MAX_TARGET_POINTS {
        return Err(CommandError::new(
            "CANDLES.TARGET_POINTS_TOO_LARGE",
            format!("target_points must be <= {MAX_TARGET_POINTS}"),
            req.envelope.correlation_id.clone(),
        ));
    }

    let manifest_hash =
        resolve_manifest_hash(layout, &req.dataset_id, req.dataset_manifest_ref.as_ref()).map_err(
            |mut e| {
                e.correlation_id = req.envelope.correlation_id.clone();
                e
            },
        )?;
    let _manifest = load_manifest(layout, &req.dataset_id, &manifest_hash).map_err(|mut e| {
        e.correlation_id = req.envelope.correlation_id.clone();
        e
    })?;

    let _prefer_tiles = req.prefer_tiles.unwrap_or(true);
    let allow_raw_fallback = req.allow_raw_fallback.unwrap_or(true);
    if !allow_raw_fallback {
        return Err(CommandError::new(
            "CANDLES.TILES_UNAVAILABLE",
            "tiles path not implemented yet; set allow_raw_fallback=true",
            req.envelope.correlation_id,
        ));
    }

    let (bucket_ms, lod_level) =
        choose_bucket_ms(&req.range, req.target_points).map_err(|mut e| {
            e.correlation_id = req.envelope.correlation_id.clone();
            e
        })?;
    let candles = build_candles_for_range(&req.dataset_id, &manifest_hash, &req.range, bucket_ms)
        .map_err(|mut e| {
        e.correlation_id = req.envelope.correlation_id.clone();
        e
    })?;
    let points_returned = candles.ts_ms.len().min(u32::MAX as usize) as u32;

    let bytes = build_arrow_stream_bytes_ohlcv(
        &candles.ts_ms,
        &candles.open,
        &candles.high,
        &candles.low,
        &candles.close,
        &candles.volume,
    )
    .map_err(|e| {
        CommandError::new(
            "CANDLES.ENCODE_FAILED",
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
        .open_pending_arrow_stream(chunks, Some(SCHEMA_ID_CANDLES_OHLCV_F64_V1.to_string()))
        .await
        .map_err(|e| match e {
            TransportErrorPublic::Unavailable => CommandError::new(
                "TRANSPORT.UNAVAILABLE",
                "loopback_ws transport unavailable",
                req.envelope.correlation_id.clone(),
            ),
            TransportErrorPublic::ResourceLimit { message } => CommandError::new(
                "TRANSPORT.RESOURCE_LIMIT",
                message,
                req.envelope.correlation_id.clone(),
            ),
        })?;

    Ok(CandlesQueryResponse {
        meta: CandlesQueryMeta {
            correlation_id: req.envelope.correlation_id,
            manifest_hash,
            lod_profile: LOD_PROFILE_CANDLES_OHLCV_V1.to_string(),
            lod_level,
            bucket_ms,
            points_returned,
            data_source: "raw_fallback".to_string(),
            cache: "miss".to_string(),
        },
        stream_ref,
    })
}

#[tauri::command]
pub async fn candles_query(
    app: AppHandle,
    transport: State<'_, TransportState>,
    req: CandlesQueryRequest,
) -> Result<CandlesQueryResponse, CommandError> {
    let layout = default_layout(&app)?;
    candles_query_impl(&transport, &layout, req).await
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

    fn envelope(correlation_id: &str) -> RequestEnvelope {
        RequestEnvelope {
            protocol_version: crate::protocol::PROTOCOL_VERSION.to_string(),
            correlation_id: correlation_id.to_string(),
            request_id: Uuid::new_v4().to_string(),
        }
    }

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
    async fn candles_query_is_pixel_bound_and_aligned() {
        let transport = TransportState::default();
        let tmp = tempfile::tempdir().expect("tempdir");
        let layout = StorageLayout {
            workspace_root: tmp.path().join("ws"),
            dataset_root: tmp.path().join("ds"),
            cache_root: tmp.path().join("cache"),
        };
        layout.ensure().expect("ensure");

        let dataset_id = "crypto.synthetic.spot.demo.series.1s.v1";
        let preview = crate::datasets::create_synthetic_dataset(&layout, dataset_id).expect("ds");

        let resp = candles_query_impl(
            &transport,
            &layout,
            CandlesQueryRequest {
                envelope: envelope("c1"),
                dataset_id: dataset_id.to_string(),
                dataset_manifest_ref: Some(DatasetManifestRef {
                    manifest_hash: preview.active_manifest_hash.clone().expect("hash"),
                    uri: None,
                }),
                range: RangeMs {
                    start_ms: 1234,
                    end_ms: 1234 + 120_000,
                },
                target_points: 240,
                prefer_tiles: Some(true),
                allow_raw_fallback: Some(true),
            },
        )
        .await
        .expect("query");

        assert!(resp.meta.points_returned <= 240);
        assert_eq!(resp.meta.correlation_id, "c1");

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
                    "correlation_id": "c1",
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
        let mut saw_alignment = false;
        while let Some(batch) = reader.next().transpose().expect("read batch") {
            rows += batch.num_rows();
            let schema = batch.schema();
            assert_eq!(schema.fields().len(), 6);
            assert_eq!(schema.fields()[0].name(), "ts_ms");
            let ts = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ts");
            for i in 0..ts.len() {
                assert_eq!(ts.value(i) % resp.meta.bucket_ms as i64, 0);
                saw_alignment = true;
            }
        }
        assert!(saw_alignment);
        assert_eq!(rows as u32, resp.meta.points_returned);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn candles_query_large_result_streams_and_decodes() {
        let transport = TransportState::default();
        let tmp = tempfile::tempdir().expect("tempdir");
        let layout = StorageLayout {
            workspace_root: tmp.path().join("ws"),
            dataset_root: tmp.path().join("ds"),
            cache_root: tmp.path().join("cache"),
        };
        layout.ensure().expect("ensure");

        let dataset_id = "crypto.synthetic.spot.demo.series.1s.v1";
        let preview = crate::datasets::create_synthetic_dataset(&layout, dataset_id).expect("ds");

        let resp = candles_query_impl(
            &transport,
            &layout,
            CandlesQueryRequest {
                envelope: envelope("c_large"),
                dataset_id: dataset_id.to_string(),
                dataset_manifest_ref: Some(DatasetManifestRef {
                    manifest_hash: preview.active_manifest_hash.clone().expect("hash"),
                    uri: None,
                }),
                range: RangeMs {
                    start_ms: 0,
                    end_ms: (MAX_TARGET_POINTS as i64) * 1_000,
                },
                target_points: MAX_TARGET_POINTS,
                prefer_tiles: Some(true),
                allow_raw_fallback: Some(true),
            },
        )
        .await
        .expect("query");

        assert!(resp.meta.points_returned <= MAX_TARGET_POINTS);
        assert_eq!(resp.meta.correlation_id, "c_large");

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
                    "max_bytes": limits::MAX_BYTES_PER_PULL,
                    "correlation_id": "c_large",
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
        }
        assert_eq!(rows as u32, resp.meta.points_returned);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn candles_query_validates_inputs() {
        let transport = TransportState::default();
        let tmp = tempfile::tempdir().expect("tempdir");
        let layout = StorageLayout {
            workspace_root: tmp.path().join("ws"),
            dataset_root: tmp.path().join("ds"),
            cache_root: tmp.path().join("cache"),
        };

        let err = candles_query_impl(
            &transport,
            &layout,
            CandlesQueryRequest {
                envelope: envelope("c2"),
                dataset_id: "bad/..".to_string(),
                dataset_manifest_ref: None,
                range: RangeMs {
                    start_ms: 10,
                    end_ms: 9,
                },
                target_points: 10,
                prefer_tiles: None,
                allow_raw_fallback: None,
            },
        )
        .await
        .expect_err("err");
        assert_eq!(err.code, "CANDLES.RANGE_INVALID");
        assert_eq!(err.correlation_id, "c2");

        let err = candles_query_impl(
            &transport,
            &layout,
            CandlesQueryRequest {
                envelope: envelope("c3"),
                dataset_id: "bad/..".to_string(),
                dataset_manifest_ref: None,
                range: RangeMs {
                    start_ms: 0,
                    end_ms: 1,
                },
                target_points: 10,
                prefer_tiles: None,
                allow_raw_fallback: Some(true),
            },
        )
        .await
        .expect_err("err");
        assert_eq!(err.code, "DATASET.ID_INVALID");
        assert_eq!(err.correlation_id, "c3");
    }

    #[test]
    fn bucket_choice_never_exceeds_target_points_after_alignment() {
        let range = RangeMs {
            start_ms: 1234,
            end_ms: 1234 + 10_000,
        };
        let (bucket_ms, _) = choose_bucket_ms(&range, 5).expect("bucket");
        let aligned_start = align_floor_ms(range.start_ms, bucket_ms).expect("align");
        let span = span_u64(range.end_ms, aligned_start).expect("span");
        let count = span.div_ceil(bucket_ms);
        assert!(count <= 5);
    }

    // streaming decode is exercised via the loopback_ws pull loop above.
}
