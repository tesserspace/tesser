use std::io::Cursor;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use tauri::State;

use crate::command_error::CommandError;
use crate::envelope::{validate_envelope, RequestEnvelope};
use crate::stream_ref::StreamRef;
use crate::transport::{TransportErrorPublic, TransportState};

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

fn build_arrow_stream_bytes(ts: &[i64], value: &[f64]) -> Result<Vec<u8>, String> {
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
    if req.kind != "synthetic.v1" {
        return Err(CommandError::new(
            "SERIES.KIND_UNSUPPORTED",
            "only kind=synthetic.v1 is supported in this stub",
            req.envelope.correlation_id,
        ));
    }

    let span_ms = (req.range.end_ms - req.range.start_ms) as u64;
    let mut points_returned = req.target_points.min(2048);
    if span_ms < points_returned as u64 {
        points_returned = span_ms as u32;
    }
    let bucket_ms = (span_ms / points_returned as u64).max(1);

    let mut ts = Vec::with_capacity(points_returned as usize);
    let mut values = Vec::with_capacity(points_returned as usize);
    for i in 0..points_returned as u64 {
        let t = req.range.start_ms + (i * bucket_ms) as i64;
        ts.push(t);
        values.push(i as f64);
    }

    let bytes = build_arrow_stream_bytes(&ts, &values).map_err(|e| {
        CommandError::new(
            "SERIES.ENCODE_FAILED",
            format!("failed to encode arrow stream: {e}"),
            req.envelope.correlation_id.clone(),
        )
    })?;

    let stream_ref = transport
        .open_pending_arrow_stream(vec![bytes], Some("series.ts_ms_value.f64.v1".to_string()))
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
            lod_profile: "series_last_v1".to_string(),
            lod_level: 0,
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

        ws.send(Message::Text(
            serde_json::to_string(&serde_json::json!({
                "type": "streams.pull",
                "stream_id": stream_id,
                "next_seq": 1,
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
        let Message::Binary(frame) = msg else {
            panic!("expected binary chunk");
        };
        let (kind, seq, _sid, payload) = parse_frame(&frame);
        assert_eq!(kind, 1);
        assert_eq!(seq, 1);

        ws.send(Message::Text(
            serde_json::to_string(&serde_json::json!({
                "type": "streams.pull",
                "stream_id": resp.stream_ref.stream_id,
                "next_seq": 2,
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
        let Message::Binary(frame) = msg else {
            panic!("expected binary eof");
        };
        let (kind, seq, _sid, eof_payload) = parse_frame(&frame);
        assert_eq!(kind, 2);
        assert_eq!(seq, 2);
        assert!(eof_payload.is_empty());

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

        let mut reader = StreamReader::try_new(Cursor::new(payload), None).expect("arrow reader");
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
