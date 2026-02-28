#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, Runtime, State};
use tokio::sync::{Mutex, Semaphore};
use uuid::Uuid;

use crate::command_error::CommandError;
use crate::datasets::create_synthetic_dataset_with_cancel_and_commit_state;
use crate::envelope::{validate_envelope, RequestEnvelope};
use crate::storage::default_layout;

const MAX_JOB_TYPE_LEN: usize = 64;
const MAX_IDEMPOTENCY_KEY_LEN: usize = 128;
const MAX_JOB_INPUT_BYTES: usize = 256 * 1024;
const MAX_EVENTS_PER_RESPONSE: usize = 500;
const RECOVERY_RESUME_CONCURRENCY: usize = 2;

const COMMIT_OPEN: u8 = 0;
const COMMIT_IN_PROGRESS: u8 = 1;
const COMMIT_DONE: u8 = 2;
const COMMIT_CANCELED: u8 = 3;

fn commit_started(state: u8) -> bool {
    matches!(state, COMMIT_IN_PROGRESS | COMMIT_DONE)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Canceled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub job_id: String,
    pub job_type: String,
    pub status: JobStatus,
    pub attempt: u32,
    pub seq: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub correlation_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<CommandError>,
}

struct JobRuntime {
    record: JobRecord,
    inputs: serde_json::Value,
    cancel: Arc<AtomicBool>,
    commit_state: Arc<AtomicU8>,
    last_units_done: u64,
    commit_completed: bool,
}

#[derive(Default)]
pub struct JobsState {
    inner: Arc<Mutex<HashMap<String, JobRuntime>>>,
}

impl JobsState {
    fn shared(&self) -> Arc<Mutex<HashMap<String, JobRuntime>>> {
        self.inner.clone()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobsStartRequest {
    pub envelope: RequestEnvelope,
    pub job_type: String,
    pub inputs: serde_json::Value,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub reuse_output: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobsStartResponse {
    pub job_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobsGetRequest {
    pub envelope: RequestEnvelope,
    pub job_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobsListRequest {
    pub envelope: RequestEnvelope,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobsCancelRequest {
    pub envelope: RequestEnvelope,
    pub job_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobsRetryRequest {
    pub envelope: RequestEnvelope,
    pub job_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobsEventsSinceRequest {
    pub envelope: RequestEnvelope,
    pub job_id: String,
    pub last_seq: u64,
    #[serde(default)]
    pub max_events: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedEvent {
    pub event: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobsEventsSinceResponse {
    pub record: JobRecord,
    pub events: Vec<PersistedEvent>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn bump_seq(record: &mut JobRecord) {
    record.seq = record.seq.saturating_add(1);
}

fn emit<R: Runtime>(app: &AppHandle<R>, event: &str, payload: &serde_json::Value) {
    let _ = app.emit(event, payload);
}

fn base_payload(record: &JobRecord, at_ms: u64) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("job_id".to_string(), serde_json::json!(record.job_id));
    obj.insert("job_type".to_string(), serde_json::json!(record.job_type));
    obj.insert("status".to_string(), serde_json::json!(record.status));
    obj.insert("attempt".to_string(), serde_json::json!(record.attempt));
    obj.insert("seq".to_string(), serde_json::json!(record.seq));
    obj.insert("at_ms".to_string(), serde_json::json!(at_ms));
    obj.insert(
        "correlation_id".to_string(),
        serde_json::json!(record.correlation_id),
    );
    if let Some(code) = &record.reason_code {
        obj.insert("reason_code".to_string(), serde_json::json!(code));
    }
    serde_json::Value::Object(obj)
}

fn jobs_dir<R: Runtime>(app: &AppHandle<R>) -> Result<std::path::PathBuf, CommandError> {
    if let Ok(layout) = default_layout(app) {
        Ok(layout.workspace_root.join("jobs"))
    } else {
        Ok(std::env::temp_dir()
            .join("tesser_visualization")
            .join("jobs"))
    }
}

fn job_snapshot_path<R: Runtime>(
    app: &AppHandle<R>,
    job_id: &str,
) -> Result<std::path::PathBuf, CommandError> {
    Ok(jobs_dir(app)?.join(format!("{job_id}.json")))
}

fn job_events_dir<R: Runtime>(
    app: &AppHandle<R>,
    job_id: &str,
) -> Result<std::path::PathBuf, CommandError> {
    Ok(jobs_dir(app)?.join("events").join(job_id))
}

fn atomic_write(path: &std::path::Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    use std::io::Write;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(format!("tmp.{}", uuid::Uuid::new_v4()));
    {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    atomic_replace(&tmp, path)?;
    if let Some(parent) = path.parent() {
        #[cfg(not(target_os = "windows"))]
        {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
    }
    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn atomic_replace(tmp: &std::path::Path, dst: &std::path::Path) -> Result<(), std::io::Error> {
    std::fs::rename(tmp, dst)?;
    Ok(())
}

#[cfg(target_os = "windows")]
fn atomic_replace(tmp: &std::path::Path, dst: &std::path::Path) -> Result<(), std::io::Error> {
    use std::os::windows::ffi::OsStrExt;

    let tmp_w: Vec<u16> = tmp.as_os_str().encode_wide().chain(Some(0)).collect();
    let dst_w: Vec<u16> = dst.as_os_str().encode_wide().chain(Some(0)).collect();

    unsafe {
        let ok = windows_sys::Win32::Storage::FileSystem::ReplaceFileW(
            dst_w.as_ptr(),
            tmp_w.as_ptr(),
            std::ptr::null(),
            windows_sys::Win32::Storage::FileSystem::REPLACEFILE_IGNORE_MERGE_ERRORS,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
        if ok == 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::NotFound {
                std::fs::rename(tmp, dst)?;
                return Ok(());
            }
            return Err(err);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedJob {
    record: JobRecord,
    inputs: serde_json::Value,
    last_units_done: u64,
    #[serde(default)]
    commit_completed: bool,
}

fn load_persisted_job<R: Runtime>(
    app: &AppHandle<R>,
    job_id: &str,
    correlation_id: &str,
) -> Result<Option<PersistedJob>, CommandError> {
    let path = job_snapshot_path(app, job_id)?;
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&path).map_err(|e| {
        CommandError::new(
            "JOB.PERSIST_IO_ERROR",
            format!("failed to read job snapshot: {e}"),
            correlation_id.to_string(),
        )
    })?;
    let pj: PersistedJob = serde_json::from_slice(&bytes).map_err(|e| {
        CommandError::new(
            "JOB.PERSIST_DECODE_FAILED",
            format!("failed to decode job snapshot: {e}"),
            correlation_id.to_string(),
        )
    })?;
    Ok(Some(pj))
}

fn job_record_from_event(
    job_id: &str,
    ev: &PersistedEvent,
    fallback_correlation_id: &str,
) -> Option<JobRecord> {
    let payload = &ev.payload;
    let job_type = payload.get("job_type")?.as_str()?.to_string();
    let status: JobStatus = serde_json::from_value(payload.get("status")?.clone()).ok()?;
    let attempt = payload.get("attempt")?.as_u64()? as u32;
    let seq = payload.get("seq")?.as_u64()?;
    let at_ms = payload
        .get("at_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or_else(now_ms);
    let correlation_id = payload
        .get("correlation_id")
        .and_then(|v| v.as_str())
        .unwrap_or(fallback_correlation_id)
        .to_string();
    let reason_code = payload
        .get("reason_code")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let error = payload.get("error").and_then(|err| {
        let code = err.get("code")?.as_str()?.to_string();
        let message = err.get("message")?.as_str()?.to_string();
        Some(CommandError::new(code, message, correlation_id.clone()))
    });

    Some(JobRecord {
        job_id: job_id.to_string(),
        job_type,
        status,
        attempt,
        seq,
        created_at_ms: at_ms,
        updated_at_ms: at_ms,
        correlation_id,
        reason_code,
        idempotency_key: None,
        result: None,
        error,
    })
}

fn load_job_record_from_latest_event<R: Runtime>(
    app: &AppHandle<R>,
    job_id: &str,
    correlation_id: &str,
) -> Result<Option<JobRecord>, CommandError> {
    let dir = job_events_dir(app, job_id)?;
    if !dir.exists() {
        return Ok(None);
    }
    let mut max_seq_seen = 0u64;
    for entry in std::fs::read_dir(&dir).map_err(|e| {
        CommandError::new(
            "JOB.EVENTS_IO_ERROR",
            format!("failed to read job events dir: {e}"),
            correlation_id.to_string(),
        )
    })? {
        let Ok(entry) = entry else { continue };
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Ok(seq) = stem.parse::<u64>() else {
            continue;
        };
        max_seq_seen = max_seq_seen.max(seq);
    }
    if max_seq_seen == 0 {
        return Ok(None);
    }
    let path = dir.join(format!("{max_seq_seen}.json"));
    let bytes = std::fs::read(&path).map_err(|e| {
        CommandError::new(
            "JOB.EVENTS_IO_ERROR",
            format!("failed to read latest event {max_seq_seen}: {e}"),
            correlation_id.to_string(),
        )
    })?;
    let ev: PersistedEvent = serde_json::from_slice(&bytes).map_err(|e| {
        CommandError::new(
            "JOB.EVENTS_DECODE_FAILED",
            format!("failed to decode latest event {max_seq_seen}: {e}"),
            correlation_id.to_string(),
        )
    })?;
    Ok(job_record_from_event(job_id, &ev, correlation_id))
}

fn persist_event<R: Runtime>(
    app: &AppHandle<R>,
    job_id: &str,
    seq: u64,
    event: &str,
    payload: &serde_json::Value,
) -> Result<(), CommandError> {
    let dir = job_events_dir(app, job_id)?;
    std::fs::create_dir_all(&dir).map_err(|e| {
        CommandError::new(
            "JOB.EVENTS_IO_ERROR",
            format!("failed to create job events dir: {e}"),
            "jobs".to_string(),
        )
    })?;
    let path = dir.join(format!("{seq}.json"));
    if path.exists() {
        return Err(CommandError::new(
            "JOB.EVENTS_SEQ_COLLISION",
            format!("event seq collision for job_id={job_id}, seq={seq}"),
            "jobs".to_string(),
        ));
    }
    let ev = PersistedEvent {
        event: event.to_string(),
        payload: payload.clone(),
    };
    let bytes = serde_json::to_vec(&ev).map_err(|e| {
        CommandError::new(
            "JOB.EVENTS_ENCODE_FAILED",
            format!("failed to encode event: {e}"),
            "jobs".to_string(),
        )
    })?;
    atomic_write(&path, &bytes).map_err(|e| {
        CommandError::new(
            "JOB.EVENTS_IO_ERROR",
            format!("failed to persist event: {e}"),
            "jobs".to_string(),
        )
    })?;
    Ok(())
}

fn persist_snapshot<R: Runtime>(
    app: &AppHandle<R>,
    snapshot: &PersistedJob,
) -> Result<(), CommandError> {
    let dir = jobs_dir(app)?;
    std::fs::create_dir_all(&dir).map_err(|e| {
        CommandError::new(
            "JOB.PERSIST_IO_ERROR",
            format!("failed to create jobs dir: {e}"),
            snapshot.record.correlation_id.clone(),
        )
    })?;
    let path = dir.join(format!("{}.json", snapshot.record.job_id));
    let bytes = serde_json::to_vec_pretty(snapshot).map_err(|e| {
        CommandError::new(
            "JOB.PERSIST_ENCODE_FAILED",
            format!("failed to encode job snapshot: {e}"),
            snapshot.record.correlation_id.clone(),
        )
    })?;
    atomic_write(&path, &bytes).map_err(|e| {
        CommandError::new(
            "JOB.PERSIST_IO_ERROR",
            format!("failed to persist job snapshot: {e}"),
            snapshot.record.correlation_id.clone(),
        )
    })?;
    Ok(())
}

async fn persist_job_id<R: Runtime>(
    app: &AppHandle<R>,
    jobs: &Arc<Mutex<HashMap<String, JobRuntime>>>,
    job_id: &str,
) {
    let snapshot = {
        let map = jobs.lock().await;
        let Some(rt) = map.get(job_id) else {
            return;
        };
        PersistedJob {
            record: rt.record.clone(),
            inputs: rt.inputs.clone(),
            last_units_done: rt.last_units_done,
            commit_completed: rt.commit_completed,
        }
    };
    let _ = persist_snapshot(app, &snapshot);
}

async fn set_status<R: Runtime>(
    app: &AppHandle<R>,
    jobs: &Arc<Mutex<HashMap<String, JobRuntime>>>,
    job_id: &str,
    status: JobStatus,
    event: &str,
    reason_code: Option<String>,
) {
    let (payload, snapshot) = {
        let mut map = jobs.lock().await;
        let Some(rt) = map.get_mut(job_id) else {
            return;
        };
        if matches!(
            rt.record.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Canceled
        ) {
            return;
        }
        let old_record = rt.record.clone();
        let old_last_units_done = rt.last_units_done;

        let at_ms = now_ms();
        rt.record.status = status;
        rt.record.reason_code = reason_code.clone();
        rt.record.updated_at_ms = at_ms;
        bump_seq(&mut rt.record);
        let mut payload = base_payload(&rt.record, at_ms);
        if let serde_json::Value::Object(ref mut obj) = payload {
            if let Some(code) = &reason_code {
                obj.insert("reason_code".to_string(), serde_json::json!(code));
            }
            if let Some(err) = &rt.record.error {
                obj.insert(
                    "error".to_string(),
                    serde_json::json!({"code": err.code, "message": err.message}),
                );
            }
        }
        let snapshot = PersistedJob {
            record: rt.record.clone(),
            inputs: rt.inputs.clone(),
            last_units_done: rt.last_units_done,
            commit_completed: rt.commit_completed,
        };
        let seq = rt.record.seq;

        if persist_event(app, job_id, seq, event, &payload).is_err() {
            rt.record = old_record;
            rt.last_units_done = old_last_units_done;
            return;
        }
        (payload, snapshot)
    };
    let _ = persist_snapshot(app, &snapshot);
    emit(app, event, &payload);
}

async fn emit_progress<R: Runtime>(
    app: &AppHandle<R>,
    jobs: &Arc<Mutex<HashMap<String, JobRuntime>>>,
    job_id: &str,
    units_done: u64,
    units_total: Option<u64>,
    phase: Option<&str>,
) {
    let (payload, snapshot) = {
        let mut map = jobs.lock().await;
        let Some(rt) = map.get_mut(job_id) else {
            return;
        };
        if matches!(
            rt.record.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Canceled
        ) {
            return;
        }
        let old_record = rt.record.clone();
        let old_last_units_done = rt.last_units_done;

        let units_done = units_done.max(rt.last_units_done);
        rt.last_units_done = units_done;
        let at_ms = now_ms();
        rt.record.updated_at_ms = at_ms;
        bump_seq(&mut rt.record);
        let mut payload = base_payload(&rt.record, at_ms);
        if let serde_json::Value::Object(ref mut obj) = payload {
            obj.insert("units_done".to_string(), serde_json::json!(units_done));
            obj.insert("units_total".to_string(), serde_json::json!(units_total));
            if units_total.is_none() {
                obj.insert(
                    "phase".to_string(),
                    serde_json::json!(phase.unwrap_or("working")),
                );
            }
        }
        let snapshot = PersistedJob {
            record: rt.record.clone(),
            inputs: rt.inputs.clone(),
            last_units_done: rt.last_units_done,
            commit_completed: rt.commit_completed,
        };
        let seq = rt.record.seq;

        if persist_event(app, job_id, seq, "job.progress", &payload).is_err() {
            rt.record = old_record;
            rt.last_units_done = old_last_units_done;
            return;
        }
        (payload, snapshot)
    };
    let _ = persist_snapshot(app, &snapshot);
    emit(app, "job.progress", &payload);
}

async fn emit_log<R: Runtime>(
    app: &AppHandle<R>,
    jobs: &Arc<Mutex<HashMap<String, JobRuntime>>>,
    job_id: &str,
    level: &str,
    message: &str,
) {
    let (payload, snapshot) = {
        let mut map = jobs.lock().await;
        let Some(rt) = map.get_mut(job_id) else {
            return;
        };
        if matches!(
            rt.record.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Canceled
        ) {
            return;
        }
        let old_record = rt.record.clone();
        let old_last_units_done = rt.last_units_done;

        let at_ms = now_ms();
        rt.record.updated_at_ms = at_ms;
        bump_seq(&mut rt.record);
        let mut payload = base_payload(&rt.record, at_ms);
        if let serde_json::Value::Object(ref mut obj) = payload {
            obj.insert("level".to_string(), serde_json::json!(level));
            obj.insert("message".to_string(), serde_json::json!(message));
        }
        let snapshot = PersistedJob {
            record: rt.record.clone(),
            inputs: rt.inputs.clone(),
            last_units_done: rt.last_units_done,
            commit_completed: rt.commit_completed,
        };
        let seq = rt.record.seq;

        if persist_event(app, job_id, seq, "job.log", &payload).is_err() {
            rt.record = old_record;
            rt.last_units_done = old_last_units_done;
            return;
        }
        (payload, snapshot)
    };
    let _ = persist_snapshot(app, &snapshot);
    emit(app, "job.log", &payload);
}

async fn complete_with_result<R: Runtime>(
    app: &AppHandle<R>,
    jobs: &Arc<Mutex<HashMap<String, JobRuntime>>>,
    job_id: &str,
    result: serde_json::Value,
) {
    let cancel_should_win = {
        let map = jobs.lock().await;
        let Some(rt) = map.get(job_id) else {
            return;
        };
        let commit_state = rt.commit_state.load(Ordering::SeqCst);
        rt.cancel.load(Ordering::SeqCst) && !commit_started(commit_state) && !rt.commit_completed
    };
    if cancel_should_win {
        set_status(
            app,
            jobs,
            job_id,
            JobStatus::Canceled,
            "job.canceled",
            Some("user_cancel".to_string()),
        )
        .await;
        return;
    }

    {
        let mut map = jobs.lock().await;
        let Some(rt) = map.get_mut(job_id) else {
            return;
        };
        rt.record.result = Some(result);
    }
    set_status(
        app,
        jobs,
        job_id,
        JobStatus::Completed,
        "job.completed",
        None,
    )
    .await;
}

async fn fail_with_error<R: Runtime>(
    app: &AppHandle<R>,
    jobs: &Arc<Mutex<HashMap<String, JobRuntime>>>,
    job_id: &str,
    err: CommandError,
) {
    let reason = err.code.clone();
    {
        let mut map = jobs.lock().await;
        let Some(rt) = map.get_mut(job_id) else {
            return;
        };
        rt.record.error = Some(err);
    }
    set_status(
        app,
        jobs,
        job_id,
        JobStatus::Failed,
        "job.failed",
        Some(reason),
    )
    .await;
}

async fn mark_canceled_if_requested<R: Runtime>(
    app: &AppHandle<R>,
    jobs: &Arc<Mutex<HashMap<String, JobRuntime>>>,
    job_id: &str,
) -> bool {
    let (canceled, commit_state, commit_completed) = {
        let map = jobs.lock().await;
        let Some(rt) = map.get(job_id) else {
            return true;
        };
        (
            rt.cancel.load(Ordering::SeqCst),
            rt.commit_state.load(Ordering::SeqCst),
            rt.commit_completed,
        )
    };
    if canceled && !commit_started(commit_state) && !commit_completed {
        set_status(
            app,
            jobs,
            job_id,
            JobStatus::Canceled,
            "job.canceled",
            Some("user_cancel".to_string()),
        )
        .await;
        return true;
    }
    false
}

async fn run_job<R: Runtime>(
    app: AppHandle<R>,
    jobs: Arc<Mutex<HashMap<String, JobRuntime>>>,
    job_id: String,
) {
    {
        let mut map = jobs.lock().await;
        let Some(rt) = map.get_mut(&job_id) else {
            return;
        };
        if matches!(
            rt.record.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Canceled
        ) {
            return;
        }
        rt.record.status = JobStatus::Running;
        rt.record.updated_at_ms = now_ms();
        rt.record.reason_code = None;
    }
    persist_job_id(&app, &jobs, &job_id).await;
    emit_log(&app, &jobs, &job_id, "info", "job started").await;
    emit_progress(&app, &jobs, &job_id, 0, None, Some("start")).await;

    let (job_type, inputs, cancel_flag, commit_state, correlation_id) = {
        let map = jobs.lock().await;
        let Some(rt) = map.get(&job_id) else {
            return;
        };
        (
            rt.record.job_type.clone(),
            rt.inputs.clone(),
            rt.cancel.clone(),
            rt.commit_state.clone(),
            rt.record.correlation_id.clone(),
        )
    };

    if cancel_flag.load(Ordering::SeqCst) {
        set_status(
            &app,
            &jobs,
            &job_id,
            JobStatus::Canceled,
            "job.canceled",
            Some("user_cancel".to_string()),
        )
        .await;
        return;
    }

    let layout = match default_layout(&app) {
        Ok(l) => l,
        Err(mut e) => {
            e.correlation_id = correlation_id;
            fail_with_error(&app, &jobs, &job_id, e).await;
            return;
        }
    };

    match job_type.as_str() {
        "dataset_index_build" => {
            emit_log(&app, &jobs, &job_id, "info", "phase: index_build").await;
            let dataset_id = inputs
                .get("dataset_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    CommandError::new(
                        "JOB.INPUT_INVALID",
                        "missing inputs.dataset_id",
                        correlation_id.clone(),
                    )
                });

            let dataset_id = match dataset_id {
                Ok(v) => v.to_string(),
                Err(e) => {
                    fail_with_error(&app, &jobs, &job_id, e).await;
                    return;
                }
            };

            emit_progress(&app, &jobs, &job_id, 0, None, Some("index_build")).await;
            if mark_canceled_if_requested(&app, &jobs, &job_id).await {
                return;
            }
            let preview = match create_synthetic_dataset_with_cancel_and_commit_state(
                &layout,
                &dataset_id,
                &cancel_flag,
                &commit_state,
            ) {
                Ok(p) => p,
                Err(mut e) => {
                    if e.code == "DATASET.CANCELED" {
                        set_status(
                            &app,
                            &jobs,
                            &job_id,
                            JobStatus::Canceled,
                            "job.canceled",
                            Some("user_cancel".to_string()),
                        )
                        .await;
                        return;
                    }
                    e.correlation_id = correlation_id;
                    fail_with_error(&app, &jobs, &job_id, e).await;
                    return;
                }
            };
            {
                let mut map = jobs.lock().await;
                if let Some(rt) = map.get_mut(&job_id) {
                    rt.commit_completed = true;
                    rt.commit_state.store(COMMIT_DONE, Ordering::SeqCst);
                }
            }
            persist_job_id(&app, &jobs, &job_id).await;
            if mark_canceled_if_requested(&app, &jobs, &job_id).await {
                return;
            }
            let result = match serde_json::to_value(preview) {
                Ok(v) => v,
                Err(e) => {
                    fail_with_error(
                        &app,
                        &jobs,
                        &job_id,
                        CommandError::new(
                            "JOB.RESULT_ENCODE_FAILED",
                            format!("failed to encode result: {e}"),
                            correlation_id,
                        ),
                    )
                    .await;
                    return;
                }
            };
            emit_progress(&app, &jobs, &job_id, 1, Some(1), None).await;
            if mark_canceled_if_requested(&app, &jobs, &job_id).await {
                return;
            }
            complete_with_result(&app, &jobs, &job_id, result).await;
        }
        _ => {
            let message = format!("unsupported job_type: {job_type}");
            emit_log(&app, &jobs, &job_id, "error", &message).await;
            fail_with_error(
                &app,
                &jobs,
                &job_id,
                CommandError::new(
                    "JOB.TYPE_UNSUPPORTED",
                    format!("unsupported job_type: {job_type}"),
                    correlation_id,
                ),
            )
            .await;
        }
    }
}

async fn jobs_start_inner<R: Runtime>(
    app: AppHandle<R>,
    state: &JobsState,
    req: JobsStartRequest,
) -> Result<JobsStartResponse, CommandError> {
    validate_envelope(&req.envelope)?;
    if req.job_type.is_empty() || req.job_type.len() > MAX_JOB_TYPE_LEN {
        return Err(CommandError::new(
            "JOB.TYPE_INVALID",
            "job_type is invalid",
            req.envelope.correlation_id,
        ));
    }
    if let Some(key) = &req.idempotency_key {
        if key.len() > MAX_IDEMPOTENCY_KEY_LEN {
            return Err(CommandError::new(
                "JOB.IDEMPOTENCY_KEY_INVALID",
                "idempotency_key is too long",
                req.envelope.correlation_id,
            ));
        }
    }
    let inputs_bytes = serde_json::to_vec(&req.inputs).map_err(|e| {
        CommandError::new(
            "JOB.INPUT_ENCODE_FAILED",
            format!("failed to encode inputs: {e}"),
            req.envelope.correlation_id.clone(),
        )
    })?;
    if inputs_bytes.len() > MAX_JOB_INPUT_BYTES {
        return Err(CommandError::new(
            "JOB.INPUT_TOO_LARGE",
            format!("inputs too large: {} bytes", inputs_bytes.len()),
            req.envelope.correlation_id,
        ));
    }

    let jobs = state.shared();
    {
        let map = jobs.lock().await;
        if let Some(key) = &req.idempotency_key {
            for rt in map.values() {
                if rt.record.job_type == req.job_type
                    && rt.record.idempotency_key.as_deref() == Some(key.as_str())
                {
                    if matches!(rt.record.status, JobStatus::Queued | JobStatus::Running) {
                        return Ok(JobsStartResponse {
                            job_id: rt.record.job_id.clone(),
                        });
                    }
                    if req.reuse_output && matches!(rt.record.status, JobStatus::Completed) {
                        return Ok(JobsStartResponse {
                            job_id: rt.record.job_id.clone(),
                        });
                    }
                }
            }
        }
    }

    let job_id = Uuid::new_v4().to_string();
    let at_ms = now_ms();
    {
        let mut map = jobs.lock().await;
        let record = JobRecord {
            job_id: job_id.clone(),
            job_type: req.job_type,
            status: JobStatus::Queued,
            attempt: 1,
            seq: 0,
            created_at_ms: at_ms,
            updated_at_ms: at_ms,
            correlation_id: req.envelope.correlation_id.clone(),
            reason_code: None,
            idempotency_key: req.idempotency_key,
            result: None,
            error: None,
        };
        map.insert(
            job_id.clone(),
            JobRuntime {
                record,
                inputs: req.inputs,
                cancel: Arc::new(AtomicBool::new(false)),
                commit_state: Arc::new(AtomicU8::new(COMMIT_OPEN)),
                last_units_done: 0,
                commit_completed: false,
            },
        );
    }
    persist_job_id(&app, &jobs, &job_id).await;
    tokio::spawn(run_job(app, jobs.clone(), job_id.clone()));
    Ok(JobsStartResponse { job_id })
}

#[tauri::command]
pub async fn jobs_start(
    app: AppHandle,
    state: State<'_, JobsState>,
    req: JobsStartRequest,
) -> Result<JobsStartResponse, CommandError> {
    jobs_start_inner(app, state.inner(), req).await
}

#[tauri::command]
pub async fn jobs_get(
    state: State<'_, JobsState>,
    req: JobsGetRequest,
) -> Result<JobRecord, CommandError> {
    validate_envelope(&req.envelope)?;
    let jobs = state.shared();
    let map = jobs.lock().await;
    let Some(rt) = map.get(&req.job_id) else {
        return Err(CommandError::new(
            "JOB.NOT_FOUND",
            "job not found",
            req.envelope.correlation_id,
        ));
    };
    Ok(rt.record.clone())
}

#[tauri::command]
pub async fn jobs_list(
    state: State<'_, JobsState>,
    req: JobsListRequest,
) -> Result<Vec<JobRecord>, CommandError> {
    validate_envelope(&req.envelope)?;
    let jobs = state.shared();
    let map = jobs.lock().await;
    Ok(map.values().map(|rt| rt.record.clone()).collect())
}

#[tauri::command]
pub async fn jobs_cancel(
    app: AppHandle,
    state: State<'_, JobsState>,
    req: JobsCancelRequest,
) -> Result<(), CommandError> {
    validate_envelope(&req.envelope)?;
    let jobs = state.shared();
    let (status, cancel, last_units_done) = {
        let mut map = jobs.lock().await;
        let Some(rt) = map.get_mut(&req.job_id) else {
            return Err(CommandError::new(
                "JOB.NOT_FOUND",
                "job not found",
                req.envelope.correlation_id,
            ));
        };
        if matches!(
            rt.record.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Canceled
        ) {
            return Ok(());
        }
        let job_type = rt.record.job_type.clone();
        let commit_state = rt.commit_state.load(Ordering::SeqCst);
        if rt.commit_completed
            || (job_type == "dataset_index_build" && commit_started(commit_state))
        {
            return Err(CommandError::new(
                "JOB.CANCEL_TOO_LATE",
                "job already committed results; cancel not allowed",
                req.envelope.correlation_id,
            ));
        }
        let status = rt.record.status.clone();
        if job_type == "dataset_index_build" && matches!(status, JobStatus::Running) {
            let _ = rt.commit_state.compare_exchange(
                COMMIT_OPEN,
                COMMIT_CANCELED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            let commit_state = rt.commit_state.load(Ordering::SeqCst);
            if commit_started(commit_state) || rt.commit_completed {
                return Err(CommandError::new(
                    "JOB.CANCEL_TOO_LATE",
                    "job already committed results; cancel not allowed",
                    req.envelope.correlation_id,
                ));
            }
        }
        rt.cancel.store(true, Ordering::SeqCst);
        rt.record.reason_code = Some("cancel_requested".to_string());
        rt.record.updated_at_ms = now_ms();
        rt.record.correlation_id = req.envelope.correlation_id.clone();
        (status, rt.cancel.clone(), rt.last_units_done)
    };
    if matches!(status, JobStatus::Queued) && cancel.load(Ordering::SeqCst) {
        set_status(
            &app,
            &jobs,
            &req.job_id,
            JobStatus::Canceled,
            "job.canceled",
            Some("user_cancel".to_string()),
        )
        .await;
    } else if matches!(status, JobStatus::Running) && cancel.load(Ordering::SeqCst) {
        emit_progress(
            &app,
            &jobs,
            &req.job_id,
            last_units_done,
            None,
            Some("cancel_requested"),
        )
        .await;
    }
    Ok(())
}

#[tauri::command]
pub async fn jobs_retry(
    app: AppHandle,
    state: State<'_, JobsState>,
    req: JobsRetryRequest,
) -> Result<(), CommandError> {
    jobs_retry_inner(app, state.inner(), req).await
}

#[tauri::command]
pub async fn jobs_events_since(
    app: AppHandle,
    state: State<'_, JobsState>,
    req: JobsEventsSinceRequest,
) -> Result<JobsEventsSinceResponse, CommandError> {
    jobs_events_since_inner(&app, state.inner(), req).await
}

async fn jobs_events_since_inner<R: Runtime>(
    app: &AppHandle<R>,
    state: &JobsState,
    req: JobsEventsSinceRequest,
) -> Result<JobsEventsSinceResponse, CommandError> {
    validate_envelope(&req.envelope)?;

    let max_events = req
        .max_events
        .unwrap_or(MAX_EVENTS_PER_RESPONSE as u32)
        .min(MAX_EVENTS_PER_RESPONSE as u32) as usize;

    let shared = state.shared();
    let mut record = {
        let map = shared.lock().await;
        map.get(&req.job_id).map(|rt| rt.record.clone())
    };
    let mut loaded_snapshot: Option<PersistedJob> = None;
    if record.is_none() {
        loaded_snapshot = load_persisted_job(app, &req.job_id, &req.envelope.correlation_id)?;
        record = loaded_snapshot.as_ref().map(|pj| pj.record.clone());
    }
    if record.is_none() {
        record = load_job_record_from_latest_event(app, &req.job_id, &req.envelope.correlation_id)?;
    }
    let Some(mut record) = record else {
        return Err(CommandError::new(
            "JOB.NOT_FOUND",
            "job not found",
            req.envelope.correlation_id,
        ));
    };

    let dir = job_events_dir(app, &req.job_id)?;
    if !dir.exists() {
        return Ok(JobsEventsSinceResponse {
            record,
            events: vec![],
        });
    }

    let mut max_seq_seen = 0u64;
    let mut seqs: Vec<u64> = Vec::new();
    for entry in std::fs::read_dir(&dir).map_err(|e| {
        CommandError::new(
            "JOB.EVENTS_IO_ERROR",
            format!("failed to read job events dir: {e}"),
            req.envelope.correlation_id.clone(),
        )
    })? {
        let Ok(entry) = entry else { continue };
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Ok(seq) = stem.parse::<u64>() else {
            continue;
        };
        max_seq_seen = max_seq_seen.max(seq);
        if seq > req.last_seq {
            seqs.push(seq);
        }
    }
    seqs.sort_unstable();
    seqs.truncate(max_events);
    record.seq = record.seq.max(max_seq_seen);
    if max_seq_seen > 0 {
        let mut maybe_snapshot = None;
        {
            let mut map = shared.lock().await;
            if let Some(rt) = map.get_mut(&req.job_id) {
                if max_seq_seen > rt.record.seq {
                    rt.record.seq = max_seq_seen;
                    maybe_snapshot = Some(PersistedJob {
                        record: rt.record.clone(),
                        inputs: rt.inputs.clone(),
                        last_units_done: rt.last_units_done,
                        commit_completed: rt.commit_completed,
                    });
                }
            }
        }
        if maybe_snapshot.is_none() {
            if let Some(mut pj) = loaded_snapshot.take() {
                if max_seq_seen > pj.record.seq {
                    pj.record.seq = max_seq_seen;
                    maybe_snapshot = Some(pj);
                }
            }
        }
        if let Some(snapshot) = maybe_snapshot {
            let _ = persist_snapshot(app, &snapshot);
            record = snapshot.record;
        }
    }

    let mut events = Vec::with_capacity(seqs.len());
    for seq in seqs {
        let path = dir.join(format!("{seq}.json"));
        let bytes = std::fs::read(&path).map_err(|e| {
            CommandError::new(
                "JOB.EVENTS_IO_ERROR",
                format!("failed to read event {seq}: {e}"),
                req.envelope.correlation_id.clone(),
            )
        })?;
        let ev: PersistedEvent = serde_json::from_slice(&bytes).map_err(|e| {
            CommandError::new(
                "JOB.EVENTS_DECODE_FAILED",
                format!("failed to decode event {seq}: {e}"),
                req.envelope.correlation_id.clone(),
            )
        })?;
        events.push(ev);
    }

    Ok(JobsEventsSinceResponse { record, events })
}

async fn jobs_retry_inner<R: Runtime>(
    app: AppHandle<R>,
    state: &JobsState,
    req: JobsRetryRequest,
) -> Result<(), CommandError> {
    validate_envelope(&req.envelope)?;
    let jobs = state.shared();
    let job_id = req.job_id.clone();
    {
        let mut map = jobs.lock().await;
        let Some(rt) = map.get_mut(&job_id) else {
            return Err(CommandError::new(
                "JOB.NOT_FOUND",
                "job not found",
                req.envelope.correlation_id,
            ));
        };
        if matches!(rt.record.status, JobStatus::Queued | JobStatus::Running) {
            return Err(CommandError::new(
                "JOB.RETRY_NOT_ALLOWED",
                "job is not in a terminal state",
                req.envelope.correlation_id,
            ));
        }
        if matches!(rt.record.status, JobStatus::Completed) {
            return Err(CommandError::new(
                "JOB.RETRY_NOT_ALLOWED",
                "job already completed",
                req.envelope.correlation_id,
            ));
        }

        rt.record.attempt = rt.record.attempt.saturating_add(1);
        rt.record.status = JobStatus::Queued;
        rt.record.updated_at_ms = now_ms();
        rt.record.reason_code = Some("retry".to_string());
        rt.record.correlation_id = req.envelope.correlation_id.clone();
        rt.record.result = None;
        rt.record.error = None;
        rt.last_units_done = 0;
        rt.commit_completed = false;
        rt.commit_state = Arc::new(AtomicU8::new(COMMIT_OPEN));
        rt.cancel = Arc::new(AtomicBool::new(false));
    }
    persist_job_id(&app, &jobs, &job_id).await;
    let attempt = {
        let map = jobs.lock().await;
        map.get(&job_id).map(|rt| rt.record.attempt).unwrap_or(0)
    };
    let message = format!("job retry queued (attempt {attempt})");
    emit_log(&app, &jobs, &job_id, "info", &message).await;
    tokio::spawn(run_job(app, jobs.clone(), job_id));
    Ok(())
}

pub async fn recover_on_start<R: Runtime>(
    app: &AppHandle<R>,
    state: &JobsState,
) -> Result<(), CommandError> {
    let dir = jobs_dir(app)?;
    if !dir.exists() {
        return Ok(());
    }
    let entries = std::fs::read_dir(&dir).map_err(|e| {
        CommandError::new(
            "JOB.RECOVER_IO_ERROR",
            format!("failed to read jobs dir: {e}"),
            "jobs".to_string(),
        )
    })?;
    let paths: Vec<std::path::PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("json"))
        .collect();

    fn is_resumable_job_type(job_type: &str) -> bool {
        matches!(job_type, "dataset_index_build")
    }

    let shared = state.shared();
    let mut to_resume: Vec<String> = Vec::new();
    {
        let mut map = shared.lock().await;
        for path in paths {
            let job_id_from_path = path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string());
            let bytes = match std::fs::read(&path) {
                Ok(b) => b,
                Err(e) => {
                    if let Some(job_id) = job_id_from_path {
                        let at_ms = now_ms();
                        let correlation_id = "jobs_recover".to_string();
                        let record = JobRecord {
                            job_id: job_id.clone(),
                            job_type: "unknown".to_string(),
                            status: JobStatus::Failed,
                            attempt: 1,
                            seq: 0,
                            created_at_ms: at_ms,
                            updated_at_ms: at_ms,
                            correlation_id: correlation_id.clone(),
                            reason_code: Some("snapshot_unreadable".to_string()),
                            idempotency_key: None,
                            result: None,
                            error: Some(CommandError::new(
                                "JOB.SNAPSHOT_UNREADABLE",
                                format!("failed to read job snapshot: {e}"),
                                correlation_id.clone(),
                            )),
                        };
                        let rt = JobRuntime {
                            record: record.clone(),
                            inputs: serde_json::json!({}),
                            cancel: Arc::new(AtomicBool::new(false)),
                            commit_state: Arc::new(AtomicU8::new(COMMIT_OPEN)),
                            last_units_done: 0,
                            commit_completed: false,
                        };
                        map.insert(job_id.clone(), rt);
                        let _ = persist_snapshot(
                            app,
                            &PersistedJob {
                                record,
                                inputs: serde_json::json!({}),
                                last_units_done: 0,
                                commit_completed: false,
                            },
                        );
                    }
                    continue;
                }
            };
            let mut pj: PersistedJob = match serde_json::from_slice(&bytes) {
                Ok(v) => v,
                Err(e) => {
                    if let Some(job_id) = job_id_from_path {
                        let at_ms = now_ms();
                        let correlation_id = "jobs_recover".to_string();
                        let record = JobRecord {
                            job_id: job_id.clone(),
                            job_type: "unknown".to_string(),
                            status: JobStatus::Failed,
                            attempt: 1,
                            seq: 0,
                            created_at_ms: at_ms,
                            updated_at_ms: at_ms,
                            correlation_id: correlation_id.clone(),
                            reason_code: Some("snapshot_corrupt".to_string()),
                            idempotency_key: None,
                            result: None,
                            error: Some(CommandError::new(
                                "JOB.SNAPSHOT_CORRUPT",
                                format!("failed to decode job snapshot: {e}"),
                                correlation_id.clone(),
                            )),
                        };
                        let rt = JobRuntime {
                            record: record.clone(),
                            inputs: serde_json::json!({}),
                            cancel: Arc::new(AtomicBool::new(false)),
                            commit_state: Arc::new(AtomicU8::new(COMMIT_OPEN)),
                            last_units_done: 0,
                            commit_completed: false,
                        };
                        map.insert(job_id.clone(), rt);
                        let _ = persist_snapshot(
                            app,
                            &PersistedJob {
                                record,
                                inputs: serde_json::json!({}),
                                last_units_done: 0,
                                commit_completed: false,
                            },
                        );
                    }
                    continue;
                }
            };

            let mut reconciled_seq = pj.record.seq;
            let mut latest_event: Option<PersistedEvent> = None;
            if let Ok(events_dir) = job_events_dir(app, &pj.record.job_id) {
                if events_dir.exists() {
                    if let Ok(entries) = std::fs::read_dir(&events_dir) {
                        for entry in entries.flatten() {
                            let path = entry.path();
                            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                                continue;
                            }
                            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                                continue;
                            };
                            if let Ok(seq) = stem.parse::<u64>() {
                                reconciled_seq = reconciled_seq.max(seq);
                            }
                        }
                    }

                    if reconciled_seq > 0 {
                        let path = events_dir.join(format!("{reconciled_seq}.json"));
                        if let Ok(bytes) = std::fs::read(&path) {
                            if let Ok(ev) = serde_json::from_slice::<PersistedEvent>(&bytes) {
                                latest_event = Some(ev);
                            }
                        }
                    }
                }
            }
            let seq_changed = reconciled_seq != pj.record.seq;
            if seq_changed {
                pj.record.seq = reconciled_seq;
            }

            let mut should_persist = false;
            if let Some(ev) = &latest_event {
                match ev.event.as_str() {
                    "job.completed" => {
                        pj.record.status = JobStatus::Completed;
                        pj.record.reason_code = ev
                            .payload
                            .get("reason_code")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        pj.record.updated_at_ms = ev
                            .payload
                            .get("at_ms")
                            .and_then(|v| v.as_u64())
                            .unwrap_or_else(now_ms);
                        pj.record.error = None;
                        pj.commit_completed = true;
                        should_persist = true;
                    }
                    "job.canceled" => {
                        pj.record.status = JobStatus::Canceled;
                        pj.record.reason_code = ev
                            .payload
                            .get("reason_code")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        pj.record.updated_at_ms = ev
                            .payload
                            .get("at_ms")
                            .and_then(|v| v.as_u64())
                            .unwrap_or_else(now_ms);
                        pj.record.error = None;
                        should_persist = true;
                    }
                    "job.failed" => {
                        pj.record.status = JobStatus::Failed;
                        pj.record.reason_code = ev
                            .payload
                            .get("reason_code")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        pj.record.updated_at_ms = ev
                            .payload
                            .get("at_ms")
                            .and_then(|v| v.as_u64())
                            .unwrap_or_else(now_ms);
                        if let Some(err) = ev.payload.get("error") {
                            if let (Some(code), Some(message)) = (
                                err.get("code").and_then(|v| v.as_str()),
                                err.get("message").and_then(|v| v.as_str()),
                            ) {
                                pj.record.error = Some(CommandError::new(
                                    code.to_string(),
                                    message.to_string(),
                                    pj.record.correlation_id.clone(),
                                ));
                            }
                        }
                        should_persist = true;
                    }
                    _ => {}
                }
            }
            if matches!(
                pj.record.reason_code.as_deref(),
                Some("cancel_requested") | Some("user_cancel")
            ) && matches!(pj.record.status, JobStatus::Queued | JobStatus::Running)
                && !pj.commit_completed
            {
                pj.record.status = JobStatus::Canceled;
                pj.record.reason_code = Some("user_cancel".to_string());
                pj.record.updated_at_ms = now_ms();
                bump_seq(&mut pj.record);
                pj.record.error = None;
                should_persist = true;
            } else if matches!(pj.record.status, JobStatus::Running) {
                let was_resumable = is_resumable_job_type(&pj.record.job_type);
                pj.record.attempt = pj.record.attempt.saturating_add(1);
                pj.record.updated_at_ms = now_ms();
                pj.record.error = None;
                pj.record.result = None;
                pj.last_units_done = 0;
                if was_resumable && !pj.commit_completed {
                    pj.record.status = JobStatus::Queued;
                    pj.record.reason_code = Some("recovered".to_string());
                } else {
                    pj.record.status = JobStatus::Failed;
                    pj.record.reason_code = Some("worker_lost".to_string());
                    pj.record.error = Some(CommandError::new(
                        "JOB.WORKER_LOST",
                        "job was running during previous shutdown",
                        pj.record.correlation_id.clone(),
                    ));
                }
                bump_seq(&mut pj.record);
                should_persist = true;
            }
            if seq_changed {
                should_persist = true;
            }
            let resume = matches!(pj.record.status, JobStatus::Queued);
            map.insert(
                pj.record.job_id.clone(),
                JobRuntime {
                    record: pj.record.clone(),
                    inputs: pj.inputs.clone(),
                    cancel: Arc::new(AtomicBool::new(false)),
                    commit_state: Arc::new(AtomicU8::new(if pj.commit_completed {
                        COMMIT_DONE
                    } else {
                        COMMIT_OPEN
                    })),
                    last_units_done: pj.last_units_done,
                    commit_completed: pj.commit_completed,
                },
            );
            if resume {
                to_resume.push(pj.record.job_id.clone());
            }
            if should_persist {
                let at_ms = pj.record.updated_at_ms;
                let (event, payload) = match pj.record.status {
                    JobStatus::Canceled => {
                        let mut payload = base_payload(&pj.record, at_ms);
                        if let serde_json::Value::Object(ref mut obj) = payload {
                            if let Some(code) = &pj.record.reason_code {
                                obj.insert("reason_code".to_string(), serde_json::json!(code));
                            }
                        }
                        ("job.canceled", payload)
                    }
                    JobStatus::Failed => {
                        let mut payload = base_payload(&pj.record, at_ms);
                        if let serde_json::Value::Object(ref mut obj) = payload {
                            if let Some(code) = &pj.record.reason_code {
                                obj.insert("reason_code".to_string(), serde_json::json!(code));
                            }
                            if let Some(err) = &pj.record.error {
                                obj.insert(
                                    "error".to_string(),
                                    serde_json::json!({"code": err.code, "message": err.message}),
                                );
                            }
                        }
                        ("job.failed", payload)
                    }
                    JobStatus::Completed => {
                        let mut payload = base_payload(&pj.record, at_ms);
                        if let serde_json::Value::Object(ref mut obj) = payload {
                            if let Some(code) = &pj.record.reason_code {
                                obj.insert("reason_code".to_string(), serde_json::json!(code));
                            }
                        }
                        ("job.completed", payload)
                    }
                    JobStatus::Queued | JobStatus::Running => {
                        let mut payload = base_payload(&pj.record, at_ms);
                        if let serde_json::Value::Object(ref mut obj) = payload {
                            obj.insert("level".to_string(), serde_json::json!("info"));
                            obj.insert(
                                "message".to_string(),
                                serde_json::json!("job recovered on startup"),
                            );
                            if let Some(code) = &pj.record.reason_code {
                                obj.insert("reason_code".to_string(), serde_json::json!(code));
                            }
                        }
                        ("job.log", payload)
                    }
                };
                let _ = persist_event(app, &pj.record.job_id, pj.record.seq, event, &payload);
                let _ = persist_snapshot(
                    app,
                    &PersistedJob {
                        record: pj.record,
                        inputs: pj.inputs,
                        last_units_done: pj.last_units_done,
                        commit_completed: pj.commit_completed,
                    },
                );
            }
        }
    }

    let app_handle = app.clone();
    let resume_sem = Arc::new(Semaphore::new(RECOVERY_RESUME_CONCURRENCY));
    for job_id in to_resume {
        let sem = resume_sem.clone();
        let app = app_handle.clone();
        let jobs = shared.clone();
        tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.ok();
            run_job(app, jobs, job_id).await;
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;
    use tokio::sync::{OwnedSemaphorePermit, Semaphore};
    use tokio::time::{sleep, timeout, Duration};

    fn envelope(correlation_id: &str) -> RequestEnvelope {
        RequestEnvelope {
            protocol_version: crate::protocol::PROTOCOL_VERSION.to_string(),
            correlation_id: correlation_id.to_string(),
            request_id: Uuid::new_v4().to_string(),
        }
    }

    fn disk_semaphore() -> &'static Arc<Semaphore> {
        static SEM: OnceLock<Arc<Semaphore>> = OnceLock::new();
        SEM.get_or_init(|| Arc::new(Semaphore::new(1)))
    }

    async fn disk_permit() -> OwnedSemaphorePermit {
        disk_semaphore()
            .clone()
            .acquire_owned()
            .await
            .expect("permit")
    }

    #[tokio::test]
    async fn idempotency_key_dedupes_active_jobs() {
        let _permit = disk_permit().await;
        let app = tauri::test::mock_app();
        let state = JobsState::default();

        let req = JobsStartRequest {
            envelope: envelope("c1"),
            job_type: "dataset_index_build".to_string(),
            inputs: serde_json::json!({"dataset_id":"crypto.binance.spot.btcusdt.candles.1m.v1"}),
            idempotency_key: Some("k1".to_string()),
            reuse_output: false,
        };
        let r1 = jobs_start_inner(app.handle().clone(), &state, req.clone())
            .await
            .expect("start 1");
        let r2 = jobs_start_inner(app.handle().clone(), &state, req)
            .await
            .expect("start 2");
        assert_eq!(r1.job_id, r2.job_id);
    }

    async fn wait_for_terminal(state: &JobsState, job_id: &str) -> JobRecord {
        let shared = state.shared();
        timeout(Duration::from_secs(15), async {
            loop {
                let status = {
                    let map = shared.lock().await;
                    map.get(job_id).map(|rt| rt.record.status.clone())
                };
                match status {
                    Some(JobStatus::Completed | JobStatus::Failed | JobStatus::Canceled) => break,
                    Some(_) => sleep(Duration::from_millis(10)).await,
                    None => sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("terminal timeout");

        let map = shared.lock().await;
        map.get(job_id).expect("job exists").record.clone()
    }

    #[tokio::test]
    async fn cancel_is_terminal_and_prevents_completion() {
        let _permit = disk_permit().await;
        let app = tauri::test::mock_app();
        let jobs = Arc::new(Mutex::new(HashMap::<String, JobRuntime>::new()));
        let job_id = Uuid::new_v4().to_string();
        let at_ms = now_ms();
        jobs.lock().await.insert(
            job_id.clone(),
            JobRuntime {
                record: JobRecord {
                    job_id: job_id.clone(),
                    job_type: "dataset_index_build".to_string(),
                    status: JobStatus::Queued,
                    attempt: 1,
                    seq: 0,
                    created_at_ms: at_ms,
                    updated_at_ms: at_ms,
                    correlation_id: "c2".to_string(),
                    reason_code: None,
                    idempotency_key: None,
                    result: None,
                    error: None,
                },
                inputs: serde_json::json!({"dataset_id":"crypto.binance.spot.btcusdt.candles.1m.v1"}),
                cancel: Arc::new(AtomicBool::new(true)),
                commit_state: Arc::new(AtomicU8::new(COMMIT_OPEN)),
                last_units_done: 0,
                commit_completed: false,
            },
        );

        run_job(app.handle().clone(), jobs.clone(), job_id.clone()).await;
        let map = jobs.lock().await;
        let rt = map.get(&job_id).expect("job exists");
        assert!(matches!(rt.record.status, JobStatus::Canceled));
    }

    #[tokio::test]
    async fn commit_state_prevents_cancel_winning_after_publish() {
        let _permit = disk_permit().await;
        let app = tauri::test::mock_app();
        let jobs = Arc::new(Mutex::new(HashMap::<String, JobRuntime>::new()));
        let job_id = Uuid::new_v4().to_string();
        let at_ms = now_ms();
        jobs.lock().await.insert(
            job_id.clone(),
            JobRuntime {
                record: JobRecord {
                    job_id: job_id.clone(),
                    job_type: "dataset_index_build".to_string(),
                    status: JobStatus::Running,
                    attempt: 1,
                    seq: 0,
                    created_at_ms: at_ms,
                    updated_at_ms: at_ms,
                    correlation_id: "c_commit".to_string(),
                    reason_code: None,
                    idempotency_key: None,
                    result: None,
                    error: None,
                },
                inputs: serde_json::json!({"dataset_id":"crypto.binance.spot.btcusdt.candles.1m.v1"}),
                cancel: Arc::new(AtomicBool::new(true)),
                commit_state: Arc::new(AtomicU8::new(COMMIT_DONE)),
                last_units_done: 0,
                commit_completed: false,
            },
        );

        complete_with_result(app.handle(), &jobs, &job_id, serde_json::json!({"ok":true})).await;
        let map = jobs.lock().await;
        let rt = map.get(&job_id).expect("job exists");
        assert!(matches!(rt.record.status, JobStatus::Completed));
    }

    #[tokio::test]
    async fn retry_increments_attempt() {
        let _permit = disk_permit().await;
        let app = tauri::test::mock_app();
        let state = JobsState::default();

        let start = JobsStartRequest {
            envelope: envelope("c3"),
            job_type: "unsupported_type".to_string(),
            inputs: serde_json::json!({}),
            idempotency_key: None,
            reuse_output: false,
        };
        let started = jobs_start_inner(app.handle().clone(), &state, start)
            .await
            .expect("start");
        let first = wait_for_terminal(&state, &started.job_id).await;
        assert!(matches!(first.status, JobStatus::Failed));
        assert_eq!(first.attempt, 1);

        let retry_req = JobsRetryRequest {
            envelope: envelope("c4"),
            job_id: started.job_id.clone(),
        };
        jobs_retry_inner(app.handle().clone(), &state, retry_req)
            .await
            .expect("retry");

        let second = wait_for_terminal(&state, &started.job_id).await;
        assert!(matches!(second.status, JobStatus::Failed));
        assert_eq!(second.attempt, 2);
    }

    #[tokio::test]
    async fn recover_marks_running_as_worker_lost_failed() {
        let _permit = disk_permit().await;
        let app = tauri::test::mock_app();
        let state = JobsState::default();
        let job_id = Uuid::new_v4().to_string();
        let at_ms = now_ms();
        let snapshot = PersistedJob {
            record: JobRecord {
                job_id: job_id.clone(),
                job_type: "unsupported_type".to_string(),
                status: JobStatus::Running,
                attempt: 1,
                seq: 7,
                created_at_ms: at_ms,
                updated_at_ms: at_ms,
                correlation_id: "c5".to_string(),
                reason_code: None,
                idempotency_key: None,
                result: None,
                error: None,
            },
            inputs: serde_json::json!({}),
            last_units_done: 0,
            commit_completed: false,
        };
        persist_snapshot(app.handle(), &snapshot).expect("persist");

        recover_on_start(app.handle(), &state)
            .await
            .expect("recover");
        let shared = state.shared();
        let map = shared.lock().await;
        let rt = map.get(&job_id).expect("recovered job exists");
        assert!(matches!(rt.record.status, JobStatus::Failed));
        assert_eq!(rt.record.reason_code.as_deref(), Some("worker_lost"));
        assert_eq!(rt.record.attempt, 2);
        assert!(rt
            .record
            .error
            .as_ref()
            .is_some_and(|e| e.code == "JOB.WORKER_LOST"));
    }

    #[tokio::test]
    async fn recover_resumes_queued_jobs_by_spawning_workers() {
        let _permit = disk_permit().await;
        let app = tauri::test::mock_app();
        let state = JobsState::default();
        let job_id = Uuid::new_v4().to_string();
        let at_ms = now_ms();
        let snapshot = PersistedJob {
            record: JobRecord {
                job_id: job_id.clone(),
                job_type: "unsupported_type".to_string(),
                status: JobStatus::Queued,
                attempt: 1,
                seq: 0,
                created_at_ms: at_ms,
                updated_at_ms: at_ms,
                correlation_id: "c_resume".to_string(),
                reason_code: None,
                idempotency_key: None,
                result: None,
                error: None,
            },
            inputs: serde_json::json!({}),
            last_units_done: 0,
            commit_completed: false,
        };
        persist_snapshot(app.handle(), &snapshot).expect("persist");

        recover_on_start(app.handle(), &state)
            .await
            .expect("recover");

        let record = wait_for_terminal(&state, &job_id).await;
        assert!(matches!(record.status, JobStatus::Failed));
    }

    #[tokio::test]
    async fn recover_requeues_resumable_running_jobs_with_new_attempt() {
        let _permit = disk_permit().await;
        let app = tauri::test::mock_app();
        let state = JobsState::default();
        let job_id = Uuid::new_v4().to_string();
        let at_ms = now_ms();
        let snapshot = PersistedJob {
            record: JobRecord {
                job_id: job_id.clone(),
                job_type: "dataset_index_build".to_string(),
                status: JobStatus::Running,
                attempt: 1,
                seq: 0,
                created_at_ms: at_ms,
                updated_at_ms: at_ms,
                correlation_id: "c_recover".to_string(),
                reason_code: None,
                idempotency_key: None,
                result: None,
                error: None,
            },
            inputs: serde_json::json!({"dataset_id":"crypto.binance.spot.btcusdt.candles.1m.v1"}),
            last_units_done: 0,
            commit_completed: false,
        };
        persist_snapshot(app.handle(), &snapshot).expect("persist");

        recover_on_start(app.handle(), &state)
            .await
            .expect("recover");

        let record = wait_for_terminal(&state, &job_id).await;
        assert!(matches!(record.status, JobStatus::Completed));
        assert_eq!(record.attempt, 2);
    }

    #[tokio::test]
    async fn events_since_replays_persisted_events() {
        let _permit = disk_permit().await;
        let app = tauri::test::mock_app();
        let state = JobsState::default();

        let start = JobsStartRequest {
            envelope: envelope("c6"),
            job_type: "unsupported_type".to_string(),
            inputs: serde_json::json!({}),
            idempotency_key: None,
            reuse_output: false,
        };
        let started = jobs_start_inner(app.handle().clone(), &state, start)
            .await
            .expect("start");
        let _ = wait_for_terminal(&state, &started.job_id).await;

        let resp = jobs_events_since_inner(
            app.handle(),
            &state,
            JobsEventsSinceRequest {
                envelope: envelope("c7"),
                job_id: started.job_id.clone(),
                last_seq: 0,
                max_events: Some(100),
            },
        )
        .await
        .expect("events_since");

        assert_eq!(resp.record.job_id, started.job_id);
        assert!(!resp.events.is_empty());
        assert!(resp
            .events
            .iter()
            .any(|e| e.event == "job.progress" || e.event == "job.failed" || e.event == "job.log"));
    }

    #[tokio::test]
    async fn events_since_works_without_snapshot_or_in_memory_state() {
        let _permit = disk_permit().await;
        let app = tauri::test::mock_app();
        let state = JobsState::default();

        let start = JobsStartRequest {
            envelope: envelope("c8"),
            job_type: "unsupported_type".to_string(),
            inputs: serde_json::json!({}),
            idempotency_key: None,
            reuse_output: false,
        };
        let started = jobs_start_inner(app.handle().clone(), &state, start)
            .await
            .expect("start");
        let _ = wait_for_terminal(&state, &started.job_id).await;

        let snapshot_path = job_snapshot_path(app.handle(), &started.job_id).expect("path");
        if snapshot_path.exists() {
            std::fs::remove_file(&snapshot_path).expect("remove snapshot");
        }

        let empty_state = JobsState::default();
        let resp = jobs_events_since_inner(
            app.handle(),
            &empty_state,
            JobsEventsSinceRequest {
                envelope: envelope("c9"),
                job_id: started.job_id.clone(),
                last_seq: 0,
                max_events: Some(100),
            },
        )
        .await
        .expect("events_since");

        assert_eq!(resp.record.job_id, started.job_id);
        assert!(!resp.events.is_empty());
    }
}
