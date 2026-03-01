#![allow(clippy::result_large_err)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tauri::AppHandle;

use crate::command_error::CommandError;
use crate::envelope::{validate_envelope, RequestEnvelope};
use crate::storage::{default_layout, StorageLayout};

const COMMIT_OPEN: u8 = 0;
const COMMIT_IN_PROGRESS: u8 = 1;
const COMMIT_DONE: u8 = 2;
const COMMIT_CANCELED: u8 = 3;

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetsCreateSyntheticRequest {
    pub envelope: RequestEnvelope,
    pub dataset_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetsGetRequest {
    pub envelope: RequestEnvelope,
    pub dataset_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatasetsListRequest {
    pub envelope: RequestEnvelope,
}

#[derive(Debug, Clone, Serialize)]
pub struct DatasetPreview {
    pub dataset_id: String,
    pub active_manifest_hash: Option<String>,
    pub fast_fingerprint: Option<FastFingerprint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Provenance>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_range: Option<TimeRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_count: Option<u64>,
    pub storage_status: StorageStatus,
    pub hints: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageStatus {
    pub roots_ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing_partitions: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_exceeded: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FastFingerprint {
    pub algo: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetPointer {
    pub schema_version: u32,
    pub active_manifest_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetManifest {
    pub schema_version: u32,
    pub dataset_id: String,
    pub provenance: Provenance,
    pub time_semantics: TimeSemantics,
    pub schema: SchemaDesc,
    pub partitions: Vec<Partition>,
    pub fingerprints: Fingerprints,
    pub build: BuildInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    pub asset_class: String,
    pub venue: String,
    pub market: String,
    pub data_kind: String,
    pub resolution: String,
    pub timezone: String,
    pub source: SourceDesc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceDesc {
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSemantics {
    pub expected_cadence_ms: u64,
    pub calendar: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDesc {
    pub schema_id: String,
    pub columns: Vec<ColumnDesc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDesc {
    pub name: String,
    pub dtype: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub partition_id: String,
    pub uri: String,
    pub time_range: TimeRange,
    pub row_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start_ms: i64,
    pub end_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fingerprints {
    pub fast: FastFingerprint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildInfo {
    pub normalize_config_hash: String,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_valid_dataset_id(dataset_id: &str) -> bool {
    !dataset_id.is_empty()
        && dataset_id.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'_' || b == b'-'
        })
}

fn validate_dataset_id(dataset_id: &str) -> Result<(), CommandError> {
    if !is_valid_dataset_id(dataset_id) {
        return Err(CommandError::new(
            "DATASET.ID_INVALID",
            "dataset_id must match [a-z0-9._-]+",
            "dataset".to_string(),
        ));
    }
    Ok(())
}

fn validate_manifest_hash(manifest_hash: &str) -> Result<(), CommandError> {
    let is_hex = manifest_hash.len() == 64 && manifest_hash.bytes().all(|b| b.is_ascii_hexdigit());
    if !is_hex {
        return Err(CommandError::new(
            "DATASET.MANIFEST_HASH_INVALID",
            "manifest_hash must be a 64-char hex sha256",
            "dataset".to_string(),
        ));
    }
    Ok(())
}

fn resolve_partition_path(
    layout: &StorageLayout,
    dataset_id: &str,
    uri: &str,
) -> Result<PathBuf, CommandError> {
    if uri.contains('\\') {
        return Err(CommandError::new(
            "DATASET.PARTITION_URI_INVALID",
            "partition uri must use '/' separators",
            "dataset".to_string(),
        ));
    }
    if !uri.starts_with("data/partitions/") {
        return Err(CommandError::new(
            "DATASET.PARTITION_URI_INVALID",
            "partition uri must be under data/partitions/",
            "dataset".to_string(),
        ));
    }

    let rel = Path::new(uri);
    if rel.is_absolute() {
        return Err(CommandError::new(
            "DATASET.PARTITION_URI_INVALID",
            "partition uri must be relative",
            "dataset".to_string(),
        ));
    }
    for c in rel.components() {
        match c {
            std::path::Component::Prefix(_) => {
                return Err(CommandError::new(
                    "DATASET.PARTITION_URI_INVALID",
                    "partition uri must not have a windows prefix",
                    "dataset".to_string(),
                ));
            }
            std::path::Component::RootDir => {
                return Err(CommandError::new(
                    "DATASET.PARTITION_URI_INVALID",
                    "partition uri must not be absolute",
                    "dataset".to_string(),
                ));
            }
            std::path::Component::ParentDir => {
                return Err(CommandError::new(
                    "DATASET.PARTITION_URI_INVALID",
                    "partition uri must not contain '..'",
                    "dataset".to_string(),
                ));
            }
            std::path::Component::CurDir | std::path::Component::Normal(_) => {}
        }
    }
    Ok(layout.dataset_dir(dataset_id).join(rel))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn jcs_sha256(value: &serde_json::Value) -> Result<String, CommandError> {
    let s = serde_jcs::to_string(value).map_err(|e| {
        CommandError::new(
            "HASH.JCS_ENCODE_FAILED",
            format!("failed to canonicalize json (JCS): {e}"),
            "hash".to_string(),
        )
    })?;
    Ok(sha256_hex(s.as_bytes()))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
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
    fsync_parent_dir(path)?;
    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn atomic_replace(tmp: &Path, dst: &Path) -> Result<(), std::io::Error> {
    std::fs::rename(tmp, dst)?;
    Ok(())
}

#[cfg(target_os = "windows")]
fn atomic_replace(tmp: &Path, dst: &Path) -> Result<(), std::io::Error> {
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

fn fsync_parent_dir(path: &Path) -> Result<(), std::io::Error> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    #[cfg(target_os = "windows")]
    {
        let _ = parent;
        return Ok(());
    }
    #[cfg(not(target_os = "windows"))]
    {
        let dir = std::fs::File::open(parent)?;
        dir.sync_all()?;
        Ok(())
    }
}

fn dataset_paths(layout: &StorageLayout, dataset_id: &str) -> DatasetPaths {
    let dataset_dir = layout.dataset_dir(dataset_id);
    DatasetPaths {
        pointer_path: dataset_dir.join("manifest.json"),
        manifests_dir: dataset_dir.join("manifests"),
        health_dir: dataset_dir.join("health"),
        fingerprints_dir: dataset_dir.join("fingerprints"),
        data_partitions_dir: dataset_dir.join("data").join("partitions"),
        raw_dir: dataset_dir.join("raw"),
        index_dir: dataset_dir.join("index"),
    }
}

struct DatasetPaths {
    pointer_path: PathBuf,
    manifests_dir: PathBuf,
    health_dir: PathBuf,
    fingerprints_dir: PathBuf,
    data_partitions_dir: PathBuf,
    raw_dir: PathBuf,
    index_dir: PathBuf,
}

fn build_synthetic_manifest(dataset_id: &str) -> DatasetManifest {
    DatasetManifest {
        schema_version: 1,
        dataset_id: dataset_id.to_string(),
        provenance: Provenance {
            asset_class: "crypto".to_string(),
            venue: "synthetic".to_string(),
            market: "spot".to_string(),
            data_kind: "series".to_string(),
            resolution: "1s".to_string(),
            timezone: "UTC".to_string(),
            source: SourceDesc {
                kind: "synthetic".to_string(),
                name: "host".to_string(),
            },
        },
        time_semantics: TimeSemantics {
            expected_cadence_ms: 1000,
            calendar: "24x7".to_string(),
        },
        schema: SchemaDesc {
            schema_id: "series.ts_ms_value.f64.v1".to_string(),
            columns: vec![
                ColumnDesc {
                    name: "ts_ms".to_string(),
                    dtype: "i64".to_string(),
                    nullable: false,
                },
                ColumnDesc {
                    name: "value".to_string(),
                    dtype: "f64".to_string(),
                    nullable: false,
                },
            ],
        },
        partitions: vec![
            Partition {
                partition_id: "p0".to_string(),
                uri: "data/partitions/p0.bin".to_string(),
                time_range: TimeRange {
                    start_ms: 0,
                    end_ms: 1000,
                },
                row_count: 1000,
            },
            Partition {
                partition_id: "p1".to_string(),
                uri: "data/partitions/p1.bin".to_string(),
                time_range: TimeRange {
                    start_ms: 1000,
                    end_ms: 2000,
                },
                row_count: 1000,
            },
        ],
        fingerprints: Fingerprints {
            fast: FastFingerprint {
                algo: "jcs_sha256".to_string(),
                value: String::new(),
            },
        },
        build: BuildInfo {
            normalize_config_hash: "synthetic.v1".to_string(),
        },
    }
}

fn summarize_partitions(manifest: &DatasetManifest) -> (Option<TimeRange>, u64) {
    let mut start: Option<i64> = None;
    let mut end: Option<i64> = None;
    let mut rows = 0u64;
    for p in &manifest.partitions {
        rows = rows.saturating_add(p.row_count);
        start = Some(match start {
            Some(s) => s.min(p.time_range.start_ms),
            None => p.time_range.start_ms,
        });
        end = Some(match end {
            Some(e) => e.max(p.time_range.end_ms),
            None => p.time_range.end_ms,
        });
    }
    let tr = match (start, end) {
        (Some(s), Some(e)) => Some(TimeRange {
            start_ms: s,
            end_ms: e,
        }),
        _ => None,
    };
    (tr, rows)
}

fn partition_file_exists(layout: &StorageLayout, dataset_id: &str, uri: &str) -> bool {
    resolve_partition_path(layout, dataset_id, uri)
        .map(|p| p.exists())
        .unwrap_or(false)
}

fn compute_fast_fingerprint(manifest: &DatasetManifest) -> Result<String, CommandError> {
    let mut v = serde_json::to_value(manifest).map_err(|e| {
        CommandError::new(
            "DATASET.MANIFEST_ENCODE_FAILED",
            format!("failed to encode manifest: {e}"),
            "dataset".to_string(),
        )
    })?;
    if let serde_json::Value::Object(ref mut obj) = v {
        obj.remove("fingerprints");
        if let Some(serde_json::Value::Object(build_obj)) = obj.get_mut("build") {
            build_obj.remove("toolchain");
        }
    }
    jcs_sha256(&v)
}

fn compute_manifest_hash(manifest: &DatasetManifest) -> Result<String, CommandError> {
    let v = serde_json::to_value(manifest).map_err(|e| {
        CommandError::new(
            "DATASET.MANIFEST_ENCODE_FAILED",
            format!("failed to encode manifest: {e}"),
            "dataset".to_string(),
        )
    })?;
    jcs_sha256(&v)
}

pub(crate) fn create_synthetic_dataset(
    layout: &StorageLayout,
    dataset_id: &str,
) -> Result<DatasetPreview, CommandError> {
    create_synthetic_dataset_inner(layout, dataset_id, None, None)
}

pub(crate) fn create_synthetic_dataset_with_cancel_and_commit_state(
    layout: &StorageLayout,
    dataset_id: &str,
    cancel: &AtomicBool,
    commit_state: &AtomicU8,
) -> Result<DatasetPreview, CommandError> {
    create_synthetic_dataset_inner(layout, dataset_id, Some(cancel), Some(commit_state))
}

fn is_canceled(cancel: Option<&AtomicBool>, commit_state: Option<&AtomicU8>) -> bool {
    cancel.is_some_and(|c| c.load(Ordering::SeqCst))
        || commit_state.is_some_and(|s| s.load(Ordering::SeqCst) == COMMIT_CANCELED)
}

fn cancel_error(dataset_id: &str) -> CommandError {
    CommandError::new(
        "DATASET.CANCELED",
        format!("cancel requested while building dataset: {dataset_id}"),
        "dataset".to_string(),
    )
}

fn create_synthetic_dataset_inner(
    layout: &StorageLayout,
    dataset_id: &str,
    cancel: Option<&AtomicBool>,
    commit_state: Option<&AtomicU8>,
) -> Result<DatasetPreview, CommandError> {
    validate_dataset_id(dataset_id)?;

    layout.ensure().map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to ensure layout: {e}"),
            "dataset".to_string(),
        )
    })?;

    let paths = dataset_paths(layout, dataset_id);
    std::fs::create_dir_all(&paths.manifests_dir).map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to create dataset manifests dir: {e}"),
            "dataset".to_string(),
        )
    })?;
    std::fs::create_dir_all(&paths.health_dir).map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to create dataset health dir: {e}"),
            "dataset".to_string(),
        )
    })?;
    std::fs::create_dir_all(&paths.fingerprints_dir).map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to create dataset fingerprints dir: {e}"),
            "dataset".to_string(),
        )
    })?;
    std::fs::create_dir_all(&paths.data_partitions_dir).map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to create dataset partitions dir: {e}"),
            "dataset".to_string(),
        )
    })?;
    std::fs::create_dir_all(&paths.raw_dir).map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to create dataset raw dir: {e}"),
            "dataset".to_string(),
        )
    })?;
    std::fs::create_dir_all(&paths.index_dir).map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to create dataset index dir: {e}"),
            "dataset".to_string(),
        )
    })?;

    let mut manifest = build_synthetic_manifest(dataset_id);
    let fast = compute_fast_fingerprint(&manifest)?;
    manifest.fingerprints.fast.value = fast.clone();
    let manifest_hash = compute_manifest_hash(&manifest)?;

    let manifest_path = paths.manifests_dir.join(format!("{manifest_hash}.json"));
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|e| {
        CommandError::new(
            "DATASET.MANIFEST_ENCODE_FAILED",
            format!("failed to encode manifest: {e}"),
            "dataset".to_string(),
        )
    })?;
    if manifest_path.exists() {
        let existing = std::fs::read(&manifest_path).map_err(|e| {
            CommandError::new(
                "STORAGE.IO_ERROR",
                format!("failed to read existing manifest: {e}"),
                "dataset".to_string(),
            )
        })?;
        if existing != manifest_bytes {
            return Err(CommandError::new(
                "DATASET.MANIFEST_IMMUTABLE_VIOLATION",
                "manifest hash already exists but content differs",
                "dataset".to_string(),
            ));
        }
    } else {
        atomic_write(&manifest_path, &manifest_bytes).map_err(|e| {
            CommandError::new(
                "STORAGE.IO_ERROR",
                format!("failed to write manifest: {e}"),
                "dataset".to_string(),
            )
        })?;
    }

    if is_canceled(cancel, commit_state) {
        return Err(cancel_error(dataset_id));
    }

    if is_canceled(cancel, commit_state) {
        return Err(cancel_error(dataset_id));
    }

    for partition in &manifest.partitions {
        let path = resolve_partition_path(layout, dataset_id, &partition.uri)?;
        if path.exists() {
            continue;
        }
        let bytes = format!(
            "synthetic dataset_id={dataset_id} partition_id={}\n",
            partition.partition_id
        )
        .into_bytes();
        atomic_write(&path, &bytes).map_err(|e| {
            CommandError::new(
                "STORAGE.IO_ERROR",
                format!("failed to write partition bytes: {e}"),
                "dataset".to_string(),
            )
        })?;
        if is_canceled(cancel, commit_state) {
            return Err(cancel_error(dataset_id));
        }
    }

    if let Some(state) = commit_state {
        if state
            .compare_exchange(
                COMMIT_OPEN,
                COMMIT_IN_PROGRESS,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            if state.load(Ordering::SeqCst) == COMMIT_CANCELED {
                return Err(cancel_error(dataset_id));
            }
            return Err(CommandError::new(
                "DATASET.COMMIT_ALREADY_STARTED",
                "commit already started for this dataset build",
                "dataset".to_string(),
            ));
        }
    }

    let pointer = DatasetPointer {
        schema_version: 1,
        active_manifest_hash: manifest_hash.clone(),
    };
    let pointer_bytes = serde_json::to_vec_pretty(&pointer).map_err(|e| {
        CommandError::new(
            "DATASET.POINTER_ENCODE_FAILED",
            format!("failed to encode dataset pointer: {e}"),
            "dataset".to_string(),
        )
    })?;
    atomic_write(&paths.pointer_path, &pointer_bytes).map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to write dataset pointer: {e}"),
            "dataset".to_string(),
        )
    })?;
    if let Some(state) = commit_state {
        state.store(COMMIT_DONE, Ordering::SeqCst);
    }

    let (time_range, row_count_total) = summarize_partitions(&manifest);
    Ok(DatasetPreview {
        dataset_id: dataset_id.to_string(),
        active_manifest_hash: Some(pointer.active_manifest_hash),
        fast_fingerprint: Some(FastFingerprint {
            algo: "jcs_sha256".to_string(),
            value: fast,
        }),
        provenance: Some(manifest.provenance.clone()),
        schema_id: Some(manifest.schema.schema_id.clone()),
        time_range,
        row_count_total: Some(row_count_total),
        partition_count: Some(manifest.partitions.len() as u64),
        storage_status: StorageStatus {
            roots_ok: true,
            missing_partitions: None,
            budget_exceeded: None,
        },
        hints: vec![
            "DATASET.HEALTH_MISSING".to_string(),
            "DATASET.FINGERPRINT_STRICT_MISSING".to_string(),
        ],
    })
}

fn load_preview(layout: &StorageLayout, dataset_id: &str) -> Result<DatasetPreview, CommandError> {
    let paths = dataset_paths(layout, dataset_id);
    let mut hints = Vec::new();
    if !paths.pointer_path.exists() {
        hints.push("DATASET.POINTER_MISSING".to_string());
        return Ok(DatasetPreview {
            dataset_id: dataset_id.to_string(),
            active_manifest_hash: None,
            fast_fingerprint: None,
            provenance: None,
            schema_id: None,
            time_range: None,
            row_count_total: None,
            partition_count: None,
            storage_status: StorageStatus {
                roots_ok: false,
                missing_partitions: None,
                budget_exceeded: None,
            },
            hints,
        });
    }

    let pointer_bytes = match std::fs::read(&paths.pointer_path) {
        Ok(b) => b,
        Err(_) => {
            hints.push("DATASET.POINTER_UNREADABLE".to_string());
            return Ok(DatasetPreview {
                dataset_id: dataset_id.to_string(),
                active_manifest_hash: None,
                fast_fingerprint: None,
                provenance: None,
                schema_id: None,
                time_range: None,
                row_count_total: None,
                partition_count: None,
                storage_status: StorageStatus {
                    roots_ok: false,
                    missing_partitions: None,
                    budget_exceeded: None,
                },
                hints,
            });
        }
    };
    let pointer: DatasetPointer = match serde_json::from_slice(&pointer_bytes) {
        Ok(p) => p,
        Err(_) => {
            hints.push("DATASET.POINTER_CORRUPT".to_string());
            return Ok(DatasetPreview {
                dataset_id: dataset_id.to_string(),
                active_manifest_hash: None,
                fast_fingerprint: None,
                provenance: None,
                schema_id: None,
                time_range: None,
                row_count_total: None,
                partition_count: None,
                storage_status: StorageStatus {
                    roots_ok: false,
                    missing_partitions: None,
                    budget_exceeded: None,
                },
                hints,
            });
        }
    };

    let manifest_path = paths
        .manifests_dir
        .join(format!("{}.json", pointer.active_manifest_hash));
    let manifest_bytes = match std::fs::read(&manifest_path) {
        Ok(b) => b,
        Err(_) => {
            hints.push("DATASET.MANIFEST_MISSING".to_string());
            return Ok(DatasetPreview {
                dataset_id: dataset_id.to_string(),
                active_manifest_hash: None,
                fast_fingerprint: None,
                provenance: None,
                schema_id: None,
                time_range: None,
                row_count_total: None,
                partition_count: None,
                storage_status: StorageStatus {
                    roots_ok: false,
                    missing_partitions: None,
                    budget_exceeded: None,
                },
                hints,
            });
        }
    };
    let manifest: DatasetManifest = match serde_json::from_slice(&manifest_bytes) {
        Ok(m) => m,
        Err(_) => {
            hints.push("DATASET.MANIFEST_CORRUPT".to_string());
            return Ok(DatasetPreview {
                dataset_id: dataset_id.to_string(),
                active_manifest_hash: None,
                fast_fingerprint: None,
                provenance: None,
                schema_id: None,
                time_range: None,
                row_count_total: None,
                partition_count: None,
                storage_status: StorageStatus {
                    roots_ok: false,
                    missing_partitions: None,
                    budget_exceeded: None,
                },
                hints,
            });
        }
    };

    let (time_range, row_count_total) = summarize_partitions(&manifest);
    let mut missing = 0u64;
    let mut budget_exceeded = false;
    const MAX_PARTITIONS_TO_STAT: usize = 256;
    for (idx, p) in manifest.partitions.iter().enumerate() {
        if idx >= MAX_PARTITIONS_TO_STAT {
            budget_exceeded = true;
            break;
        }
        if !partition_file_exists(layout, dataset_id, &p.uri) {
            missing = missing.saturating_add(1);
        }
    }
    if missing > 0 {
        hints.push("DATASET.PARTITIONS_MISSING".to_string());
    }
    if budget_exceeded {
        hints.push("DATASET.PREVIEW_BUDGET_EXCEEDED".to_string());
    }

    let health_path = paths
        .health_dir
        .join(format!("{}.health.json", pointer.active_manifest_hash));
    if !health_path.exists() {
        hints.push("DATASET.HEALTH_MISSING".to_string());
    }
    let strict_path = paths
        .fingerprints_dir
        .join(format!("{}.json", pointer.active_manifest_hash));
    if !strict_path.exists() {
        hints.push("DATASET.FINGERPRINT_STRICT_MISSING".to_string());
    }

    Ok(DatasetPreview {
        dataset_id: dataset_id.to_string(),
        active_manifest_hash: Some(pointer.active_manifest_hash),
        fast_fingerprint: Some(manifest.fingerprints.fast),
        provenance: Some(manifest.provenance),
        schema_id: Some(manifest.schema.schema_id),
        time_range,
        row_count_total: Some(row_count_total),
        partition_count: Some(manifest.partitions.len() as u64),
        storage_status: StorageStatus {
            roots_ok: true,
            missing_partitions: Some(missing),
            budget_exceeded: Some(budget_exceeded),
        },
        hints,
    })
}

fn load_pointer_strict(
    layout: &StorageLayout,
    dataset_id: &str,
) -> Result<DatasetPointer, CommandError> {
    validate_dataset_id(dataset_id)?;
    let paths = dataset_paths(layout, dataset_id);
    let bytes = std::fs::read(&paths.pointer_path).map_err(|e| {
        CommandError::new(
            "DATASET.POINTER_READ_FAILED",
            format!("failed to read dataset pointer: {e}"),
            "dataset".to_string(),
        )
    })?;
    serde_json::from_slice(&bytes).map_err(|e| {
        CommandError::new(
            "DATASET.POINTER_DECODE_FAILED",
            format!("failed to decode dataset pointer: {e}"),
            "dataset".to_string(),
        )
    })
}

fn load_manifest_strict(
    layout: &StorageLayout,
    dataset_id: &str,
    manifest_hash: &str,
) -> Result<DatasetManifest, CommandError> {
    validate_dataset_id(dataset_id)?;
    validate_manifest_hash(manifest_hash)?;
    let paths = dataset_paths(layout, dataset_id);
    let manifest_path = paths.manifests_dir.join(format!("{manifest_hash}.json"));
    let bytes = std::fs::read(&manifest_path).map_err(|e| {
        CommandError::new(
            "DATASET.MANIFEST_READ_FAILED",
            format!("failed to read dataset manifest: {e}"),
            "dataset".to_string(),
        )
    })?;
    serde_json::from_slice(&bytes).map_err(|e| {
        CommandError::new(
            "DATASET.MANIFEST_DECODE_FAILED",
            format!("failed to decode dataset manifest: {e}"),
            "dataset".to_string(),
        )
    })
}

fn atomic_write_immutable_sidecar(
    path: &Path,
    bytes: &[u8],
    violation_code: &str,
) -> Result<(), CommandError> {
    use std::io::Write;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            CommandError::new(
                "STORAGE.IO_ERROR",
                format!("failed to create sidecar parent dir: {e}"),
                "dataset".to_string(),
            )
        })?;
    }

    let tmp = path.with_extension(format!("tmp.{}", uuid::Uuid::new_v4()));
    {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)
            .map_err(|e| {
                CommandError::new(
                    "STORAGE.IO_ERROR",
                    format!("failed to create sidecar tmp: {e}"),
                    "dataset".to_string(),
                )
            })?;
        file.write_all(bytes).map_err(|e| {
            CommandError::new(
                "STORAGE.IO_ERROR",
                format!("failed to write sidecar tmp: {e}"),
                "dataset".to_string(),
            )
        })?;
        file.sync_all().map_err(|e| {
            CommandError::new(
                "STORAGE.IO_ERROR",
                format!("failed to fsync sidecar tmp: {e}"),
                "dataset".to_string(),
            )
        })?;
    }

    match std::fs::hard_link(&tmp, path) {
        Ok(()) => {
            let _ = std::fs::remove_file(&tmp);
            fsync_parent_dir(path).map_err(|e| {
                CommandError::new(
                    "STORAGE.IO_ERROR",
                    format!("failed to fsync sidecar parent dir: {e}"),
                    "dataset".to_string(),
                )
            })?;
            return Ok(());
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = std::fs::remove_file(&tmp);
            let existing = std::fs::read(path).map_err(|e| {
                CommandError::new(
                    "STORAGE.IO_ERROR",
                    format!("failed to read existing sidecar: {e}"),
                    "dataset".to_string(),
                )
            })?;
            if existing == bytes {
                return Ok(());
            }
            return Err(CommandError::new(
                violation_code,
                "sidecar already exists but content differs",
                "dataset".to_string(),
            ));
        }
        Err(_) => {}
    }

    let _ = std::fs::remove_file(&tmp);
    match std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
    {
        Ok(mut file) => {
            file.write_all(bytes).map_err(|e| {
                CommandError::new(
                    "STORAGE.IO_ERROR",
                    format!("failed to write sidecar: {e}"),
                    "dataset".to_string(),
                )
            })?;
            file.sync_all().map_err(|e| {
                CommandError::new(
                    "STORAGE.IO_ERROR",
                    format!("failed to fsync sidecar: {e}"),
                    "dataset".to_string(),
                )
            })?;
            fsync_parent_dir(path).map_err(|e| {
                CommandError::new(
                    "STORAGE.IO_ERROR",
                    format!("failed to fsync sidecar parent dir: {e}"),
                    "dataset".to_string(),
                )
            })?;
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = std::fs::read(path).map_err(|e| {
                CommandError::new(
                    "STORAGE.IO_ERROR",
                    format!("failed to read existing sidecar: {e}"),
                    "dataset".to_string(),
                )
            })?;
            if existing == bytes {
                Ok(())
            } else {
                Err(CommandError::new(
                    violation_code,
                    "sidecar already exists but content differs",
                    "dataset".to_string(),
                ))
            }
        }
        Err(e) => Err(CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to create sidecar: {e}"),
            "dataset".to_string(),
        )),
    }
}

pub(crate) fn datasets_health_quick(
    layout: &StorageLayout,
    dataset_id: &str,
    manifest_hash: Option<&str>,
    cancel: &AtomicBool,
) -> Result<serde_json::Value, CommandError> {
    if cancel.load(Ordering::SeqCst) {
        return Err(cancel_error(dataset_id));
    }

    let active = load_pointer_strict(layout, dataset_id)?;
    let hash = manifest_hash.unwrap_or(&active.active_manifest_hash);
    let manifest = load_manifest_strict(layout, dataset_id, hash)?;
    if cancel.load(Ordering::SeqCst) {
        return Err(cancel_error(dataset_id));
    }

    let mut partitions = manifest.partitions.clone();
    partitions.sort_by_key(|p| p.time_range.start_ms);

    let mut overlaps = 0u64;
    let mut gaps = 0u64;
    let mut missing_files = 0u64;
    let mut issues: Vec<serde_json::Value> = Vec::new();

    let mut prev_end: Option<i64> = None;
    for p in &partitions {
        if cancel.load(Ordering::SeqCst) {
            return Err(cancel_error(dataset_id));
        }
        if !partition_file_exists(layout, dataset_id, &p.uri) {
            missing_files = missing_files.saturating_add(1);
            if issues.len() < 32 {
                issues.push(serde_json::json!({
                    "code": "DATASET.PARTITION_MISSING",
                    "severity": "error",
                    "message": format!("partition file missing: {}", p.uri),
                    "evidence": { "partition_id": p.partition_id, "uri": p.uri }
                }));
            }
        }
        if let Some(end_ms) = prev_end {
            if p.time_range.start_ms < end_ms {
                overlaps = overlaps.saturating_add(1);
            } else if p.time_range.start_ms > end_ms {
                gaps = gaps.saturating_add(1);
            }
        }
        prev_end = Some(p.time_range.end_ms);
    }

    let (time_range, _rows) = summarize_partitions(&manifest);
    let empty_partitions = manifest.partitions.is_empty();
    if empty_partitions && issues.len() < 32 {
        issues.push(serde_json::json!({
            "code": "DATASET.EMPTY",
            "severity": "warn",
            "message": "dataset has no partitions",
            "evidence": {}
        }));
    }
    let issues_total = overlaps
        .saturating_add(gaps)
        .saturating_add(missing_files)
        .saturating_add(if empty_partitions { 1 } else { 0 });
    let status = if missing_files > 0 {
        "error"
    } else if overlaps > 0 || gaps > 0 || empty_partitions {
        "warn"
    } else {
        "ok"
    };

    let report = serde_json::json!({
        "schema_version": 1,
        "dataset_id": dataset_id,
        "manifest_hash": hash,
        "level": "quick",
        "generated_at_ms": now_ms(),
        "summary": {
            "status": status,
            "issues_total": issues_total,
            "partitions_total": manifest.partitions.len(),
            "missing_files": missing_files,
            "gaps": gaps,
            "overlaps": overlaps,
            "time_range": time_range,
        },
        "issues": issues,
    });
    let bytes = serde_json::to_vec_pretty(&report).map_err(|e| {
        CommandError::new(
            "DATASET.HEALTH_ENCODE_FAILED",
            format!("failed to encode health report: {e}"),
            "dataset".to_string(),
        )
    })?;

    let paths = dataset_paths(layout, dataset_id);
    let path = paths.health_dir.join(format!("{hash}.health.json"));
    atomic_write_immutable_sidecar(&path, &bytes, "DATASET.HEALTH_IMMUTABLE_VIOLATION")?;
    Ok(report)
}

fn sha256_file(
    path: &Path,
    cancel: &AtomicBool,
    dataset_id: &str,
) -> Result<(String, u64), CommandError> {
    use std::io::Read;

    let mut file = std::fs::File::open(path).map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to open partition file: {e}"),
            "dataset".to_string(),
        )
    })?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut size = 0u64;
    loop {
        if cancel.load(Ordering::SeqCst) {
            return Err(cancel_error(dataset_id));
        }
        let n = file.read(&mut buf).map_err(|e| {
            CommandError::new(
                "STORAGE.IO_ERROR",
                format!("failed to read partition file: {e}"),
                "dataset".to_string(),
            )
        })?;
        if n == 0 {
            break;
        }
        size = size.saturating_add(n as u64);
        hasher.update(&buf[..n]);
    }
    Ok((hex::encode(hasher.finalize()), size))
}

pub(crate) fn datasets_fingerprint_strict(
    layout: &StorageLayout,
    dataset_id: &str,
    manifest_hash: Option<&str>,
    cancel: &AtomicBool,
    progress: &mut dyn FnMut(u64, u64),
) -> Result<serde_json::Value, CommandError> {
    if cancel.load(Ordering::SeqCst) {
        return Err(cancel_error(dataset_id));
    }

    let active = load_pointer_strict(layout, dataset_id)?;
    let hash = manifest_hash.unwrap_or(&active.active_manifest_hash);
    let manifest = load_manifest_strict(layout, dataset_id, hash)?;

    let mut partitions = manifest.partitions.clone();
    partitions.sort_by(|a, b| a.partition_id.cmp(&b.partition_id));
    let total = partitions.len() as u64;

    let mut content_list: Vec<serde_json::Value> = Vec::with_capacity(partitions.len());
    let mut seen_partition_ids = std::collections::HashSet::<String>::new();
    let mut agg = Sha256::new();

    for (idx, p) in partitions.iter().enumerate() {
        if !seen_partition_ids.insert(p.partition_id.clone()) {
            return Err(CommandError::new(
                "DATASET.PARTITION_ID_DUPLICATE",
                format!("duplicate partition_id: {}", p.partition_id),
                "dataset".to_string(),
            ));
        }

        let path = resolve_partition_path(layout, dataset_id, &p.uri)?;
        if !path.exists() {
            return Err(CommandError::new(
                "DATASET.PARTITION_MISSING",
                format!("partition file missing: {}", p.uri),
                "dataset".to_string(),
            ));
        }
        let (file_hash, size_bytes) = sha256_file(&path, cancel, dataset_id)?;

        agg.update(p.partition_id.as_bytes());
        agg.update([0u8]);
        agg.update(p.uri.as_bytes());
        agg.update([0u8]);
        agg.update(p.time_range.start_ms.to_le_bytes());
        agg.update(p.time_range.end_ms.to_le_bytes());
        agg.update(p.row_count.to_le_bytes());
        agg.update(file_hash.as_bytes());
        agg.update([0u8]);

        content_list.push(serde_json::json!({
            "partition_id": p.partition_id,
            "uri": p.uri,
            "time_range": p.time_range,
            "row_count": p.row_count,
            "size_bytes": size_bytes,
            "sha256": file_hash,
        }));

        let done = (idx as u64).saturating_add(1);
        progress(done, total);
    }

    let sidecar = serde_json::json!({
        "schema_version": 1,
        "dataset_id": dataset_id,
        "manifest_hash": hash,
        "algo": "strict.sha256.partitions.v1",
        "value": hex::encode(agg.finalize()),
        "generated_at_ms": now_ms(),
        "partitions": content_list,
    });
    let bytes = serde_json::to_vec_pretty(&sidecar).map_err(|e| {
        CommandError::new(
            "DATASET.FINGERPRINT_ENCODE_FAILED",
            format!("failed to encode strict fingerprint: {e}"),
            "dataset".to_string(),
        )
    })?;

    let paths = dataset_paths(layout, dataset_id);
    let path = paths.fingerprints_dir.join(format!("{hash}.json"));
    atomic_write_immutable_sidecar(
        &path,
        &bytes,
        "DATASET.FINGERPRINT_STRICT_IMMUTABLE_VIOLATION",
    )?;
    Ok(sidecar)
}

#[tauri::command]
pub fn datasets_create_synthetic(
    app: AppHandle,
    req: DatasetsCreateSyntheticRequest,
) -> Result<DatasetPreview, CommandError> {
    validate_envelope(&req.envelope)?;
    let layout = default_layout(&app)?;
    create_synthetic_dataset(&layout, &req.dataset_id).map_err(|mut e| {
        e.correlation_id = req.envelope.correlation_id;
        e
    })
}

#[tauri::command]
pub fn datasets_get(
    app: AppHandle,
    req: DatasetsGetRequest,
) -> Result<DatasetPreview, CommandError> {
    validate_envelope(&req.envelope)?;
    if !is_valid_dataset_id(&req.dataset_id) {
        return Err(CommandError::new(
            "DATASET.ID_INVALID",
            "dataset_id must match [a-z0-9._-]+",
            req.envelope.correlation_id,
        ));
    }
    let layout = default_layout(&app)?;
    load_preview(&layout, &req.dataset_id).map_err(|mut e| {
        e.correlation_id = req.envelope.correlation_id;
        e
    })
}

fn list_published_datasets(
    layout: &StorageLayout,
    correlation_id: &str,
) -> Result<Vec<DatasetPreview>, CommandError> {
    layout.ensure().map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to ensure layout: {e}"),
            correlation_id.to_string(),
        )
    })?;
    let datasets_dir = layout.datasets_dir();
    let entries = std::fs::read_dir(&datasets_dir).map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to list datasets: {e}"),
            correlation_id.to_string(),
        )
    })?;
    let mut dataset_ids: Vec<String> = Vec::new();
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        let dataset_id = entry.file_name().to_string_lossy().to_string();
        if !is_valid_dataset_id(&dataset_id) {
            continue;
        }
        let paths = dataset_paths(layout, &dataset_id);
        if !paths.pointer_path.exists() {
            continue;
        }
        dataset_ids.push(dataset_id);
    }
    dataset_ids.sort();

    let mut out = Vec::with_capacity(dataset_ids.len());
    for dataset_id in dataset_ids {
        if let Ok(preview) = load_preview(layout, &dataset_id) {
            out.push(preview);
        }
    }
    Ok(out)
}

#[tauri::command]
pub fn datasets_list(
    app: AppHandle,
    req: DatasetsListRequest,
) -> Result<Vec<DatasetPreview>, CommandError> {
    validate_envelope(&req.envelope)?;
    let layout = default_layout(&app)?;
    list_published_datasets(&layout, &req.envelope.correlation_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_dataset_creates_pointer_and_immutable_manifest() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let layout = StorageLayout {
            workspace_root: tmp.path().join("ws"),
            dataset_root: tmp.path().join("ds"),
            cache_root: tmp.path().join("cache"),
        };

        let dataset_id = "crypto.synthetic.spot.demo.series.1s.v1";
        let preview = create_synthetic_dataset(&layout, dataset_id).expect("create dataset");
        assert_eq!(preview.dataset_id, dataset_id);
        assert_eq!(
            preview.fast_fingerprint.as_ref().unwrap().algo,
            "jcs_sha256"
        );
        assert!(!preview.fast_fingerprint.as_ref().unwrap().value.is_empty());

        let paths = dataset_paths(&layout, dataset_id);
        assert!(paths.pointer_path.exists());
        let manifest_hash = preview.active_manifest_hash.as_ref().unwrap();
        let manifest_path = paths.manifests_dir.join(format!("{manifest_hash}.json"));
        assert!(manifest_path.exists());
        let health_path = paths
            .health_dir
            .join(format!("{manifest_hash}.health.json"));
        assert!(!health_path.exists());
        let strict_path = paths.fingerprints_dir.join(format!("{manifest_hash}.json"));
        assert!(!strict_path.exists());

        let cancel = AtomicBool::new(false);
        let _ = datasets_health_quick(&layout, dataset_id, Some(manifest_hash), &cancel)
            .expect("health quick");
        assert!(health_path.exists());

        let mut progress = |_done: u64, _total: u64| {};
        let _ = datasets_fingerprint_strict(
            &layout,
            dataset_id,
            Some(manifest_hash),
            &cancel,
            &mut progress,
        )
        .expect("fingerprint strict");
        assert!(strict_path.exists());
    }

    #[test]
    fn datasets_list_hides_unpublished_dirs_and_sorts() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let layout = StorageLayout {
            workspace_root: tmp.path().join("ws"),
            dataset_root: tmp.path().join("ds"),
            cache_root: tmp.path().join("cache"),
        };
        layout.ensure().expect("ensure");

        std::fs::create_dir_all(layout.dataset_dir("crypto.unpublished.test.v1"))
            .expect("create unpublished dir");

        create_synthetic_dataset(&layout, "crypto.b.published.test.v1").expect("create b");
        create_synthetic_dataset(&layout, "crypto.a.published.test.v1").expect("create a");

        let out = list_published_datasets(&layout, "c_list").expect("list");
        let ids: Vec<String> = out.into_iter().map(|p| p.dataset_id).collect();

        assert_eq!(
            ids,
            vec!["crypto.a.published.test.v1", "crypto.b.published.test.v1"]
        );
    }
}
