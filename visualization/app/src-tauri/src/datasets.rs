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
    pub storage_status: StorageStatus,
    pub hints: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageStatus {
    pub roots_ok: bool,
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
        partitions: vec![],
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
    if !is_valid_dataset_id(dataset_id) {
        return Err(CommandError::new(
            "DATASET.ID_INVALID",
            "dataset_id must match [a-z0-9._-]+",
            "dataset".to_string(),
        ));
    }

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

    let health_path = paths
        .health_dir
        .join(format!("{manifest_hash}.health.json"));
    let health = serde_json::json!({
        "schema_version": 1,
        "dataset_id": dataset_id,
        "manifest_hash": manifest_hash,
        "level": "quick",
        "generated_at_ms": now_ms(),
        "summary": { "status": "ok", "issues_total": 0 }
    });
    let health_bytes = serde_json::to_vec_pretty(&health).map_err(|e| {
        CommandError::new(
            "DATASET.HEALTH_ENCODE_FAILED",
            format!("failed to encode health report: {e}"),
            "dataset".to_string(),
        )
    })?;
    atomic_write(&health_path, &health_bytes).map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to write health report: {e}"),
            "dataset".to_string(),
        )
    })?;

    if is_canceled(cancel, commit_state) {
        return Err(cancel_error(dataset_id));
    }

    let strict_path = paths.fingerprints_dir.join(format!("{manifest_hash}.json"));
    let strict = serde_json::json!({
        "schema_version": 1,
        "dataset_id": dataset_id,
        "manifest_hash": manifest_hash,
        "algo": "strict.sha256.empty.v1",
        "value": sha256_hex(b""),
        "generated_at_ms": now_ms()
    });
    let strict_bytes = serde_json::to_vec_pretty(&strict).map_err(|e| {
        CommandError::new(
            "DATASET.FINGERPRINT_ENCODE_FAILED",
            format!("failed to encode strict fingerprint: {e}"),
            "dataset".to_string(),
        )
    })?;
    atomic_write(&strict_path, &strict_bytes).map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to write strict fingerprint: {e}"),
            "dataset".to_string(),
        )
    })?;

    if is_canceled(cancel, commit_state) {
        return Err(cancel_error(dataset_id));
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

    Ok(DatasetPreview {
        dataset_id: dataset_id.to_string(),
        active_manifest_hash: Some(pointer.active_manifest_hash),
        fast_fingerprint: Some(FastFingerprint {
            algo: "jcs_sha256".to_string(),
            value: fast,
        }),
        storage_status: StorageStatus { roots_ok: true },
        hints: vec![],
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
            storage_status: StorageStatus { roots_ok: false },
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
                storage_status: StorageStatus { roots_ok: false },
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
                storage_status: StorageStatus { roots_ok: false },
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
                storage_status: StorageStatus { roots_ok: false },
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
                storage_status: StorageStatus { roots_ok: false },
                hints,
            });
        }
    };

    Ok(DatasetPreview {
        dataset_id: dataset_id.to_string(),
        active_manifest_hash: Some(pointer.active_manifest_hash),
        fast_fingerprint: Some(manifest.fingerprints.fast),
        storage_status: StorageStatus { roots_ok: true },
        hints,
    })
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
        assert!(health_path.exists());
        let strict_path = paths.fingerprints_dir.join(format!("{manifest_hash}.json"));
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
