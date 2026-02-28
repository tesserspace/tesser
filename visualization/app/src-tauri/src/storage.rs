use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tauri::Manager;
use tauri::{AppHandle, Runtime};

use crate::command_error::CommandError;
use crate::envelope::{validate_envelope, RequestEnvelope};

#[derive(Debug, Clone, Deserialize)]
pub struct StorageGetRootsRequest {
    pub envelope: RequestEnvelope,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageRoots {
    pub workspace_root: String,
    pub dataset_root: String,
    pub cache_root: String,
    pub workspaces_dir: String,
    pub datasets_dir: String,
    pub cache_dir: String,
}

#[derive(Debug, Clone)]
pub struct StorageLayout {
    pub workspace_root: PathBuf,
    pub dataset_root: PathBuf,
    pub cache_root: PathBuf,
}

impl StorageLayout {
    pub fn workspaces_dir(&self) -> PathBuf {
        self.workspace_root.join("workspaces")
    }

    pub fn datasets_dir(&self) -> PathBuf {
        self.dataset_root.join("datasets")
    }

    pub fn cache_dir(&self) -> PathBuf {
        self.cache_root.join("cache")
    }

    pub fn dataset_dir(&self, dataset_id: &str) -> PathBuf {
        self.datasets_dir().join(dataset_id)
    }

    pub fn ensure(&self) -> Result<(), std::io::Error> {
        std::fs::create_dir_all(self.workspaces_dir())?;
        std::fs::create_dir_all(self.datasets_dir())?;
        std::fs::create_dir_all(self.cache_dir())?;
        Ok(())
    }
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

#[allow(clippy::result_large_err)]
pub fn default_layout<R: Runtime>(app: &AppHandle<R>) -> Result<StorageLayout, CommandError> {
    let app_data_dir = app.path().app_data_dir().map_err(|_| {
        CommandError::new(
            "STORAGE.UNKNOWN_PATH",
            "failed to resolve app_data_dir",
            "storage".to_string(),
        )
    })?;
    let app_cache_dir = app.path().app_cache_dir().map_err(|_| {
        CommandError::new(
            "STORAGE.UNKNOWN_PATH",
            "failed to resolve app_cache_dir",
            "storage".to_string(),
        )
    })?;

    Ok(StorageLayout {
        workspace_root: app_data_dir.join("workspace_root"),
        dataset_root: app_data_dir.join("dataset_root"),
        cache_root: app_cache_dir.join("cache_root"),
    })
}

fn layout_to_roots(layout: &StorageLayout) -> StorageRoots {
    StorageRoots {
        workspace_root: path_to_string(&layout.workspace_root),
        dataset_root: path_to_string(&layout.dataset_root),
        cache_root: path_to_string(&layout.cache_root),
        workspaces_dir: path_to_string(&layout.workspaces_dir()),
        datasets_dir: path_to_string(&layout.datasets_dir()),
        cache_dir: path_to_string(&layout.cache_dir()),
    }
}

#[tauri::command]
#[allow(clippy::result_large_err)]
pub fn storage_get_roots(
    app: AppHandle,
    req: StorageGetRootsRequest,
) -> Result<StorageRoots, CommandError> {
    validate_envelope(&req.envelope)?;
    let layout = default_layout(&app)?;
    Ok(layout_to_roots(&layout))
}

#[tauri::command]
#[allow(clippy::result_large_err)]
pub fn storage_ensure_layout(
    app: AppHandle,
    req: StorageGetRootsRequest,
) -> Result<StorageRoots, CommandError> {
    validate_envelope(&req.envelope)?;
    let layout = default_layout(&app)?;
    layout.ensure().map_err(|e| {
        CommandError::new(
            "STORAGE.IO_ERROR",
            format!("failed to create storage directories: {e}"),
            req.envelope.correlation_id,
        )
    })?;
    Ok(layout_to_roots(&layout))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_paths_match_prd_convention() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let layout = StorageLayout {
            workspace_root: tmp.path().join("ws_root"),
            dataset_root: tmp.path().join("ds_root"),
            cache_root: tmp.path().join("cache_root"),
        };

        assert!(layout.workspaces_dir().ends_with("workspaces"));
        assert!(layout.datasets_dir().ends_with("datasets"));
        assert!(layout.cache_dir().ends_with("cache"));
        assert!(layout
            .dataset_dir("crypto.binance.spot.btcusdt.candles.1m.v1")
            .is_absolute());
    }
}
