mod arrow_ipc_chunker;
mod command_error;
mod datasets;
mod envelope;
mod jobs;
mod limits;
mod protocol;
mod series;
mod storage;
mod stream_ref;
mod transport;

use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tauri::Builder::default()
        .manage(transport::TransportState::default())
        .manage(jobs::JobsState::default())
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            let handle = app.handle().clone();
            let state = app.state::<jobs::JobsState>();
            tauri::async_runtime::block_on(async move {
                if let Err(e) = jobs::recover_on_start(&handle, state.inner()).await {
                    tracing::warn!(code = %e.code, message = %e.message, "jobs recovery failed");
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            protocol::protocol_get_info,
            series::series_query,
            datasets::datasets_create_synthetic,
            datasets::datasets_get,
            datasets::datasets_list,
            jobs::jobs_start,
            jobs::jobs_get,
            jobs::jobs_list,
            jobs::jobs_cancel,
            jobs::jobs_retry,
            jobs::jobs_events_since,
            storage::storage_get_roots,
            storage::storage_ensure_layout,
            transport::transport_start,
            transport::debug_open_demo_stream,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
