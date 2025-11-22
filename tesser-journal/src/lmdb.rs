use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use heed::types::{SerdeBincode, Str};
use heed::{Database, Env, EnvOpenOptions};
use tesser_execution::{AlgoStateRepository, StoredAlgoState};
use tesser_portfolio::{LiveState, PortfolioError, PortfolioResult, StateRepository};
use uuid::Uuid;

/// Default map size allocated to the LMDB environment (10 GiB).
pub const MAP_SIZE_BYTES: usize = 10 * 1024 * 1024 * 1024;
const LIVE_STATE_DB: &str = "live_state";
const ALGO_STATES_DB: &str = "algo_states";
const LIVE_STATE_KEY: &str = "snapshot";

#[derive(Clone)]
pub struct LmdbJournal {
    path: PathBuf,
    env: Env,
    live_state: Database<Str, SerdeBincode<LiveState>>,
    algo_states: Database<SerdeBincode<Uuid>, SerdeBincode<StoredAlgoState>>,
}

impl LmdbJournal {
    /// Open (or create) an LMDB environment rooted at `path`.
    ///
    /// The path should be a directory; if it does not exist it will be created.
    /// A large map size is reserved up front to avoid growth pauses during
    /// trading, but LMDB will not claim physical disk space until pages are used.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let base_dir = normalize_path(path.as_ref());
        ensure_directory(&base_dir)?;

        let reserved_gib = reserved_gib();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(MAP_SIZE_BYTES)
                .max_dbs(8)
                .open(&base_dir)
        }
        .with_context(|| {
            format!(
                "failed to open LMDB environment at {} (map_size ~= {:.1} GiB)",
                base_dir.display(),
                reserved_gib
            )
        })?;

        let mut wtxn = env
            .write_txn()
            .context("failed to open write transaction for schema")?;
        let live_state = env
            .create_database(&mut wtxn, Some(LIVE_STATE_DB))
            .context("failed to open live_state database")?;
        let algo_states = env
            .create_database(&mut wtxn, Some(ALGO_STATES_DB))
            .context("failed to open algo_states database")?;
        wtxn.commit()
            .context("failed to commit LMDB schema initialization")?;

        Ok(Self {
            path: base_dir.clone(),
            env,
            live_state,
            algo_states,
        })
    }

    /// Build a repository for live state snapshots.
    #[must_use]
    pub fn state_repo(&self) -> LmdbStateRepository {
        LmdbStateRepository {
            env: self.env.clone(),
            live_state: self.live_state,
        }
    }

    /// Build a repository for algorithm snapshots.
    #[must_use]
    pub fn algo_repo(&self) -> LmdbAlgoStateRepository {
        LmdbAlgoStateRepository {
            env: self.env.clone(),
            algo_states: self.algo_states,
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Clone)]
pub struct LmdbStateRepository {
    env: Env,
    live_state: Database<Str, SerdeBincode<LiveState>>,
}

impl StateRepository for LmdbStateRepository {
    type Snapshot = LiveState;

    fn load(&self) -> PortfolioResult<LiveState> {
        let txn = self
            .env
            .read_txn()
            .map_err(|err| map_portfolio_err("begin read transaction", err))?;
        let state = self
            .live_state
            .get(&txn, LIVE_STATE_KEY)
            .map_err(|err| map_portfolio_err("read live state", err))?
            .unwrap_or_default();
        Ok(state)
    }

    fn save(&self, state: &LiveState) -> PortfolioResult<()> {
        let mut txn = self
            .env
            .write_txn()
            .map_err(|err| map_portfolio_err("begin write transaction", err))?;
        self.live_state
            .put(&mut txn, LIVE_STATE_KEY, state)
            .map_err(|err| map_portfolio_err("persist live state", err))?;
        txn.commit()
            .map_err(|err| map_portfolio_err("commit live state", err))
    }
}

#[derive(Clone)]
pub struct LmdbAlgoStateRepository {
    env: Env,
    algo_states: Database<SerdeBincode<Uuid>, SerdeBincode<StoredAlgoState>>,
}

impl AlgoStateRepository for LmdbAlgoStateRepository {
    type State = StoredAlgoState;

    fn save(&self, id: &Uuid, state: &StoredAlgoState) -> Result<()> {
        let mut txn = self.env.write_txn().context("failed to open write txn")?;
        self.algo_states
            .put(&mut txn, id, state)
            .context("failed to upsert algo state")?;
        txn.commit().context("failed to commit algo state")
    }

    fn load_all(&self) -> Result<HashMap<Uuid, StoredAlgoState>> {
        let txn = self.env.read_txn().context("failed to open read txn")?;
        let mut cursor = self
            .algo_states
            .iter(&txn)
            .context("failed to iterate algo state database")?;
        let mut states = HashMap::new();
        while let Some((id, state)) = cursor
            .next()
            .transpose()
            .context("failed to decode algo state entry")?
        {
            states.insert(id, state);
        }
        Ok(states)
    }

    fn delete(&self, id: &Uuid) -> Result<()> {
        let mut txn = self.env.write_txn().context("failed to open write txn")?;
        self.algo_states
            .delete(&mut txn, id)
            .context("failed to delete algo state")?;
        txn.commit().context("failed to commit delete")
    }
}

fn ensure_directory(path: &Path) -> Result<()> {
    if path.exists() && !path.is_dir() {
        bail!(
            "LMDB path {} already exists and is not a directory",
            path.display()
        );
    }
    fs::create_dir_all(path).with_context(|| {
        format!(
            "failed to create LMDB directory {}",
            path.canonicalize()
                .unwrap_or_else(|_| path.to_path_buf())
                .display()
        )
    })?;
    Ok(())
}

fn normalize_path(path: &Path) -> PathBuf {
    if path.exists() && path.is_file() {
        return path.with_extension("lmdb");
    }
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("db"))
        .unwrap_or(false)
    {
        return path.with_extension("lmdb");
    }
    path.to_path_buf()
}

fn map_portfolio_err(action: &str, err: heed::Error) -> PortfolioError {
    PortfolioError::Internal(format!("lmdb: failed to {action}: {err}"))
}

fn reserved_gib() -> f64 {
    let bytes = LittleEndian::read_u64(&(MAP_SIZE_BYTES as u64).to_le_bytes());
    bytes as f64 / (1024_f64 * 1024_f64 * 1024_f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Instant;
    use tempfile::TempDir;
    use tesser_execution::SqliteAlgoStateRepository;

    fn sample_state() -> LiveState {
        LiveState {
            portfolio: None,
            open_orders: Vec::new(),
            last_prices: HashMap::new(),
            last_candle_ts: None,
            strategy_state: None,
        }
    }

    #[test]
    fn roundtrip_live_state() {
        let dir = TempDir::new().unwrap();
        let journal = LmdbJournal::open(dir.path()).unwrap();
        let repo = journal.state_repo();
        let state = sample_state();
        repo.save(&state).unwrap();
        let loaded = repo.load().unwrap();
        assert_eq!(loaded.open_orders.len(), 0);
    }

    #[test]
    fn roundtrip_algo_states() {
        let dir = TempDir::new().unwrap();
        let journal = LmdbJournal::open(dir.path()).unwrap();
        let repo = journal.algo_repo();
        let id = Uuid::new_v4();
        let state = StoredAlgoState {
            algo_type: "TWAP".into(),
            state: json!({"progress": 0.5}),
        };
        repo.save(&id, &state).unwrap();
        let loaded = repo.load_all().unwrap();
        assert_eq!(loaded.get(&id).unwrap().algo_type, "TWAP");
    }

    #[test]
    #[ignore]
    fn benchmark_sqlite_vs_lmdb_write_speed() {
        let temp = TempDir::new().unwrap();
        let sqlite_path = temp.path().join("algo_states.db");
        let sqlite = SqliteAlgoStateRepository::new(&sqlite_path).unwrap();
        let lmdb_dir = temp.path().join("journal");
        let journal = LmdbJournal::open(&lmdb_dir).unwrap();
        let lmdb = journal.algo_repo();

        let sample = StoredAlgoState {
            algo_type: "TWAP".into(),
            state: json!({"progress": 0.5, "active_child": "A"}),
        };
        let sqlite_time = time_writes(|| {
            for i in 0..10_000 {
                let id = Uuid::from_u128(i as u128 + 1);
                sqlite.save(&id, &sample).unwrap();
            }
        });
        let lmdb_time = time_writes(|| {
            for i in 0..10_000 {
                let id = Uuid::from_u128(i as u128 + 1);
                lmdb.save(&id, &sample).unwrap();
            }
        });

        println!(
            "sqlite algo writes: {:?}, lmdb algo writes: {:?}",
            sqlite_time, lmdb_time
        );
        assert!(lmdb_time < sqlite_time);
    }

    fn time_writes<F: FnOnce()>(f: F) -> std::time::Duration {
        let start = Instant::now();
        f();
        start.elapsed()
    }
}
