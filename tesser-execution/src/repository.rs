//! State persistence for algorithmic orders.

use anyhow::Result;
use rusqlite::{params, Connection};
use serde_json;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use uuid::Uuid;

/// Trait for persisting algorithm state.
pub trait AlgoStateRepository: Send + Sync {
    /// Save algorithm state.
    fn save(&self, id: &Uuid, state: serde_json::Value) -> Result<()>;

    /// Load all persisted algorithm states.
    fn load_all(&self) -> Result<HashMap<Uuid, serde_json::Value>>;

    /// Delete algorithm state.
    fn delete(&self, id: &Uuid) -> Result<()>;
}

/// SQLite-based implementation of algorithm state repository.
pub struct SqliteAlgoStateRepository {
    conn: Mutex<Connection>,
}

impl SqliteAlgoStateRepository {
    /// Create a new repository with the given database path.
    pub fn new(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;

        // Create the table if it doesn't exist
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS algo_states (
                id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_algo_states_updated_at ON algo_states(updated_at);
            "#,
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Create an in-memory repository for testing.
    #[cfg(test)]
    pub fn new_in_memory() -> Result<Self> {
        let conn = Connection::open(":memory:")?;
        conn.execute_batch(
            r#"
            CREATE TABLE algo_states (
                id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            "#,
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl AlgoStateRepository for SqliteAlgoStateRepository {
    fn save(&self, id: &Uuid, state: serde_json::Value) -> Result<()> {
        let payload = serde_json::to_string(&state)?;

        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO algo_states (id, payload)
            VALUES (?1, ?2)
            ON CONFLICT(id) DO UPDATE SET
                payload = excluded.payload,
                updated_at = CURRENT_TIMESTAMP
            "#,
            params![id.to_string(), payload],
        )?;

        Ok(())
    }

    fn load_all(&self) -> Result<HashMap<Uuid, serde_json::Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, payload FROM algo_states")?;

        let rows = stmt.query_map([], |row| {
            let id_str: String = row.get(0)?;
            let payload_str: String = row.get(1)?;
            Ok((id_str, payload_str))
        })?;

        let mut states = HashMap::new();

        for row in rows {
            let (id_str, payload_str) = row?;
            let id = Uuid::parse_str(&id_str)?;
            let payload = serde_json::from_str(&payload_str)?;
            states.insert(id, payload);
        }

        Ok(states)
    }

    fn delete(&self, id: &Uuid) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM algo_states WHERE id = ?1",
            params![id.to_string()],
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_save_and_load() -> Result<()> {
        let repo = SqliteAlgoStateRepository::new_in_memory()?;
        let id = Uuid::new_v4();
        let state = json!({"status": "Working", "progress": 50});

        // Save state
        repo.save(&id, state.clone())?;

        // Load all states
        let states = repo.load_all()?;
        assert_eq!(states.len(), 1);
        assert_eq!(states.get(&id), Some(&state));

        Ok(())
    }

    #[test]
    fn test_update_existing() -> Result<()> {
        let repo = SqliteAlgoStateRepository::new_in_memory()?;
        let id = Uuid::new_v4();
        let initial_state = json!({"status": "Working", "progress": 25});
        let updated_state = json!({"status": "Working", "progress": 75});

        // Save initial state
        repo.save(&id, initial_state)?;

        // Update state
        repo.save(&id, updated_state.clone())?;

        // Load and verify
        let states = repo.load_all()?;
        assert_eq!(states.len(), 1);
        assert_eq!(states.get(&id), Some(&updated_state));

        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        let repo = SqliteAlgoStateRepository::new_in_memory()?;
        let id = Uuid::new_v4();
        let state = json!({"status": "Cancelled"});

        // Save and verify
        repo.save(&id, state)?;
        assert_eq!(repo.load_all()?.len(), 1);

        // Delete and verify
        repo.delete(&id)?;
        assert_eq!(repo.load_all()?.len(), 0);

        Ok(())
    }
}
