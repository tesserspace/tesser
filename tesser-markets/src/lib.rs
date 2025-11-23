//! Thread-safe registry providing authoritative instrument metadata.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::Deserialize;
use tesser_broker::{BrokerError, ExecutionClient};
use tesser_core::{Instrument, Symbol};
use thiserror::Error;

/// Shared registry storing immutable instrument definitions.
#[derive(Clone, Default)]
pub struct MarketRegistry {
    inner: Arc<RwLock<HashMap<Symbol, Instrument>>>,
}

impl MarketRegistry {
    /// Construct a registry from the provided instruments.
    pub fn from_instruments(instruments: Vec<Instrument>) -> Result<Self, MarketRegistryError> {
        if instruments.is_empty() {
            return Err(MarketRegistryError::Empty);
        }
        let mut map = HashMap::new();
        for instrument in instruments {
            map.insert(instrument.symbol, instrument);
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(map)),
        })
    }

    /// Load instrument metadata from a static TOML file.
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, MarketRegistryError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|err| MarketRegistryError::Io {
            path: path.to_path_buf(),
            source: err,
        })?;
        let file: MarketFile =
            toml::from_str(&contents).map_err(MarketRegistryError::InvalidFormat)?;
        Self::from_instruments(file.into_instruments())
    }

    /// Fetch market metadata from the execution client.
    pub async fn load_from_exchange(
        client: &(dyn ExecutionClient + Send + Sync),
        category: &str,
    ) -> Result<Self, MarketRegistryError> {
        let instruments = client
            .list_instruments(category)
            .await
            .map_err(MarketRegistryError::Broker)?;
        Self::from_instruments(instruments)
    }

    /// Retrieve instrument metadata for a symbol.
    pub fn get(&self, symbol: impl Into<Symbol>) -> Option<Instrument> {
        let symbol = symbol.into();
        self.inner
            .read()
            .ok()
            .and_then(|map| map.get(&symbol).cloned())
    }

    /// Validate that each supplied symbol exists.
    pub fn validate_symbols<I, S>(&self, symbols: I) -> Result<(), MarketRegistryError>
    where
        I: IntoIterator<Item = S>,
        S: Into<Symbol>,
    {
        let map = self
            .inner
            .read()
            .map_err(|_| MarketRegistryError::Poisoned)?;
        for symbol in symbols {
            let symbol = symbol.into();
            if !map.contains_key(&symbol) {
                return Err(MarketRegistryError::UnknownSymbol(symbol));
            }
        }
        Ok(())
    }

    /// Return a snapshot of all registered instruments.
    pub fn instruments(&self) -> Vec<Instrument> {
        self.inner
            .read()
            .map(|map| map.values().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Debug, Error)]
pub enum MarketRegistryError {
    #[error("no instruments were loaded into the registry")]
    Empty,
    #[error("unknown symbol '{0}'")]
    UnknownSymbol(Symbol),
    #[error("markets file is invalid: {0}")]
    InvalidFormat(toml::de::Error),
    #[error("failed to read markets file at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("market registry poisoned by concurrent access")]
    Poisoned,
    #[error("broker error: {0}")]
    Broker(BrokerError),
}

#[derive(Deserialize)]
struct MarketFile {
    #[serde(default)]
    markets: Vec<Instrument>,
}

impl MarketFile {
    fn into_instruments(self) -> Vec<Instrument> {
        self.markets
    }
}
