//! Thread-safe registry providing authoritative instrument metadata.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use rust_decimal::{Decimal, MathematicalOps};
use serde::Deserialize;
use tesser_broker::{BrokerError, ExecutionClient};
use tesser_core::{AssetId, ExchangeId, Instrument, InstrumentKind, Price, Quantity, Symbol};
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
        let mut catalog = InstrumentCatalog::new();
        catalog.add_file(path)?;
        catalog.build()
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

    /// Round a quantity down to the coarsest lot size shared by two venues.
    pub fn normalize_pair_quantity(
        &self,
        first: Symbol,
        second: Symbol,
        quantity: Quantity,
    ) -> Option<Quantity> {
        let first_instr = self.get(first)?;
        let second_instr = self.get(second)?;
        let step = first_instr.lot_size.max(second_instr.lot_size);
        if step <= Decimal::ZERO {
            return Some(quantity);
        }
        let normalized = (quantity / step).floor() * step;
        Some(normalized)
    }
}

#[derive(Debug, Error)]
pub enum MarketRegistryError {
    #[error("no instruments were loaded into the registry")]
    Empty,
    #[error("unknown symbol '{0}'")]
    UnknownSymbol(Symbol),
    #[error("instrument '{symbol}' on {exchange} missing required field '{field}'")]
    MissingField {
        exchange: ExchangeId,
        symbol: String,
        field: &'static str,
    },
    #[error("instrument '{symbol}' on {exchange} has conflicting value for '{field}'")]
    ConflictingField {
        exchange: ExchangeId,
        symbol: String,
        field: &'static str,
    },
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

/// Accumulates instrument metadata from multiple sources before producing a registry.
#[derive(Default)]
pub struct InstrumentCatalog {
    entries: HashMap<(ExchangeId, String), InstrumentInfo>,
}

impl InstrumentCatalog {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn add_file(&mut self, path: impl AsRef<Path>) -> Result<(), MarketRegistryError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|err| MarketRegistryError::Io {
            path: path.to_path_buf(),
            source: err,
        })?;
        let file: MarketFile =
            toml::from_str(&contents).map_err(MarketRegistryError::InvalidFormat)?;
        for info in file.markets {
            self.insert(info)?;
        }
        Ok(())
    }

    pub fn add_instruments(
        &mut self,
        instruments: Vec<Instrument>,
    ) -> Result<(), MarketRegistryError> {
        for instrument in instruments {
            self.insert(InstrumentInfo::from(instrument))?;
        }
        Ok(())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn build(self) -> Result<MarketRegistry, MarketRegistryError> {
        let instruments = self
            .entries
            .into_values()
            .map(|info| info.into_instrument())
            .collect::<Result<Vec<_>, _>>()?;
        MarketRegistry::from_instruments(instruments)
    }

    fn insert(&mut self, info: InstrumentInfo) -> Result<(), MarketRegistryError> {
        let key = info.key();
        if let Some(existing) = self.entries.remove(&key) {
            let merged = existing.merge(info)?;
            self.entries.insert(key, merged);
        } else {
            self.entries.insert(key, info);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct MarketFile {
    #[serde(default)]
    markets: Vec<InstrumentInfo>,
}

#[derive(Clone, Debug, Deserialize)]
struct InstrumentInfo {
    exchange: ExchangeId,
    symbol: String,
    #[serde(default)]
    base: Option<String>,
    #[serde(default)]
    quote: Option<String>,
    #[serde(default)]
    settlement_currency: Option<String>,
    #[serde(default)]
    kind: Option<InstrumentKind>,
    #[serde(default)]
    tick_size: Option<Price>,
    #[serde(default)]
    lot_size: Option<Quantity>,
}

impl InstrumentInfo {
    fn key(&self) -> (ExchangeId, String) {
        (self.exchange, self.symbol.trim().to_ascii_uppercase())
    }

    fn merge(self, other: InstrumentInfo) -> Result<InstrumentInfo, MarketRegistryError> {
        let (exchange, symbol) = self.key();
        let (other_exchange, other_symbol) = other.key();
        if exchange != other_exchange || symbol != other_symbol {
            return Err(MarketRegistryError::ConflictingField {
                exchange,
                symbol,
                field: "symbol",
            });
        }
        Ok(InstrumentInfo {
            exchange,
            symbol: symbol.clone(),
            base: merge_field(exchange, &symbol, "base", self.base, other.base)?,
            quote: merge_field(exchange, &symbol, "quote", self.quote, other.quote)?,
            settlement_currency: merge_field(
                exchange,
                &symbol,
                "settlement_currency",
                self.settlement_currency,
                other.settlement_currency,
            )?,
            kind: merge_field(exchange, &symbol, "kind", self.kind, other.kind)?,
            tick_size: merge_field(
                exchange,
                &symbol,
                "tick_size",
                self.tick_size,
                other.tick_size,
            )?,
            lot_size: merge_field(exchange, &symbol, "lot_size", self.lot_size, other.lot_size)?,
        })
    }

    fn into_instrument(self) -> Result<Instrument, MarketRegistryError> {
        let (exchange, symbol_code) = self.key();
        let base = require_field(exchange, &symbol_code, "base", self.base)?;
        let quote = require_field(exchange, &symbol_code, "quote", self.quote)?;
        let settlement = require_field(
            exchange,
            &symbol_code,
            "settlement_currency",
            self.settlement_currency,
        )?;
        let kind = require_field(exchange, &symbol_code, "kind", self.kind)?;
        let tick_size = require_field(exchange, &symbol_code, "tick_size", self.tick_size)?;
        let lot_size = require_field(exchange, &symbol_code, "lot_size", self.lot_size)?;

        Ok(Instrument {
            symbol: Symbol::from_code(exchange, symbol_code.clone()),
            base: AssetId::from_code(exchange, base),
            quote: AssetId::from_code(exchange, quote),
            kind,
            settlement_currency: AssetId::from_code(exchange, settlement),
            tick_size,
            lot_size,
        })
    }
}

fn merge_field<T: Clone + PartialEq>(
    exchange: ExchangeId,
    symbol: &str,
    field: &'static str,
    primary: Option<T>,
    secondary: Option<T>,
) -> Result<Option<T>, MarketRegistryError> {
    match (primary, secondary) {
        (Some(existing), Some(replacement)) => {
            if existing != replacement {
                Err(MarketRegistryError::ConflictingField {
                    exchange,
                    symbol: symbol.to_string(),
                    field,
                })
            } else {
                Ok(Some(existing))
            }
        }
        (Some(existing), None) => Ok(Some(existing)),
        (None, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

fn require_field<T>(
    exchange: ExchangeId,
    symbol: &str,
    field: &'static str,
    value: Option<T>,
) -> Result<T, MarketRegistryError> {
    value.ok_or_else(|| MarketRegistryError::MissingField {
        exchange,
        symbol: symbol.to_string(),
        field,
    })
}

impl From<Instrument> for InstrumentInfo {
    fn from(value: Instrument) -> Self {
        Self {
            exchange: value.symbol.exchange,
            symbol: value.symbol.code().to_string(),
            base: Some(value.base.code().to_string()),
            quote: Some(value.quote.code().to_string()),
            settlement_currency: Some(value.settlement_currency.code().to_string()),
            kind: Some(value.kind),
            tick_size: Some(value.tick_size),
            lot_size: Some(value.lot_size),
        }
    }
}
