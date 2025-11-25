//! Thread-safe registry providing authoritative instrument metadata.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use rust_decimal::Decimal;
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
    #[error("instrument '{symbol}' missing required field '{field}'")]
    MissingField { symbol: Symbol, field: &'static str },
    #[error("instrument '{symbol}' has conflicting value for '{field}'")]
    ConflictingField { symbol: Symbol, field: &'static str },
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
    entries: HashMap<Symbol, InstrumentInfo>,
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
        for raw in file.markets {
            self.insert(raw.into_typed())?;
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
    markets: Vec<RawInstrumentInfo>,
}

#[derive(Clone, Debug, Deserialize)]
struct RawInstrumentInfo {
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

impl RawInstrumentInfo {
    fn into_typed(self) -> InstrumentInfo {
        let exchange = self.exchange;
        let symbol = Symbol::from_code(exchange, self.symbol);
        InstrumentInfo {
            symbol,
            base: self.base.map(|code| AssetId::from_code(exchange, code)),
            quote: self.quote.map(|code| AssetId::from_code(exchange, code)),
            settlement_currency: self
                .settlement_currency
                .map(|code| AssetId::from_code(exchange, code)),
            kind: self.kind,
            tick_size: self.tick_size,
            lot_size: self.lot_size,
        }
    }
}

#[derive(Clone, Debug)]
struct InstrumentInfo {
    symbol: Symbol,
    base: Option<AssetId>,
    quote: Option<AssetId>,
    settlement_currency: Option<AssetId>,
    kind: Option<InstrumentKind>,
    tick_size: Option<Price>,
    lot_size: Option<Quantity>,
}

impl InstrumentInfo {
    fn key(&self) -> Symbol {
        self.symbol
    }

    fn merge(self, other: InstrumentInfo) -> Result<InstrumentInfo, MarketRegistryError> {
        let symbol = self.key();
        if symbol != other.key() {
            return Err(MarketRegistryError::ConflictingField {
                symbol,
                field: "symbol",
            });
        }
        Ok(InstrumentInfo {
            symbol,
            base: merge_field(symbol, "base", self.base, other.base)?,
            quote: merge_field(symbol, "quote", self.quote, other.quote)?,
            settlement_currency: merge_field(
                symbol,
                "settlement_currency",
                self.settlement_currency,
                other.settlement_currency,
            )?,
            kind: merge_field(symbol, "kind", self.kind, other.kind)?,
            tick_size: merge_field(symbol, "tick_size", self.tick_size, other.tick_size)?,
            lot_size: merge_field(symbol, "lot_size", self.lot_size, other.lot_size)?,
        })
    }

    fn into_instrument(self) -> Result<Instrument, MarketRegistryError> {
        let symbol = self.symbol;
        let base = require_field(symbol, "base", self.base)?;
        let quote = require_field(symbol, "quote", self.quote)?;
        let settlement = require_field(symbol, "settlement_currency", self.settlement_currency)?;
        let kind = require_field(symbol, "kind", self.kind)?;
        let tick_size = require_field(symbol, "tick_size", self.tick_size)?;
        let lot_size = require_field(symbol, "lot_size", self.lot_size)?;

        Ok(Instrument {
            symbol,
            base,
            quote,
            kind,
            settlement_currency: settlement,
            tick_size,
            lot_size,
        })
    }
}

fn merge_field<T: Clone + PartialEq>(
    symbol: Symbol,
    field: &'static str,
    primary: Option<T>,
    secondary: Option<T>,
) -> Result<Option<T>, MarketRegistryError> {
    match (primary, secondary) {
        (Some(existing), Some(replacement)) => {
            if existing != replacement {
                Err(MarketRegistryError::ConflictingField { symbol, field })
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
    symbol: Symbol,
    field: &'static str,
    value: Option<T>,
) -> Result<T, MarketRegistryError> {
    value.ok_or_else(|| MarketRegistryError::MissingField { symbol, field })
}

impl From<Instrument> for InstrumentInfo {
    fn from(value: Instrument) -> Self {
        Self {
            symbol: value.symbol,
            base: Some(value.base),
            quote: Some(value.quote),
            settlement_currency: Some(value.settlement_currency),
            kind: Some(value.kind),
            tick_size: Some(value.tick_size),
            lot_size: Some(value.lot_size),
        }
    }
}
