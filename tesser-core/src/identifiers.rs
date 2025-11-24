use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

const UNSPECIFIED_EXCHANGE_ID: u16 = 0;

static EXCHANGES: Lazy<RwLock<ExchangeRegistry>> = Lazy::new(|| {
    RwLock::new(ExchangeRegistry {
        next_id: 1,
        ..ExchangeRegistry::default()
    })
});

static ASSETS: Lazy<RwLock<AssetRegistry>> = Lazy::new(|| RwLock::new(AssetRegistry::default()));
static SYMBOLS: Lazy<RwLock<SymbolRegistry>> = Lazy::new(|| RwLock::new(SymbolRegistry::default()));

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExchangeId(u16);

impl ExchangeId {
    pub const UNSPECIFIED: Self = Self(UNSPECIFIED_EXCHANGE_ID);

    #[must_use]
    pub const fn from_raw(value: u16) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_raw(self) -> u16 {
        self.0
    }

    #[must_use]
    pub fn is_specified(self) -> bool {
        self.0 != UNSPECIFIED_EXCHANGE_ID
    }

    pub fn register(name: impl AsRef<str>) -> Self {
        let name = canonicalize(name.as_ref());
        if name.is_empty() {
            return Self::UNSPECIFIED;
        }
        let mut registry = EXCHANGES.write();
        if let Some(id) = registry.name_to_id.get(&name) {
            return *id;
        }
        let id = ExchangeId(registry.next_id);
        registry.next_id = registry.next_id.saturating_add(1);
        let stored = leak_string(name.clone());
        registry.id_to_name.insert(id, stored);
        registry.name_to_id.insert(name, id);
        id
    }

    #[must_use]
    pub fn name(self) -> &'static str {
        if self == Self::UNSPECIFIED {
            return "unspecified";
        }
        let registry = EXCHANGES.read();
        registry
            .id_to_name
            .get(&self)
            .copied()
            .unwrap_or_else(|| leak_string(format!("exchange#{}", self.0)))
    }
}

impl Default for ExchangeId {
    fn default() -> Self {
        Self::UNSPECIFIED
    }
}

impl fmt::Display for ExchangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for ExchangeId {
    type Err = IdentifierParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let name = canonicalize(s);
        if name.is_empty() {
            return Err(IdentifierParseError::new("exchange", s));
        }
        let mut registry = EXCHANGES.write();
        if let Some(id) = registry.name_to_id.get(&name) {
            return Ok(*id);
        }
        let id = ExchangeId(registry.next_id);
        registry.next_id = registry.next_id.saturating_add(1);
        let stored = leak_string(name.clone());
        registry.id_to_name.insert(id, stored);
        registry.name_to_id.insert(name, id);
        Ok(id)
    }
}

impl Serialize for ExchangeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_ref())
    }
}

impl<'de> Deserialize<'de> for ExchangeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(D::Error::custom)
    }
}

impl From<&str> for ExchangeId {
    fn from(value: &str) -> Self {
        value.parse().unwrap_or(Self::UNSPECIFIED)
    }
}

impl From<String> for ExchangeId {
    fn from(value: String) -> Self {
        value.parse().unwrap_or(Self::UNSPECIFIED)
    }
}

impl From<&ExchangeId> for ExchangeId {
    fn from(value: &ExchangeId) -> Self {
        *value
    }
}

impl AsRef<str> for ExchangeId {
    fn as_ref(&self) -> &str {
        self.name()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AssetId {
    pub exchange: ExchangeId,
    pub asset_id: u32,
}

impl AssetId {
    pub const fn new(exchange: ExchangeId, asset_id: u32) -> Self {
        Self { exchange, asset_id }
    }

    pub fn from_code(exchange: ExchangeId, code: impl AsRef<str>) -> Self {
        let code = canonicalize_asset(code.as_ref());
        if code.is_empty() {
            return Self::unspecified();
        }
        let mut registry = ASSETS.write();
        let key = (exchange, code.clone());
        if let Some(existing) = registry.name_to_id.get(&key) {
            return Self::new(exchange, *existing);
        }
        let next = registry
            .next_per_exchange
            .entry(exchange)
            .and_modify(|id| *id = id.saturating_add(1))
            .or_insert(1);
        let id = *next;
        registry.name_to_id.insert(key, id);
        registry
            .id_to_name
            .insert((exchange, id), leak_string(code));
        Self::new(exchange, id)
    }

    #[must_use]
    pub fn code(&self) -> &'static str {
        asset_code_lookup(self.exchange, self.asset_id)
    }

    pub const fn unspecified() -> Self {
        Self::new(ExchangeId::UNSPECIFIED, 0)
    }
}

impl Default for AssetId {
    fn default() -> Self {
        Self::unspecified()
    }
}

impl fmt::Display for AssetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.exchange, self.code())
    }
}

impl FromStr for AssetId {
    type Err = IdentifierParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (exchange, code) = split_identifier(s, "asset")?;
        Ok(Self::from_code(exchange, code))
    }
}

impl Serialize for AssetId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_ref())
    }
}

impl<'de> Deserialize<'de> for AssetId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(D::Error::custom)
    }
}

impl From<&str> for AssetId {
    fn from(value: &str) -> Self {
        value
            .parse()
            .unwrap_or_else(|_| Self::from_code(ExchangeId::UNSPECIFIED, value))
    }
}

impl From<String> for AssetId {
    fn from(value: String) -> Self {
        AssetId::from(value.as_str())
    }
}

impl From<&AssetId> for AssetId {
    fn from(value: &AssetId) -> Self {
        *value
    }
}

impl AsRef<str> for AssetId {
    fn as_ref(&self) -> &str {
        self.code()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Symbol {
    pub exchange: ExchangeId,
    pub market_id: u32,
}

impl Symbol {
    pub const fn new(exchange: ExchangeId, market_id: u32) -> Self {
        Self {
            exchange,
            market_id,
        }
    }

    pub fn from_code(exchange: ExchangeId, code: impl AsRef<str>) -> Self {
        let code = code.as_ref().trim();
        if code.is_empty() {
            return Self::unspecified();
        }
        let normalized = code.to_uppercase();
        let mut registry = SYMBOLS.write();
        let key = (exchange, normalized.clone());
        if let Some(existing) = registry.name_to_id.get(&key) {
            return Self::new(exchange, *existing);
        }
        let next = registry
            .next_per_exchange
            .entry(exchange)
            .and_modify(|id| *id = id.saturating_add(1))
            .or_insert(1);
        let id = *next;
        registry.name_to_id.insert(key, id);
        registry
            .id_to_name
            .insert((exchange, id), leak_string(normalized));
        Self::new(exchange, id)
    }

    #[must_use]
    pub fn code(&self) -> &'static str {
        symbol_code_lookup(self.exchange, self.market_id)
    }

    pub const fn unspecified() -> Self {
        Self::new(ExchangeId::UNSPECIFIED, 0)
    }
}

impl Default for Symbol {
    fn default() -> Self {
        Self::unspecified()
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.exchange, self.code())
    }
}

impl FromStr for Symbol {
    type Err = IdentifierParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (exchange, symbol) = split_identifier(s, "symbol")?;
        Ok(Self::from_code(exchange, symbol))
    }
}

impl Serialize for Symbol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_ref())
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(D::Error::custom)
    }
}

impl From<&str> for Symbol {
    fn from(value: &str) -> Self {
        value
            .parse()
            .unwrap_or_else(|_| Self::from_code(ExchangeId::UNSPECIFIED, value))
    }
}

impl From<String> for Symbol {
    fn from(value: String) -> Self {
        Symbol::from(value.as_str())
    }
}

impl From<&Symbol> for Symbol {
    fn from(value: &Symbol) -> Self {
        *value
    }
}

impl AsRef<str> for Symbol {
    fn as_ref(&self) -> &str {
        self.code()
    }
}

#[derive(Debug, Clone)]
pub struct IdentifierParseError {
    msg: String,
}

impl IdentifierParseError {
    fn new(kind: &str, raw: impl AsRef<str>) -> Self {
        Self {
            msg: format!("invalid {kind} identifier: '{}'", raw.as_ref()),
        }
    }
}

impl fmt::Display for IdentifierParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for IdentifierParseError {}

#[derive(Default)]
struct ExchangeRegistry {
    name_to_id: HashMap<String, ExchangeId>,
    id_to_name: HashMap<ExchangeId, &'static str>,
    next_id: u16,
}

#[derive(Default)]
struct AssetRegistry {
    name_to_id: HashMap<(ExchangeId, String), u32>,
    id_to_name: HashMap<(ExchangeId, u32), &'static str>,
    next_per_exchange: HashMap<ExchangeId, u32>,
}

#[derive(Default)]
struct SymbolRegistry {
    name_to_id: HashMap<(ExchangeId, String), u32>,
    id_to_name: HashMap<(ExchangeId, u32), &'static str>,
    next_per_exchange: HashMap<ExchangeId, u32>,
}

fn canonicalize(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn canonicalize_asset(code: &str) -> String {
    code.trim().to_ascii_uppercase()
}

fn split_identifier<'a>(
    value: &'a str,
    kind: &'static str,
) -> Result<(ExchangeId, &'a str), IdentifierParseError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(IdentifierParseError::new(kind, value));
    }
    if let Some((exchange, rest)) = value.split_once(':') {
        let exchange = exchange.parse()?;
        let rest = rest.trim();
        if rest.is_empty() {
            return Err(IdentifierParseError::new(kind, value));
        }
        Ok((exchange, rest))
    } else {
        Ok((ExchangeId::UNSPECIFIED, value))
    }
}

fn asset_code_lookup(exchange: ExchangeId, id: u32) -> &'static str {
    if id == 0 {
        return "UNKNOWN";
    }
    let registry = ASSETS.read();
    registry
        .id_to_name
        .get(&(exchange, id))
        .copied()
        .unwrap_or_else(|| leak_string(format!("asset#{}", id)))
}

fn symbol_code_lookup(exchange: ExchangeId, id: u32) -> &'static str {
    if id == 0 {
        return "UNKNOWN";
    }
    let registry = SYMBOLS.read();
    registry
        .id_to_name
        .get(&(exchange, id))
        .copied()
        .unwrap_or_else(|| leak_string(format!("symbol#{}", id)))
}

fn leak_string(value: String) -> &'static str {
    Box::leak(value.into_boxed_str())
}
