use std::collections::HashSet;
use std::fs::File as StdFile;
use std::io::{BufRead as StdBufRead, BufReader as StdBufReader};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Days, Utc};
use futures::StreamExt;
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tesser_core::{Candle, Interval, Side, Symbol, Tick};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::task;
use tracing::{debug, info};
use zip::ZipArchive;

const MAX_LIMIT: usize = 1000;
const BYBIT_PUBLIC_BASE_URL: &str = "https://public.bybit.com/trading";
const BINANCE_PUBLIC_BASE_URL: &str = "https://data.binance.vision/data/futures/um/daily/aggTrades";
const NANOS_PER_SECOND: i64 = 1_000_000_000;

#[async_trait]
pub trait MarketDataDownloader {
    async fn download_klines(&self, req: &KlineRequest<'_>) -> Result<Vec<Candle>>;
    async fn download_trades(&self, req: &TradeRequest<'_>) -> Result<Vec<NormalizedTrade>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TradeSource {
    Rest,
    BybitPublicArchive,
    BinancePublicArchive,
}

/// Parameters for a trade download request.
#[derive(Clone)]
pub struct TradeRequest<'a> {
    pub symbol: &'a str,
    pub category: Option<&'a str>,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub limit: usize,
    pub source: TradeSource,
    pub public_data_url: Option<&'a str>,
    pub archive_cache_dir: Option<PathBuf>,
    pub resume_archives: bool,
}

impl<'a> TradeRequest<'a> {
    pub fn new(symbol: &'a str, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            symbol,
            category: None,
            start,
            end,
            limit: MAX_LIMIT,
            source: TradeSource::Rest,
            public_data_url: None,
            archive_cache_dir: None,
            resume_archives: false,
        }
    }

    #[must_use]
    pub fn with_category(mut self, category: &'a str) -> Self {
        self.category = Some(category);
        self
    }

    #[must_use]
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit.clamp(1, MAX_LIMIT);
        self
    }

    #[must_use]
    pub fn with_source(mut self, source: TradeSource) -> Self {
        self.source = source;
        self
    }

    #[must_use]
    pub fn with_public_data_url(mut self, url: &'a str) -> Self {
        self.public_data_url = Some(url);
        self
    }

    #[must_use]
    pub fn with_archive_cache_dir(mut self, dir: PathBuf) -> Self {
        self.archive_cache_dir = Some(dir);
        self
    }

    #[must_use]
    pub fn with_resume_archives(mut self, resume: bool) -> Self {
        self.resume_archives = resume;
        self
    }
}

/// Normalized trade enriched with the exchange-provided identifier.
#[derive(Clone, Debug)]
pub struct NormalizedTrade {
    pub tick: Tick,
    pub trade_id: Option<String>,
}

impl NormalizedTrade {
    pub fn new(tick: Tick, trade_id: Option<String>) -> Self {
        Self { tick, trade_id }
    }
}

/// Parameters for a kline download request.
pub struct KlineRequest<'a> {
    pub category: &'a str,
    pub symbol: &'a str,
    pub interval: Interval,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub limit: usize,
}

impl<'a> KlineRequest<'a> {
    pub fn new(
        category: &'a str,
        symbol: &'a str,
        interval: Interval,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Self {
        Self {
            category,
            symbol,
            interval,
            start,
            end,
            limit: MAX_LIMIT,
        }
    }
}

/// Simple Bybit REST downloader for kline data.
pub struct BybitDownloader {
    client: Client,
    base_url: String,
}

impl BybitDownloader {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into(),
        }
    }

    fn endpoint(&self, path: &str) -> String {
        let base = self.base_url.trim_end_matches('/');
        format!("{base}/{path}")
    }

    /// Download klines from Bybit, returning a chronologically sorted list of candles.
    pub async fn download_klines(&self, req: &KlineRequest<'_>) -> Result<Vec<Candle>> {
        <Self as MarketDataDownloader>::download_klines(self, req).await
    }

    /// Download historical trades from Bybit within the requested range.
    pub async fn download_trades(&self, req: &TradeRequest<'_>) -> Result<Vec<NormalizedTrade>> {
        <Self as MarketDataDownloader>::download_trades(self, req).await
    }
}

#[async_trait]
impl MarketDataDownloader for BybitDownloader {
    async fn download_klines(&self, req: &KlineRequest<'_>) -> Result<Vec<Candle>> {
        let mut cursor = req.start.timestamp_millis();
        let end_ms = req.end.timestamp_millis();
        if cursor >= end_ms {
            return Err(anyhow!("start must be earlier than end"));
        }

        let mut candles = Vec::new();
        let interval_ms = req.interval.as_duration().num_milliseconds();

        while cursor < end_ms {
            let limit = req.limit.min(MAX_LIMIT).to_string();
            let response = self
                .client
                .get(self.endpoint("v5/market/kline"))
                .query(&[
                    ("category", req.category),
                    ("symbol", req.symbol),
                    ("interval", req.interval.to_bybit()),
                    ("start", &cursor.to_string()),
                    ("end", &end_ms.to_string()),
                    ("limit", &limit),
                ])
                .send()
                .await
                .context("request to Bybit failed")?;

            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read Bybit response body")?;
            debug!(
                "bybit kline response (status {}): {}",
                status,
                truncate(&body, 512)
            );
            if !status.is_success() {
                return Err(anyhow!(
                    "Bybit responded with status {}: {}",
                    status,
                    truncate(&body, 256)
                ));
            }

            let response: BybitKlineResponse = serde_json::from_str(&body).map_err(|err| {
                anyhow!(
                    "failed to parse Bybit response: {} (body snippet: {})",
                    err,
                    truncate(&body, 256)
                )
            })?;

            if response.ret_code != 0 {
                return Err(anyhow!(
                    "Bybit returned error {}: {}",
                    response.ret_code,
                    response.ret_msg
                ));
            }

            let result = match response.result {
                Some(result) => result,
                None => break,
            };
            if result.list.is_empty() {
                break;
            }

            let mut batch = Vec::new();
            for entry in result.list {
                if let Some(candle) = parse_entry(&entry, req.symbol, req.interval) {
                    if candle.timestamp.timestamp_millis() >= cursor
                        && candle.timestamp.timestamp_millis() <= end_ms
                    {
                        batch.push(candle);
                    }
                }
            }

            if batch.is_empty() {
                break;
            }

            batch.sort_by_key(|c| c.timestamp);
            cursor = batch
                .last()
                .map(|c| c.timestamp.timestamp_millis() + interval_ms)
                .unwrap_or(end_ms);
            candles.extend(batch);
        }

        candles.sort_by_key(|c| c.timestamp);
        candles.dedup_by_key(|c| c.timestamp);
        Ok(candles)
    }

    async fn download_trades(&self, req: &TradeRequest<'_>) -> Result<Vec<NormalizedTrade>> {
        match req.source {
            TradeSource::Rest => self.download_trades_rest(req).await,
            TradeSource::BybitPublicArchive => self.download_trades_public(req).await,
            TradeSource::BinancePublicArchive => Err(anyhow!(
                "binance public archive source is invalid for Bybit requests"
            )),
        }
    }
}

impl BybitDownloader {
    async fn download_trades_rest(&self, req: &TradeRequest<'_>) -> Result<Vec<NormalizedTrade>> {
        let start_ms = req.start.timestamp_millis();
        let end_ms = req.end.timestamp_millis();
        if start_ms >= end_ms {
            return Err(anyhow!("start must be earlier than end"));
        }

        let mut trades = Vec::new();
        let mut seen_ids = HashSet::new();
        let mut cursor: Option<String> = None;
        let limit = req.limit.min(MAX_LIMIT);

        loop {
            let mut params = Vec::with_capacity(6);
            if let Some(category) = req.category {
                params.push(("category", category.to_string()));
            }
            params.push(("symbol", req.symbol.to_string()));
            params.push(("start", start_ms.to_string()));
            params.push(("end", end_ms.to_string()));
            params.push(("limit", limit.to_string()));
            if let Some(token) = &cursor {
                params.push(("cursor", token.clone()));
            }
            let params_ref: Vec<(&str, &str)> =
                params.iter().map(|(k, v)| (*k, v.as_str())).collect();

            let response = self
                .client
                .get(self.endpoint("v5/market/history-trade"))
                .query(&params_ref)
                .send()
                .await
                .context("request to Bybit failed")?;

            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read Bybit response body")?;
            debug!(
                "bybit trades response (status {}): {}",
                status,
                truncate(&body, 512)
            );
            if !status.is_success() {
                return Err(anyhow!(
                    "Bybit responded with status {}: {}",
                    status,
                    truncate(&body, 256)
                ));
            }

            let response: BybitTradeResponse = serde_json::from_str(&body).map_err(|err| {
                anyhow!(
                    "failed to parse Bybit response: {} (body snippet: {})",
                    err,
                    truncate(&body, 256)
                )
            })?;

            if response.ret_code != 0 {
                return Err(anyhow!(
                    "Bybit returned error {}: {}",
                    response.ret_code,
                    response.ret_msg
                ));
            }
            let Some(result) = response.result else {
                break;
            };
            if result.list.is_empty() {
                break;
            }

            for entry in result.list {
                if !seen_ids.insert(entry.exec_id.clone()) {
                    continue;
                }
                if let Some(trade) = parse_bybit_trade(req.symbol, entry) {
                    if trade.tick.exchange_timestamp.timestamp_millis() < start_ms
                        || trade.tick.exchange_timestamp.timestamp_millis() > end_ms
                    {
                        continue;
                    }
                    trades.push(trade);
                }
            }

            if let Some(next_cursor) = result.next_page_cursor {
                cursor = Some(next_cursor);
            } else {
                break;
            }
        }

        trades.sort_by_key(|trade| trade.tick.exchange_timestamp);
        trades.dedup_by(|a, b| {
            a.tick.exchange_timestamp == b.tick.exchange_timestamp
                && a.tick.price == b.tick.price
                && a.tick.size == b.tick.size
                && a.tick.side == b.tick.side
        });
        Ok(trades)
    }

    async fn download_trades_public(&self, req: &TradeRequest<'_>) -> Result<Vec<NormalizedTrade>> {
        let mut cursor_date = req.start.date_naive();
        let end_date = req.end.date_naive();
        let mut trades = Vec::new();
        let mut seen_ids = HashSet::new();
        let base_url = req.public_data_url.unwrap_or(BYBIT_PUBLIC_BASE_URL);
        let cache_root = resolve_archive_cache_dir(req, "bybit", req.symbol);
        let total_days = (end_date
            .signed_duration_since(cursor_date)
            .num_days()
            .max(0)
            + 1)
        .try_into()
        .unwrap_or(0u32);
        info!(
            symbol = req.symbol,
            "downloading {} day(s) from Bybit public archive", total_days
        );

        while cursor_date <= end_date {
            let next_date = cursor_date
                .checked_add_days(Days::new(1))
                .unwrap_or(cursor_date);
            let day_start = DateTime::<Utc>::from_naive_utc_and_offset(
                cursor_date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow!("invalid day {}", cursor_date))?,
                Utc,
            )
            .max(req.start);
            let day_end = DateTime::<Utc>::from_naive_utc_and_offset(
                next_date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow!("invalid day {}", cursor_date))?,
                Utc,
            )
            .min(req.end);
            if day_start >= day_end {
                if next_date == cursor_date {
                    break;
                }
                cursor_date = next_date;
                continue;
            }

            let filename = format!("{}_{}.csv.gz", req.symbol, cursor_date.format("%Y-%m-%d"));
            let cache_path = cache_root.join(&filename);
            let url = format!(
                "{}/{symbol}/{symbol}{}.csv.gz",
                base_url,
                cursor_date.format("%Y-%m-%d"),
                symbol = req.symbol
            );
            if download_archive_file(&self.client, &url, &cache_path, req.resume_archives)
                .await?
                .is_none()
            {
                if next_date == cursor_date {
                    break;
                }
                cursor_date = next_date;
                continue;
            }
            let mut day_trades = read_bybit_archive(
                &cache_path,
                req.symbol,
                day_start.timestamp_millis(),
                day_end.timestamp_millis(),
                &mut seen_ids,
            )
            .await?;
            trades.append(&mut day_trades);

            if next_date == cursor_date {
                break;
            }
            cursor_date = next_date;
        }

        trades.sort_by_key(|trade| trade.tick.exchange_timestamp);
        trades.dedup_by(|a, b| {
            a.tick.exchange_timestamp == b.tick.exchange_timestamp
                && a.tick.price == b.tick.price
                && a.tick.size == b.tick.size
                && a.tick.side == b.tick.side
        });
        Ok(trades)
    }
}

fn parse_entry(entry: &[String], symbol: &str, interval: Interval) -> Option<Candle> {
    if entry.len() < 6 {
        return None;
    }
    let ts = entry.first()?.parse::<i64>().ok()?;
    let timestamp = DateTime::<Utc>::from_timestamp_millis(ts)?;
    let open = entry.get(1)?.parse::<Decimal>().ok()?;
    let high = entry.get(2)?.parse::<Decimal>().ok()?;
    let low = entry.get(3)?.parse::<Decimal>().ok()?;
    let close = entry.get(4)?.parse::<Decimal>().ok()?;
    let volume = entry.get(5)?.parse::<Decimal>().ok()?;
    Some(Candle {
        symbol: Symbol::from(symbol),
        interval,
        open,
        high,
        low,
        close,
        volume,
        timestamp,
    })
}

#[derive(Debug, Deserialize)]
struct BybitKlineResponse {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: Option<KlineResult>,
}

#[derive(Debug, Deserialize)]
struct KlineResult {
    list: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct BybitTradeResponse {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: Option<BybitTradeResult>,
}

#[derive(Debug, Deserialize)]
struct BybitTradeResult {
    list: Vec<BybitTradeEntry>,
    #[serde(rename = "nextPageCursor")]
    next_page_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BybitTradeEntry {
    #[serde(rename = "execId")]
    exec_id: String,
    price: String,
    size: String,
    side: String,
    #[serde(rename = "time", alias = "execTime", alias = "tradeTime")]
    time: String,
}

/// Simple Binance REST downloader for kline data.
pub struct BinanceDownloader {
    client: Client,
    base_url: String,
}

impl BinanceDownloader {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into(),
        }
    }

    fn endpoint(&self, path: &str) -> String {
        let base = self.base_url.trim_end_matches('/');
        format!("{base}/{path}")
    }

    pub async fn download_klines(&self, req: &KlineRequest<'_>) -> Result<Vec<Candle>> {
        <Self as MarketDataDownloader>::download_klines(self, req).await
    }

    /// Download aggregated trades via Binance's `aggTrades` endpoint.
    pub async fn download_agg_trades(
        &self,
        req: &TradeRequest<'_>,
    ) -> Result<Vec<NormalizedTrade>> {
        self.fetch_agg_trades(req).await
    }

    /// Exchange-agnostic wrapper for parity with Bybit downloader.
    pub async fn download_trades(&self, req: &TradeRequest<'_>) -> Result<Vec<NormalizedTrade>> {
        <Self as MarketDataDownloader>::download_trades(self, req).await
    }

    async fn fetch_agg_trades(&self, req: &TradeRequest<'_>) -> Result<Vec<NormalizedTrade>> {
        let mut cursor = req.start.timestamp_millis();
        let end_ms = req.end.timestamp_millis();
        if cursor >= end_ms {
            return Err(anyhow!("start must be earlier than end"));
        }
        let limit = req.limit.min(MAX_LIMIT);
        let mut trades = Vec::new();
        let mut seen_ids = HashSet::new();
        while cursor < end_ms {
            let response = self
                .client
                .get(self.endpoint("fapi/v1/aggTrades"))
                .query(&[
                    ("symbol", req.symbol),
                    ("startTime", &cursor.to_string()),
                    ("endTime", &end_ms.to_string()),
                    ("limit", &limit.to_string()),
                ])
                .send()
                .await
                .context("request to Binance failed")?;
            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read Binance response body")?;
            debug!(
                "binance aggTrades response (status {}): {}",
                status,
                truncate(&body, 512)
            );
            if !status.is_success() {
                return Err(anyhow!(
                    "Binance responded with status {}: {}",
                    status,
                    truncate(&body, 256)
                ));
            }
            let entries: Vec<BinanceAggTrade> = serde_json::from_str(&body).map_err(|err| {
                anyhow!(
                    "failed to parse Binance response: {} (body snippet: {})",
                    err,
                    truncate(&body, 256)
                )
            })?;
            if entries.is_empty() {
                break;
            }
            let mut last_ts: Option<i64> = None;
            for entry in entries {
                if !seen_ids.insert(entry.agg_id) {
                    continue;
                }
                if let Some(trade) = parse_binance_trade(req.symbol, entry) {
                    let ts = trade.tick.exchange_timestamp.timestamp_millis();
                    last_ts = Some(last_ts.map_or(ts, |prev| prev.max(ts)));
                    trades.push(trade);
                }
            }
            if let Some(ts) = last_ts {
                cursor = ts + 1;
            } else {
                break;
            }
        }
        trades.sort_by_key(|trade| trade.tick.exchange_timestamp);
        trades.dedup_by(|a, b| {
            a.tick.exchange_timestamp == b.tick.exchange_timestamp
                && a.tick.price == b.tick.price
                && a.tick.size == b.tick.size
                && a.tick.side == b.tick.side
        });
        Ok(trades)
    }

    async fn download_trades_public(&self, req: &TradeRequest<'_>) -> Result<Vec<NormalizedTrade>> {
        let mut cursor_date = req.start.date_naive();
        let end_date = req.end.date_naive();
        let mut trades = Vec::new();
        let mut seen_ids = HashSet::new();
        let base_url = req.public_data_url.unwrap_or(BINANCE_PUBLIC_BASE_URL);
        let cache_root = resolve_archive_cache_dir(req, "binance", req.symbol);

        while cursor_date <= end_date {
            let next_date = cursor_date
                .checked_add_days(Days::new(1))
                .unwrap_or(cursor_date);
            let day_start = DateTime::<Utc>::from_naive_utc_and_offset(
                cursor_date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow!("invalid date {}", cursor_date))?,
                Utc,
            )
            .max(req.start);
            let day_end = DateTime::<Utc>::from_naive_utc_and_offset(
                next_date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow!("invalid date {}", next_date))?,
                Utc,
            )
            .min(req.end);
            if day_start >= day_end {
                if next_date == cursor_date {
                    break;
                }
                cursor_date = next_date;
                continue;
            }

            let filename = format!(
                "{}-aggTrades-{}.zip",
                req.symbol,
                cursor_date.format("%Y-%m-%d")
            );
            let cache_path = cache_root.join(&filename);
            let url = format!("{}/{symbol}/{filename}", base_url, symbol = req.symbol);
            if download_archive_file(&self.client, &url, &cache_path, req.resume_archives)
                .await?
                .is_none()
            {
                if next_date == cursor_date {
                    break;
                }
                cursor_date = next_date;
                continue;
            }
            let parsed = read_binance_archive(cache_path.clone(), req.symbol.to_string()).await?;
            let start_ms = day_start.timestamp_millis();
            let end_ms = day_end.timestamp_millis();
            for trade in parsed {
                let ts = trade.tick.exchange_timestamp.timestamp_millis();
                if ts < start_ms || ts > end_ms {
                    continue;
                }
                if let Some(id) = trade.trade_id.as_ref() {
                    if !seen_ids.insert(id.clone()) {
                        continue;
                    }
                }
                trades.push(trade);
            }

            if next_date == cursor_date {
                break;
            }
            cursor_date = next_date;
        }

        trades.sort_by_key(|trade| trade.tick.exchange_timestamp);
        trades.dedup_by(|a, b| {
            a.tick.exchange_timestamp == b.tick.exchange_timestamp
                && a.tick.price == b.tick.price
                && a.tick.size == b.tick.size
                && a.tick.side == b.tick.side
        });
        Ok(trades)
    }
}

#[async_trait]
impl MarketDataDownloader for BinanceDownloader {
    async fn download_klines(&self, req: &KlineRequest<'_>) -> Result<Vec<Candle>> {
        let mut cursor = req.start.timestamp_millis();
        let end_ms = req.end.timestamp_millis();
        if cursor >= end_ms {
            return Err(anyhow!("start must be earlier than end"));
        }
        let mut candles = Vec::new();
        let interval_ms = req.interval.as_duration().num_milliseconds();
        while cursor < end_ms {
            let response = self
                .client
                .get(self.endpoint("fapi/v1/klines"))
                .query(&[
                    ("symbol", req.symbol),
                    ("interval", req.interval.to_binance()),
                    ("startTime", &cursor.to_string()),
                    ("endTime", &end_ms.to_string()),
                    ("limit", &req.limit.min(MAX_LIMIT).to_string()),
                ])
                .send()
                .await
                .context("request to Binance failed")?;
            let status = response.status();
            let body = response
                .text()
                .await
                .context("failed to read Binance response body")?;
            debug!(
                "binance kline response (status {}): {}",
                status,
                truncate(&body, 512)
            );
            if !status.is_success() {
                return Err(anyhow!(
                    "Binance responded with status {}: {}",
                    status,
                    truncate(&body, 256)
                ));
            }
            let entries: Vec<Vec<JsonValue>> = serde_json::from_str(&body).map_err(|err| {
                anyhow!(
                    "failed to parse Binance response: {} (body snippet: {})",
                    err,
                    truncate(&body, 256)
                )
            })?;
            if entries.is_empty() {
                break;
            }
            let mut batch = Vec::new();
            for entry in entries {
                if let Some(candle) = parse_binance_entry(&entry, req.symbol, req.interval) {
                    if candle.timestamp.timestamp_millis() >= cursor
                        && candle.timestamp.timestamp_millis() <= end_ms
                    {
                        batch.push(candle);
                    }
                }
            }
            if batch.is_empty() {
                break;
            }
            batch.sort_by_key(|c| c.timestamp);
            cursor = batch
                .last()
                .map(|c| c.timestamp.timestamp_millis() + interval_ms)
                .unwrap_or(end_ms);
            candles.extend(batch);
        }
        candles.sort_by_key(|c| c.timestamp);
        candles.dedup_by_key(|c| c.timestamp);
        Ok(candles)
    }

    async fn download_trades(&self, req: &TradeRequest<'_>) -> Result<Vec<NormalizedTrade>> {
        match req.source {
            TradeSource::Rest => self.fetch_agg_trades(req).await,
            TradeSource::BinancePublicArchive => self.download_trades_public(req).await,
            TradeSource::BybitPublicArchive => Err(anyhow!(
                "bybit public archive source is invalid for Binance requests"
            )),
        }
    }
}

fn parse_binance_entry(entry: &[JsonValue], symbol: &str, interval: Interval) -> Option<Candle> {
    if entry.len() < 6 {
        return None;
    }
    let ts = entry.first()?.as_i64()?;
    let timestamp = DateTime::<Utc>::from_timestamp_millis(ts)?;
    let open = entry.get(1)?.as_str()?.parse::<Decimal>().ok()?;
    let high = entry.get(2)?.as_str()?.parse::<Decimal>().ok()?;
    let low = entry.get(3)?.as_str()?.parse::<Decimal>().ok()?;
    let close = entry.get(4)?.as_str()?.parse::<Decimal>().ok()?;
    let volume = entry.get(5)?.as_str()?.parse::<Decimal>().ok()?;
    Some(Candle {
        symbol: Symbol::from(symbol),
        interval,
        open,
        high,
        low,
        close,
        volume,
        timestamp,
    })
}

#[derive(Debug, Deserialize)]
struct BinanceAggTrade {
    #[serde(rename = "a")]
    agg_id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    timestamp: i64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

fn parse_bybit_public_line(symbol: &str, line: &str) -> Option<NormalizedTrade> {
    let mut columns = line.split(',');
    let timestamp = parse_public_timestamp(columns.next()?.trim())?;
    let _symbol = columns.next()?;
    let side = parse_side(columns.next()?.trim())?;
    let size = columns.next()?.trim().parse::<Decimal>().ok()?;
    let price = columns.next()?.trim().parse::<Decimal>().ok()?;
    columns.next()?; // tickDirection
    let trade_id = columns.next().map(|value| value.trim().to_string());

    let tick = Tick {
        symbol: Symbol::from(symbol),
        price,
        size,
        side,
        exchange_timestamp: timestamp,
        received_at: timestamp,
    };
    Some(NormalizedTrade::new(tick, trade_id))
}

fn parse_binance_public_line(symbol: &str, line: &str) -> Option<NormalizedTrade> {
    let mut columns = line.split(',');
    let agg_id = columns.next()?.trim().parse::<u64>().ok()?;
    let price = columns.next()?.trim().parse::<Decimal>().ok()?;
    let size = columns.next()?.trim().parse::<Decimal>().ok()?;
    columns.next()?; // firstTradeId
    columns.next()?; // lastTradeId
    let timestamp = columns
        .next()?
        .trim()
        .parse::<i64>()
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)?;
    let maker_flag = columns.next()?.trim();
    let is_buyer_maker = match maker_flag {
        "true" | "True" | "1" => true,
        "false" | "False" | "0" => false,
        _ => return None,
    };
    let _ = columns.next(); // ignore bestPriceMatch flag
    let side = if is_buyer_maker {
        Side::Sell
    } else {
        Side::Buy
    };
    let tick = Tick {
        symbol: Symbol::from(symbol),
        price,
        size,
        side,
        exchange_timestamp: timestamp,
        received_at: timestamp,
    };
    Some(NormalizedTrade::new(tick, Some(agg_id.to_string())))
}

fn parse_public_timestamp(value: &str) -> Option<DateTime<Utc>> {
    let seconds = value.parse::<f64>().ok()?;
    let secs = seconds.trunc() as i64;
    let fractional = seconds - secs as f64;
    let nanos = (fractional * NANOS_PER_SECOND as f64).round() as i64;
    let clamped = nanos.clamp(0, NANOS_PER_SECOND - 1) as u32;
    DateTime::<Utc>::from_timestamp(secs, clamped)
}

fn parse_bybit_trade(symbol: &str, entry: BybitTradeEntry) -> Option<NormalizedTrade> {
    let timestamp = entry
        .time
        .parse::<i64>()
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)?;
    let price = entry.price.parse::<Decimal>().ok()?;
    let size = entry.size.parse::<Decimal>().ok()?;
    let side = parse_side(&entry.side)?;
    let tick = Tick {
        symbol: Symbol::from(symbol),
        price,
        size,
        side,
        exchange_timestamp: timestamp,
        received_at: timestamp,
    };
    Some(NormalizedTrade::new(tick, Some(entry.exec_id)))
}

fn parse_binance_trade(symbol: &str, entry: BinanceAggTrade) -> Option<NormalizedTrade> {
    let timestamp = DateTime::<Utc>::from_timestamp_millis(entry.timestamp)?;
    let price = entry.price.parse::<Decimal>().ok()?;
    let size = entry.quantity.parse::<Decimal>().ok()?;
    let side = if entry.is_buyer_maker {
        Side::Sell
    } else {
        Side::Buy
    };
    let tick = Tick {
        symbol: Symbol::from(symbol),
        price,
        size,
        side,
        exchange_timestamp: timestamp,
        received_at: timestamp,
    };
    Some(NormalizedTrade::new(tick, Some(entry.agg_id.to_string())))
}

fn parse_side(value: &str) -> Option<Side> {
    match value.to_ascii_lowercase().as_str() {
        "buy" => Some(Side::Buy),
        "sell" => Some(Side::Sell),
        _ => None,
    }
}

fn truncate(body: &str, max: usize) -> String {
    if body.len() <= max {
        body.to_string()
    } else {
        format!("{}…", &body[..max])
    }
}

fn resolve_archive_cache_dir(req: &TradeRequest<'_>, exchange: &str, symbol: &str) -> PathBuf {
    req.archive_cache_dir.clone().unwrap_or_else(|| {
        std::env::temp_dir()
            .join("tesser-data")
            .join(exchange)
            .join(symbol)
    })
}

async fn download_archive_file(
    client: &Client,
    url: &str,
    cache_path: &Path,
    resume: bool,
) -> Result<Option<()>> {
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut start = 0;
    if resume {
        if let Ok(meta) = fs::metadata(cache_path).await {
            start = meta.len();
        }
    } else if fs::try_exists(cache_path).await? {
        fs::remove_file(cache_path).await?;
    }
    let mut request = client.get(url);
    if resume && start > 0 {
        request = request.header(reqwest::header::RANGE, format!("bytes={start}-"));
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("failed to fetch archive {url}"))?;
    let status = response.status();
    if status == StatusCode::NOT_FOUND {
        debug!("archive missing {}", url);
        return Ok(None);
    }
    if resume && status == StatusCode::RANGE_NOT_SATISFIABLE {
        debug!("archive already complete {}", url);
        return Ok(Some(()));
    }
    if !(status.is_success() || status == StatusCode::PARTIAL_CONTENT) {
        return Err(anyhow!(
            "archive request {} failed with status {}",
            url,
            status
        ));
    }

    let mut file = if start > 0 {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(cache_path)
            .await?
    } else {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(cache_path)
            .await?
    };
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let bytes = chunk.context("failed to read archive chunk")?;
        file.write_all(&bytes).await?;
    }
    file.flush().await?;
    Ok(Some(()))
}

async fn read_bybit_archive(
    cache_path: &Path,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
    seen_ids: &mut HashSet<String>,
) -> Result<Vec<NormalizedTrade>> {
    let file = tokio::fs::File::open(cache_path)
        .await
        .with_context(|| format!("failed to open {}", cache_path.display()))?;
    let reader = BufReader::new(file);
    let decoder = async_compression::tokio::bufread::GzipDecoder::new(reader);
    let reader = BufReader::new(decoder);
    let mut lines = reader.lines();
    let mut trades = Vec::new();
    while let Some(line) = lines.next_line().await? {
        if line.starts_with("timestamp") {
            continue;
        }
        let Some(trade) = parse_bybit_public_line(symbol, line.trim()) else {
            continue;
        };
        let ts = trade.tick.exchange_timestamp.timestamp_millis();
        if ts < start_ms || ts > end_ms {
            continue;
        }
        if let Some(id) = trade.trade_id.as_ref() {
            if !seen_ids.insert(id.clone()) {
                continue;
            }
        }
        trades.push(trade);
    }
    Ok(trades)
}

async fn read_binance_archive(cache_path: PathBuf, symbol: String) -> Result<Vec<NormalizedTrade>> {
    task::spawn_blocking(move || -> Result<Vec<NormalizedTrade>> {
        let file = StdFile::open(&cache_path)
            .with_context(|| format!("failed to open {}", cache_path.display()))?;
        let mut archive = ZipArchive::new(file)
            .with_context(|| format!("failed to open zip {}", cache_path.display()))?;
        let mut trades = Vec::new();
        for index in 0..archive.len() {
            let file = archive.by_index(index)?;
            if !file.name().ends_with(".csv") {
                continue;
            }
            let reader = StdBufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.starts_with("aggTradeId") || line.trim().is_empty() {
                    continue;
                }
                if let Some(trade) = parse_binance_public_line(&symbol, line.trim()) {
                    trades.push(trade);
                }
            }
        }
        Ok(trades)
    })
    .await?
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn parses_public_trade_line() {
        let line = "1585180700.0647,BTCUSDT,Buy,0.042,6698.5,PlusTick,08ff9568-cb50-55d6-b497-13727eec09dc,28133700000.0,0.042,281.337";
        let trade = parse_bybit_public_line("BTCUSDT", line).expect("trade");
        assert_eq!(trade.tick.symbol.code(), "BTCUSDT");
        assert_eq!(
            trade.trade_id.as_deref(),
            Some("08ff9568-cb50-55d6-b497-13727eec09dc")
        );
        assert_eq!(trade.tick.side, Side::Buy);
        assert_eq!(trade.tick.price, Decimal::from_str("6698.5").unwrap());
        assert_eq!(trade.tick.size, Decimal::from_str("0.042").unwrap());
    }

    #[test]
    fn parses_public_timestamp_fractional_seconds() {
        let ts = parse_public_timestamp("1585180700.0647").expect("timestamp");
        assert_eq!(ts.timestamp(), 1_585_180_700);
        assert!(ts.timestamp_subsec_nanos() > 0);
    }

    #[test]
    fn parses_binance_public_line() {
        let line = "1001,51234.5,0.010,200,205,1585180700064,true,false";
        let trade = parse_binance_public_line("BTCUSDT", line).expect("trade");
        assert_eq!(trade.trade_id.as_deref(), Some("1001"));
        assert_eq!(trade.tick.price, Decimal::from_str("51234.5").unwrap());
        assert_eq!(trade.tick.side, Side::Sell);
    }
}
