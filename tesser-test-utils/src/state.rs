use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tokio::sync::{mpsc, Mutex};

use tesser_core::{
    AccountBalance, Candle, Fill, Order, OrderId, OrderStatus, Position, Price, Quantity, Side,
    Symbol, Tick,
};

use crate::scenario::ScenarioManager;

const EXEC_HISTORY_LIMIT: usize = 1024;
const DEFAULT_QUOTE_CURRENCY: &str = "USDT";

pub type ApiKey = String;

/// Message pushed onto the private WebSocket stream.
pub type PrivateMessage = serde_json::Value;

/// Shared state for the in-memory mock exchange.
#[derive(Clone)]
pub struct MockExchangeState {
    inner: Arc<Mutex<Inner>>,
    scenarios: ScenarioManager,
}

#[allow(dead_code)]
pub(crate) struct Inner {
    pub accounts: HashMap<ApiKey, AccountState>,
    pub market_data: MarketDataQueues,
    pub private_ws_sender: Option<mpsc::UnboundedSender<PrivateMessage>>,
    pub order_seq: u64,
}

#[derive(Clone)]
pub struct AccountState {
    pub api_secret: String,
    pub balances: HashMap<String, AccountBalance>,
    pub positions: HashMap<Symbol, Position>,
    pub executions: VecDeque<Fill>,
    pub orders: HashMap<OrderId, Order>,
}

impl AccountState {
    fn from_config(config: AccountConfig) -> Self {
        Self {
            api_secret: config.api_secret,
            balances: config
                .balances
                .into_iter()
                .map(|balance| (balance.currency.clone(), balance))
                .collect(),
            positions: config
                .positions
                .into_iter()
                .map(|position| (position.symbol.clone(), position))
                .collect(),
            executions: VecDeque::new(),
            orders: HashMap::new(),
        }
    }

    pub fn insert_order(&mut self, order: Order) {
        self.orders.insert(order.id.clone(), order);
    }

    pub fn update_order<F>(&mut self, order_id: &OrderId, mut update: F) -> Result<Order>
    where
        F: FnMut(&mut Order) -> Result<()>,
    {
        let order = self
            .orders
            .get_mut(order_id)
            .ok_or_else(|| anyhow!("unknown order id {order_id}"))?;
        update(order)?;
        Ok(order.clone())
    }

    pub fn apply_fill(&mut self, fill: &Fill) {
        self.executions.push_back(fill.clone());
        if self.executions.len() > EXEC_HISTORY_LIMIT {
            self.executions.pop_front();
        }
        self.update_positions(fill);
        self.update_balances(fill);
    }

    fn update_positions(&mut self, fill: &Fill) {
        let entry = self
            .positions
            .entry(fill.symbol.clone())
            .or_insert_with(|| Position {
                symbol: fill.symbol.clone(),
                side: None,
                quantity: Decimal::ZERO,
                entry_price: None,
                unrealized_pnl: Decimal::ZERO,
                updated_at: Utc::now(),
            });
        entry.updated_at = Utc::now();

        match (entry.side, fill.side) {
            (None, Side::Buy) => {
                entry.side = Some(Side::Buy);
                entry.quantity = fill.fill_quantity;
                entry.entry_price = Some(fill.fill_price);
            }
            (None, Side::Sell) => {
                entry.side = Some(Side::Sell);
                entry.quantity = fill.fill_quantity;
                entry.entry_price = Some(fill.fill_price);
            }
            (Some(Side::Buy), Side::Buy) => {
                let total_qty = entry.quantity + fill.fill_quantity;
                let existing_value = entry.entry_price.unwrap_or(Decimal::ZERO) * entry.quantity;
                let fill_value = fill.fill_price * fill.fill_quantity;
                entry.quantity = total_qty;
                entry.entry_price = Some((existing_value + fill_value) / total_qty);
            }
            (Some(Side::Sell), Side::Sell) => {
                let total_qty = entry.quantity + fill.fill_quantity;
                let existing_value = entry.entry_price.unwrap_or(Decimal::ZERO) * entry.quantity;
                let fill_value = fill.fill_price * fill.fill_quantity;
                entry.quantity = total_qty;
                entry.entry_price = Some((existing_value + fill_value) / total_qty);
            }
            (Some(Side::Buy), Side::Sell) => {
                if fill.fill_quantity < entry.quantity {
                    entry.quantity -= fill.fill_quantity;
                } else if fill.fill_quantity == entry.quantity {
                    entry.quantity = Decimal::ZERO;
                    entry.side = None;
                    entry.entry_price = None;
                } else {
                    entry.side = Some(Side::Sell);
                    entry.quantity = fill.fill_quantity - entry.quantity;
                    entry.entry_price = Some(fill.fill_price);
                }
            }
            (Some(Side::Sell), Side::Buy) => {
                if fill.fill_quantity < entry.quantity {
                    entry.quantity -= fill.fill_quantity;
                } else if fill.fill_quantity == entry.quantity {
                    entry.quantity = Decimal::ZERO;
                    entry.side = None;
                    entry.entry_price = None;
                } else {
                    entry.side = Some(Side::Buy);
                    entry.quantity = fill.fill_quantity - entry.quantity;
                    entry.entry_price = Some(fill.fill_price);
                }
            }
        }
    }

    fn update_balances(&mut self, fill: &Fill) {
        let quote = self
            .balances
            .entry(DEFAULT_QUOTE_CURRENCY.to_string())
            .or_insert(AccountBalance {
                currency: DEFAULT_QUOTE_CURRENCY.into(),
                total: Decimal::ZERO,
                available: Decimal::ZERO,
                updated_at: Utc::now(),
            });
        let notional = fill.fill_price * fill.fill_quantity;
        match fill.side {
            Side::Buy => {
                quote.total -= notional;
                quote.available = quote.total;
            }
            Side::Sell => {
                quote.total += notional;
                quote.available = quote.total;
            }
        }
        quote.updated_at = Utc::now();
    }

    pub fn order_by_link_id(&self, client_id: &str) -> Option<OrderId> {
        self.orders
            .values()
            .find(|order| order.request.client_order_id.as_deref() == Some(client_id))
            .map(|order| order.id.clone())
    }

    pub fn order(&self, order_id: &OrderId) -> Option<Order> {
        self.orders.get(order_id).cloned()
    }

    pub fn balances_snapshot(&self) -> Vec<AccountBalance> {
        self.balances.values().cloned().collect()
    }

    pub fn positions_snapshot(&self) -> Vec<Position> {
        self.positions.values().cloned().collect()
    }

    pub fn open_orders_snapshot(&self, symbol: Option<&str>) -> Vec<Order> {
        self.orders
            .values()
            .filter(|order| {
                let active = !matches!(
                    order.status,
                    OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
                );
                let symbol_matches = symbol
                    .map(|value| order.request.symbol == value)
                    .unwrap_or(true);
                active && symbol_matches
            })
            .cloned()
            .collect()
    }

    pub fn executions_in_range(
        &self,
        start: DateTime<Utc>,
        end: Option<DateTime<Utc>>,
    ) -> Vec<Fill> {
        self.executions
            .iter()
            .filter(|fill| {
                fill.timestamp >= start && end.map(|limit| fill.timestamp <= limit).unwrap_or(true)
            })
            .cloned()
            .collect()
    }
}

#[derive(Default)]
pub struct MarketDataQueues {
    pub candles: VecDeque<Candle>,
    pub ticks: VecDeque<Tick>,
}

impl MarketDataQueues {
    pub fn push_candle(&mut self, candle: Candle) {
        self.candles.push_back(candle);
    }

    pub fn push_tick(&mut self, tick: Tick) {
        self.ticks.push_back(tick);
    }

    pub fn next_candle(&mut self) -> Option<Candle> {
        self.candles.pop_front()
    }

    pub fn next_tick(&mut self) -> Option<Tick> {
        self.ticks.pop_front()
    }
}

/// Declarative account bootstrap configuration.
#[derive(Clone)]
pub struct AccountConfig {
    pub api_key: String,
    pub api_secret: String,
    pub balances: Vec<AccountBalance>,
    pub positions: Vec<Position>,
}

impl AccountConfig {
    pub fn new(api_key: impl Into<String>, api_secret: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            api_secret: api_secret.into(),
            balances: Vec::new(),
            positions: Vec::new(),
        }
    }

    pub fn with_balance(mut self, balance: AccountBalance) -> Self {
        self.balances.push(balance);
        self
    }

    pub fn with_position(mut self, position: Position) -> Self {
        self.positions.push(position);
        self
    }
}

/// Configuration object passed into [`MockExchangeState::new`].
#[derive(Clone)]
pub struct MockExchangeConfig {
    pub accounts: Vec<AccountConfig>,
    pub candles: Vec<Candle>,
    pub ticks: Vec<Tick>,
    pub scenarios: ScenarioManager,
}

impl MockExchangeConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_account(mut self, account: AccountConfig) -> Self {
        self.accounts.push(account);
        self
    }

    pub fn with_candles(mut self, candles: impl IntoIterator<Item = Candle>) -> Self {
        self.candles.extend(candles);
        self
    }

    pub fn with_ticks(mut self, ticks: impl IntoIterator<Item = Tick>) -> Self {
        self.ticks.extend(ticks);
        self
    }

    pub fn with_scenarios(mut self, scenarios: ScenarioManager) -> Self {
        self.scenarios = scenarios;
        self
    }
}

impl Default for MockExchangeConfig {
    fn default() -> Self {
        Self {
            accounts: Vec::new(),
            candles: Vec::new(),
            ticks: Vec::new(),
            scenarios: ScenarioManager::new(),
        }
    }
}

impl MockExchangeState {
    pub fn new(config: MockExchangeConfig) -> Self {
        let market_data = MarketDataQueues {
            candles: config.candles.into_iter().collect(),
            ticks: config.ticks.into_iter().collect(),
        };
        let accounts = config
            .accounts
            .into_iter()
            .map(|account| {
                let api_key = account.api_key.clone();
                (api_key, AccountState::from_config(account))
            })
            .collect();
        let inner = Inner {
            accounts,
            market_data,
            private_ws_sender: None,
            order_seq: 1,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
            scenarios: config.scenarios,
        }
    }

    pub fn scenarios(&self) -> ScenarioManager {
        self.scenarios.clone()
    }

    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &Arc<Mutex<Inner>> {
        &self.inner
    }

    pub async fn set_private_ws_sender(&self, sender: mpsc::UnboundedSender<PrivateMessage>) {
        let mut guard = self.inner.lock().await;
        guard.private_ws_sender = Some(sender);
    }

    pub async fn clear_private_ws_sender(&self) {
        let mut guard = self.inner.lock().await;
        guard.private_ws_sender = None;
    }

    pub async fn emit_private_message(&self, payload: PrivateMessage) -> Result<()> {
        let sender = {
            let guard = self.inner.lock().await;
            guard.private_ws_sender.clone()
        };
        if let Some(tx) = sender {
            tx.send(payload)
                .map_err(|err| anyhow!("failed to deliver private stream message: {err}"))
        } else {
            Ok(())
        }
    }

    pub async fn account_secret(&self, api_key: &str) -> Option<String> {
        let guard = self.inner.lock().await;
        guard
            .accounts
            .get(api_key)
            .map(|account| account.api_secret.clone())
    }

    pub async fn with_account_mut<F, T>(&self, api_key: &str, f: F) -> Result<T>
    where
        F: FnOnce(&mut AccountState) -> Result<T>,
    {
        let mut guard = self.inner.lock().await;
        let account = guard
            .accounts
            .get_mut(api_key)
            .ok_or_else(|| anyhow!("unknown API key {api_key}"))?;
        f(account)
    }

    pub async fn with_account<F, T>(&self, api_key: &str, f: F) -> Result<T>
    where
        F: FnOnce(&AccountState) -> Result<T>,
    {
        let guard = self.inner.lock().await;
        let account = guard
            .accounts
            .get(api_key)
            .ok_or_else(|| anyhow!("unknown API key {api_key}"))?;
        f(account)
    }

    pub async fn next_order_id(&self) -> OrderId {
        let mut guard = self.inner.lock().await;
        let id = guard.order_seq;
        guard.order_seq += 1;
        format!("MOCK-ORDER-{id}")
    }

    pub async fn register_order(&self, api_key: &str, order: Order) -> Result<Order> {
        self.with_account_mut(api_key, |account| {
            account.insert_order(order.clone());
            Ok(order)
        })
        .await
    }

    pub async fn get_order(&self, api_key: &str, order_id: &OrderId) -> Result<Order> {
        self.with_account(api_key, |account| {
            account
                .order(order_id)
                .ok_or_else(|| anyhow!("unknown order id {order_id}"))
        })
        .await
    }

    pub async fn find_order_id(
        &self,
        api_key: &str,
        order_id: Option<&str>,
        order_link_id: Option<&str>,
    ) -> Result<OrderId> {
        if let Some(id) = order_id {
            return Ok(id.to_string());
        }
        if let Some(link) = order_link_id {
            return self
                .with_account(api_key, |account| {
                    account
                        .order_by_link_id(link)
                        .ok_or_else(|| anyhow!("unknown order link id {link}"))
                })
                .await;
        }
        Err(anyhow!(
            "request must provide either orderId or orderLinkId"
        ))
    }

    pub async fn cancel_order(&self, api_key: &str, order_id: &OrderId) -> Result<Order> {
        self.with_account_mut(api_key, |account| {
            account.update_order(order_id, |order| {
                order.status = OrderStatus::Canceled;
                order.updated_at = Utc::now();
                Ok(())
            })
        })
        .await
    }

    pub async fn fill_order(
        &self,
        api_key: &str,
        order_id: &OrderId,
        quantity: Quantity,
        price: Price,
    ) -> Result<(Order, Fill)> {
        let mut guard = self.inner.lock().await;
        let account = guard
            .accounts
            .get_mut(api_key)
            .ok_or_else(|| anyhow!("unknown API key {api_key}"))?;
        let (fill, order_snapshot) = {
            let order = account
                .orders
                .get_mut(order_id)
                .ok_or_else(|| anyhow!("unknown order id {order_id}"))?;
            let remaining = (order.request.quantity - order.filled_quantity).max(Decimal::ZERO);
            if remaining.is_zero() {
                return Err(anyhow!("order already fully filled"));
            }
            let exec_quantity = quantity.min(remaining);
            if exec_quantity.is_zero() {
                return Err(anyhow!("fill quantity resolved to zero"));
            }
            let filled_before = order.filled_quantity;
            let new_filled = filled_before + exec_quantity;
            let avg_price = if filled_before.is_zero() {
                price
            } else {
                let previous_total = order.avg_fill_price.unwrap_or(price) * filled_before;
                (previous_total + price * exec_quantity) / new_filled
            };
            order.filled_quantity = new_filled;
            order.avg_fill_price = Some(avg_price);
            order.status = if new_filled >= order.request.quantity {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            };
            order.updated_at = Utc::now();
            let fill = Fill {
                order_id: order.id.clone(),
                symbol: order.request.symbol.clone(),
                side: order.request.side,
                fill_price: price,
                fill_quantity: exec_quantity,
                fee: None,
                timestamp: Utc::now(),
            };
            let order_snapshot = order.clone();
            (fill, order_snapshot)
        };
        account.apply_fill(&fill);
        Ok((order_snapshot, fill))
    }

    pub async fn account_balances(&self, api_key: &str) -> Result<Vec<AccountBalance>> {
        self.with_account(api_key, |account| Ok(account.balances_snapshot()))
            .await
    }

    pub async fn account_positions(&self, api_key: &str) -> Result<Vec<Position>> {
        self.with_account(api_key, |account| Ok(account.positions_snapshot()))
            .await
    }

    pub async fn open_orders(&self, api_key: &str, symbol: Option<&str>) -> Result<Vec<Order>> {
        self.with_account(api_key, |account| Ok(account.open_orders_snapshot(symbol)))
            .await
    }

    pub async fn executions_between(
        &self,
        api_key: &str,
        start: DateTime<Utc>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Vec<Fill>> {
        self.with_account(api_key, |account| {
            Ok(account.executions_in_range(start, end))
        })
        .await
    }

    pub async fn next_candle(&self) -> Option<Candle> {
        let mut guard = self.inner.lock().await;
        guard.market_data.next_candle()
    }

    pub async fn next_tick(&self) -> Option<Tick> {
        let mut guard = self.inner.lock().await;
        guard.market_data.next_tick()
    }
}
