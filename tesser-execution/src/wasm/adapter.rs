use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tesser_core::{
    Fill, OrderRequest, OrderType, OrderUpdateRequest, Quantity, Side, Signal, SignalKind, Symbol,
    Tick, TimeInForce,
};
use tesser_wasm::{
    PluginChildOrderAction, PluginChildOrderRequest, PluginFill, PluginInitContext,
    PluginOrderRequest, PluginOrderType, PluginOrderUpdateRequest, PluginResult, PluginRiskContext,
    PluginSide, PluginSignal, PluginTick, PluginTimeInForce,
};
use tracing::debug;
use uuid::Uuid;

use crate::algorithm::{AlgoStatus, ChildOrderAction, ChildOrderRequest, ExecutionAlgorithm};
use crate::RiskContext;

use super::engine::{WasmInstance, WasmPluginEngine};

const KIND: &str = "WASM_PLUGIN";

/// Serialized representation of a plugin-backed algorithm.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WasmAlgorithmState {
    pub plugin: PluginInitContext,
    pub plugin_state: Value,
    pub status: AlgoStatus,
    pub next_client_seq: u64,
}

/// Execution algorithm wrapper that delegates to a WASM plugin.
pub struct WasmAlgorithm {
    id: Uuid,
    status: AlgoStatus,
    started: bool,
    instance: Mutex<WasmInstance>,
    context: PluginInitContext,
    plugin_state: Value,
    next_client_seq: u64,
}

impl WasmAlgorithm {
    /// Build a plugin context from a signal and risk metadata.
    pub fn context_from_signal(
        plugin_name: &str,
        params: Value,
        signal: &Signal,
        total_quantity: Quantity,
        ctx: &RiskContext,
    ) -> PluginInitContext {
        let plugin_signal = PluginSignal {
            id: signal.id.to_string(),
            symbol: signal.symbol.code().to_string(),
            side: to_plugin_side(signal.kind.side()),
            kind: signal_kind_label(signal.kind).to_string(),
            confidence: signal.confidence,
            target_quantity: total_quantity.abs(),
            note: signal.note.clone(),
            group_id: signal.group_id.map(|id| id.to_string()),
        };
        let risk = PluginRiskContext {
            last_price: ctx.last_price,
            portfolio_equity: ctx.portfolio_equity,
            exchange_equity: ctx.exchange_equity,
            signed_position_qty: ctx.signed_position_qty,
            base_available: ctx.base_available,
            quote_available: ctx.quote_available,
            settlement_available: ctx.settlement_available,
            instrument_kind: ctx.instrument_kind.map(|kind| format!("{:?}", kind)),
        };
        let metadata = json!({
            "panic_behavior": signal.panic_behavior,
            "stop_loss": signal.stop_loss,
            "take_profit": signal.take_profit,
        });
        PluginInitContext {
            plugin: plugin_name.to_string(),
            params,
            signal: plugin_signal,
            risk,
            metadata,
        }
    }

    pub fn new(engine: Arc<WasmPluginEngine>, context: PluginInitContext) -> Result<Self> {
        let instance = engine
            .instantiate(&context.plugin)
            .with_context(|| format!("failed to instantiate plugin {}", context.plugin))?;
        Ok(Self {
            id: Uuid::new_v4(),
            status: AlgoStatus::Working,
            started: false,
            instance: Mutex::new(instance),
            context,
            plugin_state: Value::Null,
            next_client_seq: 0,
        })
    }

    pub fn from_snapshot(
        engine: Arc<WasmPluginEngine>,
        algo_id: Uuid,
        snapshot: WasmAlgorithmState,
    ) -> Result<Self> {
        let mut instance = engine.instantiate(&snapshot.plugin.plugin)?;
        let context_json = serde_json::to_string(&snapshot.plugin)?;
        let state_json = serde_json::to_string(&snapshot.plugin_state)?;
        // Initialize the plugin to rebuild any static state, then restore.
        let _ = instance.call_init(&context_json);
        instance.call_restore(&state_json)?;
        Ok(Self {
            id: algo_id,
            status: snapshot.status.clone(),
            started: true,
            instance: Mutex::new(instance),
            context: snapshot.plugin,
            plugin_state: snapshot.plugin_state,
            next_client_seq: snapshot.next_client_seq,
        })
    }

    fn context_json(&self) -> Result<String> {
        serde_json::to_string(&self.context).map_err(|err| anyhow!(err))
    }

    fn refresh_snapshot(&mut self) -> Result<()> {
        let mut instance = self
            .instance
            .lock()
            .map_err(|_| anyhow!("plugin instance poisoned"))?;
        let raw = instance.call_snapshot()?;
        self.plugin_state = serde_json::from_str(&raw)?;
        Ok(())
    }

    fn decode_result(&mut self, raw: String) -> Result<Vec<ChildOrderRequest>> {
        let result: PluginResult = serde_json::from_str(&raw)?;
        if !result.logs.is_empty() {
            for entry in result.logs {
                debug!(target: "plugin", plugin = %self.context.plugin, algo = %self.id, "{}", entry);
            }
        }
        if result.completed {
            self.status = AlgoStatus::Completed;
        } else {
            self.status = AlgoStatus::Working;
        }
        let mut mapped = Vec::with_capacity(result.orders.len());
        for req in result.orders {
            mapped.push(self.build_child_request(req)?);
        }
        Ok(mapped)
    }

    fn call_init(&mut self) -> Result<Vec<ChildOrderRequest>> {
        let payload = self.context_json()?;
        let raw = {
            let mut instance = self
                .instance
                .lock()
                .map_err(|_| anyhow!("plugin instance poisoned"))?;
            instance.call_init(&payload)?
        };
        let orders = self.decode_result(raw)?;
        self.refresh_snapshot()?;
        self.started = true;
        Ok(orders)
    }

    fn call_tick(&mut self, tick: &Tick) -> Result<Vec<ChildOrderRequest>> {
        let plugin_tick = to_plugin_tick(tick);
        let raw = {
            let mut instance = self
                .instance
                .lock()
                .map_err(|_| anyhow!("plugin instance poisoned"))?;
            instance.call_on_tick(&plugin_tick)?
        };
        let orders = self.decode_result(raw)?;
        self.refresh_snapshot()?;
        Ok(orders)
    }

    fn call_fill(&mut self, fill: &Fill) -> Result<Vec<ChildOrderRequest>> {
        let plugin_fill = to_plugin_fill(fill);
        let payload = serde_json::to_string(&plugin_fill)?;
        let raw = {
            let mut instance = self
                .instance
                .lock()
                .map_err(|_| anyhow!("plugin instance poisoned"))?;
            instance.call_on_fill(&payload)?
        };
        let orders = self.decode_result(raw)?;
        self.refresh_snapshot()?;
        Ok(orders)
    }

    fn call_timer(&mut self) -> Result<Vec<ChildOrderRequest>> {
        let raw = {
            let mut instance = self
                .instance
                .lock()
                .map_err(|_| anyhow!("plugin instance poisoned"))?;
            instance.call_on_timer()?
        };
        let orders = self.decode_result(raw)?;
        self.refresh_snapshot()?;
        Ok(orders)
    }

    fn build_child_request(&mut self, req: PluginChildOrderRequest) -> Result<ChildOrderRequest> {
        match req.action {
            PluginChildOrderAction::Place(order) => {
                let mut request = convert_order_request(order)?;
                self.ensure_client_id(&mut request);
                Ok(ChildOrderRequest {
                    parent_algo_id: self.id,
                    action: ChildOrderAction::Place(request),
                })
            }
            PluginChildOrderAction::Amend(update) => {
                let request = convert_order_update(update)?;
                Ok(ChildOrderRequest {
                    parent_algo_id: self.id,
                    action: ChildOrderAction::Amend(request),
                })
            }
        }
    }

    fn ensure_client_id(&mut self, order: &mut OrderRequest) {
        if order.client_order_id.is_none() {
            self.next_client_seq += 1;
            let id = format!("plugin-{}-{:04}", self.id.simple(), self.next_client_seq);
            order.client_order_id = Some(id);
        }
    }
}

impl ExecutionAlgorithm for WasmAlgorithm {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn id(&self) -> &Uuid {
        &self.id
    }

    fn status(&self) -> AlgoStatus {
        self.status.clone()
    }

    fn start(&mut self) -> Result<Vec<ChildOrderRequest>> {
        if self.started {
            return Ok(Vec::new());
        }
        self.call_init()
    }

    fn on_child_order_placed(&mut self, _order: &tesser_core::Order) {}

    fn on_fill(&mut self, fill: &Fill) -> Result<Vec<ChildOrderRequest>> {
        self.call_fill(fill)
    }

    fn bind_child_order(&mut self, _order: tesser_core::Order) -> Result<()> {
        Ok(())
    }

    fn on_tick(&mut self, tick: &Tick) -> Result<Vec<ChildOrderRequest>> {
        self.call_tick(tick)
    }

    fn on_timer(&mut self) -> Result<Vec<ChildOrderRequest>> {
        self.call_timer()
    }

    fn cancel(&mut self) -> Result<()> {
        self.status = AlgoStatus::Cancelled;
        Ok(())
    }

    fn state(&self) -> serde_json::Value {
        serde_json::to_value(WasmAlgorithmState {
            plugin: self.context.clone(),
            plugin_state: self.plugin_state.clone(),
            status: self.status.clone(),
            next_client_seq: self.next_client_seq,
        })
        .unwrap_or(Value::Null)
    }

    fn from_state(_state: serde_json::Value) -> Result<Self>
    where
        Self: Sized,
    {
        Err(anyhow!("use WasmAlgorithm::from_snapshot for restoration"))
    }
}

fn to_plugin_side(side: Side) -> PluginSide {
    match side {
        Side::Buy => PluginSide::Buy,
        Side::Sell => PluginSide::Sell,
    }
}

fn to_plugin_tick(tick: &Tick) -> PluginTick {
    PluginTick {
        symbol: tick.symbol.code().to_string(),
        price: tick.price,
        size: tick.size,
        side: to_plugin_side(tick.side),
        timestamp_ms: tesser_core::to_millis(&tick.exchange_timestamp),
    }
}

fn to_plugin_fill(fill: &Fill) -> PluginFill {
    PluginFill {
        order_id: fill.order_id.clone(),
        symbol: fill.symbol.code().to_string(),
        side: to_plugin_side(fill.side),
        fill_price: fill.fill_price,
        fill_quantity: fill.fill_quantity,
        fee: fill.fee,
        fee_asset: fill.fee_asset.map(|asset| asset.to_string()),
        timestamp_ms: tesser_core::to_millis(&fill.timestamp),
    }
}

fn convert_order_request(req: PluginOrderRequest) -> Result<OrderRequest> {
    let symbol = Symbol::from(req.symbol.as_str());
    let side = match req.side {
        PluginSide::Buy => Side::Buy,
        PluginSide::Sell => Side::Sell,
    };
    let order_type = match req.order_type {
        PluginOrderType::Market => OrderType::Market,
        PluginOrderType::Limit => OrderType::Limit,
    };
    let time_in_force = match req.time_in_force {
        Some(PluginTimeInForce::Gtc) => Some(TimeInForce::GoodTilCanceled),
        Some(PluginTimeInForce::Ioc) => Some(TimeInForce::ImmediateOrCancel),
        Some(PluginTimeInForce::Fok) => Some(TimeInForce::FillOrKill),
        Some(PluginTimeInForce::PostOnly) => None,
        None => None,
    };
    Ok(OrderRequest {
        symbol,
        side,
        order_type,
        quantity: req.quantity,
        price: req.price,
        trigger_price: req.trigger_price,
        time_in_force,
        client_order_id: req.client_order_id,
        take_profit: req.take_profit,
        stop_loss: req.stop_loss,
        display_quantity: req.display_quantity,
    })
}

fn convert_order_update(req: PluginOrderUpdateRequest) -> Result<OrderUpdateRequest> {
    let symbol = Symbol::from(req.symbol.as_str());
    let side = match req.side {
        PluginSide::Buy => Side::Buy,
        PluginSide::Sell => Side::Sell,
    };
    Ok(OrderUpdateRequest {
        order_id: req.order_id,
        symbol,
        side,
        new_price: req.new_price,
        new_quantity: req.new_quantity,
    })
}

fn signal_kind_label(kind: SignalKind) -> &'static str {
    match kind {
        SignalKind::EnterLong => "enter_long",
        SignalKind::ExitLong => "exit_long",
        SignalKind::EnterShort => "enter_short",
        SignalKind::ExitShort => "exit_short",
        SignalKind::Flatten => "flatten",
    }
}
