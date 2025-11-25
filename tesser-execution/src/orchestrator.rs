//! Order orchestrator for managing algorithmic execution.

use anyhow::{anyhow, bail, Result};
use chrono::Duration;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant};
use uuid::Uuid;

use crate::algorithm::{
    AlgoStatus, ChildOrderAction, ChildOrderRequest, ExecutionAlgorithm, IcebergAlgorithm,
    PeggedBestAlgorithm, SniperAlgorithm, TrailingStopAlgorithm, TwapAlgorithm, VwapAlgorithm,
};
use crate::repository::{AlgoStateRepository, StoredAlgoState};
use crate::{ExecutionEngine, PanicCloseConfig, PanicCloseMode, PanicObserver, RiskContext};
use tesser_core::{
    ExecutionHint, Fill, Order, OrderRequest, OrderStatus, OrderType, Price, Quantity, Side,
    Signal, SignalPanicBehavior, Symbol, Tick, TimeInForce,
};

/// Maps order IDs to their parent algorithm IDs for routing fills.
type OrderToAlgoMap = HashMap<String, Uuid>;

#[derive(Clone)]
struct PendingOrder {
    request: OrderRequest,
    placed_at: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExecutionLegStatus {
    Pending,
    Filled,
    Failed,
    Closed,
}

#[derive(Clone, Debug)]
struct ExecutionGroupLeg {
    symbol: Symbol,
    side: Side,
    target: Quantity,
    filled: Quantity,
    status: ExecutionLegStatus,
}

impl ExecutionGroupLeg {
    fn new(symbol: Symbol, side: Side) -> Self {
        Self {
            symbol,
            side,
            target: Decimal::ZERO,
            filled: Decimal::ZERO,
            status: ExecutionLegStatus::Pending,
        }
    }
}

struct ExecutionGroupState {
    legs: HashMap<Symbol, ExecutionGroupLeg>,
    panic_config: PanicCloseConfig,
    panic_triggered: bool,
}

impl ExecutionGroupState {
    fn new(config: PanicCloseConfig) -> Self {
        Self {
            legs: HashMap::new(),
            panic_config: config,
            panic_triggered: false,
        }
    }

    fn leg_entry(&mut self, symbol: Symbol, side: Side) -> &mut ExecutionGroupLeg {
        self.legs
            .entry(symbol)
            .or_insert_with(|| ExecutionGroupLeg::new(symbol, side))
    }

    fn is_complete(&self) -> bool {
        !self.legs.is_empty()
            && self
                .legs
                .values()
                .all(|leg| leg.status == ExecutionLegStatus::Filled)
    }

    fn is_finished(&self) -> bool {
        self.legs.values().all(|leg| {
            matches!(
                leg.status,
                ExecutionLegStatus::Filled
                    | ExecutionLegStatus::Failed
                    | ExecutionLegStatus::Closed
            )
        })
    }

    fn panic_actions(&mut self) -> Option<Vec<(Symbol, Side, Quantity)>> {
        if self.panic_triggered {
            return None;
        }
        let actions: Vec<_> = self
            .legs
            .values_mut()
            .filter(|leg| leg.filled > Decimal::ZERO)
            .map(|leg| {
                leg.status = ExecutionLegStatus::Closed;
                (leg.symbol, leg.side, leg.filled)
            })
            .collect();
        if actions.is_empty() {
            return None;
        }
        self.panic_triggered = true;
        Some(actions)
    }

    fn override_config(&mut self, config: PanicCloseConfig) {
        self.panic_config = config;
    }
}

pub const ORDER_TIMEOUT: StdDuration = StdDuration::from_secs(60);
pub const ORDER_POLL_INTERVAL: StdDuration = StdDuration::from_secs(15);

/// Core orchestrator for managing algorithmic order execution.
///
/// The orchestrator is responsible for:
/// - Creating and managing algorithm instances
/// - Routing events (fills, ticks, timers) to appropriate algorithms
/// - Persisting algorithm state for crash recovery
/// - Handling the lifecycle of algorithmic orders
#[derive(Clone)]
pub struct OrderOrchestrator {
    /// Active algorithm instances.
    algorithms: Arc<Mutex<HashMap<Uuid, Box<dyn ExecutionAlgorithm>>>>,

    /// Maps order IDs to their parent algorithm IDs.
    order_mapping: Arc<Mutex<OrderToAlgoMap>>,
    /// Tracks pending orders for timeout handling.
    pending_orders: Arc<Mutex<HashMap<String, PendingOrder>>>,

    /// Cached risk context per symbol supplied by the portfolio.
    risk_contexts: Arc<Mutex<HashMap<Symbol, RiskContext>>>,

    /// Underlying execution engine for placing child orders.
    execution_engine: Arc<ExecutionEngine>,

    /// State persistence backend.
    state_repo: Arc<dyn AlgoStateRepository<State = StoredAlgoState>>,

    panic_config: PanicCloseConfig,
    panic_observer: Option<Arc<dyn PanicObserver>>,

    /// Active multi-leg execution groups keyed by identifier.
    execution_groups: Arc<Mutex<HashMap<Uuid, ExecutionGroupState>>>,
    /// Maps order IDs to their execution group identifiers.
    group_order_mapping: Arc<Mutex<HashMap<String, (Uuid, Symbol)>>>,
}

impl OrderOrchestrator {
    /// Create a new orchestrator and restore any persisted algorithms.
    pub async fn new(
        execution_engine: Arc<ExecutionEngine>,
        state_repo: Arc<dyn AlgoStateRepository<State = StoredAlgoState>>,
        open_orders: Vec<Order>,
        panic_config: PanicCloseConfig,
        panic_observer: Option<Arc<dyn PanicObserver>>,
    ) -> Result<Self> {
        let algorithms = Arc::new(Mutex::new(HashMap::new()));
        let order_mapping = Arc::new(Mutex::new(HashMap::new()));
        let pending_orders = Arc::new(Mutex::new(HashMap::new()));
        let risk_contexts = Arc::new(Mutex::new(HashMap::new()));

        let orchestrator = Self {
            algorithms,
            order_mapping,
            pending_orders,
            risk_contexts,
            execution_engine,
            state_repo,
            panic_config,
            panic_observer,
            execution_groups: Arc::new(Mutex::new(HashMap::new())),
            group_order_mapping: Arc::new(Mutex::new(HashMap::new())),
        };

        // Restore algorithms from persistent state
        orchestrator.restore_algorithms().await?;
        orchestrator.adopt_open_orders(open_orders).await?;

        Ok(orchestrator)
    }

    /// Restore algorithms from persistent state.
    async fn restore_algorithms(&self) -> Result<()> {
        let states = self.state_repo.load_all()?;

        let mut algorithms = self.algorithms.lock().unwrap();

        let mut restored = 0usize;

        for (id, stored) in states {
            match Self::instantiate_algorithm(&stored.algo_type, stored.state.clone()) {
                Ok(algo) => {
                    tracing::info!(
                        id = %id,
                        algo_type = algo.kind(),
                        "Restored algorithm from state"
                    );
                    algorithms.insert(id, algo);
                    restored += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        id = %id,
                        algo_type = stored.algo_type,
                        error = %e,
                        "Failed to restore algorithm, deleting state"
                    );
                    if let Err(delete_err) = self.state_repo.delete(&id) {
                        tracing::error!(
                            id = %id,
                            error = %delete_err,
                            "Failed to delete corrupted algorithm state"
                        );
                    }
                }
            }
        }

        tracing::info!(
            count = restored,
            "Restored algorithms from persistent state"
        );

        Ok(())
    }

    async fn adopt_open_orders(&self, open_orders: Vec<Order>) -> Result<()> {
        if open_orders.is_empty() {
            return Ok(());
        }

        let mut touched_algorithms = HashSet::new();

        for order in open_orders {
            if !Self::is_active_order_status(order.status) {
                continue;
            }
            let Some(client_id) = order.request.client_order_id.clone() else {
                continue;
            };
            let Some(algo_id) = Self::extract_algo_id(&client_id) else {
                continue;
            };

            let mut should_register = false;
            {
                let mut algorithms = self.algorithms.lock().unwrap();
                if let Some(algo) = algorithms.get_mut(&algo_id) {
                    if let Err(err) = algo.bind_child_order(order.clone()) {
                        tracing::warn!(
                            algo_id = %algo_id,
                            order_id = %order.id,
                            error = %err,
                            "failed to bind recovered child order"
                        );
                    } else {
                        should_register = true;
                        touched_algorithms.insert(algo_id);
                    }
                } else {
                    tracing::debug!(
                        algo_id = %algo_id,
                        order_id = %order.id,
                        "no matching algorithm found for recovered order"
                    );
                }
            }

            if !should_register {
                continue;
            }

            {
                let mut mapping = self.order_mapping.lock().unwrap();
                mapping.insert(order.id.clone(), algo_id);
            }
            self.register_pending(&order);
            tracing::info!(
                algo_id = %algo_id,
                order_id = %order.id,
                symbol = %order.request.symbol,
                status = ?order.status,
                "adopted in-flight child order"
            );
        }

        for algo_id in touched_algorithms {
            if let Err(err) = self.persist_algo_state(&algo_id).await {
                tracing::error!(
                    algo_id = %algo_id,
                    error = %err,
                    "failed to persist algorithm state after adoption"
                );
            }
        }

        Ok(())
    }

    fn instantiate_algorithm(
        algo_type: &str,
        state: serde_json::Value,
    ) -> Result<Box<dyn ExecutionAlgorithm>> {
        match algo_type {
            "TWAP" => Ok(Box::new(TwapAlgorithm::from_state(state)?)),
            "VWAP" => Ok(Box::new(VwapAlgorithm::from_state(state)?)),
            "ICEBERG" => Ok(Box::new(IcebergAlgorithm::from_state(state)?)),
            "PEGGED_BEST" => Ok(Box::new(PeggedBestAlgorithm::from_state(state)?)),
            "SNIPER" => Ok(Box::new(SniperAlgorithm::from_state(state)?)),
            "TRAILING_STOP" => Ok(Box::new(TrailingStopAlgorithm::from_state(state)?)),
            other => bail!("unsupported algorithm type '{other}'"),
        }
    }

    /// Update the latest risk context for a symbol.
    pub fn update_risk_context(&self, symbol: Symbol, ctx: RiskContext) {
        let mut contexts = self.risk_contexts.lock().unwrap();
        contexts.insert(symbol, ctx);
    }

    fn cached_risk_context(&self, symbol: Symbol) -> Option<RiskContext> {
        let contexts = self.risk_contexts.lock().unwrap();
        contexts.get(&symbol).copied()
    }

    fn register_group_signal(&self, signal: &Signal) {
        if let Some(group_id) = signal.group_id {
            let mut groups = self.execution_groups.lock().unwrap();
            let default_config = self.panic_config;
            let state = groups
                .entry(group_id)
                .or_insert_with(|| ExecutionGroupState::new(default_config));
            if let Some(override_cfg) = self.panic_override_from_signal(signal) {
                state.override_config(override_cfg);
            }
            state.leg_entry(signal.symbol, signal.kind.side());
        }
    }

    fn panic_override_from_signal(&self, signal: &Signal) -> Option<PanicCloseConfig> {
        signal.panic_behavior.map(|behavior| match behavior {
            SignalPanicBehavior::Market => PanicCloseConfig {
                mode: PanicCloseMode::Market,
                ..self.panic_config
            },
            SignalPanicBehavior::AggressiveLimit { offset_bps } => PanicCloseConfig {
                mode: PanicCloseMode::AggressiveLimit,
                limit_offset_bps: offset_bps.max(Decimal::ZERO),
            },
        })
    }

    fn track_group_order(&self, group_id: Uuid, order: &Order) {
        {
            let mut groups = self.execution_groups.lock().unwrap();
            if let Some(state) = groups.get_mut(&group_id) {
                let leg = state.leg_entry(order.request.symbol, order.request.side);
                leg.target = order.request.quantity.abs();
                leg.side = order.request.side;
                leg.status = ExecutionLegStatus::Pending;
            }
        }
        let mut mapping = self.group_order_mapping.lock().unwrap();
        mapping.insert(order.id.clone(), (group_id, order.request.symbol));
    }

    fn purge_group_orders(&self, group_id: Uuid) {
        let mut mapping = self.group_order_mapping.lock().unwrap();
        mapping.retain(|_, (gid, _)| *gid != group_id);
    }

    fn handle_group_fill(&self, fill: &Fill) {
        let key = {
            let mapping = self.group_order_mapping.lock().unwrap();
            mapping.get(&fill.order_id).copied()
        };
        let Some((group_id, symbol)) = key else {
            return;
        };
        let mut remove_group = false;
        {
            let mut groups = self.execution_groups.lock().unwrap();
            if let Some(state) = groups.get_mut(&group_id) {
                if let Some(leg) = state.legs.get_mut(&symbol) {
                    leg.filled += fill.fill_quantity.abs();
                    if leg.filled >= leg.target && leg.status == ExecutionLegStatus::Pending {
                        leg.status = ExecutionLegStatus::Filled;
                        self.group_order_mapping
                            .lock()
                            .unwrap()
                            .remove(&fill.order_id);
                    }
                }
                if state.is_complete() {
                    remove_group = true;
                }
            }
            if remove_group {
                groups.remove(&group_id);
            }
        }
        if remove_group {
            self.purge_group_orders(group_id);
        }
    }

    async fn fail_group_leg_by_order(&self, order_id: &str, reason: &str) -> Result<()> {
        let key = {
            let mut mapping = self.group_order_mapping.lock().unwrap();
            mapping.remove(order_id)
        };
        if let Some((group_id, symbol)) = key {
            self.fail_group_leg(group_id, symbol, reason).await?;
        }
        Ok(())
    }

    async fn fail_group_leg(&self, group_id: Uuid, symbol: Symbol, reason: &str) -> Result<()> {
        let mut maybe_actions = None;
        let mut group_config = None;
        {
            let mut groups = self.execution_groups.lock().unwrap();
            if let Some(state) = groups.get_mut(&group_id) {
                if let Some(leg) = state.legs.get_mut(&symbol) {
                    leg.status = ExecutionLegStatus::Failed;
                }
                group_config = Some(state.panic_config);
                if let Some(actions) = state.panic_actions() {
                    maybe_actions = Some(actions);
                } else if state.is_finished() {
                    groups.remove(&group_id);
                }
            }
        }
        if let Some(actions) = maybe_actions {
            let config = group_config.unwrap_or(self.panic_config);
            self.execute_panic_actions(group_id, reason, actions, config)
                .await?;
            self.purge_group_orders(group_id);
        } else if group_config.is_some() {
            self.notify_panic(group_id, symbol, Decimal::ZERO, reason);
        }
        Ok(())
    }

    async fn execute_panic_actions(
        &self,
        group_id: Uuid,
        reason: &str,
        actions: Vec<(Symbol, Side, Quantity)>,
        config: PanicCloseConfig,
    ) -> Result<()> {
        for (symbol, original_side, qty) in actions {
            if qty <= Decimal::ZERO {
                continue;
            }
            let panic_side = match original_side {
                Side::Buy => Side::Sell,
                Side::Sell => Side::Buy,
            };
            let Some(ctx) = self.cached_risk_context(symbol) else {
                tracing::warn!(
                    %symbol,
                    group = %group_id,
                    "skipping panic close due to missing risk context"
                );
                continue;
            };
            let (order_type, price, time_in_force) =
                self.panic_order_parameters(config, panic_side, &ctx);
            let mut request = OrderRequest {
                symbol,
                side: panic_side,
                order_type,
                quantity: qty,
                price,
                trigger_price: None,
                time_in_force,
                client_order_id: Some(format!("panic-{group_id}")),
                take_profit: None,
                stop_loss: None,
                display_quantity: None,
            };
            let mut sent = self
                .execution_engine
                .send_order(request.clone(), &ctx)
                .await;
            if sent.is_err() && matches!(order_type, OrderType::Limit) {
                request.order_type = OrderType::Market;
                request.price = None;
                request.time_in_force = None;
                sent = self.execution_engine.send_order(request, &ctx).await;
            }
            if let Err(err) = sent {
                tracing::error!(
                    %symbol,
                    qty = %qty,
                    group = %group_id,
                    error = %err,
                    "failed to place panic close order"
                );
            } else {
                self.notify_panic(group_id, symbol, qty, reason);
                tracing::error!(
                    %symbol,
                    qty = %qty,
                    group = %group_id,
                    reason = reason,
                    "issued panic close to unwind stranded leg"
                );
            }
        }
        Ok(())
    }

    fn is_active_order_status(status: OrderStatus) -> bool {
        matches!(
            status,
            OrderStatus::PendingNew | OrderStatus::Accepted | OrderStatus::PartiallyFilled
        )
    }

    fn panic_order_parameters(
        &self,
        config: PanicCloseConfig,
        side: Side,
        ctx: &RiskContext,
    ) -> (OrderType, Option<Price>, Option<TimeInForce>) {
        match config.mode {
            PanicCloseMode::Market => (OrderType::Market, None, None),
            PanicCloseMode::AggressiveLimit => {
                if let Some(price) = self.panic_limit_price(config, side, ctx) {
                    (
                        OrderType::Limit,
                        Some(price),
                        Some(TimeInForce::ImmediateOrCancel),
                    )
                } else {
                    (OrderType::Market, None, None)
                }
            }
        }
    }

    fn panic_limit_price(
        &self,
        config: PanicCloseConfig,
        side: Side,
        ctx: &RiskContext,
    ) -> Option<Price> {
        let last = ctx.last_price;
        if last <= Decimal::ZERO {
            return None;
        }
        let offset_fraction = (config.limit_offset_bps.max(Decimal::ZERO)
            / Decimal::from(10_000u32))
        .max(Decimal::ZERO);
        let multiplier = match side {
            Side::Buy => Decimal::ONE + offset_fraction,
            Side::Sell => Decimal::ONE - offset_fraction,
        };
        if multiplier <= Decimal::ZERO {
            return None;
        }
        Some(last * multiplier)
    }

    fn notify_panic(&self, group_id: Uuid, symbol: Symbol, qty: Quantity, reason: &str) {
        if let Some(observer) = &self.panic_observer {
            observer.on_group_event(group_id, symbol, qty, reason);
        }
    }

    fn extract_algo_id(client_id: &str) -> Option<Uuid> {
        if let Some(rest) = client_id.strip_prefix("twap-") {
            let (id_part, _) = rest.split_once("-slice-")?;
            return Uuid::parse_str(id_part).ok();
        }
        if let Some(rest) = client_id.strip_prefix("iceberg-") {
            let (id_part, _) = rest.rsplit_once('-')?;
            return Uuid::parse_str(id_part).ok();
        }
        if let Some(rest) = client_id.strip_prefix("peg-") {
            let (id_part, _) = rest.rsplit_once('-')?;
            return Uuid::parse_str(id_part).ok();
        }
        if let Some(rest) = client_id.strip_prefix("vwap-") {
            let (id_part, _) = rest.rsplit_once('-')?;
            return Uuid::parse_str(id_part).ok();
        }
        if let Some(rest) = client_id.strip_prefix("sniper-") {
            return Uuid::parse_str(rest).ok();
        }
        if let Some(rest) = client_id.strip_prefix("trailing-") {
            return Uuid::parse_str(rest).ok();
        }
        None
    }

    fn group_from_request(request: &OrderRequest) -> Option<Uuid> {
        request
            .client_order_id
            .as_deref()
            .and_then(|value| value.split("|grp:").nth(1))
            .and_then(|suffix| suffix.split('|').next())
            .and_then(|raw| Uuid::parse_str(raw).ok())
    }

    /// Handle a signal from a strategy.
    pub async fn on_signal(&self, signal: &Signal, ctx: &RiskContext) -> Result<()> {
        match &signal.execution_hint {
            Some(ExecutionHint::Twap { duration }) => {
                self.handle_twap_signal(signal.clone(), *duration, ctx)
                    .await
            }
            Some(ExecutionHint::Vwap {
                duration,
                participation_rate,
            }) => {
                self.handle_vwap_signal(signal.clone(), *duration, *participation_rate, ctx)
                    .await
            }
            Some(ExecutionHint::IcebergSimulated {
                display_size,
                limit_offset_bps,
            }) => {
                self.handle_iceberg_signal(signal.clone(), *display_size, *limit_offset_bps, ctx)
                    .await
            }
            Some(ExecutionHint::PeggedBest {
                offset_bps,
                clip_size,
                refresh_secs,
                min_chase_distance,
            }) => {
                self.handle_pegged_signal(
                    signal.clone(),
                    *offset_bps,
                    *clip_size,
                    *refresh_secs,
                    *min_chase_distance,
                    ctx,
                )
                .await
            }
            Some(ExecutionHint::Sniper {
                trigger_price,
                timeout,
            }) => {
                self.handle_sniper_signal(signal.clone(), *trigger_price, *timeout, ctx)
                    .await
            }
            Some(ExecutionHint::TrailingStop {
                activation_price,
                callback_rate,
            }) => {
                self.handle_trailing_stop_signal(
                    signal.clone(),
                    *activation_price,
                    *callback_rate,
                    ctx,
                )
                .await
            }
            None => {
                // Handle normal, non-algorithmic orders
                self.register_group_signal(signal);
                match self
                    .execution_engine
                    .handle_signal(signal.clone(), *ctx)
                    .await
                {
                    Ok(Some(order)) => {
                        if let Some(group_id) = signal.group_id {
                            self.track_group_order(group_id, &order);
                        }
                        self.register_pending(&order);
                        Ok(())
                    }
                    Ok(None) => {
                        if let Some(group_id) = signal.group_id {
                            self.fail_group_leg(
                                group_id,
                                signal.symbol,
                                "order size resolved to zero",
                            )
                            .await?;
                        }
                        Ok(())
                    }
                    Err(err) => {
                        if let Some(group_id) = signal.group_id {
                            if let Err(panic_err) = self
                                .fail_group_leg(group_id, signal.symbol, "order routing error")
                                .await
                            {
                                tracing::error!(
                                    %panic_err,
                                    %group_id,
                                    symbol = %signal.symbol,
                                    "failed to trigger panic-close after routing failure"
                                );
                            }
                        }
                        Err(err.into())
                    }
                }
            }
        }
    }

    /// Handle a TWAP signal by creating a new TWAP algorithm instance.
    async fn handle_twap_signal(
        &self,
        signal: Signal,
        duration: Duration,
        ctx: &RiskContext,
    ) -> Result<()> {
        self.update_risk_context(signal.symbol, *ctx);
        // Calculate total quantity using the execution engine's sizing logic
        let total_quantity = self.execution_engine.determine_quantity(&signal, ctx)?;

        if total_quantity <= Decimal::ZERO {
            tracing::warn!("TWAP order size is zero, skipping");
            return Ok(());
        }

        // Use a sensible default for number of slices (seconds granularity for shorter runs)
        // TODO: Make this configurable
        let mut slice_guess = duration.num_minutes() as u32;
        if slice_guess == 0 {
            let seconds = duration.num_seconds().max(1);
            slice_guess = seconds as u32;
        }
        let num_slices = slice_guess.clamp(1, 30);

        // Create and start the algorithm
        let mut algo = TwapAlgorithm::new(signal, total_quantity, duration, num_slices)?;
        let algo_id = *algo.id();

        tracing::info!(
            id = %algo_id,
            total_qty = %total_quantity,
            duration_mins = duration.num_minutes(),
            slices = num_slices,
            "Starting new TWAP algorithm"
        );

        // Start the algorithm and get any initial orders
        let initial_orders = algo.start()?;

        // Add to active algorithms
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            algorithms.insert(algo_id, Box::new(algo));
        }

        // Persist initial state
        self.persist_algo_state(&algo_id).await?;

        // Send initial orders (if any)
        for child_req in initial_orders {
            self.send_child_order(child_req, Some(*ctx)).await?;
        }

        Ok(())
    }

    async fn handle_vwap_signal(
        &self,
        signal: Signal,
        duration: Duration,
        participation_rate: Option<Decimal>,
        ctx: &RiskContext,
    ) -> Result<()> {
        self.update_risk_context(signal.symbol, *ctx);
        let total_quantity = self.execution_engine.determine_quantity(&signal, ctx)?;
        if total_quantity <= Decimal::ZERO {
            tracing::warn!("VWAP order size is zero, skipping");
            return Ok(());
        }
        let mut algo = VwapAlgorithm::new(signal, total_quantity, duration, participation_rate)?;
        let algo_id = *algo.id();
        tracing::info!(
            id = %algo_id,
            qty = %total_quantity,
            duration_mins = duration.num_minutes(),
            participation = ?participation_rate,
            "Starting new VWAP algorithm"
        );

        let initial_orders = algo.start()?;
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            algorithms.insert(algo_id, Box::new(algo));
        }
        self.persist_algo_state(&algo_id).await?;
        for child in initial_orders {
            self.send_child_order(child, Some(*ctx)).await?;
        }
        Ok(())
    }

    async fn handle_iceberg_signal(
        &self,
        signal: Signal,
        display_size: Quantity,
        limit_offset_bps: Option<Decimal>,
        ctx: &RiskContext,
    ) -> Result<()> {
        self.update_risk_context(signal.symbol, *ctx);
        let total_quantity = self.execution_engine.determine_quantity(&signal, ctx)?;
        if total_quantity <= Decimal::ZERO {
            tracing::warn!("Iceberg order size is zero, skipping");
            return Ok(());
        }
        let limit_price = if ctx.last_price > Decimal::ZERO {
            ctx.last_price
        } else {
            tracing::warn!(
                "last price unavailable for iceberg order; defaulting to 1.0 for {}",
                signal.symbol
            );
            Decimal::ONE
        };

        let mut algo = IcebergAlgorithm::new(
            signal,
            total_quantity,
            display_size,
            limit_price,
            limit_offset_bps,
        )?;
        let algo_id = *algo.id();
        tracing::info!(
            id = %algo_id,
            qty = %total_quantity,
            display = %display_size,
            "Starting new Iceberg algorithm"
        );

        let initial_orders = algo.start()?;
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            algorithms.insert(algo_id, Box::new(algo));
        }
        self.persist_algo_state(&algo_id).await?;
        for child in initial_orders {
            self.send_child_order(child, Some(*ctx)).await?;
        }
        Ok(())
    }

    async fn handle_pegged_signal(
        &self,
        signal: Signal,
        offset_bps: Decimal,
        clip_size: Option<Quantity>,
        refresh_secs: Option<u64>,
        min_chase_distance: Option<Price>,
        ctx: &RiskContext,
    ) -> Result<()> {
        self.update_risk_context(signal.symbol, *ctx);
        let total_quantity = self.execution_engine.determine_quantity(&signal, ctx)?;
        if total_quantity <= Decimal::ZERO {
            tracing::warn!("Pegged order size is zero, skipping");
            return Ok(());
        }
        let secs = refresh_secs.unwrap_or(1).max(1) as i64;
        let refresh = Duration::seconds(secs);
        let mut algo = PeggedBestAlgorithm::new(
            signal,
            total_quantity,
            offset_bps,
            clip_size,
            refresh,
            min_chase_distance,
        )?;
        let algo_id = *algo.id();
        tracing::info!(
            id = %algo_id,
            qty = %total_quantity,
            offset = %offset_bps,
            chase = ?min_chase_distance,
            "Starting new PeggedBest algorithm"
        );
        let initial_orders = algo.start()?;
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            algorithms.insert(algo_id, Box::new(algo));
        }
        self.persist_algo_state(&algo_id).await?;
        for child in initial_orders {
            self.send_child_order(child, Some(*ctx)).await?;
        }
        Ok(())
    }

    async fn handle_sniper_signal(
        &self,
        signal: Signal,
        trigger_price: Price,
        timeout: Option<Duration>,
        ctx: &RiskContext,
    ) -> Result<()> {
        self.update_risk_context(signal.symbol, *ctx);
        let total_quantity = self.execution_engine.determine_quantity(&signal, ctx)?;
        if total_quantity <= Decimal::ZERO {
            tracing::warn!("Sniper order size is zero, skipping");
            return Ok(());
        }
        let mut algo = SniperAlgorithm::new(signal, total_quantity, trigger_price, timeout)?;
        let algo_id = *algo.id();
        tracing::info!(id = %algo_id, qty = %total_quantity, "Starting new Sniper algorithm");
        let initial_orders = algo.start()?;
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            algorithms.insert(algo_id, Box::new(algo));
        }
        self.persist_algo_state(&algo_id).await?;
        for child in initial_orders {
            self.send_child_order(child, Some(*ctx)).await?;
        }
        Ok(())
    }

    async fn handle_trailing_stop_signal(
        &self,
        signal: Signal,
        activation_price: Price,
        callback_rate: Decimal,
        ctx: &RiskContext,
    ) -> Result<()> {
        self.update_risk_context(signal.symbol, *ctx);
        let total_quantity = self.execution_engine.determine_quantity(&signal, ctx)?;
        if total_quantity <= Decimal::ZERO {
            tracing::warn!("Trailing stop order size is zero, skipping");
            return Ok(());
        }
        let mut algo =
            TrailingStopAlgorithm::new(signal, total_quantity, activation_price, callback_rate)?;
        let algo_id = *algo.id();
        tracing::info!(
            id = %algo_id,
            qty = %total_quantity,
            activation = %activation_price,
            callback = %callback_rate,
            "Starting new TrailingStop algorithm"
        );
        let initial_orders = algo.start()?;
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            algorithms.insert(algo_id, Box::new(algo));
        }
        self.persist_algo_state(&algo_id).await?;
        for child in initial_orders {
            self.send_child_order(child, Some(*ctx)).await?;
        }
        Ok(())
    }

    /// Send a child order and track the mapping.
    async fn send_child_order(
        &self,
        child_req: ChildOrderRequest,
        ctx: Option<RiskContext>,
    ) -> Result<Order> {
        let parent_algo_id = child_req.parent_algo_id;
        match child_req.action {
            ChildOrderAction::Place(order_request) => {
                let symbol = order_request.symbol;
                tracing::debug!(%symbol, "Sending child order");
                let resolved_ctx = ctx
                    .or_else(|| self.cached_risk_context(symbol))
                    .ok_or_else(|| anyhow!("missing risk context for symbol {}", symbol))?;
                // Keep cache warm with the latest context.
                self.update_risk_context(symbol, resolved_ctx);
                let group_hint = Self::group_from_request(&order_request);
                let order = match self
                    .execution_engine
                    .send_order(order_request.clone(), &resolved_ctx)
                    .await
                {
                    Ok(order) => order,
                    Err(err) => {
                        if let Some(group_id) = group_hint {
                            let message = format!("order placement failed: {err}");
                            self.fail_group_leg(group_id, symbol, &message).await?;
                        }
                        return Err(anyhow!(err.to_string()));
                    }
                };

                {
                    let mut mapping = self.order_mapping.lock().unwrap();
                    mapping.insert(order.id.clone(), parent_algo_id);
                }
                self.register_pending(&order);
                if let Some(group_id) = group_hint {
                    self.track_group_order(group_id, &order);
                }
                self.notify_algo_child(parent_algo_id, &order);
                Ok(order)
            }
            ChildOrderAction::Amend(update_request) => {
                let order = self.execution_engine.amend_order(update_request).await?;
                self.refresh_pending(&order);
                self.ensure_order_mapping(&order.id, parent_algo_id);
                self.notify_algo_child(parent_algo_id, &order);
                Ok(order)
            }
        }
    }

    /// Handle a fill from the execution engine.
    pub async fn on_fill(&self, fill: &Fill) -> Result<()> {
        // Find the parent algorithm for this fill
        let parent_algo_id = {
            let mapping = self.order_mapping.lock().unwrap();
            mapping.get(&fill.order_id).copied()
        };
        self.clear_pending(&fill.order_id);
        self.handle_group_fill(fill);

        let Some(algo_id) = parent_algo_id else {
            // Non-algorithmic fill handled via group bookkeeping
            return Ok(());
        };

        tracing::debug!(
            algo_id = %algo_id,
            order_id = %fill.order_id,
            fill_qty = %fill.fill_quantity,
            "Routing fill to algorithm"
        );

        let mut new_child_orders = Vec::new();
        let mut algo_completed = false;

        // Process the fill with the algorithm
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            if let Some(algo) = algorithms.get_mut(&algo_id) {
                match algo.on_fill(fill) {
                    Ok(orders) => {
                        new_child_orders = orders;
                        if !matches!(algo.status(), AlgoStatus::Working) {
                            algo_completed = true;
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            algo_id = %algo_id,
                            error = %e,
                            "Algorithm failed to process fill"
                        );
                        // Mark the algorithm as failed
                        let _ = algo.cancel();
                        algo_completed = true;
                    }
                }
            }
        }

        // Send new child orders if any
        for child_req in &new_child_orders {
            if let Err(e) = self.send_child_order(child_req.clone(), None).await {
                tracing::error!(
                    algo_id = %algo_id,
                    error = %e,
                    "Failed to send child order"
                );
            }
        }

        // Persist updated state
        self.persist_algo_state(&algo_id).await?;

        // Clean up completed algorithm
        if algo_completed {
            self.cleanup_algo(&algo_id).await?;
        }

        Ok(())
    }

    /// Handle market tick data.
    pub async fn on_tick(&self, tick: &Tick) -> Result<()> {
        let mut algorithms_to_process = Vec::new();

        // Collect algorithms that might be interested in this tick
        {
            let algorithms = self.algorithms.lock().unwrap();
            for (id, algo) in algorithms.iter() {
                if matches!(algo.status(), AlgoStatus::Working) {
                    algorithms_to_process.push(*id);
                }
            }
        }

        // Process tick with each algorithm
        for algo_id in algorithms_to_process {
            let mut new_child_orders = Vec::new();
            let mut algo_completed = false;

            {
                let mut algorithms = self.algorithms.lock().unwrap();
                if let Some(algo) = algorithms.get_mut(&algo_id) {
                    match algo.on_tick(tick) {
                        Ok(orders) => {
                            new_child_orders = orders;
                            if !matches!(algo.status(), AlgoStatus::Working) {
                                algo_completed = true;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                algo_id = %algo_id,
                                error = %e,
                                "Algorithm failed to process tick"
                            );
                        }
                    }
                }
            }

            // Send new child orders
            let has_orders = !new_child_orders.is_empty();
            for child_req in new_child_orders {
                if let Err(e) = self.send_child_order(child_req, None).await {
                    tracing::error!(
                        algo_id = %algo_id,
                        error = %e,
                        "Failed to send child order from tick"
                    );
                }
            }

            // Handle state persistence and cleanup
            if has_orders || algo_completed {
                self.persist_algo_state(&algo_id).await?;
            }

            if algo_completed {
                self.cleanup_algo(&algo_id).await?;
            }
        }

        Ok(())
    }

    /// Handle timer events (mainly for TWAP algorithms).
    pub async fn on_timer_tick(&self) -> Result<()> {
        let mut completed_ids = Vec::new();
        let mut new_child_orders: HashMap<Uuid, Vec<ChildOrderRequest>> = HashMap::new();

        // Process timer event for all working algorithms
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            for (id, algo) in algorithms.iter_mut() {
                if matches!(algo.status(), AlgoStatus::Working) {
                    match algo.on_timer() {
                        Ok(requests) if !requests.is_empty() => {
                            new_child_orders.insert(*id, requests);
                        }
                        Ok(_) => {
                            // No new orders, but check if algorithm completed
                            if !matches!(algo.status(), AlgoStatus::Working) {
                                completed_ids.push(*id);
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                id = %id,
                                error = %e,
                                "Algorithm on_timer failed"
                            );
                            // Mark as failed
                            let _ = algo.cancel();
                            completed_ids.push(*id);
                        }
                    }
                } else {
                    // Algorithm is no longer working
                    completed_ids.push(*id);
                }
            }
        }

        // Send new child orders (outside the lock)
        for (id, requests) in new_child_orders {
            for req in requests {
                if let Err(e) = self.send_child_order(req, None).await {
                    tracing::error!(id = %id, error = %e, "Failed to send child order");
                }
            }
            self.persist_algo_state(&id).await?;
        }

        // Clean up completed algorithms
        for id in completed_ids {
            self.cleanup_algo(&id).await?;
        }

        Ok(())
    }

    /// Cancel an algorithmic order.
    pub async fn cancel_algo(&self, algo_id: &Uuid) -> Result<()> {
        let mut algo_completed = false;

        // Cancel the algorithm
        {
            let mut algorithms = self.algorithms.lock().unwrap();
            if let Some(algo) = algorithms.get_mut(algo_id) {
                algo.cancel()?;
                algo_completed = true;
            }
        }

        if algo_completed {
            self.persist_algo_state(algo_id).await?;
            self.cleanup_algo(algo_id).await?;
        }

        Ok(())
    }

    /// Persist the state of a specific algorithm.
    async fn persist_algo_state(&self, id: &Uuid) -> Result<()> {
        let payload = {
            let algorithms = self.algorithms.lock().unwrap();
            let algo = algorithms
                .get(id)
                .ok_or_else(|| anyhow!("Algorithm not found for persistence: {}", id))?;
            StoredAlgoState {
                algo_type: algo.kind().to_string(),
                state: algo.state(),
            }
        };

        self.state_repo.save(id, &payload)?;
        Ok(())
    }

    /// Clean up a completed algorithm.
    async fn cleanup_algo(&self, id: &Uuid) -> Result<()> {
        let status = {
            let mut algorithms = self.algorithms.lock().unwrap();
            let status = algorithms.get(id).map(|algo| algo.status());
            algorithms.remove(id);
            status
        };

        // Clean up order mappings
        {
            let mut mapping = self.order_mapping.lock().unwrap();
            mapping.retain(|order_id, algo_id| {
                let retain = algo_id != id;
                if !retain {
                    self.clear_pending(order_id);
                }
                retain
            });
        }

        // Delete persistent state
        self.state_repo.delete(id)?;

        tracing::info!(
            id = %id,
            status = ?status,
            "Algorithm cleaned up"
        );

        Ok(())
    }

    /// Get the number of active algorithms.
    pub fn active_algorithms_count(&self) -> usize {
        let algorithms = self.algorithms.lock().unwrap();
        algorithms.len()
    }

    /// Get a snapshot of algorithm statuses for monitoring.
    pub fn algorithm_statuses(&self) -> HashMap<Uuid, AlgoStatus> {
        let algorithms = self.algorithms.lock().unwrap();
        algorithms
            .iter()
            .map(|(id, algo)| (*id, algo.status()))
            .collect()
    }

    /// Access to the underlying execution engine.
    pub fn execution_engine(&self) -> Arc<ExecutionEngine> {
        Arc::clone(&self.execution_engine)
    }

    /// Remove a pending order when an update arrives.
    pub async fn on_order_update(&self, order: &Order) {
        if matches!(
            order.status,
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
        ) {
            self.clear_pending(&order.id);
            if matches!(order.status, OrderStatus::Canceled | OrderStatus::Rejected) {
                let reason = format!("order marked {:?}", order.status);
                let _ = self.fail_group_leg_by_order(&order.id, &reason).await;
            }
        }
    }

    /// Poll the exchange for any pending orders that exceeded the timeout.
    /// Returns synthesized order updates for downstream handling.
    pub async fn poll_stale_orders(&self) -> Result<Vec<Order>> {
        let stale: Vec<(String, PendingOrder)> = {
            let mut pending = self.pending_orders.lock().unwrap();
            let now = Instant::now();
            let expired: Vec<String> = pending
                .iter()
                .filter_map(|(id, entry)| {
                    if now.duration_since(entry.placed_at) >= ORDER_TIMEOUT {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect();
            expired
                .into_iter()
                .filter_map(|id| pending.remove(&id).map(|entry| (id, entry)))
                .collect()
        };

        let mut updates = Vec::new();
        for (order_id, entry) in stale {
            let client = self.execution_engine.client();
            let synthesized = match client.list_open_orders(entry.request.symbol).await {
                Ok(remote_orders) => {
                    if let Some(existing) =
                        remote_orders.into_iter().find(|order| order.id == order_id)
                    {
                        // If the order is still working, cancel it proactively.
                        if matches!(
                            existing.status,
                            OrderStatus::PendingNew
                                | OrderStatus::Accepted
                                | OrderStatus::PartiallyFilled
                        ) {
                            let _ = client
                                .cancel_order(order_id.clone(), entry.request.symbol)
                                .await;
                            let mut canceled = existing.clone();
                            canceled.status = OrderStatus::Canceled;
                            canceled.updated_at = chrono::Utc::now();
                            Some(canceled)
                        } else {
                            Some(existing)
                        }
                    } else {
                        Some(build_timeout_order(
                            order_id.clone(),
                            entry.request.clone(),
                            OrderStatus::Rejected,
                        ))
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        order_id = %order_id,
                        symbol = %entry.request.symbol,
                        error = %err,
                        "failed to query stale order; canceling defensively"
                    );
                    let _ = client
                        .cancel_order(order_id.clone(), entry.request.symbol)
                        .await;
                    Some(build_timeout_order(
                        order_id.clone(),
                        entry.request.clone(),
                        OrderStatus::Canceled,
                    ))
                }
            };

            if let Some(order) = synthesized {
                self.on_order_update(&order).await;
                self.clear_order_mapping(&order_id);
                updates.push(order);
            }
        }

        Ok(updates)
    }

    fn register_pending(&self, order: &Order) {
        let mut pending = self.pending_orders.lock().unwrap();
        pending.insert(
            order.id.clone(),
            PendingOrder {
                request: order.request.clone(),
                placed_at: Instant::now(),
            },
        );
    }

    fn clear_pending(&self, order_id: &str) {
        let mut pending = self.pending_orders.lock().unwrap();
        pending.remove(order_id);
    }

    fn refresh_pending(&self, order: &Order) {
        let mut pending = self.pending_orders.lock().unwrap();
        pending
            .entry(order.id.clone())
            .and_modify(|entry| {
                entry.request = order.request.clone();
                entry.placed_at = Instant::now();
            })
            .or_insert(PendingOrder {
                request: order.request.clone(),
                placed_at: Instant::now(),
            });
    }

    fn clear_order_mapping(&self, order_id: &str) {
        let mut mapping = self.order_mapping.lock().unwrap();
        mapping.remove(order_id);
    }

    fn ensure_order_mapping(&self, order_id: &str, algo_id: Uuid) {
        let mut mapping = self.order_mapping.lock().unwrap();
        mapping.entry(order_id.to_string()).or_insert(algo_id);
    }

    fn notify_algo_child(&self, algo_id: Uuid, order: &Order) {
        let mut algorithms = self.algorithms.lock().unwrap();
        if let Some(algo) = algorithms.get_mut(&algo_id) {
            algo.on_child_order_placed(order);
        }
    }
}

fn build_timeout_order(id: String, request: OrderRequest, status: OrderStatus) -> Order {
    Order {
        id,
        request,
        status,
        filled_quantity: Decimal::ZERO,
        avg_fill_price: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}
