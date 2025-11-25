use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use uuid::Uuid;

use crate::{BrokerError, BrokerInfo, BrokerResult, ExecutionClient};
use tesser_core::{
    AccountBalance, ExchangeId, Instrument, Order, OrderRequest, OrderUpdateRequest, Position,
    Symbol,
};

const CLIENT_NAMESPACE: &str = "TESSER";

/// Execution client that routes orders to venue-specific connectors using the
/// [`Symbol`] metadata embedded in each [`OrderRequest`].
pub struct RouterExecutionClient {
    routes: HashMap<ExchangeId, Arc<dyn ExecutionClient>>,
    info: BrokerInfo,
    state: Mutex<RouteState>,
}

impl RouterExecutionClient {
    /// Build a router from the provided map of exchange -> execution client.
    ///
    /// # Panics
    ///
    /// Panics when no routes are supplied.
    pub fn new(routes: HashMap<ExchangeId, Arc<dyn ExecutionClient>>) -> Self {
        assert!(
            !routes.is_empty(),
            "router execution client requires at least one route"
        );
        let info = aggregate_info(&routes);
        Self {
            routes,
            info,
            state: Mutex::new(RouteState::default()),
        }
    }

    fn client_for(&self, exchange: ExchangeId) -> BrokerResult<Arc<dyn ExecutionClient>> {
        self.routes.get(&exchange).cloned().ok_or_else(|| {
            BrokerError::InvalidRequest(format!(
                "no execution client registered for exchange {}",
                exchange
            ))
        })
    }

    fn record_order(
        &self,
        internal_id: &str,
        exchange: ExchangeId,
        symbol: Symbol,
        external_id: String,
        original_client_id: Option<String>,
    ) {
        let mut guard = self.state.lock().unwrap();
        guard.insert(
            internal_id.to_string(),
            exchange,
            symbol,
            external_id,
            original_client_id,
        );
    }

    fn translate_internal(&self, order_id: &str) -> BrokerResult<RoutedOrder> {
        let guard = self.state.lock().unwrap();
        guard
            .get(order_id)
            .cloned()
            .ok_or_else(|| BrokerError::InvalidRequest(format!("unknown order id {order_id}")))
    }

    fn normalize_order(&self, exchange: ExchangeId, mut order: Order) -> Order {
        let mut guard = self.state.lock().unwrap();
        let external_id = order.id.clone();
        let external_client_id = order.request.client_order_id.clone();

        if let Some(existing) = guard.lookup_external_order(exchange, &external_id).cloned() {
            if let Some(entry) = guard.get(&existing) {
                order.id = existing;
                order.request.client_order_id = entry.original_client_id.clone();
            }
            return order;
        }

        let internal_id = generate_internal_id();
        let original_client_id = match external_client_id {
            Some(ref value) if is_router_namespace(value) => None,
            Some(ref value) => Some(value.clone()),
            None => None,
        };
        guard.insert(
            internal_id.clone(),
            exchange,
            order.request.symbol,
            external_id,
            original_client_id.clone(),
        );
        order.id = internal_id;
        order.request.client_order_id = original_client_id;
        order
    }
}

#[async_trait]
impl ExecutionClient for RouterExecutionClient {
    fn info(&self) -> BrokerInfo {
        self.info.clone()
    }

    async fn place_order(&self, request: OrderRequest) -> BrokerResult<Order> {
        let exchange = request.symbol.exchange;
        if !exchange.is_specified() {
            return Err(BrokerError::InvalidRequest(
                "order symbol must specify exchange".into(),
            ));
        }
        let client = self.client_for(exchange)?;
        let internal_id = generate_internal_id();
        let group_suffix = request
            .client_order_id
            .as_deref()
            .and_then(extract_group_suffix);
        let external_client_id = if let Some(tag) = group_suffix {
            format!("{}-{}-{}", CLIENT_NAMESPACE, tag, internal_id)
        } else {
            format!("{}-{}", CLIENT_NAMESPACE, internal_id)
        };

        let mut outbound = request.clone();
        outbound.client_order_id = Some(external_client_id.clone());

        let mut order = client.place_order(outbound).await?;
        let original_client_id = request.client_order_id.clone();

        self.record_order(
            &internal_id,
            exchange,
            request.symbol,
            order.id.clone(),
            original_client_id.clone(),
        );

        order.id = internal_id;
        order.request = request;
        order.request.client_order_id = original_client_id;

        Ok(order)
    }

    async fn cancel_order(&self, order_id: String, _symbol: Symbol) -> BrokerResult<()> {
        let routed = self.translate_internal(&order_id)?;
        let client = self.client_for(routed.exchange)?;
        client
            .cancel_order(routed.external_order_id.clone(), routed.symbol)
            .await
    }

    async fn amend_order(&self, request: OrderUpdateRequest) -> BrokerResult<Order> {
        let routed = self.translate_internal(&request.order_id)?;
        let client = self.client_for(routed.exchange)?;
        let mut outbound = request.clone();
        outbound.order_id = routed.external_order_id.clone();
        outbound.symbol = routed.symbol;

        let mut order = client.amend_order(outbound).await?;
        if order.id != routed.external_order_id {
            // Exchange assigned a new order ID after amendment; refresh map.
            let mut guard = self.state.lock().unwrap();
            guard.insert(
                request.order_id.clone(),
                routed.exchange,
                routed.symbol,
                order.id.clone(),
                routed.original_client_id.clone(),
            );
        }
        order.id = request.order_id;
        order.request.client_order_id = routed.original_client_id;
        Ok(order)
    }

    async fn list_open_orders(&self, symbol: Symbol) -> BrokerResult<Vec<Order>> {
        let exchange = symbol.exchange;
        let client = self.client_for(exchange)?;
        let mut orders = client.list_open_orders(symbol).await?;
        let mut normalized = Vec::with_capacity(orders.len());
        for order in orders.drain(..) {
            normalized.push(self.normalize_order(exchange, order));
        }
        Ok(normalized)
    }

    async fn account_balances(&self) -> BrokerResult<Vec<AccountBalance>> {
        let mut balances = Vec::new();
        for client in self.routes.values() {
            balances.extend(client.account_balances().await?);
        }
        Ok(balances)
    }

    async fn positions(&self) -> BrokerResult<Vec<Position>> {
        let mut positions = Vec::new();
        for client in self.routes.values() {
            positions.extend(client.positions().await?);
        }
        Ok(positions)
    }

    async fn list_instruments(&self, category: &str) -> BrokerResult<Vec<Instrument>> {
        let mut instruments = Vec::new();
        for client in self.routes.values() {
            instruments.extend(client.list_instruments(category).await?);
        }
        Ok(instruments)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn aggregate_info(routes: &HashMap<ExchangeId, Arc<dyn ExecutionClient>>) -> BrokerInfo {
    let mut markets = Vec::new();
    let mut supports_testnet = true;
    for client in routes.values() {
        let info = client.info();
        supports_testnet &= info.supports_testnet;
        markets.extend(info.markets);
    }
    markets.sort();
    markets.dedup();
    BrokerInfo {
        name: format!("router-{}-venues", routes.len()),
        markets,
        supports_testnet,
    }
}

fn generate_internal_id() -> String {
    Uuid::new_v4().simple().to_string()
}

fn is_router_namespace(value: &str) -> bool {
    value.starts_with(CLIENT_NAMESPACE)
}

fn extract_group_suffix(value: &str) -> Option<String> {
    value.split("|grp:").nth(1).map(|segment| {
        let token = segment.split('|').next().unwrap_or(segment);
        let mut label: String = token.chars().take(8).collect();
        if label.is_empty() {
            label = "grp".into();
        }
        label.to_ascii_uppercase()
    })
}

#[derive(Clone, Debug)]
struct RoutedOrder {
    exchange: ExchangeId,
    symbol: Symbol,
    external_order_id: String,
    original_client_id: Option<String>,
}

#[derive(Default)]
struct RouteState {
    by_internal: HashMap<String, RoutedOrder>,
    by_exchange_order: HashMap<(ExchangeId, String), String>,
}

impl RouteState {
    fn insert(
        &mut self,
        internal: String,
        exchange: ExchangeId,
        symbol: Symbol,
        external_order_id: String,
        original_client_id: Option<String>,
    ) {
        self.by_exchange_order
            .insert((exchange, external_order_id.clone()), internal.clone());
        self.by_internal.insert(
            internal,
            RoutedOrder {
                exchange,
                symbol,
                external_order_id,
                original_client_id,
            },
        );
    }

    fn get(&self, internal: &str) -> Option<&RoutedOrder> {
        self.by_internal.get(internal)
    }

    fn lookup_external_order(&self, exchange: ExchangeId, external_id: &str) -> Option<&String> {
        self.by_exchange_order
            .get(&(exchange, external_id.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use tesser_core::{OrderStatus, OrderType, Side};

    struct TestClient {
        exchange: ExchangeId,
        next_id: Mutex<u64>,
        placed: Mutex<Vec<OrderRequest>>,
        canceled: Mutex<Vec<(String, Symbol)>>,
    }

    impl TestClient {
        fn new(exchange: ExchangeId) -> Self {
            Self {
                exchange,
                next_id: Mutex::new(0),
                placed: Mutex::new(Vec::new()),
                canceled: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ExecutionClient for TestClient {
        fn info(&self) -> BrokerInfo {
            BrokerInfo {
                name: format!("test-{}", self.exchange),
                markets: vec!["futures".into()],
                supports_testnet: true,
            }
        }

        async fn place_order(&self, request: OrderRequest) -> BrokerResult<Order> {
            self.placed.lock().unwrap().push(request.clone());
            let mut guard = self.next_id.lock().unwrap();
            *guard += 1;
            let id = format!("{}-{}", self.exchange.as_raw(), guard);
            Ok(Order {
                id,
                request,
                status: OrderStatus::Accepted,
                filled_quantity: Decimal::ZERO,
                avg_fill_price: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
        }

        async fn cancel_order(&self, order_id: String, symbol: Symbol) -> BrokerResult<()> {
            self.canceled.lock().unwrap().push((order_id, symbol));
            Ok(())
        }

        async fn amend_order(&self, request: OrderUpdateRequest) -> BrokerResult<Order> {
            let mut guard = self.next_id.lock().unwrap();
            *guard += 1;
            let id = format!("{}-{}", self.exchange.as_raw(), guard);
            let client_id = format!("amend-{}", id);
            Ok(Order {
                id,
                request: OrderRequest {
                    symbol: request.symbol,
                    side: request.side,
                    order_type: OrderType::Limit,
                    quantity: request.new_quantity.unwrap_or(Decimal::ZERO),
                    price: request.new_price,
                    trigger_price: None,
                    time_in_force: None,
                    client_order_id: Some(client_id),
                    take_profit: None,
                    stop_loss: None,
                    display_quantity: None,
                },
                status: OrderStatus::Accepted,
                filled_quantity: Decimal::ZERO,
                avg_fill_price: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
        }

        async fn list_open_orders(&self, _symbol: Symbol) -> BrokerResult<Vec<Order>> {
            Ok(Vec::new())
        }

        async fn account_balances(&self) -> BrokerResult<Vec<AccountBalance>> {
            Ok(Vec::new())
        }

        async fn positions(&self) -> BrokerResult<Vec<Position>> {
            Ok(Vec::new())
        }

        async fn list_instruments(&self, _category: &str) -> BrokerResult<Vec<Instrument>> {
            Ok(Vec::new())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn sample_request(symbol: Symbol) -> OrderRequest {
        OrderRequest {
            symbol,
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: Decimal::ONE,
            price: None,
            trigger_price: None,
            time_in_force: None,
            client_order_id: Some("client-123".into()),
            take_profit: None,
            stop_loss: None,
            display_quantity: None,
        }
    }

    #[tokio::test]
    async fn routes_orders_and_translates_ids() {
        let bybit = ExchangeId::from("bybit_linear");
        let binance = ExchangeId::from("binance_perp");
        let client_a = Arc::new(TestClient::new(bybit));
        let client_b = Arc::new(TestClient::new(binance));
        let mut routes: HashMap<ExchangeId, Arc<dyn ExecutionClient>> = HashMap::new();
        routes.insert(bybit, client_a.clone());
        routes.insert(binance, client_b.clone());
        let router = RouterExecutionClient::new(routes);

        let symbol_a = Symbol::from_code(bybit, "BTCUSDT");
        let order_a = router
            .place_order(sample_request(symbol_a))
            .await
            .expect("order");
        assert_eq!(order_a.id.len(), 32);
        assert_eq!(order_a.request.symbol, symbol_a);
        assert_eq!(
            client_a.placed.lock().unwrap().len(),
            1,
            "bybit client should receive order"
        );

        router
            .cancel_order(order_a.id.clone(), symbol_a)
            .await
            .unwrap();
        {
            let canceled = client_a.canceled.lock().unwrap();
            assert_eq!(canceled.len(), 1);
            assert_ne!(canceled[0].0, order_a.id);
        }

        let symbol_b = Symbol::from_code(binance, "ETHUSDT");
        let order_b = router
            .place_order(sample_request(symbol_b))
            .await
            .expect("order");
        assert!(order_b.request.client_order_id.is_some());
        assert_eq!(
            client_b.placed.lock().unwrap().len(),
            1,
            "binance client should receive order"
        );
        assert_ne!(order_a.id, order_b.id, "internal ids must be unique");
    }
}
