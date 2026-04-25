use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use alloy::signers::local::PrivateKeySigner as LocalSigner;
use chrono::{DateTime, Utc};
use log::info;
use polymarket_client_sdk::auth::Kind;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::Client as ClobClient;
use polymarket_client_sdk::clob::types::request::OrdersRequest;
use polymarket_client_sdk::clob::types::response::OpenOrderResponse;
use polymarket_client_sdk::clob::types::{OrderStatusType, OrderType, Side};
use polymarket_client_sdk::types::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use tokio::sync::{Mutex, mpsc::UnboundedSender};
use tokio::time::Duration;

use crate::engine::core::inventory::{FillDelta, OrderSide, OutcomeSide};
use crate::engine::core::params::StrategyConfig;
use crate::engine::core::state::OutcomeResolver;
use crate::engine::core::strategy::TradeAction;
use crate::engine::execution::order_manager::OrderManager;
use crate::engine::execution::user_ws::UserChannelMsg;
use crate::engine::telemetry::events::UserChannelDynamicEvent;
use crate::engine::telemetry::latency::{BestBidAskStage, ExecutorPrepStage};
use crate::telemetry::TelemetryTx;

#[derive(Debug, Clone)]
pub struct FillExecutionUpdate {
    pub fill_id: String,
    pub order_id: String,
    pub asset_id: String,
    pub outcome_side: OutcomeSide,
    pub price: f64,
    pub filled_amount: f64,
    pub fill_timestamp: DateTime<Utc>,
}

impl FillExecutionUpdate {
    pub fn into_fill_delta(self) -> FillDelta {
        FillDelta {
            fill_id: self.fill_id,
            order_id: self.order_id,
            asset_id: self.asset_id,
            outcome_side: self.outcome_side,
            order_side: OrderSide::Buy,
            price: self.price,
            size: self.filled_amount,
            fill_timestamp: self.fill_timestamp,
        }
    }
}

#[derive(Debug, Clone)]
pub enum InventoryUpdate {
    Fill(FillExecutionUpdate),
    DoubleBatchSubmitted {
        trace_id: u64,
        up_price: f64,
        up_size: f64,
        down_price: f64,
        down_size: f64,
    },
    BatchCancelled {
        reason: String,
    },
    BatchCancelRequestFailed {
        reason: String,
    },
    BatchReconcileRequired {
        reason: String,
    },
    ReconcileResolved {
        reason: String,
        clear_active_batch: bool,
    },
    BatchSubmissionFailed {
        reason: String,
    },
}

#[derive(Debug, Clone)]
struct PendingOrderIntent {
    local_intent_id: u64,
    trace_id: u64,
    outcome_side: OutcomeSide,
    asset_id: String,
    price: f64,
    size: f64,
}

#[derive(Debug, Clone)]
struct DoubleBatchSubmission {
    trace_id: u64,
    up_price: f64,
    up_size: f64,
    down_price: f64,
    down_size: f64,
    local_intent_ids: HashSet<u64>,
}

#[derive(Debug, Clone)]
struct PendingSubmittedDoubleBatch {
    trace_id: u64,
    submitted_at: DateTime<Utc>,
    pending_local_intent_ids: HashSet<u64>,
    bound_order_ids: HashSet<String>,
    reconcile_required_emitted: bool,
}

#[derive(Debug, Clone)]
struct PendingCancelBatch {
    trace_id: u64,
    reason: String,
    requested_at: DateTime<Utc>,
    pending_order_ids: HashSet<String>,
    reconcile_required_emitted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CancelRequestOutcome {
    NoOpenOrders,
    RequestSent,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PendingCancelResolution {
    Confirmed { reason: String },
    ReconcileRequired { reason: String },
}

#[derive(Debug, Clone)]
struct OrderSnapshot {
    order_id: String,
    market: String,
    asset_id: String,
    outcome: String,
    side: String,
    price: f64,
    original_size: f64,
    size_matched: f64,
    status: OrderStatusType,
}

impl OrderSnapshot {
    fn from_response(response: OpenOrderResponse) -> Self {
        Self {
            order_id: response.id,
            market: response.market,
            asset_id: response.asset_id,
            outcome: response.outcome,
            side: response.side.to_string(),
            price: response.price.to_f64().unwrap_or(0.0),
            original_size: response.original_size.to_f64().unwrap_or(0.0),
            size_matched: response.size_matched.to_f64().unwrap_or(0.0),
            status: response.status,
        }
    }

    fn remaining_size(&self) -> f64 {
        (self.original_size - self.size_matched).max(0.0)
    }

    fn is_open(&self) -> bool {
        matches!(
            self.status,
            OrderStatusType::Live | OrderStatusType::Delayed | OrderStatusType::Unmatched
        )
    }
}

#[derive(Debug, Clone, Default)]
struct MarketExecutionScope {
    last_batch_ids: Vec<String>,
    pending_order_intents: Vec<PendingOrderIntent>,
    pending_submitted_double_batch: Option<PendingSubmittedDoubleBatch>,
    pending_cancel_batch: Option<PendingCancelBatch>,
    quarantined_order_ids: HashSet<String>,
    reconcile_required: bool,
    reconcile_reason: Option<String>,
    reconcile_requested_at: Option<DateTime<Utc>>,
}

impl MarketExecutionScope {
    fn is_action_blocked(&self) -> bool {
        self.pending_cancel_batch.is_some() || self.reconcile_required
    }

    fn mark_reconcile_required(&mut self, reason: String, now: DateTime<Utc>) {
        self.reconcile_required = true;
        self.reconcile_reason = Some(reason);
        if self.reconcile_requested_at.is_none() {
            self.reconcile_requested_at = Some(now);
        }
    }

    fn clear_reconcile_required(&mut self) {
        self.reconcile_required = false;
        self.reconcile_reason = None;
        self.reconcile_requested_at = None;
    }

    fn blocking_trace_id(&self) -> Option<u64> {
        self.pending_cancel_batch
            .as_ref()
            .map(|batch| batch.trace_id)
            .or_else(|| self.pending_submitted_double_batch.as_ref().map(|batch| batch.trace_id))
    }

    fn blocking_reason(&self) -> Option<String> {
        self.reconcile_reason.clone().or_else(|| {
            self.pending_cancel_batch
                .as_ref()
                .map(|batch| batch.reason.clone())
        })
    }

    fn blocked_since(&self) -> Option<DateTime<Utc>> {
        self.reconcile_requested_at
            .or_else(|| self.pending_cancel_batch.as_ref().map(|batch| batch.requested_at))
    }

    fn pending_order_count(&self) -> usize {
        let mut ids = HashSet::new();
        ids.extend(self.last_batch_ids.iter().cloned());
        if let Some(batch) = self.pending_cancel_batch.as_ref() {
            ids.extend(batch.pending_order_ids.iter().cloned());
        }
        ids.len()
    }

    fn tracked_order_ids(&self) -> Vec<String> {
        let mut ids = self.last_batch_ids.iter().cloned().collect::<HashSet<_>>();
        if let Some(batch) = self.pending_cancel_batch.as_ref() {
            ids.extend(batch.pending_order_ids.iter().cloned());
        }
        ids.into_iter().collect()
    }
}

pub struct Executor<K: Kind + Send + Sync + 'static> {
    pub client: Arc<ClobClient<Authenticated<K>>>,
    pub signer: LocalSigner,
    pub inventory_tx: Arc<Mutex<Option<UnboundedSender<InventoryUpdate>>>>,
    pub active_market: Arc<Mutex<Option<String>>>,
    pub active_outcome_resolver: Arc<Mutex<Option<OutcomeResolver>>>,
    pub cancel_timeout_ms: u64,
    pub cancel_confirm_timeout_ms: u64,
    pub dust_share_threshold: f64,
    pub dust_notional_threshold: f64,
    pub bind_price_tolerance: f64,
    pub bind_size_tolerance: f64,
}

impl<K: Kind + Send + Sync + 'static> Executor<K> {
    pub fn new(
        client: Arc<ClobClient<Authenticated<K>>>,
        signer: LocalSigner,
        config: &StrategyConfig,
    ) -> Self {
        Self {
            client,
            signer,
            inventory_tx: Arc::new(Mutex::new(None)),
            active_market: Arc::new(Mutex::new(None)),
            active_outcome_resolver: Arc::new(Mutex::new(None)),
            cancel_timeout_ms: (config.t_cancel.max(1.0) * 1000.0) as u64,
            cancel_confirm_timeout_ms: (config.t_cancel_confirm.max(1.0) * 1000.0) as u64,
            dust_share_threshold: config.dust_share_threshold.max(0.0),
            dust_notional_threshold: config.dust_notional_threshold.max(0.0),
            bind_price_tolerance: config.bind_price_tolerance(),
            bind_size_tolerance: config.bind_size_tolerance(),
        }
    }

    pub async fn set_inventory_tx(&self, tx: UnboundedSender<InventoryUpdate>) {
        let mut guard = self.inventory_tx.lock().await;
        *guard = Some(tx);
    }

    pub async fn set_active_market(&self, market: String) {
        let mut guard = self.active_market.lock().await;
        *guard = Some(market);
    }

    pub async fn set_active_market_context(
        &self,
        market: String,
        outcome_resolver: OutcomeResolver,
    ) {
        {
            let mut guard = self.active_market.lock().await;
            *guard = Some(market);
        }
        let mut resolver_guard = self.active_outcome_resolver.lock().await;
        *resolver_guard = Some(outcome_resolver);
    }

    pub async fn run(
        &self,
        mut action_rx: tokio::sync::watch::Receiver<(TradeAction, u64)>,
        mut user_ws_rx: tokio::sync::mpsc::UnboundedReceiver<UserChannelMsg>,
        telemetry: TelemetryTx,
    ) {
        info!("[Executor] started");
        telemetry.log("[Executor] started".to_string());

        let mut manager = OrderManager::new(telemetry.clone());
        let mut scopes: HashMap<String, MarketExecutionScope> = HashMap::new();
        let mut next_local_intent_id: u64 = 1;
        let mut cancel_confirm_watchdog = tokio::time::interval(Duration::from_millis(250));

        loop {
            tokio::select! {
                _ = cancel_confirm_watchdog.tick() => {
                    let now = Utc::now();
                    let mut reconcile_markets = Vec::new();

                    for (market, scope) in scopes.iter_mut() {
                        if let Some(reason) = pending_cancel_timeout_reason(
                            &mut scope.pending_cancel_batch,
                            now,
                            self.cancel_confirm_timeout_ms,
                        ) {
                            scope.mark_reconcile_required(reason.clone(), now);
                            reconcile_markets.push((market.clone(), reason));
                        }

                        if let Some(reason) = pending_double_bind_timeout_reason(
                            &mut scope.pending_submitted_double_batch,
                            now,
                            self.cancel_confirm_timeout_ms,
                        ) {
                            scope.mark_reconcile_required(reason.clone(), now);
                            reconcile_markets.push((market.clone(), reason));
                        }
                    }

                    for (market, reason) in reconcile_markets {
                        if let Some(scope_snapshot) = scopes.get(&market).cloned() {
                            self.emit_scope_blocking_event(
                                &telemetry,
                                &market,
                                &scope_snapshot,
                                0,
                                "required",
                                Some(reason.clone()),
                            );
                        }

                        if self.is_active_market(&market).await {
                            self.emit_strategy_update(InventoryUpdate::BatchReconcileRequired {
                                reason: reason.clone(),
                            }).await;
                        }

                        if let Some(scope) = scopes.get_mut(&market) {
                            if let Some(update) = self
                                .reconcile_market_scope(&market, scope, &mut manager, &telemetry)
                                .await
                            {
                                if self.is_active_market(&market).await {
                                    self.emit_strategy_update(update).await;
                                }
                            }
                        }
                    }
                }

                changed = action_rx.changed() => {
                    match changed {
                        Ok(()) => {
                            let (action, trace_id) = action_rx.borrow().clone();
                            telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::ExecutorWakeup);
                            telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::ExecutorReceived);
                            telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::ExecutorReceived);

                            match action {
                                TradeAction::DoNothing | TradeAction::AbortMarket => {}
                                TradeAction::CancelAllOpenOrders { reason } => {
                                    let Some(active_market) = self.active_market.lock().await.clone() else {
                                        continue;
                                    };
                                    let scope = scopes.entry(active_market.clone()).or_default();
                                    match self
                                        .cancel_previous_batch(
                                            &mut manager,
                                            &scope.last_batch_ids,
                                            &mut scope.pending_cancel_batch,
                                            trace_id,
                                            &reason,
                                            &active_market,
                                            &telemetry,
                                        )
                                        .await
                                    {
                                        CancelRequestOutcome::NoOpenOrders => {
                                            if let Some(reason) =
                                                mark_pending_double_batch_reconcile_required(
                                                    &mut scope.pending_submitted_double_batch,
                                                    format!(
                                                        "cancel_requested_without_bound_order_ids trace_id={} cancel_reason={}",
                                                        trace_id, reason
                                                    ),
                                                )
                                            {
                                                scope.mark_reconcile_required(reason.clone(), Utc::now());
                                                self.emit_scope_blocking_event(
                                                    &telemetry,
                                                    &active_market,
                                                    scope,
                                                    trace_id,
                                                    "required",
                                                    Some(reason.clone()),
                                                );
                                                self.emit_strategy_update(
                                                    InventoryUpdate::BatchReconcileRequired {
                                                        reason,
                                                    },
                                                )
                                                .await;
                                            } else {
                                                self.emit_strategy_update(
                                                    InventoryUpdate::BatchCancelled { reason },
                                                )
                                                .await;
                                            }
                                        }
                                        CancelRequestOutcome::RequestSent => {}
                                        CancelRequestOutcome::Failed(reason) => {
                                            self.emit_strategy_update(
                                                InventoryUpdate::BatchCancelRequestFailed { reason },
                                            )
                                            .await;
                                        }
                                    }
                                }
                                TradeAction::PlaceDoubleSidedOrder {
                                    up_token_id,
                                    up_price,
                                    up_size,
                                    down_token_id,
                                    down_price,
                                    down_size,
                                } => {
                                    let Some(active_market) = self.active_market.lock().await.clone() else {
                                        continue;
                                    };
                                    let scope = scopes.entry(active_market.clone()).or_default();

                                    if scope.is_action_blocked() {
                                        let reason = scope
                                            .blocking_reason()
                                            .unwrap_or_else(|| "executor_scope_blocked".to_string());
                                        self.emit_scope_blocking_event(
                                            &telemetry,
                                            &active_market,
                                            scope,
                                            trace_id,
                                            "required",
                                            Some(reason.clone()),
                                        );
                                        self.emit_strategy_update(
                                            InventoryUpdate::BatchSubmissionFailed {
                                                reason: format!("execution_blocked:{reason}"),
                                            },
                                        )
                                        .await;
                                        continue;
                                    }

                                    if !scope.last_batch_ids.is_empty() {
                                        match self
                                            .cancel_previous_batch(
                                                &mut manager,
                                                &scope.last_batch_ids,
                                                &mut scope.pending_cancel_batch,
                                                trace_id,
                                                "executor_clean_book_guard",
                                                &active_market,
                                                &telemetry,
                                            )
                                            .await
                                        {
                                            CancelRequestOutcome::NoOpenOrders => {}
                                            CancelRequestOutcome::RequestSent => {}
                                            CancelRequestOutcome::Failed(reason) => {
                                                self.emit_strategy_update(
                                                    InventoryUpdate::BatchCancelRequestFailed { reason },
                                                )
                                                .await;
                                            }
                                        }
                                        continue;
                                    }

                                    match self
                                        .execute_batch_maker(
                                            &up_token_id,
                                            up_price,
                                            up_size,
                                            &down_token_id,
                                        down_price,
                                        down_size,
                                        &mut scope.pending_order_intents,
                                        &mut next_local_intent_id,
                                        trace_id,
                                        &telemetry,
                                        )
                                        .await
                                    {
                                        Ok(submission) => {
                                            scope.pending_submitted_double_batch =
                                                Some(PendingSubmittedDoubleBatch {
                                                    trace_id: submission.trace_id,
                                                    submitted_at: Utc::now(),
                                                    pending_local_intent_ids: submission
                                                        .local_intent_ids,
                                                    bound_order_ids: HashSet::new(),
                                                    reconcile_required_emitted: false,
                                                });
                                            self.emit_strategy_update(
                                                InventoryUpdate::DoubleBatchSubmitted {
                                                    trace_id: submission.trace_id,
                                                    up_price: submission.up_price,
                                                    up_size: submission.up_size,
                                                    down_price: submission.down_price,
                                                    down_size: submission.down_size,
                                                },
                                            )
                                            .await;
                                        }
                                        Err(reason) => {
                                            self.emit_strategy_update(
                                                InventoryUpdate::BatchSubmissionFailed { reason },
                                            )
                                            .await;
                                        }
                                    }
                                }
                                TradeAction::PlaceSingleSidedAdjustmentOrder {
                                    outcome_side,
                                    asset_id,
                                    price,
                                    size,
                                } => {
                                    let Some(active_market) = self.active_market.lock().await.clone() else {
                                        continue;
                                    };
                                    let scope = scopes.entry(active_market.clone()).or_default();

                                    if scope.is_action_blocked() {
                                        let reason = scope
                                            .blocking_reason()
                                            .unwrap_or_else(|| "executor_scope_blocked".to_string());
                                        self.emit_scope_blocking_event(
                                            &telemetry,
                                            &active_market,
                                            scope,
                                            trace_id,
                                            "required",
                                            Some(reason.clone()),
                                        );
                                        self.emit_strategy_update(
                                            InventoryUpdate::BatchSubmissionFailed {
                                                reason: format!("execution_blocked:{reason}"),
                                            },
                                        )
                                        .await;
                                        continue;
                                    }

                                    if !scope.last_batch_ids.is_empty() {
                                        match self
                                            .cancel_previous_batch(
                                                &mut manager,
                                                &scope.last_batch_ids,
                                                &mut scope.pending_cancel_batch,
                                                trace_id,
                                                "executor_clean_book_guard",
                                                &active_market,
                                                &telemetry,
                                            )
                                            .await
                                        {
                                            CancelRequestOutcome::NoOpenOrders => {}
                                            CancelRequestOutcome::RequestSent => {}
                                            CancelRequestOutcome::Failed(reason) => {
                                                self.emit_strategy_update(
                                                    InventoryUpdate::BatchCancelRequestFailed { reason },
                                                )
                                                .await;
                                            }
                                        }
                                        continue;
                                    }

                                    let submitted = self
                                        .execute_single_order(
                                            outcome_side,
                                            &asset_id,
                                            price,
                                            size,
                                            &mut scope.pending_order_intents,
                                            &mut next_local_intent_id,
                                            trace_id,
                                            &telemetry,
                                        )
                                        .await;

                                    if !submitted {
                                        self.emit_strategy_update(InventoryUpdate::BatchSubmissionFailed {
                                            reason: "single_sided_submit_failed".to_string(),
                                        }).await;
                                    }
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }

                msg_opt = user_ws_rx.recv() => {
                    let Some(msg) = msg_opt else {
                        break;
                    };

                    match msg {
                        UserChannelMsg::TradeObserved {
                            event_type,
                            market,
                            status,
                            asset_id,
                            outcome,
                            side,
                            order_id,
                        } => {
                            let trace_id = manager
                                .account_trace_id(&order_id);

                            telemetry.emit_user_channel_event(UserChannelDynamicEvent {
                                trace_id,
                                local_ts: Utc::now(),
                                source: "trade".to_string(),
                                market: Some(market.clone()),
                                summary: Some(format!(
                                    "status={} side={} outcome={} asset_id={} order_id={} event_type={} market={}",
                                    status,
                                    side,
                                    outcome.clone().unwrap_or_else(|| "missing".to_string()),
                                    asset_id,
                                    order_id,
                                    event_type,
                                    market,
                                )),
                                order_id,
                                status,
                                batch_trace_id: None,
                                blocking: None,
                                blocking_reason: None,
                                pending_orders: None,
                                blocked_since: None,
                                side: Some(side),
                                outcome,
                                asset_id: Some(asset_id),
                                fill_id: None,
                                matched_amount_cumulative: None,
                                fill_delta: None,
                                filled_amount: None,
                                notional: None,
                                price: None,
                                official_order_type: None,
                                official_status: None,
                                raw_event_type: Some(event_type),
                                raw_type: None,
                                raw_status: None,
                                detail: Some("user_channel_trade".to_string()),
                            });
                        }
                        UserChannelMsg::OrderFilled {
                            order_id,
                            market,
                            fill_id,
                            size_matched,
                            original_size,
                            side,
                            asset_id,
                            outcome,
                            is_full,
                            fill_price,
                            fill_timestamp,
                            raw_event_type,
                            raw_type,
                            raw_status,
                        } => {
                            let scope = scopes.entry(market.clone()).or_default();
                            if let Some(intent) = self.bind_pending_order_from_user_channel(
                                &mut manager,
                                &mut scope.pending_order_intents,
                                &mut scope.last_batch_ids,
                                &order_id,
                                asset_id.as_deref(),
                                fill_price,
                                original_size,
                            ) {
                                note_double_batch_binding(
                                    &mut scope.pending_submitted_double_batch,
                                    intent.local_intent_id,
                                    &order_id,
                                    &telemetry,
                                );
                            }

                            let account_delta = manager.observe_order_event(
                                &order_id,
                                Some(&market),
                                visible_fill_status(is_full, raw_status.as_deref()),
                                side.as_deref(),
                                asset_id.as_deref(),
                                outcome.as_deref(),
                                fill_price,
                                original_size,
                                Some(size_matched),
                            );
                            let delta = account_delta.as_ref().map(|entry| entry.delta);
                            let tracker_trace_id = manager.account_trace_id(&order_id);
                            let tracker_asset_id =
                                manager.orders.get(&order_id).map(|tracker| tracker.asset_id.clone());
                            let tracker_price =
                                manager.orders.get(&order_id).map(|tracker| tracker.price);
                            let tracker_inventory_side =
                                manager.orders.get(&order_id).map(|tracker| tracker.inventory_side);
                            let tracker_order_side =
                                manager.orders.get(&order_id).map(|tracker| tracker.order_side.clone());
                            let account_asset_id = manager
                                .account_order(&order_id)
                                .and_then(|tracker| tracker.asset_id.clone());
                            let account_price = manager
                                .account_order(&order_id)
                                .and_then(|tracker| tracker.price);
                            let account_side = manager
                                .account_order(&order_id)
                                .and_then(|tracker| tracker.side.clone());
                            let account_outcome = manager
                                .account_order(&order_id)
                                .and_then(|tracker| tracker.outcome.clone());
                            if tracker_trace_id != 0 {
                                telemetry.mark_best_bid_ask_stage(
                                    tracker_trace_id,
                                    BestBidAskStage::UserChannelReported,
                                );
                            }

                            let resolved_asset_id = asset_id
                                .clone()
                                .or(tracker_asset_id.clone())
                                .or(account_asset_id.clone());
                            let order_price = fill_price.or(tracker_price).or(account_price);
                            let status = visible_fill_status(is_full, raw_status.as_deref());
                            let resolved_fill_id =
                                resolved_fill_id(fill_id.as_deref(), &order_id, size_matched);
                            let visible_side = side
                                .clone()
                                .or(tracker_order_side.clone())
                                .or(account_side.clone());
                            let visible_outcome = outcome.clone().or(account_outcome.clone());

                            telemetry.emit_user_channel_event(UserChannelDynamicEvent {
                                trace_id: tracker_trace_id,
                                local_ts: Utc::now(),
                                source: "order".to_string(),
                                market: Some(market.clone()),
                                summary: Some(format!(
                                    "status={} side={} outcome={} asset_id={} price={:.6} cumulative={:.6} fill_delta={} order_id={}",
                                    status,
                                    visible_side.clone().unwrap_or_else(|| "missing".to_string()),
                                    visible_outcome.clone().unwrap_or_else(|| "missing".to_string()),
                                    resolved_asset_id.clone().unwrap_or_else(|| "missing".to_string()),
                                    order_price.unwrap_or(0.0),
                                    size_matched,
                                    delta
                                        .map(|value| format!("{value:.6}"))
                                        .unwrap_or_else(|| "N/A".to_string()),
                                    order_id,
                                )),
                                order_id: order_id.clone(),
                                status: status.to_string(),
                                batch_trace_id: None,
                                blocking: None,
                                blocking_reason: None,
                                pending_orders: None,
                                blocked_since: None,
                                side: visible_side.clone(),
                                outcome: visible_outcome.clone(),
                                asset_id: resolved_asset_id.clone(),
                                fill_id: Some(resolved_fill_id.clone()),
                                matched_amount_cumulative: Some(size_matched),
                                fill_delta: delta,
                                filled_amount: delta,
                                notional: delta.zip(order_price).map(|(size, price)| size * price),
                                price: order_price,
                                official_order_type: raw_type.clone(),
                                official_status: raw_status.clone(),
                                raw_event_type: raw_event_type.clone(),
                                raw_type: raw_type.clone(),
                                raw_status: raw_status.clone(),
                                detail: Some("user_channel_fill".to_string()),
                            });

                            if is_full {
                                if let Some(resolution) = release_closed_order_from_tracking(
                                    &mut scope.last_batch_ids,
                                    &mut scope.pending_cancel_batch,
                                    &order_id,
                                ) {
                                    match resolution {
                                        PendingCancelResolution::Confirmed { reason } => {
                                            if let Some(reason) =
                                                mark_pending_double_batch_reconcile_required(
                                                    &mut scope.pending_submitted_double_batch,
                                                    format!(
                                                        "cancel_confirmed_with_unbound_double_leg trace_id={} reason={}",
                                                        tracker_trace_id, reason
                                                    ),
                                                )
                                            {
                                                scope.mark_reconcile_required(reason.clone(), Utc::now());
                                                self.emit_scope_blocking_event(
                                                    &telemetry,
                                                    &market,
                                                    scope,
                                                    tracker_trace_id,
                                                    "required",
                                                    Some(reason.clone()),
                                                );
                                                if self.is_active_market(&market).await {
                                                    self.emit_strategy_update(
                                                        InventoryUpdate::BatchReconcileRequired {
                                                            reason,
                                                        },
                                                    )
                                                    .await;
                                                }
                                            } else {
                                                scope.clear_reconcile_required();
                                                if self.is_active_market(&market).await {
                                                    self.emit_strategy_update(
                                                        InventoryUpdate::BatchCancelled { reason },
                                                    )
                                                    .await;
                                                }
                                            }
                                        }
                                        PendingCancelResolution::ReconcileRequired { reason } => {
                                            scope.mark_reconcile_required(reason.clone(), Utc::now());
                                            self.emit_scope_blocking_event(
                                                &telemetry,
                                                &market,
                                                scope,
                                                tracker_trace_id,
                                                "required",
                                                Some(reason.clone()),
                                            );
                                            if self.is_active_market(&market).await {
                                                self.emit_strategy_update(
                                                    InventoryUpdate::BatchReconcileRequired { reason },
                                                )
                                                .await;
                                            }
                                        }
                                    }
                                }
                            }

                            if !self.is_active_market(&market).await {
                                continue;
                            }

                            let Some(delta) = delta else {
                                continue;
                            };

                            let strategy_side = visible_side
                                .clone()
                                .unwrap_or_else(|| "BUY".to_string());
                            if !strategy_side.eq_ignore_ascii_case("BUY") {
                                telemetry.log(format!(
                                    "ignoring non-buy order fill for strategy order_id={} market={} side={}",
                                    order_id, market, strategy_side
                                ));
                                continue;
                            }

                            let Some(resolved_asset_id) = resolved_asset_id else {
                                telemetry.log(format!(
                                    "missing asset_id for strategy fill order_id={} market={}",
                                    order_id, market
                                ));
                                continue;
                            };

                            let Some(order_price) = order_price else {
                                telemetry.log(format!(
                                    "missing price for strategy fill order_id={} market={} asset_id={}",
                                    order_id, market, resolved_asset_id
                                ));
                                continue;
                            };

                            let resolver = self.active_outcome_resolver.lock().await.clone();
                            let Some(resolver) = resolver else {
                                telemetry.log(format!(
                                    "missing active outcome resolver for market fill order_id={}",
                                    order_id
                                ));
                                continue;
                            };
                            let resolved_outcome_side = match resolver.resolve_outcome_side(
                                &resolved_asset_id,
                                visible_outcome.as_deref(),
                            ) {
                                Ok(side) => side,
                                Err(err) => {
                                    telemetry.log(format!(
                                        "fill outcome validation failed order_id={} asset_id={} outcome={:?}: {}",
                                        order_id, resolved_asset_id, visible_outcome, err
                                    ));
                                    continue;
                                }
                            };
                            if let Some(local_side) = tracker_inventory_side {
                                if local_side != resolved_outcome_side {
                                    telemetry.log(format!(
                                        "local intent mismatch order_id={} tracker_side={} resolved_side={} asset_id={} outcome={:?}",
                                        order_id,
                                        outcome_side_label(local_side),
                                        outcome_side_label(resolved_outcome_side),
                                        resolved_asset_id,
                                        visible_outcome,
                                    ));
                                }
                            }

                            self.emit_strategy_update(InventoryUpdate::Fill(FillExecutionUpdate {
                                fill_id: resolved_fill_id,
                                order_id,
                                asset_id: resolved_asset_id,
                                outcome_side: resolved_outcome_side,
                                price: order_price,
                                filled_amount: delta,
                                fill_timestamp,
                            })).await;
                        }
                        UserChannelMsg::ObservedEvent {
                            order_id,
                            market,
                            status,
                            fill_id,
                            size_matched,
                            original_size,
                            side,
                            asset_id,
                            outcome,
                            fill_price,
                            fill_timestamp: _,
                            raw_event_type,
                            raw_type,
                            raw_status,
                            detail,
                        } => {
                            let scope = scopes.entry(market.clone()).or_default();
                            if let Some(intent) = self.bind_pending_order_from_user_channel(
                                &mut manager,
                                &mut scope.pending_order_intents,
                                &mut scope.last_batch_ids,
                                &order_id,
                                asset_id.as_deref(),
                                fill_price,
                                original_size,
                            ) {
                                note_double_batch_binding(
                                    &mut scope.pending_submitted_double_batch,
                                    intent.local_intent_id,
                                    &order_id,
                                    &telemetry,
                                );
                            }
                            manager.observe_order_event(
                                &order_id,
                                Some(&market),
                                &status,
                                side.as_deref(),
                                asset_id.as_deref(),
                                outcome.as_deref(),
                                fill_price,
                                original_size,
                                size_matched,
                            );

                            telemetry.emit_user_channel_event(UserChannelDynamicEvent {
                                trace_id: manager.account_trace_id(&order_id),
                                local_ts: Utc::now(),
                                source: "order".to_string(),
                                market: Some(market.clone()),
                                summary: Some(format!(
                                    "status={} asset_id={} cumulative={} order_id={}",
                                    status,
                                    asset_id.clone().unwrap_or_else(|| "missing".to_string()),
                                    size_matched
                                        .map(|value| format!("{value:.6}"))
                                        .unwrap_or_else(|| "N/A".to_string()),
                                    order_id,
                                )),
                                order_id,
                                status,
                                batch_trace_id: None,
                                blocking: None,
                                blocking_reason: None,
                                pending_orders: None,
                                blocked_since: None,
                                side,
                                outcome,
                                asset_id,
                                fill_id,
                                matched_amount_cumulative: size_matched,
                                fill_delta: None,
                                filled_amount: None,
                                notional: None,
                                price: fill_price,
                                official_order_type: raw_type.clone(),
                                official_status: raw_status.clone(),
                                raw_event_type: raw_event_type.clone(),
                                raw_type,
                                raw_status,
                                detail: Some(detail),
                            });
                        }
                        UserChannelMsg::OrderTerminated {
                            order_id,
                            market,
                            reason,
                            raw_event_type,
                            raw_type,
                            raw_status,
                        } => {
                            let scope = scopes.entry(market.clone()).or_default();
                            let trace_id = manager.account_trace_id(&order_id);

                            manager.on_terminated(&order_id, &reason);

                            telemetry.emit_user_channel_event(UserChannelDynamicEvent {
                                trace_id,
                                local_ts: Utc::now(),
                                source: "order".to_string(),
                                market: Some(market.clone()),
                                summary: Some(format!(
                                    "status=terminated reason={} order_id={}",
                                    reason, order_id
                                )),
                                order_id: order_id.clone(),
                                status: "terminated".to_string(),
                                batch_trace_id: None,
                                blocking: None,
                                blocking_reason: None,
                                pending_orders: None,
                                blocked_since: None,
                                side: None,
                                outcome: None,
                                asset_id: None,
                                fill_id: None,
                                matched_amount_cumulative: None,
                                fill_delta: None,
                                filled_amount: None,
                                notional: None,
                                price: None,
                                official_order_type: raw_type.clone(),
                                official_status: raw_status.clone(),
                                raw_event_type: raw_event_type.clone(),
                                raw_type,
                                raw_status,
                                detail: Some(reason.clone()),
                            });

                            if let Some(resolution) = release_closed_order_from_tracking(
                                &mut scope.last_batch_ids,
                                &mut scope.pending_cancel_batch,
                                &order_id,
                            ) {
                                match resolution {
                                    PendingCancelResolution::Confirmed { reason } => {
                                        if let Some(reason) =
                                            mark_pending_double_batch_reconcile_required(
                                                &mut scope.pending_submitted_double_batch,
                                                format!(
                                                    "cancel_confirmed_with_unbound_double_leg trace_id={} reason={}",
                                                    trace_id, reason
                                                ),
                                            )
                                        {
                                            scope.mark_reconcile_required(reason.clone(), Utc::now());
                                            self.emit_scope_blocking_event(
                                                &telemetry,
                                                &market,
                                                scope,
                                                trace_id,
                                                "required",
                                                Some(reason.clone()),
                                            );
                                            if self.is_active_market(&market).await {
                                                self.emit_strategy_update(
                                                    InventoryUpdate::BatchReconcileRequired {
                                                        reason,
                                                    },
                                                )
                                                .await;
                                            }
                                        } else {
                                            scope.clear_reconcile_required();
                                            if self.is_active_market(&market).await {
                                                self.emit_strategy_update(
                                                    InventoryUpdate::BatchCancelled { reason },
                                                )
                                                .await;
                                            }
                                        }
                                    }
                                    PendingCancelResolution::ReconcileRequired { reason } => {
                                        scope.mark_reconcile_required(reason.clone(), Utc::now());
                                        self.emit_scope_blocking_event(
                                            &telemetry,
                                            &market,
                                            scope,
                                            trace_id,
                                            "required",
                                            Some(reason.clone()),
                                        );
                                        if self.is_active_market(&market).await {
                                            self.emit_strategy_update(
                                                InventoryUpdate::BatchReconcileRequired { reason },
                                            )
                                            .await;
                                        }
                                    }
                                }
                            } else if scope.pending_cancel_batch.is_none() && scope.last_batch_ids.is_empty() {
                                if let Some(reason) = mark_pending_double_batch_reconcile_required(
                                    &mut scope.pending_submitted_double_batch,
                                    format!(
                                        "termination_observed_with_unbound_double_leg trace_id={} reason={}",
                                        trace_id, reason
                                    ),
                                ) {
                                    scope.mark_reconcile_required(reason.clone(), Utc::now());
                                    self.emit_scope_blocking_event(
                                        &telemetry,
                                        &market,
                                        scope,
                                        trace_id,
                                        "required",
                                        Some(reason.clone()),
                                    );
                                    if self.is_active_market(&market).await {
                                        self.emit_strategy_update(
                                            InventoryUpdate::BatchReconcileRequired { reason },
                                        )
                                        .await;
                                    }
                                } else {
                                    scope.clear_reconcile_required();
                                    if self.is_active_market(&market).await {
                                        self.emit_strategy_update(InventoryUpdate::BatchCancelled {
                                            reason,
                                        }).await;
                                    }
                                }
                            }
                        }
                        UserChannelMsg::ReconnectGap { market, reason } => {
                            let now = Utc::now();
                            let scope = scopes.entry(market.clone()).or_default();
                            let gap_reason = format!("user_channel_gap:{reason}");
                            scope.mark_reconcile_required(gap_reason.clone(), now);
                            if let Some(reason) = mark_pending_cancel_reconcile_required(
                                &mut scope.pending_cancel_batch,
                                format!("cancel_confirmation_gap:{reason}"),
                            ) {
                                scope.mark_reconcile_required(reason.clone(), now);
                                self.emit_scope_blocking_event(
                                    &telemetry,
                                    &market,
                                    scope,
                                    0,
                                    "required",
                                    Some(reason.clone()),
                                );
                                if self.is_active_market(&market).await {
                                    self.emit_strategy_update(InventoryUpdate::BatchReconcileRequired {
                                        reason,
                                    }).await;
                                }
                            }
                            self.emit_scope_blocking_event(
                                &telemetry,
                                &market,
                                scope,
                                0,
                                "required",
                                Some(gap_reason.clone()),
                            );
                            if self.is_active_market(&market).await {
                                self.emit_strategy_update(InventoryUpdate::BatchReconcileRequired {
                                    reason: gap_reason,
                                }).await;
                            }
                            telemetry.emit_user_channel_event(UserChannelDynamicEvent {
                                trace_id: 0,
                                local_ts: Utc::now(),
                                source: "reconcile".to_string(),
                                market: Some(market.clone()),
                                summary: Some(format!(
                                    "status=required market={} detail={}",
                                    market, reason
                                )),
                                order_id: "N/A".to_string(),
                                status: "required".to_string(),
                                batch_trace_id: scope.blocking_trace_id(),
                                blocking: Some(true),
                                blocking_reason: Some(reason.clone()),
                                pending_orders: Some(scope.pending_order_count()),
                                blocked_since: scope.blocked_since(),
                                side: None,
                                outcome: None,
                                asset_id: None,
                                fill_id: None,
                                matched_amount_cumulative: None,
                                fill_delta: None,
                                filled_amount: None,
                                notional: None,
                                price: None,
                                official_order_type: None,
                                official_status: None,
                                raw_event_type: None,
                                raw_type: Some("RECONNECT_GAP".to_string()),
                                raw_status: Some("BACKFILL_REQUIRED".to_string()),
                                detail: Some(reason),
                            });

                            if let Some(update) = self
                                .reconcile_market_scope(&market, scope, &mut manager, &telemetry)
                                .await
                            {
                                if self.is_active_market(&market).await {
                                    self.emit_strategy_update(update).await;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn emit_strategy_update(&self, update: InventoryUpdate) {
        let maybe_tx = self.inventory_tx.lock().await.clone();
        if let Some(tx) = maybe_tx {
            let _ = tx.send(update);
        }
    }

    async fn is_active_market(&self, market: &str) -> bool {
        self.active_market
            .lock()
            .await
            .as_deref()
            .map(|active| active == market)
            .unwrap_or(false)
    }

    fn emit_scope_blocking_event(
        &self,
        telemetry: &TelemetryTx,
        market: &str,
        scope: &MarketExecutionScope,
        trace_id: u64,
        status: &str,
        reason: Option<String>,
    ) {
        telemetry.emit_user_channel_event(UserChannelDynamicEvent {
            trace_id: if trace_id != 0 {
                trace_id
            } else {
                scope.blocking_trace_id().unwrap_or(0)
            },
            local_ts: Utc::now(),
            source: "reconcile".to_string(),
            market: Some(market.to_string()),
            summary: Some(format!(
                "status={} market={} pending_orders={} reason={}",
                status,
                market,
                scope.pending_order_count(),
                reason
                    .clone()
                    .or_else(|| scope.blocking_reason())
                    .unwrap_or_else(|| "missing".to_string()),
            )),
            order_id: "N/A".to_string(),
            status: status.to_string(),
            batch_trace_id: scope.blocking_trace_id(),
            blocking: Some(status != "resolved"),
            blocking_reason: reason.or_else(|| scope.blocking_reason()),
            pending_orders: Some(scope.pending_order_count()),
            blocked_since: scope.blocked_since(),
            side: None,
            outcome: None,
            asset_id: None,
            fill_id: None,
            matched_amount_cumulative: None,
            fill_delta: None,
            filled_amount: None,
            notional: None,
            price: None,
            official_order_type: None,
            official_status: None,
            raw_event_type: None,
            raw_type: Some("RECONCILE_SCOPE".to_string()),
            raw_status: Some(status.to_ascii_uppercase()),
            detail: Some("scope_blocking_state".to_string()),
        });
    }

    async fn reconcile_market_scope(
        &self,
        market: &str,
        scope: &mut MarketExecutionScope,
        manager: &mut OrderManager,
        telemetry: &TelemetryTx,
    ) -> Option<InventoryUpdate> {
        let now = Utc::now();
        let open_orders = match self.fetch_open_orders_for_market(market).await {
            Ok(orders) => orders,
            Err(reason) => {
                scope.mark_reconcile_required(reason.clone(), now);
                self.emit_scope_blocking_event(
                    telemetry,
                    market,
                    scope,
                    0,
                    "required",
                    Some(reason),
                );
                return None;
            }
        };

        let mut live_ids = Vec::new();
        for snapshot in open_orders.values() {
            if let Some(intent) = self.bind_pending_order_from_user_channel(
                manager,
                &mut scope.pending_order_intents,
                &mut scope.last_batch_ids,
                &snapshot.order_id,
                Some(&snapshot.asset_id),
                Some(snapshot.price),
                Some(snapshot.original_size),
            ) {
                note_double_batch_binding(
                    &mut scope.pending_submitted_double_batch,
                    intent.local_intent_id,
                    &snapshot.order_id,
                    telemetry,
                );
            }

            self.observe_snapshot_fill(snapshot, manager, telemetry).await;

            if self.is_dust_snapshot(snapshot) {
                scope.quarantined_order_ids.insert(snapshot.order_id.clone());
                telemetry.emit_user_channel_event(UserChannelDynamicEvent {
                    trace_id: manager.account_trace_id(&snapshot.order_id),
                    local_ts: Utc::now(),
                    source: "reconcile".to_string(),
                    market: Some(market.to_string()),
                    summary: Some(format!(
                        "status=isolated order_id={} remaining_size={:.6} price={:.6}",
                        snapshot.order_id,
                        snapshot.remaining_size(),
                        snapshot.price,
                    )),
                    order_id: snapshot.order_id.clone(),
                    status: "isolated".to_string(),
                    batch_trace_id: scope.blocking_trace_id(),
                    blocking: Some(false),
                    blocking_reason: Some("dust_open_order_isolated".to_string()),
                    pending_orders: Some(scope.pending_order_count()),
                    blocked_since: scope.blocked_since(),
                    side: Some(snapshot.side.clone()),
                    outcome: Some(snapshot.outcome.clone()),
                    asset_id: Some(snapshot.asset_id.clone()),
                    fill_id: None,
                    matched_amount_cumulative: Some(snapshot.size_matched),
                    fill_delta: None,
                    filled_amount: None,
                    notional: Some(snapshot.remaining_size() * snapshot.price),
                    price: Some(snapshot.price),
                    official_order_type: None,
                    official_status: Some(snapshot.status.to_string()),
                    raw_event_type: None,
                    raw_type: Some("RECONCILE_DUST".to_string()),
                    raw_status: Some(snapshot.status.to_string()),
                    detail: Some("dust_open_order_isolated".to_string()),
                });
                let _ = release_closed_order_from_tracking(
                    &mut scope.last_batch_ids,
                    &mut scope.pending_cancel_batch,
                    &snapshot.order_id,
                );
                continue;
            }

            if !live_ids.iter().any(|id| id == &snapshot.order_id) {
                live_ids.push(snapshot.order_id.clone());
            }
        }

        let tracked_missing_ids = scope
            .tracked_order_ids()
            .into_iter()
            .filter(|order_id| !open_orders.contains_key(order_id))
            .collect::<Vec<_>>();

        for order_id in tracked_missing_ids {
            match self.fetch_order_snapshot(&order_id).await {
                Ok(Some(snapshot)) => {
                    if let Some(intent) = self.bind_pending_order_from_user_channel(
                        manager,
                        &mut scope.pending_order_intents,
                        &mut scope.last_batch_ids,
                        &snapshot.order_id,
                        Some(&snapshot.asset_id),
                        Some(snapshot.price),
                        Some(snapshot.original_size),
                    ) {
                        note_double_batch_binding(
                            &mut scope.pending_submitted_double_batch,
                            intent.local_intent_id,
                            &snapshot.order_id,
                            telemetry,
                        );
                    }

                    self.observe_snapshot_fill(&snapshot, manager, telemetry).await;
                    if snapshot.is_open() {
                        if self.is_dust_snapshot(&snapshot) {
                            scope.quarantined_order_ids.insert(snapshot.order_id.clone());
                            let _ = release_closed_order_from_tracking(
                                &mut scope.last_batch_ids,
                                &mut scope.pending_cancel_batch,
                                &snapshot.order_id,
                            );
                        } else if !live_ids.iter().any(|id| id == &snapshot.order_id) {
                            live_ids.push(snapshot.order_id.clone());
                        }
                    } else {
                        manager.on_terminated(
                            &snapshot.order_id,
                            &format!("reconcile terminal status={}", snapshot.status),
                        );
                        let _ = release_closed_order_from_tracking(
                            &mut scope.last_batch_ids,
                            &mut scope.pending_cancel_batch,
                            &snapshot.order_id,
                        );
                    }
                }
                Ok(None) => {
                    manager.on_terminated(&order_id, "reconcile missing order snapshot");
                    let _ = release_closed_order_from_tracking(
                        &mut scope.last_batch_ids,
                        &mut scope.pending_cancel_batch,
                        &order_id,
                    );
                }
                Err(reason) => {
                    scope.mark_reconcile_required(reason.clone(), now);
                    self.emit_scope_blocking_event(
                        telemetry,
                        market,
                        scope,
                        0,
                        "required",
                        Some(reason),
                    );
                    return None;
                }
            }
        }

        scope.last_batch_ids = live_ids;

        if scope.last_batch_ids.is_empty() {
            scope.pending_cancel_batch = None;
            if scope.pending_submitted_double_batch.is_some() || !scope.pending_order_intents.is_empty()
            {
                scope.pending_submitted_double_batch = None;
                scope.pending_order_intents.clear();
                scope.clear_reconcile_required();
                self.emit_scope_blocking_event(
                    telemetry,
                    market,
                    scope,
                    0,
                    "resolved",
                    Some("reconcile_cleared_unbound_submission".to_string()),
                );
                return Some(InventoryUpdate::BatchSubmissionFailed {
                    reason: "reconcile_cleared_unbound_submission".to_string(),
                });
            }

            scope.clear_reconcile_required();
            self.emit_scope_blocking_event(
                telemetry,
                market,
                scope,
                0,
                "resolved",
                Some("reconcile_book_clean".to_string()),
            );
            return Some(InventoryUpdate::ReconcileResolved {
                reason: "reconcile_book_clean".to_string(),
                clear_active_batch: true,
            });
        }

        scope.pending_cancel_batch = None;
        let trace_id = scope
            .blocking_trace_id()
            .unwrap_or_else(|| now.timestamp_micros() as u64);
        match self
            .cancel_previous_batch(
                manager,
                &scope.last_batch_ids,
                &mut scope.pending_cancel_batch,
                trace_id,
                "reconcile_retry",
                market,
                telemetry,
            )
            .await
        {
            CancelRequestOutcome::NoOpenOrders => {
                scope.clear_reconcile_required();
                self.emit_scope_blocking_event(
                    telemetry,
                    market,
                    scope,
                    trace_id,
                    "resolved",
                    Some("reconcile_retry_no_open_orders".to_string()),
                );
                Some(InventoryUpdate::ReconcileResolved {
                    reason: "reconcile_retry_no_open_orders".to_string(),
                    clear_active_batch: true,
                })
            }
            CancelRequestOutcome::RequestSent => {
                self.emit_scope_blocking_event(
                    telemetry,
                    market,
                    scope,
                    trace_id,
                    "retry_cancel_requested",
                    Some("reconcile_retry".to_string()),
                );
                None
            }
            CancelRequestOutcome::Failed(reason) => {
                scope.mark_reconcile_required(reason.clone(), now);
                self.emit_scope_blocking_event(
                    telemetry,
                    market,
                    scope,
                    trace_id,
                    "required",
                    Some(reason),
                );
                None
            }
        }
    }

    async fn fetch_open_orders_for_market(
        &self,
        market: &str,
    ) -> Result<HashMap<String, OrderSnapshot>, String> {
        let request = OrdersRequest::builder().market(market.to_string()).build();
        let mut next_cursor = None;
        let mut snapshots = HashMap::new();

        loop {
            let page = self
                .client
                .orders(&request, next_cursor.clone())
                .await
                .map_err(|err| format!("reconcile_orders_fetch_failed:{err:?}"))?;
            for response in page.data {
                let snapshot = OrderSnapshot::from_response(response);
                snapshots.insert(snapshot.order_id.clone(), snapshot);
            }

            let cursor = page.next_cursor.trim().to_string();
            if cursor.is_empty() || cursor.eq_ignore_ascii_case("LTE=") {
                break;
            }
            next_cursor = Some(cursor);
        }

        Ok(snapshots)
    }

    async fn fetch_order_snapshot(&self, order_id: &str) -> Result<Option<OrderSnapshot>, String> {
        match self.client.order(order_id).await {
            Ok(response) => Ok(Some(OrderSnapshot::from_response(response))),
            Err(err) => {
                let text = format!("{err:?}");
                if text.contains("404") || text.contains("NotFound") {
                    Ok(None)
                } else {
                    Err(format!("reconcile_order_fetch_failed:{order_id}:{text}"))
                }
            }
        }
    }

    async fn observe_snapshot_fill(
        &self,
        snapshot: &OrderSnapshot,
        manager: &mut OrderManager,
        telemetry: &TelemetryTx,
    ) {
        let effective_full = !snapshot.is_open()
            && self.is_dust_remaining(snapshot.remaining_size(), snapshot.price);
        let status = snapshot_fill_status(snapshot, effective_full);
        let delta = manager
            .observe_order_event(
                &snapshot.order_id,
                Some(&snapshot.market),
                status,
                Some(snapshot.side.as_str()),
                Some(snapshot.asset_id.as_str()),
                Some(snapshot.outcome.as_str()),
                Some(snapshot.price),
                Some(snapshot.original_size),
                Some(snapshot.size_matched),
            )
            .map(|entry| entry.delta);
        let trace_id = manager.account_trace_id(&snapshot.order_id);

        if let Some(delta) = delta {
            telemetry.emit_user_channel_event(UserChannelDynamicEvent {
                trace_id,
                local_ts: Utc::now(),
                source: "reconcile".to_string(),
                market: Some(snapshot.market.clone()),
                summary: Some(format!(
                    "status={} order_id={} cumulative={:.6} delta={:.6}",
                    status, snapshot.order_id, snapshot.size_matched, delta
                )),
                order_id: snapshot.order_id.clone(),
                status: status.to_string(),
                batch_trace_id: None,
                blocking: None,
                blocking_reason: None,
                pending_orders: None,
                blocked_since: None,
                side: Some(snapshot.side.clone()),
                outcome: Some(snapshot.outcome.clone()),
                asset_id: Some(snapshot.asset_id.clone()),
                fill_id: Some(format!(
                    "{}-reconcile-{:.8}",
                    snapshot.order_id, snapshot.size_matched
                )),
                matched_amount_cumulative: Some(snapshot.size_matched),
                fill_delta: Some(delta),
                filled_amount: Some(delta),
                notional: Some(delta * snapshot.price),
                price: Some(snapshot.price),
                official_order_type: None,
                official_status: Some(snapshot.status.to_string()),
                raw_event_type: Some("reconcile".to_string()),
                raw_type: Some("SNAPSHOT".to_string()),
                raw_status: Some(snapshot.status.to_string()),
                detail: Some("reconcile_snapshot_fill".to_string()),
            });
        }

        if !self.is_active_market(&snapshot.market).await {
            return;
        }

        let Some(delta) = delta else {
            return;
        };

        if !snapshot.side.eq_ignore_ascii_case("BUY") {
            return;
        }

        let resolver = self.active_outcome_resolver.lock().await.clone();
        let Some(resolver) = resolver else {
            return;
        };
        let Ok(outcome_side) = resolver.resolve_outcome_side(
            &snapshot.asset_id,
            Some(snapshot.outcome.as_str()),
        ) else {
            return;
        };

        self.emit_strategy_update(InventoryUpdate::Fill(FillExecutionUpdate {
            fill_id: format!("{}-reconcile-{:.8}", snapshot.order_id, snapshot.size_matched),
            order_id: snapshot.order_id.clone(),
            asset_id: snapshot.asset_id.clone(),
            outcome_side,
            price: snapshot.price,
            filled_amount: delta,
            fill_timestamp: Utc::now(),
        }))
        .await;
    }

    fn is_dust_remaining(&self, remaining_size: f64, price: f64) -> bool {
        if remaining_size <= 0.0 {
            return true;
        }

        remaining_size <= self.dust_share_threshold
            || remaining_size * price.max(0.0) <= self.dust_notional_threshold
    }

    fn is_dust_snapshot(&self, snapshot: &OrderSnapshot) -> bool {
        self.is_dust_remaining(snapshot.remaining_size(), snapshot.price)
    }

    fn bind_pending_order_from_user_channel(
        &self,
        manager: &mut OrderManager,
        pending_order_intents: &mut Vec<PendingOrderIntent>,
        last_batch_ids: &mut Vec<String>,
        order_id: &str,
        asset_id: Option<&str>,
        price: Option<f64>,
        original_size: Option<f64>,
    ) -> Option<PendingOrderIntent> {
        if order_id.is_empty() || manager.orders.contains_key(order_id) {
            return None;
        }

        let Some(asset_id) = asset_id else {
            return None;
        };
        let Some(price) = price else {
            return None;
        };
        let Some(original_size) = original_size else {
            return None;
        };

        let matching_positions: Vec<usize> = pending_order_intents
            .iter()
            .enumerate()
            .filter_map(|(idx, intent)| {
                (intent.asset_id == asset_id
                    && self.matches_bind_value(intent.price, price, self.bind_price_tolerance)
                    && self.matches_bind_value(intent.size, original_size, self.bind_size_tolerance))
                .then_some(idx)
            })
            .collect();
        let Some(position) = matching_positions.first().copied() else {
            return None;
        };

        if matching_positions.len() > 1 {
            let candidate_ids = matching_positions
                .iter()
                .filter_map(|idx| pending_order_intents.get(*idx))
                .map(|intent| intent.local_intent_id.to_string())
                .collect::<Vec<_>>()
                .join(",");
            manager.telemetry.log(format!(
                "ambiguous pending intent bind order_id={} asset_id={} price={:.6} size={:.6} local_intents=[{}]",
                order_id, asset_id, price, original_size, candidate_ids
            ));
        }

        let Some(intent) = pending_order_intents.get(position).cloned() else {
            return None;
        };
        pending_order_intents.remove(position);
        manager.on_sent(
            order_id.to_string(),
            intent.price,
            intent.size,
            intent.outcome_side,
            "BUY",
            &intent.asset_id,
            intent.trace_id,
            intent.local_intent_id,
        );
        manager.on_acked(order_id, "live");

        if !last_batch_ids.iter().any(|id| id == order_id) {
            last_batch_ids.push(order_id.to_string());
        }

        Some(intent)
    }

    fn matches_bind_value(&self, left: f64, right: f64, tolerance: f64) -> bool {
        (left - right).abs() <= tolerance.max(1e-9)
    }

    async fn execute_batch_maker(
        &self,
        up_id: &str,
        up_price: f64,
        up_size: f64,
        down_id: &str,
        down_price: f64,
        down_size: f64,
        pending_order_intents: &mut Vec<PendingOrderIntent>,
        next_local_intent_id: &mut u64,
        trace_id: u64,
        telemetry: &TelemetryTx,
    ) -> Result<DoubleBatchSubmission, String> {
        telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::PreprocessCompleted);

        let mut signed_orders = Vec::new();
        let mut intents = Vec::new();

        let up_price_decimal = Decimal::from_f64(up_price).unwrap_or(Decimal::ZERO);
        let up_size_decimal = Decimal::from_f64(up_size).unwrap_or(Decimal::ZERO);
        telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::ParametersNormalized);
        let up_order = self
            .client
            .limit_order()
            .token_id(up_id)
            .order_type(OrderType::GTC)
            .price(up_price_decimal)
            .size(up_size_decimal)
            .side(Side::Buy)
            .build()
            .await
            .map_err(|err| {
                telemetry.log(format!(
                    "double batch prepare failed: leg=UP stage=build err={err:?}"
                ));
                "double_batch_prepare_up_build_failed".to_string()
            })?;
        telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::RequestConstructed);
        let up_signed = self
            .client
            .sign(&self.signer, up_order)
            .await
            .map_err(|err| {
                telemetry.log(format!(
                    "double batch prepare failed: leg=UP stage=sign err={err:?}"
                ));
                "double_batch_prepare_up_sign_failed".to_string()
            })?;
        telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::Signed);
        signed_orders.push(up_signed);
        intents.push(PendingOrderIntent {
            local_intent_id: allocate_local_intent_id(next_local_intent_id),
            trace_id,
            outcome_side: OutcomeSide::Up,
            asset_id: up_id.to_string(),
            price: up_price,
            size: up_size,
        });

        let down_price_decimal = Decimal::from_f64(down_price).unwrap_or(Decimal::ZERO);
        let down_size_decimal = Decimal::from_f64(down_size).unwrap_or(Decimal::ZERO);
        telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::ParametersNormalized);
        let down_order = self
            .client
            .limit_order()
            .token_id(down_id)
            .order_type(OrderType::GTC)
            .price(down_price_decimal)
            .size(down_size_decimal)
            .side(Side::Buy)
            .build()
            .await
            .map_err(|err| {
                telemetry.log(format!(
                    "double batch prepare failed: leg=DOWN stage=build err={err:?}"
                ));
                "double_batch_prepare_down_build_failed".to_string()
            })?;
        telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::RequestConstructed);
        let down_signed = self
            .client
            .sign(&self.signer, down_order)
            .await
            .map_err(|err| {
                telemetry.log(format!(
                    "double batch prepare failed: leg=DOWN stage=sign err={err:?}"
                ));
                "double_batch_prepare_down_sign_failed".to_string()
            })?;
        telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::Signed);
        signed_orders.push(down_signed);
        intents.push(PendingOrderIntent {
            local_intent_id: allocate_local_intent_id(next_local_intent_id),
            trace_id,
            outcome_side: OutcomeSide::Down,
            asset_id: down_id.to_string(),
            price: down_price,
            size: down_size,
        });

        if signed_orders.len() != 2 || intents.len() != 2 {
            telemetry.log(format!(
                "double batch aborted locally: signed_legs={} intent_legs={}",
                signed_orders.len(),
                intents.len()
            ));
            return Err("double_batch_aborted_local_all_or_nothing".to_string());
        }

        telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::RequestSent);
        telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::RequestSent);
        match self.client.post_orders(signed_orders).await {
            Ok(_) => {
                let local_intent_ids = intents
                    .iter()
                    .map(|intent| intent.local_intent_id)
                    .collect::<HashSet<_>>();
                pending_order_intents.extend(intents);
                Ok(DoubleBatchSubmission {
                    trace_id,
                    up_price,
                    up_size,
                    down_price,
                    down_size,
                    local_intent_ids,
                })
            }
            Err(err) => {
                telemetry.log(format!("batch post failed: {:?}", err));
                Err("double_batch_post_failed".to_string())
            }
        }
    }

    async fn execute_single_order(
        &self,
        outcome_side: OutcomeSide,
        asset_id: &str,
        price: f64,
        size: f64,
        pending_order_intents: &mut Vec<PendingOrderIntent>,
        next_local_intent_id: &mut u64,
        trace_id: u64,
        telemetry: &TelemetryTx,
    ) -> bool {
        telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::PreprocessCompleted);

        let price_decimal = Decimal::from_f64(price).unwrap_or(Decimal::ZERO);
        let size_decimal = Decimal::from_f64(size).unwrap_or(Decimal::ZERO);
        telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::ParametersNormalized);

        if let Ok(order) = self
            .client
            .limit_order()
            .token_id(asset_id)
            .order_type(OrderType::GTC)
            .price(price_decimal)
            .size(size_decimal)
            .side(Side::Buy)
            .build()
            .await
        {
            telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::RequestConstructed);
            if let Ok(signed) = self.client.sign(&self.signer, order).await {
                telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::Signed);
                telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::RequestSent);
                telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::RequestSent);
                return match self.client.post_orders(vec![signed]).await {
                    Ok(_) => {
                        pending_order_intents.push(PendingOrderIntent {
                            local_intent_id: allocate_local_intent_id(next_local_intent_id),
                            trace_id,
                            outcome_side,
                            asset_id: asset_id.to_string(),
                            price,
                            size,
                        });
                        true
                    }
                    Err(err) => {
                        telemetry.log(format!("single-sided post failed: {:?}", err));
                        false
                    }
                };
            }
        }

        false
    }

    async fn cancel_previous_batch(
        &self,
        manager: &mut OrderManager,
        last_batch_ids: &[String],
        pending_cancel_batch: &mut Option<PendingCancelBatch>,
        trace_id: u64,
        reason: &str,
        market: &str,
        telemetry: &TelemetryTx,
    ) -> CancelRequestOutcome {
        if last_batch_ids.is_empty() {
            return CancelRequestOutcome::NoOpenOrders;
        }

        if pending_cancel_batch.is_some() {
            return CancelRequestOutcome::RequestSent;
        }

        let ids = last_batch_ids.to_vec();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();

        for order_id in &ids {
            manager.on_cancelling(order_id);
        }

        match tokio::time::timeout(
            Duration::from_millis(self.cancel_timeout_ms.max(1)),
            self.client.cancel_orders(&id_refs),
        )
        .await
        {
            Ok(Ok(_)) => {
                *pending_cancel_batch = Some(PendingCancelBatch {
                    trace_id,
                    reason: reason.to_string(),
                    requested_at: Utc::now(),
                    pending_order_ids: ids.iter().cloned().collect(),
                    reconcile_required_emitted: false,
                });
                telemetry.emit_user_channel_event(UserChannelDynamicEvent {
                    trace_id,
                    local_ts: Utc::now(),
                    source: "local".to_string(),
                    market: Some(market.to_string()),
                    summary: Some("status=cancel_requested".to_string()),
                    order_id: ids.join(","),
                    status: "cancel_requested".to_string(),
                    batch_trace_id: Some(trace_id),
                    blocking: Some(true),
                    blocking_reason: Some(reason.to_string()),
                    pending_orders: Some(ids.len()),
                    blocked_since: Some(Utc::now()),
                    side: None,
                    outcome: None,
                    asset_id: None,
                    fill_id: None,
                    matched_amount_cumulative: None,
                    fill_delta: None,
                    filled_amount: None,
                    notional: None,
                    price: None,
                    official_order_type: None,
                    official_status: None,
                    raw_event_type: None,
                    raw_type: None,
                    raw_status: None,
                    detail: Some("batch_cancel_requested".to_string()),
                });
                CancelRequestOutcome::RequestSent
            }
            Ok(Err(err)) => {
                telemetry.log(format!("cancel batch failed: {:?}", err));
                CancelRequestOutcome::Failed("cancel_request_failed".to_string())
            }
            Err(_) => {
                telemetry.log("cancel batch timed out".to_string());
                CancelRequestOutcome::Failed("cancel_request_timed_out".to_string())
            }
        }
    }
}

fn allocate_local_intent_id(next_local_intent_id: &mut u64) -> u64 {
    let current = *next_local_intent_id;
    *next_local_intent_id = (*next_local_intent_id).saturating_add(1);
    current
}

fn note_double_batch_binding(
    pending_double_batch: &mut Option<PendingSubmittedDoubleBatch>,
    local_intent_id: u64,
    order_id: &str,
    telemetry: &TelemetryTx,
) {
    let Some(batch) = pending_double_batch.as_mut() else {
        return;
    };

    if !batch.pending_local_intent_ids.remove(&local_intent_id) {
        return;
    }

    batch.bound_order_ids.insert(order_id.to_string());
    telemetry.log(format!(
        "double batch bind progress: trace_id={} bound_orders={} missing_legs={}",
        batch.trace_id,
        batch.bound_order_ids.len(),
        batch.pending_local_intent_ids.len()
    ));

    if batch.pending_local_intent_ids.is_empty() {
        *pending_double_batch = None;
    }
}

fn pending_double_bind_timeout_reason(
    pending_double_batch: &mut Option<PendingSubmittedDoubleBatch>,
    now: DateTime<Utc>,
    bind_confirm_timeout_ms: u64,
) -> Option<String> {
    let batch = pending_double_batch.as_ref()?;
    if batch.reconcile_required_emitted {
        return None;
    }

    let elapsed_ms = now
        .signed_duration_since(batch.submitted_at)
        .num_milliseconds()
        .max(0) as u64;
    if elapsed_ms < bind_confirm_timeout_ms.max(1) {
        return None;
    }

    mark_pending_double_batch_reconcile_required(
        pending_double_batch,
        format!(
            "double_batch_bind_timeout trace_id={} bound_legs={} missing_legs={}",
            batch.trace_id,
            batch.bound_order_ids.len(),
            batch.pending_local_intent_ids.len()
        ),
    )
}

fn mark_pending_double_batch_reconcile_required(
    pending_double_batch: &mut Option<PendingSubmittedDoubleBatch>,
    reason: String,
) -> Option<String> {
    let batch = pending_double_batch.as_mut()?;
    if batch.reconcile_required_emitted {
        return None;
    }

    batch.reconcile_required_emitted = true;
    Some(reason)
}

fn pending_cancel_timeout_reason(
    pending_cancel_batch: &mut Option<PendingCancelBatch>,
    now: DateTime<Utc>,
    cancel_confirm_timeout_ms: u64,
) -> Option<String> {
    let batch = pending_cancel_batch.as_mut()?;
    if batch.reconcile_required_emitted {
        return None;
    }

    let elapsed_ms = now
        .signed_duration_since(batch.requested_at)
        .num_milliseconds()
        .max(0) as u64;
    if elapsed_ms < cancel_confirm_timeout_ms.max(1) {
        return None;
    }

    batch.reconcile_required_emitted = true;
    Some(format!(
        "cancel_confirmation_timeout trace_id={} reason={} pending_orders={}",
        batch.trace_id,
        batch.reason,
        batch.pending_order_ids.len()
    ))
}

fn mark_pending_cancel_reconcile_required(
    pending_cancel_batch: &mut Option<PendingCancelBatch>,
    reason: String,
) -> Option<String> {
    let batch = pending_cancel_batch.as_mut()?;
    if batch.reconcile_required_emitted {
        return None;
    }

    batch.reconcile_required_emitted = true;
    Some(format!(
        "{} trace_id={} cancel_reason={} pending_orders={}",
        reason,
        batch.trace_id,
        batch.reason,
        batch.pending_order_ids.len()
    ))
}

fn release_closed_order_from_tracking(
    last_batch_ids: &mut Vec<String>,
    pending_cancel_batch: &mut Option<PendingCancelBatch>,
    order_id: &str,
) -> Option<PendingCancelResolution> {
    last_batch_ids.retain(|id| id != order_id);

    let batch = pending_cancel_batch.as_mut()?;
    batch.pending_order_ids.remove(order_id);

    if !batch.pending_order_ids.is_empty() {
        return None;
    }

    if last_batch_ids.is_empty() {
        let reason = batch.reason.clone();
        *pending_cancel_batch = None;
        return Some(PendingCancelResolution::Confirmed { reason });
    }

    if batch.reconcile_required_emitted {
        return None;
    }

    batch.reconcile_required_emitted = true;
    Some(PendingCancelResolution::ReconcileRequired {
        reason: format!(
            "cancel_confirmed_but_live_order_ids_remain trace_id={} cancel_reason={} live_order_ids={}",
            batch.trace_id,
            batch.reason,
            last_batch_ids.join(",")
        ),
    })
}

fn resolved_fill_id(fill_id: Option<&str>, order_id: &str, cumulative_size_matched: f64) -> String {
    fill_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("{order_id}-matched-{cumulative_size_matched:.8}"))
}

fn outcome_side_label(side: OutcomeSide) -> &'static str {
    match side {
        OutcomeSide::Up => "UP",
        OutcomeSide::Down => "DOWN",
    }
}

fn visible_fill_status(is_full: bool, raw_status: Option<&str>) -> &'static str {
    if is_full {
        return "full_fill";
    }

    match raw_status.unwrap_or("").to_ascii_uppercase().as_str() {
        "MATCHED" | "MINED" | "EXECUTED" | "CONFIRMED" => "matched",
        _ => "partial_fill",
    }
}

fn snapshot_fill_status(snapshot: &OrderSnapshot, effective_full: bool) -> &'static str {
    if effective_full || snapshot.remaining_size() <= 0.0 {
        return "full_fill";
    }

    match snapshot.status {
        OrderStatusType::Matched => "matched",
        _ => "partial_fill",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::collections::HashSet;

    fn telemetry() -> TelemetryTx {
        crate::telemetry::start_telemetry_thread()
    }

    #[test]
    fn resolved_fill_id_uses_cumulative_matched_size_when_official_fill_id_is_missing() {
        let first = resolved_fill_id(None, "order-1", 1.0);
        let second = resolved_fill_id(None, "order-1", 2.0);

        assert_ne!(first, second);
        assert!(first.contains("order-1"));
        assert!(second.contains("2.00000000"));
    }

    #[test]
    fn resolved_fill_id_prefers_official_fill_id_when_present() {
        assert_eq!(
            resolved_fill_id(Some("official-fill"), "order-1", 3.0),
            "official-fill"
        );
    }

    #[test]
    fn pending_cancel_is_confirmed_only_after_all_ids_terminate() {
        let mut pending = Some(PendingCancelBatch {
            trace_id: 7,
            reason: "order_timeout".to_string(),
            requested_at: Utc::now(),
            pending_order_ids: ["order-1".to_string(), "order-2".to_string()]
                .into_iter()
                .collect(),
            reconcile_required_emitted: false,
        });
        let mut live_ids = vec!["order-1".to_string(), "order-2".to_string()];

        let first = release_closed_order_from_tracking(&mut live_ids, &mut pending, "order-1");
        assert!(first.is_none());
        assert!(pending.is_some());
        assert_eq!(live_ids, vec!["order-2".to_string()]);

        let second = release_closed_order_from_tracking(&mut live_ids, &mut pending, "order-2");
        assert_eq!(
            second,
            Some(PendingCancelResolution::Confirmed {
                reason: "order_timeout".to_string(),
            })
        );
        assert!(pending.is_none());
        assert!(live_ids.is_empty());
    }

    #[test]
    fn pending_cancel_requires_reconcile_when_other_live_ids_remain() {
        let mut pending = Some(PendingCancelBatch {
            trace_id: 9,
            reason: "executor_clean_book_guard".to_string(),
            requested_at: Utc::now(),
            pending_order_ids: ["order-1".to_string()].into_iter().collect(),
            reconcile_required_emitted: false,
        });
        let mut live_ids = vec!["order-1".to_string(), "late-order".to_string()];

        let resolution = release_closed_order_from_tracking(&mut live_ids, &mut pending, "order-1");

        assert!(matches!(
            resolution,
            Some(PendingCancelResolution::ReconcileRequired { .. })
        ));
        assert!(
            pending
                .as_ref()
                .map(|batch| batch.reconcile_required_emitted)
                .unwrap_or(false)
        );
    }

    #[test]
    fn full_fill_releases_order_from_live_tracking_without_pending_cancel() {
        let mut live_ids = vec!["order-1".to_string(), "order-2".to_string()];
        let mut pending = None;

        let resolution = release_closed_order_from_tracking(&mut live_ids, &mut pending, "order-1");

        assert!(resolution.is_none());
        assert_eq!(live_ids, vec!["order-2".to_string()]);
        assert!(pending.is_none());
    }

    #[test]
    fn late_full_fill_releases_order_from_pending_cancel_ids() {
        let mut pending = Some(PendingCancelBatch {
            trace_id: 13,
            reason: "order_timeout".to_string(),
            requested_at: Utc::now(),
            pending_order_ids: ["order-1".to_string(), "order-2".to_string()]
                .into_iter()
                .collect(),
            reconcile_required_emitted: false,
        });
        let mut live_ids = vec!["order-1".to_string(), "order-2".to_string()];

        let resolution = release_closed_order_from_tracking(&mut live_ids, &mut pending, "order-1");

        assert!(resolution.is_none());
        assert_eq!(live_ids, vec!["order-2".to_string()]);
        assert_eq!(
            pending
                .as_ref()
                .map(|batch| batch.pending_order_ids.iter().cloned().collect::<Vec<_>>())
                .unwrap(),
            vec!["order-2".to_string()]
        );
    }

    #[test]
    fn pending_cancel_timeout_emits_once() {
        let now = Utc::now();
        let mut pending = Some(PendingCancelBatch {
            trace_id: 11,
            reason: "order_timeout".to_string(),
            requested_at: now - Duration::seconds(20),
            pending_order_ids: ["order-1".to_string()].into_iter().collect(),
            reconcile_required_emitted: false,
        });

        let first = pending_cancel_timeout_reason(&mut pending, now, 5_000);
        let second = pending_cancel_timeout_reason(&mut pending, now, 5_000);

        assert!(first.is_some());
        assert!(second.is_none());
    }

    #[test]
    fn double_batch_bind_progress_clears_tracker_after_second_leg_binds() {
        let telemetry = telemetry();
        let mut pending = Some(PendingSubmittedDoubleBatch {
            trace_id: 21,
            submitted_at: Utc::now(),
            pending_local_intent_ids: [1_u64, 2_u64].into_iter().collect(),
            bound_order_ids: HashSet::new(),
            reconcile_required_emitted: false,
        });

        note_double_batch_binding(&mut pending, 1, "order-up", &telemetry);
        assert!(pending.is_some());
        assert_eq!(
            pending
                .as_ref()
                .map(|batch| batch.pending_local_intent_ids.clone())
                .unwrap(),
            [2_u64].into_iter().collect()
        );

        note_double_batch_binding(&mut pending, 2, "order-down", &telemetry);
        assert!(pending.is_none());
    }

    #[test]
    fn double_batch_bind_timeout_emits_once() {
        let now = Utc::now();
        let mut pending = Some(PendingSubmittedDoubleBatch {
            trace_id: 31,
            submitted_at: now - Duration::seconds(10),
            pending_local_intent_ids: [2_u64].into_iter().collect(),
            bound_order_ids: ["order-up".to_string()].into_iter().collect(),
            reconcile_required_emitted: false,
        });

        let first = pending_double_bind_timeout_reason(&mut pending, now, 5_000);
        let second = pending_double_bind_timeout_reason(&mut pending, now, 5_000);

        assert!(
            first
                .as_ref()
                .map(|reason| reason.contains("double_batch_bind_timeout"))
                .unwrap_or(false)
        );
        assert!(second.is_none());
    }

    #[test]
    fn snapshot_fill_status_treats_terminal_dust_as_full_fill() {
        let snapshot = OrderSnapshot {
            order_id: "order-1".to_string(),
            market: "market-1".to_string(),
            asset_id: "asset-up".to_string(),
            outcome: "YES".to_string(),
            side: "BUY".to_string(),
            price: 0.79,
            original_size: 5.0,
            size_matched: 4.997_618,
            status: OrderStatusType::Canceled,
        };

        assert_eq!(snapshot_fill_status(&snapshot, true), "full_fill");
    }
}
