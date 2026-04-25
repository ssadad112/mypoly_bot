use std::error::Error;

use futures::StreamExt;
use log::{error, info};
use polymarket_client_sdk::clob::ws::Client as WebSocketClient;
use rust_decimal::prelude::ToPrimitive;
use tokio::time::Duration;

use crate::engine::core::params::StrategyConfig;
use crate::engine::core::state::MarketState;
use crate::engine::core::strategy::{Strategy, TradeAction};
use crate::engine::execution::executor::InventoryUpdate;
use crate::engine::telemetry::events::MarketDynamicEvent;
use crate::engine::telemetry::latency::{BestBidAskStage, ExecutorPrepStage};
use crate::telemetry::TelemetryTx;

fn handle_action(
    action: TradeAction,
    state: &mut MarketState,
    telemetry: &TelemetryTx,
    action_tx: &tokio::sync::watch::Sender<(TradeAction, u64)>,
    trace_id: u64,
) -> bool {
    match action {
        TradeAction::AbortMarket => {
            telemetry.log("[market] abort requested by strategy".to_string());
            true
        }
        TradeAction::DoNothing => false,
        TradeAction::CancelAllOpenOrders { reason } => {
            telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::ActionSent);
            telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::ActionChannelSent);
            if action_tx
                .send((TradeAction::CancelAllOpenOrders { reason }, trace_id))
                .is_ok()
            {
                state.mark_active_batch_cancelling();
            }
            false
        }
        TradeAction::PlaceDoubleSidedOrder {
            up_token_id,
            up_price,
            up_size,
            down_token_id,
            down_price,
            down_size,
        } => {
            telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::ActionSent);
            telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::ActionChannelSent);
            if action_tx
                .send((
                    TradeAction::PlaceDoubleSidedOrder {
                        up_token_id,
                        up_price,
                        up_size,
                        down_token_id,
                        down_price,
                        down_size,
                    },
                    trace_id,
                ))
                .is_ok()
            {
                state.mark_double_submission_pending(trace_id);
            }
            false
        }
        TradeAction::PlaceSingleSidedAdjustmentOrder {
            outcome_side,
            asset_id,
            price,
            size,
        } => {
            telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::ActionSent);
            telemetry.mark_executor_stage(trace_id, ExecutorPrepStage::ActionChannelSent);
            if action_tx
                .send((
                    TradeAction::PlaceSingleSidedAdjustmentOrder {
                        outcome_side,
                        asset_id: asset_id.clone(),
                        price,
                        size,
                    },
                    trace_id,
                ))
                .is_ok()
            {
                state.record_single_sided_batch(
                    trace_id,
                    chrono::Utc::now(),
                    outcome_side,
                    &asset_id,
                    price,
                    size,
                );
            }
            false
        }
    }
}

fn apply_inventory_update(
    state: &mut MarketState,
    update: InventoryUpdate,
    telemetry: &TelemetryTx,
) {
    match update {
        InventoryUpdate::Fill(fill) => {
            if let Err(err) = state.apply_fill_delta(fill.into_fill_delta()) {
                telemetry.log(format!("inventory update rejected: {err}"));
            }
        }
        InventoryUpdate::DoubleBatchSubmitted {
            trace_id,
            up_price,
            up_size,
            down_price,
            down_size,
        } => {
            state.clear_execution_reconcile_required();
            state.clear_double_submission_pending();
            state.record_double_sided_batch(
                trace_id,
                chrono::Utc::now(),
                up_price,
                up_size,
                down_price,
                down_size,
            );
            telemetry.log(format!(
                "[market] double batch submitted: trace_id={} up={:.6}@{:.6} down={:.6}@{:.6}",
                trace_id, up_size, up_price, down_size, down_price
            ));
        }
        InventoryUpdate::BatchCancelled { reason } => {
            state.clear_execution_reconcile_required();
            state.clear_double_submission_pending();
            state.clear_active_batch();
            telemetry.log(format!("[market] batch cleared: {reason}"));
        }
        InventoryUpdate::BatchCancelRequestFailed { reason } => {
            state.mark_active_batch_open_again_after_cancel_failure();
            telemetry.log(format!("[market] batch cancel request failed: {reason}"));
        }
        InventoryUpdate::BatchReconcileRequired { reason } => {
            state.mark_execution_reconcile_required();
            state.clear_double_submission_pending();
            state.mark_active_batch_reconcile_required();
            telemetry.log(format!("[market] batch reconcile required: {reason}"));
        }
        InventoryUpdate::ReconcileResolved {
            reason,
            clear_active_batch,
        } => {
            state.clear_execution_reconcile_required();
            state.clear_double_submission_pending();
            if clear_active_batch {
                state.clear_active_batch();
            }
            telemetry.log(format!("[market] batch reconcile resolved: {reason}"));
        }
        InventoryUpdate::BatchSubmissionFailed { reason } => {
            state.clear_double_submission_pending();
            state.clear_active_batch();
            telemetry.log(format!("[market] batch submit failed: {reason}"));
        }
    }
}

fn evaluate_and_dispatch(
    state: &mut MarketState,
    config: &StrategyConfig,
    telemetry: &TelemetryTx,
    action_tx: &tokio::sync::watch::Sender<(TradeAction, u64)>,
    trace_id: u64,
) -> bool {
    telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::StrategyStarted);
    let action = Strategy::next_action(state, config);
    telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::StrategyCompleted);
    telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::TradeActionCreated);
    handle_action(action, state, telemetry, action_tx, trace_id)
}

fn build_market_dynamic_event(
    state: &MarketState,
    config: &StrategyConfig,
    trace_id: u64,
    event_type: &'static str,
    market_ts_ms: Option<u64>,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    asset_id: Option<String>,
) -> MarketDynamicEvent {
    let local_ts = chrono::Utc::now();
    MarketDynamicEvent {
        trace_id,
        event_type,
        market_ts_ms,
        local_ts,
        eth_price: Some(state.eth_price),
        ptb_price: Some(state.strike_price),
        eth_ptb_spread: Some(state.eth_price - state.strike_price),
        best_bid,
        best_ask,
        asset_id,
        entry_gate_passed: Some(state.entry_gate_passed),
        elapsed_secs: Some(state.elapsed_secs(local_ts)),
        seconds_to_start: Some(state.seconds_to_start(local_ts)),
        q_up: Some(state.q_up),
        q_down: Some(state.q_down),
        q_diff: Some(state.q_diff),
        q_allow_abs: Some(config.q_allow_abs),
        q_balance_band: Some(config.q_balance_band),
        q_flip_stop_band: Some(config.q_flip_stop_band),
        adjustment_active: Some(state.adjustment_active),
        cost_up: Some(state.cost_up),
        cost_down: Some(state.cost_down),
        cost_total: Some(state.cost_total),
        avg_up_cost: state.avg_up_cost,
        avg_down_cost: state.avg_down_cost,
        avg_cost_sum: state.avg_cost_sum,
        k_cost_sum: Some(config.k_cost_sum),
        profit_if_up: Some(state.profit_if_up),
        profit_if_down: Some(state.profit_if_down),
        profit_min: Some(state.profit_min),
        g_target: Some(config.g_target),
        active_order_age_secs: state.active_order_age_secs(local_ts),
        t_cancel: Some(config.t_cancel),
    }
}

pub async fn run_market_loop(
    ws_client: &WebSocketClient,
    mut state: MarketState,
    active_asset_ids: Vec<String>,
    current_subscribed_ts: u64,
    mut price_rx: tokio::sync::watch::Receiver<f64>,
    action_tx: tokio::sync::watch::Sender<(TradeAction, u64)>,
    config: StrategyConfig,
    telemetry: TelemetryTx,
    mut death_rx: tokio::sync::mpsc::Receiver<()>,
    inventory_rx: &mut tokio::sync::mpsc::UnboundedReceiver<InventoryUpdate>,
) -> Result<(), Box<dyn Error>> {
    let market_stream = ws_client.subscribe_orderbook(active_asset_ids.clone())?;
    let mut market_stream = Box::pin(market_stream);

    info!("[init] fetching orderbook snapshot");
    match tokio::time::timeout(Duration::from_secs(3), market_stream.next()).await {
        Ok(Some(Ok(_))) => info!("[init] snapshot ready"),
        _ => error!("[init] snapshot timeout"),
    }

    let _ = ws_client.unsubscribe_orderbook(&active_asset_ids);
    drop(market_stream);

    let prices_stream = ws_client.subscribe_prices(active_asset_ids.clone())?;
    let mut prices_stream = Box::pin(prices_stream);

    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(10));
    let mut refresh_timer = Box::pin(tokio::time::sleep(Duration::from_secs(5)));
    let mut has_refreshed = false;

    while (chrono::Utc::now().timestamp() as u64 / 300) * 300 == current_subscribed_ts {
        tokio::select! {
            Some(update) = inventory_rx.recv() => {
                apply_inventory_update(&mut state, update, &telemetry);

                let trace_id = chrono::Utc::now().timestamp_micros() as u64;
                if evaluate_and_dispatch(&mut state, &config, &telemetry, &action_tx, trace_id) {
                    return Ok(());
                }
            }

            res = tokio::time::timeout(Duration::from_secs(15), prices_stream.next()) => {
                match res {
                    Ok(Some(Ok(price_event))) => {
                        let local_now_micros = chrono::Utc::now().timestamp_micros() as u64;
                        let trace_id = local_now_micros;

                        let server_ts_ms = price_event.timestamp;
                        let local_now_ms = local_now_micros / 1000;
                        let t1_latency_ms = local_now_ms.saturating_sub(server_ts_ms.try_into().unwrap_or(0));
                        telemetry.mark_t1(trace_id, t1_latency_ms);
                        telemetry.mark_best_bid_ask_anchor(trace_id, server_ts_ms as u64);
                        telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::LocalWsReceived);
                        telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::PayloadReady);

                        let mut state_changed = false;
                        for change in price_event.price_changes {
                            let asset_id = change.asset_id.to_string();
                            let best_bid = change.best_bid.and_then(|d| d.to_f64()).unwrap_or(0.0);
                            let best_ask = change.best_ask.and_then(|d| d.to_f64()).unwrap_or(0.0);

                            if best_bid > 0.0 || best_ask > 0.0 {
                                telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::ParseCompleted);
                                state.update_prices_by_asset_id(&asset_id, best_bid, best_ask);
                                state.update_entry_gate(&config, chrono::Utc::now());
                                telemetry.mark_best_bid_ask_stage(trace_id, BestBidAskStage::MarketStateUpdated);
                                state_changed = true;

                                telemetry.emit_market_event(build_market_dynamic_event(
                                    &state,
                                    &config,
                                    trace_id,
                                    "best_bid_ask",
                                    Some(server_ts_ms as u64),
                                    Some(best_bid),
                                    Some(best_ask),
                                    Some(asset_id),
                                ));
                            }
                        }

                        if state_changed
                            && evaluate_and_dispatch(&mut state, &config, &telemetry, &action_tx, trace_id)
                        {
                            return Ok(());
                        }
                    }
                    Ok(Some(Err(err))) => telemetry.log(format!("price stream error: {:?}", err)),
                    Ok(None) => break,
                    Err(_) => {
                        telemetry.log("price stream watchdog fired".to_string());
                        break;
                    }
                }
            }

            _ = price_rx.changed() => {
                let eth = *price_rx.borrow_and_update();
                state.update_eth_price(eth);
                let trace_id = chrono::Utc::now().timestamp_micros() as u64;
                telemetry.emit_market_event(build_market_dynamic_event(
                    &state,
                    &config,
                    trace_id,
                    "eth_price_update",
                    None,
                    None,
                    None,
                    None,
                ));
            }

            _ = &mut refresh_timer, if !has_refreshed => {
                has_refreshed = true;
                telemetry.log("[market] official strike refresh window opened".to_string());
            }

            _ = death_rx.recv() => {
                telemetry.log("[market] user ws disconnected".to_string());
            }

            _ = heartbeat_interval.tick() => {}
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration as ChronoDuration, TimeZone, Utc};

    fn telemetry() -> TelemetryTx {
        crate::telemetry::start_telemetry_thread()
    }

    fn start_time() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 4, 24, 0, 0, 0).unwrap()
    }

    #[test]
    fn double_sided_action_does_not_record_active_batch_before_submit_success() {
        let telemetry = telemetry();
        let (action_tx, _action_rx) = tokio::sync::watch::channel((TradeAction::DoNothing, 0));
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");

        let action = TradeAction::PlaceDoubleSidedOrder {
            up_token_id: "up".to_string(),
            up_price: 0.28,
            up_size: 5.0,
            down_token_id: "down".to_string(),
            down_price: 0.69,
            down_size: 5.0,
        };

        handle_action(action, &mut state, &telemetry, &action_tx, 42);

        assert!(state.active_batch.is_none());
        assert_eq!(state.pending_double_submission_trace_id, Some(42));
    }

    #[test]
    fn double_batch_submitted_update_records_active_batch() {
        let telemetry = telemetry();
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.mark_double_submission_pending(7);

        apply_inventory_update(
            &mut state,
            InventoryUpdate::DoubleBatchSubmitted {
                trace_id: 7,
                up_price: 0.28,
                up_size: 5.0,
                down_price: 0.69,
                down_size: 5.0,
            },
            &telemetry,
        );

        let batch = state.active_batch.as_ref().expect("double batch recorded");
        assert_eq!(batch.trace_id, 7);
        assert_eq!(batch.orders.len(), 2);
        assert!(!state.has_double_submission_pending());
    }

    #[test]
    fn batch_submission_failed_does_not_create_active_batch() {
        let telemetry = telemetry();
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.mark_double_submission_pending(8);

        apply_inventory_update(
            &mut state,
            InventoryUpdate::BatchSubmissionFailed {
                reason: "double_batch_prepare_up_build_failed".to_string(),
            },
            &telemetry,
        );

        assert!(state.active_batch.is_none());
        assert!(!state.has_double_submission_pending());
    }

    #[test]
    fn batch_submission_failed_clears_existing_batch() {
        let telemetry = telemetry();
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.record_double_sided_batch(
            1,
            start_time() + ChronoDuration::seconds(1),
            0.28,
            5.0,
            0.69,
            5.0,
        );

        apply_inventory_update(
            &mut state,
            InventoryUpdate::BatchSubmissionFailed {
                reason: "double_batch_post_failed".to_string(),
            },
            &telemetry,
        );

        assert!(state.active_batch.is_none());
    }

    #[test]
    fn reconcile_required_update_blocks_strategy_state() {
        let telemetry = telemetry();
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.record_single_sided_batch(
            11,
            start_time() + ChronoDuration::seconds(1),
            crate::engine::core::inventory::OutcomeSide::Up,
            "up",
            0.28,
            5.0,
        );

        apply_inventory_update(
            &mut state,
            InventoryUpdate::BatchReconcileRequired {
                reason: "cancel_confirmation_timeout".to_string(),
            },
            &telemetry,
        );

        assert!(state.execution_reconcile_required());
        assert_eq!(
            state.active_batch.as_ref().map(|batch| batch.status),
            Some(crate::engine::core::state::ActiveBatchStatus::ReconcileRequired)
        );
    }

    #[test]
    fn reconcile_resolved_update_clears_block_and_batch() {
        let telemetry = telemetry();
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.record_single_sided_batch(
            12,
            start_time() + ChronoDuration::seconds(1),
            crate::engine::core::inventory::OutcomeSide::Down,
            "down",
            0.72,
            5.0,
        );
        state.mark_execution_reconcile_required();

        apply_inventory_update(
            &mut state,
            InventoryUpdate::ReconcileResolved {
                reason: "reconcile_book_clean".to_string(),
                clear_active_batch: true,
            },
            &telemetry,
        );

        assert!(!state.execution_reconcile_required());
        assert!(state.active_batch.is_none());
    }
}
