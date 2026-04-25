use chrono::{DateTime, Utc};

use super::inventory::OutcomeSide;
use super::matching::{build_double_sided_quote, single_sided_adjustment_price};
use super::params::StrategyConfig;
use super::risk::{evaluate, is_inventory_balanced, should_cancel_for_timeout};
use super::state::{ActiveBatchStatus, MarketState};

#[derive(Debug, Clone, PartialEq)]
pub enum TradeAction {
    DoNothing,
    CancelAllOpenOrders {
        reason: String,
    },
    PlaceDoubleSidedOrder {
        up_token_id: String,
        up_price: f64,
        up_size: f64,
        down_token_id: String,
        down_price: f64,
        down_size: f64,
    },
    PlaceSingleSidedAdjustmentOrder {
        outcome_side: OutcomeSide,
        asset_id: String,
        price: f64,
        size: f64,
    },
    AbortMarket,
}

impl TradeAction {
    pub fn requires_clean_book(&self) -> bool {
        matches!(
            self,
            Self::PlaceDoubleSidedOrder { .. } | Self::PlaceSingleSidedAdjustmentOrder { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
struct AdjustmentPlan {
    outcome_side: OutcomeSide,
    asset_id: String,
    price: f64,
    size: f64,
    raw_target: f64,
}

pub struct Strategy;

impl Strategy {
    pub fn next_action(state: &mut MarketState, config: &StrategyConfig) -> TradeAction {
        let now = Utc::now();
        state.compute_pnl_live();

        if state.execution_reconcile_required() {
            return TradeAction::DoNothing;
        }

        if state.has_double_submission_pending() {
            return TradeAction::DoNothing;
        }

        match state.active_batch.as_ref() {
            Some(batch)
                if matches!(
                    batch.status,
                    ActiveBatchStatus::Cancelling | ActiveBatchStatus::ReconcileRequired
                ) =>
            {
                TradeAction::DoNothing
            }
            Some(batch) => {
                if should_cancel_for_timeout(batch.sent_at, now, config) {
                    TradeAction::CancelAllOpenOrders {
                        reason: "order_timeout".to_string(),
                    }
                } else {
                    TradeAction::DoNothing
                }
            }
            None => Self::planned_action(state, config, now),
        }
    }

    fn planned_action(
        state: &mut MarketState,
        config: &StrategyConfig,
        now: DateTime<Utc>,
    ) -> TradeAction {
        if state.up_token_id.is_empty() || state.down_token_id.is_empty() {
            return TradeAction::AbortMarket;
        }

        if !state.has_live_quotes() {
            return TradeAction::DoNothing;
        }

        state.update_entry_gate(config, now);

        if state.time_left_secs(config, now) <= 0.0 || state.elapsed_secs(now) >= config.t_cut {
            return TradeAction::DoNothing;
        }

        if !state.entry_gate_passed {
            return TradeAction::DoNothing;
        }

        if !has_inventory(state) {
            return Self::build_double_sided_action(state, config);
        }

        let risk = evaluate(state, config, now);

        if state.adjustment_active {
            if risk.inventory_balanced || risk.flip_overshoot_acceptable {
                state.clear_adjustment();
                return Self::action_after_balanced_inventory(state, config, &risk);
            }

            return Self::build_imbalance_adjustment(state, config);
        }

        if risk.should_enter_adjustment {
            if let Some(sign) = state.current_imbalance_sign() {
                state.enter_adjustment(sign);
                return Self::build_imbalance_adjustment(state, config);
            }
        }

        let balanced = is_inventory_balanced(state.q_diff, config);
        if balanced {
            return Self::action_after_balanced_inventory(state, config, &risk);
        }

        if risk.needs_loss_reduction {
            return Self::build_double_sided_action(state, config);
        }

        if risk.abs_q_diff < risk.q_allow {
            return Self::build_double_sided_action(state, config);
        }

        TradeAction::DoNothing
    }

    fn build_double_sided_action(state: &MarketState, config: &StrategyConfig) -> TradeAction {
        let Some(quote) = build_double_sided_quote(
            state.up_bid,
            state.up_ask,
            state.down_bid,
            state.down_ask,
            config,
        ) else {
            return TradeAction::DoNothing;
        };

        let Some(q_min_up) = config.q_min_at_price(quote.up_price) else {
            return TradeAction::DoNothing;
        };
        let Some(q_min_down) = config.q_min_at_price(quote.down_price) else {
            return TradeAction::DoNothing;
        };
        let Some(q_order) = config.legalize_share_to_step(q_min_up.max(q_min_down)) else {
            return TradeAction::DoNothing;
        };

        TradeAction::PlaceDoubleSidedOrder {
            up_token_id: state.up_token_id.clone(),
            up_price: quote.up_price,
            up_size: q_order,
            down_token_id: state.down_token_id.clone(),
            down_price: quote.down_price,
            down_size: q_order,
        }
    }

    fn build_imbalance_adjustment(state: &MarketState, config: &StrategyConfig) -> TradeAction {
        let outcome_side = if state.q_diff > 0.0 {
            OutcomeSide::Down
        } else if state.q_diff < 0.0 {
            OutcomeSide::Up
        } else {
            return TradeAction::DoNothing;
        };

        Self::build_adjustment_action(state, config, outcome_side)
    }

    fn action_after_balanced_inventory(
        state: &MarketState,
        config: &StrategyConfig,
        risk: &super::risk::RiskSnapshot,
    ) -> TradeAction {
        if risk.profit_target_reached {
            return TradeAction::DoNothing;
        }

        if risk.needs_loss_reduction {
            return Self::build_double_sided_action(state, config);
        }

        TradeAction::DoNothing
    }

    fn build_adjustment_action(
        state: &MarketState,
        config: &StrategyConfig,
        outcome_side: OutcomeSide,
    ) -> TradeAction {
        match Self::adjustment_plan(state, config, outcome_side) {
            Some(plan) => TradeAction::PlaceSingleSidedAdjustmentOrder {
                outcome_side: plan.outcome_side,
                asset_id: plan.asset_id,
                price: plan.price,
                size: plan.size,
            },
            None => TradeAction::DoNothing,
        }
    }

    fn adjustment_plan(
        state: &MarketState,
        config: &StrategyConfig,
        outcome_side: OutcomeSide,
    ) -> Option<AdjustmentPlan> {
        let price =
            single_sided_adjustment_price(outcome_side, state.up_bid, state.down_bid, config)?;
        let q_min = config.q_min_at_price(price)?;
        if is_inventory_balanced(state.q_diff, config) {
            return None;
        }

        let raw_target = state.q_diff.abs();

        if raw_target <= 0.0 {
            return None;
        }

        let size = config.legalize_share_to_step(raw_target.max(q_min))?;
        let asset_id = match outcome_side {
            OutcomeSide::Up => state.up_token_id.clone(),
            OutcomeSide::Down => state.down_token_id.clone(),
        };

        Some(AdjustmentPlan {
            outcome_side,
            asset_id,
            price,
            size,
            raw_target,
        })
    }
}

fn has_inventory(state: &MarketState) -> bool {
    state.q_up > 0.0 || state.q_down > 0.0
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;
    use crate::engine::core::inventory::{FillDelta, OrderSide};
    use crate::engine::core::state::{
        ActiveBatchKind, ActiveOrder, ActiveOrderBatch, ImbalanceSign,
    };

    fn start_time() -> DateTime<Utc> {
        Utc::now() + Duration::seconds(60)
    }

    fn quoted_state() -> MarketState {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.update_prices_by_asset_id("up", 0.25, 0.27);
        state.update_prices_by_asset_id("down", 0.76, 0.78);
        state
    }

    fn buy_fill(asset_id: &str, outcome_side: OutcomeSide, price: f64, size: f64) -> FillDelta {
        FillDelta {
            fill_id: format!("fill-{asset_id}-{price}-{size}"),
            order_id: "order-1".to_string(),
            asset_id: asset_id.to_string(),
            outcome_side,
            order_side: OrderSide::Buy,
            price,
            size,
            fill_timestamp: Utc::now(),
        }
    }

    #[test]
    fn entry_gate_passed_market_places_double_sided_order() {
        let mut state = quoted_state();
        let config = StrategyConfig::default();

        let action =
            Strategy::planned_action(&mut state, &config, start_time() + Duration::seconds(30));

        assert!(matches!(action, TradeAction::PlaceDoubleSidedOrder { .. }));
        assert!(state.entry_gate_passed);
    }

    #[test]
    fn market_without_entry_gate_signal_does_nothing() {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.update_prices_by_asset_id("up", 0.51, 0.53);
        state.update_prices_by_asset_id("down", 0.46, 0.48);
        let config = StrategyConfig::default();

        let action =
            Strategy::planned_action(&mut state, &config, start_time() + Duration::seconds(30));

        assert!(matches!(action, TradeAction::DoNothing));
        assert!(!state.entry_gate_passed);
    }

    #[test]
    fn positive_q_only_buys_down_in_adjustment_stage() {
        let mut state = quoted_state();
        let mut config = StrategyConfig::default();
        config.q_allow_abs = 5.0;
        state.entry_gate_passed = true;
        state.q_up = 5.0;
        state.q_down = 0.0;
        state.q_diff = 5.0;
        state.cost_total = 2.4;
        state.value_live = 2.3;
        state.pnl_live = -0.1;

        let action =
            Strategy::planned_action(&mut state, &config, start_time() + Duration::seconds(30));

        assert!(matches!(
            action,
            TradeAction::PlaceSingleSidedAdjustmentOrder {
                outcome_side: OutcomeSide::Down,
                ..
            }
        ));
        assert!(state.adjustment_active);
    }

    #[test]
    fn active_adjustment_continues_until_balance_band_even_below_allowance() {
        let mut state = quoted_state();
        let mut config = StrategyConfig::default();
        config.q_allow_abs = 5.0;
        config.q_balance_band = 0.5;
        state.entry_gate_passed = true;
        state.q_up = 5.0;
        state.q_down = 2.0;
        state.q_diff = 3.0;
        state.adjustment_active = true;
        state.adjustment_started_sign = Some(ImbalanceSign::UpHeavy);

        let action =
            Strategy::planned_action(&mut state, &config, start_time() + Duration::seconds(30));

        assert!(matches!(
            action,
            TradeAction::PlaceSingleSidedAdjustmentOrder {
                outcome_side: OutcomeSide::Down,
                ..
            }
        ));
    }

    #[test]
    fn pending_double_submission_blocks_new_actions() {
        let mut state = quoted_state();
        let config = StrategyConfig::default();
        state.mark_double_submission_pending(99);

        let action = Strategy::next_action(&mut state, &config);

        assert!(matches!(action, TradeAction::DoNothing));
    }

    #[test]
    fn small_reverse_overshoot_stops_adjustment() {
        let mut state = quoted_state();
        let mut config = StrategyConfig::default();
        config.q_flip_stop_band = 3.0;
        state.entry_gate_passed = true;
        state.q_up = 5.0;
        state.q_down = 7.0;
        state.q_diff = -2.0;
        state.adjustment_active = true;
        state.adjustment_started_sign = Some(ImbalanceSign::UpHeavy);

        let action =
            Strategy::planned_action(&mut state, &config, start_time() + Duration::seconds(30));

        assert!(matches!(action, TradeAction::PlaceDoubleSidedOrder { .. }));
        assert!(!state.adjustment_active);
    }

    #[test]
    fn balanced_inventory_with_target_profit_stops() {
        let mut state = quoted_state();
        let mut config = StrategyConfig::default();
        config.g_target = 0.5;
        state.entry_gate_passed = true;
        state
            .apply_fill_delta(buy_fill("up", OutcomeSide::Up, 0.45, 5.0))
            .unwrap();
        state
            .apply_fill_delta(buy_fill("down", OutcomeSide::Down, 0.45, 5.0))
            .unwrap();

        let action =
            Strategy::planned_action(&mut state, &config, start_time() + Duration::seconds(30));

        assert!(matches!(action, TradeAction::DoNothing));
    }

    #[test]
    fn balanced_inventory_below_g_target_uses_double_sided_reduction() {
        let mut state = quoted_state();
        let mut config = StrategyConfig::default();
        config.g_target = 0.5;
        state.entry_gate_passed = true;
        state
            .apply_fill_delta(buy_fill("up", OutcomeSide::Up, 0.48, 5.0))
            .unwrap();
        state
            .apply_fill_delta(buy_fill("down", OutcomeSide::Down, 0.48, 5.0))
            .unwrap();

        let action =
            Strategy::planned_action(&mut state, &config, start_time() + Duration::seconds(30));

        assert!(matches!(action, TradeAction::PlaceDoubleSidedOrder { .. }));
    }

    #[test]
    fn adjustment_plan_targets_full_inventory_imbalance_without_balance_band_deduction() {
        let mut state = quoted_state();
        let mut config = StrategyConfig::default();
        config.q_balance_band = 1.0;
        state.q_up = 10.0;
        state.q_down = 0.0;
        state.q_diff = 10.0;

        let plan = Strategy::adjustment_plan(&state, &config, OutcomeSide::Down)
            .expect("adjustment plan should exist");

        assert!((plan.raw_target - 10.0).abs() < 1e-9);
        assert!(plan.size >= 10.0);
    }

    #[test]
    fn open_batch_waits_until_timeout_instead_of_repricing_immediately() {
        let mut state = quoted_state();
        let config = StrategyConfig::default();
        state.entry_gate_passed = true;
        state
            .apply_fill_delta(buy_fill("up", OutcomeSide::Up, 0.48, 5.0))
            .unwrap();
        state.active_batch = Some(ActiveOrderBatch {
            kind: ActiveBatchKind::DoubleSided,
            status: ActiveBatchStatus::Open,
            trace_id: 1,
            sent_at: Utc::now(),
            orders: vec![ActiveOrder {
                asset_id: "up".to_string(),
                outcome_side: OutcomeSide::Up,
                price: 0.51,
                original_size: 5.0,
                remaining_size: 2.0,
            }],
        });

        let action = Strategy::next_action(&mut state, &config);

        assert!(matches!(action, TradeAction::DoNothing));
    }

    #[test]
    fn timed_out_batch_is_cancelled_even_without_new_quotes() {
        let mut state = quoted_state();
        let config = StrategyConfig::default();
        state.active_batch = Some(ActiveOrderBatch {
            kind: ActiveBatchKind::DoubleSided,
            status: ActiveBatchStatus::Open,
            trace_id: 1,
            sent_at: Utc::now() - Duration::seconds(11),
            orders: vec![ActiveOrder {
                asset_id: "up".to_string(),
                outcome_side: OutcomeSide::Up,
                price: 0.51,
                original_size: 5.0,
                remaining_size: 5.0,
            }],
        });

        let action = Strategy::next_action(&mut state, &config);

        assert!(matches!(action, TradeAction::CancelAllOpenOrders { .. }));
    }

    #[test]
    fn reconcile_required_batch_blocks_new_actions() {
        let mut state = quoted_state();
        let config = StrategyConfig::default();
        state.active_batch = Some(ActiveOrderBatch {
            kind: ActiveBatchKind::DoubleSided,
            status: ActiveBatchStatus::ReconcileRequired,
            trace_id: 1,
            sent_at: Utc::now(),
            orders: vec![ActiveOrder {
                asset_id: "up".to_string(),
                outcome_side: OutcomeSide::Up,
                price: 0.51,
                original_size: 5.0,
                remaining_size: 2.0,
            }],
        });

        let action = Strategy::next_action(&mut state, &config);

        assert!(matches!(action, TradeAction::DoNothing));
    }

    #[test]
    fn execution_reconcile_flag_blocks_new_actions() {
        let mut state = quoted_state();
        let config = StrategyConfig::default();
        state.mark_execution_reconcile_required();

        let action = Strategy::next_action(&mut state, &config);

        assert!(matches!(action, TradeAction::DoNothing));
    }
}
