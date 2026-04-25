use chrono::{DateTime, Utc};

use super::inventory::{FillDelta, InventoryLedger, OutcomeSide};
use super::params::StrategyConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeResolver {
    pub up_asset_id: String,
    pub down_asset_id: String,
    pub up_raw_outcome: String,
    pub down_raw_outcome: String,
}

impl OutcomeResolver {
    pub fn new(
        up_asset_id: impl Into<String>,
        down_asset_id: impl Into<String>,
        up_raw_outcome: impl Into<String>,
        down_raw_outcome: impl Into<String>,
    ) -> Self {
        Self {
            up_asset_id: up_asset_id.into(),
            down_asset_id: down_asset_id.into(),
            up_raw_outcome: up_raw_outcome.into(),
            down_raw_outcome: down_raw_outcome.into(),
        }
    }

    pub fn for_up_down_market(up_asset_id: &str, down_asset_id: &str) -> Self {
        Self::new(up_asset_id, down_asset_id, "YES", "NO")
    }

    pub fn side_for_asset_id(&self, asset_id: &str) -> Option<OutcomeSide> {
        if asset_id == self.up_asset_id {
            Some(OutcomeSide::Up)
        } else if asset_id == self.down_asset_id {
            Some(OutcomeSide::Down)
        } else {
            None
        }
    }

    pub fn side_for_raw_outcome(&self, raw_outcome: &str) -> Option<OutcomeSide> {
        let normalized = normalize_outcome_label(raw_outcome);
        if normalized == normalize_outcome_label(&self.up_raw_outcome) {
            Some(OutcomeSide::Up)
        } else if normalized == normalize_outcome_label(&self.down_raw_outcome) {
            Some(OutcomeSide::Down)
        } else {
            None
        }
    }

    pub fn resolve_outcome_side(
        &self,
        asset_id: &str,
        raw_outcome: Option<&str>,
    ) -> Result<OutcomeSide, String> {
        let asset_side = self
            .side_for_asset_id(asset_id)
            .ok_or_else(|| format!("unknown asset_id for active market: {asset_id}"))?;

        if let Some(outcome) = raw_outcome {
            let outcome_side = self
                .side_for_raw_outcome(outcome)
                .ok_or_else(|| format!("unknown raw outcome for active market: {outcome}"))?;
            if outcome_side != asset_side {
                return Err(format!(
                    "asset_id {} resolved to {:?}, but raw outcome {} resolved to {:?}",
                    asset_id, asset_side, outcome, outcome_side
                ));
            }
        }

        Ok(asset_side)
    }
}

fn normalize_outcome_label(label: &str) -> String {
    label.trim().to_ascii_uppercase()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveBatchKind {
    DoubleSided,
    SingleSidedAdjustment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveBatchStatus {
    Open,
    Cancelling,
    ReconcileRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImbalanceSign {
    UpHeavy,
    DownHeavy,
}

impl ImbalanceSign {
    pub fn from_q_diff(q_diff: f64) -> Option<Self> {
        if q_diff > 0.0 {
            Some(Self::UpHeavy)
        } else if q_diff < 0.0 {
            Some(Self::DownHeavy)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActiveOrder {
    pub asset_id: String,
    pub outcome_side: OutcomeSide,
    pub price: f64,
    pub original_size: f64,
    pub remaining_size: f64,
}

#[derive(Debug, Clone)]
pub struct ActiveOrderBatch {
    pub kind: ActiveBatchKind,
    pub status: ActiveBatchStatus,
    pub trace_id: u64,
    pub sent_at: DateTime<Utc>,
    pub orders: Vec<ActiveOrder>,
}

impl ActiveOrderBatch {
    pub fn remaining_total_size(&self) -> f64 {
        self.orders
            .iter()
            .map(|order| order.remaining_size.max(0.0))
            .sum()
    }

    pub fn has_remaining_open_size(&self) -> bool {
        self.remaining_total_size() > 0.0
    }
}

pub struct MarketState {
    pub strike_price: f64,
    pub eth_price: f64,
    pub up_token_id: String,
    pub down_token_id: String,
    pub outcome_resolver: OutcomeResolver,
    pub up_bid: f64,
    pub up_ask: f64,
    pub down_bid: f64,
    pub down_ask: f64,
    pub up_tick_size: f64,
    pub down_tick_size: f64,
    pub start_time: DateTime<Utc>,
    pub q_up: f64,
    pub q_down: f64,
    pub q_diff: f64,
    pub cost_up: f64,
    pub cost_down: f64,
    pub cost_total: f64,
    pub avg_up_cost: Option<f64>,
    pub avg_down_cost: Option<f64>,
    pub avg_cost_sum: Option<f64>,
    pub profit_if_up: f64,
    pub profit_if_down: f64,
    pub profit_min: f64,
    pub value_live: f64,
    pub pnl_live: f64,
    pub entry_gate_passed: bool,
    pub entry_gate_window_seen: bool,
    pub entry_gate_failed_final: bool,
    pub adjustment_active: bool,
    pub adjustment_started_sign: Option<ImbalanceSign>,
    pub last_adjustment_side: Option<OutcomeSide>,
    pub pending_double_submission_trace_id: Option<u64>,
    pub execution_reconcile_required: bool,
    pub active_batch: Option<ActiveOrderBatch>,
    inventory: InventoryLedger,
}

impl MarketState {
    pub fn new(strike_price: f64, start_time: DateTime<Utc>, up_id: &str, down_id: &str) -> Self {
        Self::new_with_outcome_resolver(
            strike_price,
            start_time,
            OutcomeResolver::for_up_down_market(up_id, down_id),
        )
    }

    pub fn new_with_outcome_resolver(
        strike_price: f64,
        start_time: DateTime<Utc>,
        outcome_resolver: OutcomeResolver,
    ) -> Self {
        Self {
            strike_price,
            eth_price: 0.0,
            up_token_id: outcome_resolver.up_asset_id.clone(),
            down_token_id: outcome_resolver.down_asset_id.clone(),
            outcome_resolver,
            up_bid: 0.0,
            up_ask: 0.0,
            down_bid: 0.0,
            down_ask: 0.0,
            up_tick_size: 0.01,
            down_tick_size: 0.01,
            start_time,
            q_up: 0.0,
            q_down: 0.0,
            q_diff: 0.0,
            cost_up: 0.0,
            cost_down: 0.0,
            cost_total: 0.0,
            avg_up_cost: None,
            avg_down_cost: None,
            avg_cost_sum: None,
            profit_if_up: 0.0,
            profit_if_down: 0.0,
            profit_min: 0.0,
            value_live: 0.0,
            pnl_live: 0.0,
            entry_gate_passed: false,
            entry_gate_window_seen: false,
            entry_gate_failed_final: false,
            adjustment_active: false,
            adjustment_started_sign: None,
            last_adjustment_side: None,
            pending_double_submission_trace_id: None,
            execution_reconcile_required: false,
            active_batch: None,
            inventory: InventoryLedger::default(),
        }
    }

    pub fn resolve_outcome_side(
        &self,
        asset_id: &str,
        raw_outcome: Option<&str>,
    ) -> Result<OutcomeSide, String> {
        self.outcome_resolver
            .resolve_outcome_side(asset_id, raw_outcome)
    }

    pub fn update_prices_by_asset_id(&mut self, asset_id: &str, bid: f64, ask: f64) {
        if asset_id == self.up_token_id {
            if bid > 0.0 {
                self.up_bid = bid;
            }
            if ask > 0.0 {
                self.up_ask = ask;
            }
        } else if asset_id == self.down_token_id {
            if bid > 0.0 {
                self.down_bid = bid;
            }
            if ask > 0.0 {
                self.down_ask = ask;
            }
        }

        self.refresh_live_metrics();
    }

    pub fn update_eth_price(&mut self, price: f64) {
        self.eth_price = price;
    }

    pub fn apply_fill_delta(&mut self, fill: FillDelta) -> Result<bool, String> {
        let applied = self.inventory.apply_fill_delta(&fill)?;
        if !applied {
            return Ok(false);
        }

        if let Some(batch) = self.active_batch.as_mut() {
            apply_fill_to_batch(batch, &fill);
            if !batch.has_remaining_open_size() {
                self.active_batch = None;
            }
        }

        self.refresh_live_metrics();
        Ok(true)
    }

    pub fn compute_q_diff(&mut self) -> f64 {
        self.refresh_live_metrics();
        self.q_diff
    }

    pub fn compute_value_live(&mut self) -> f64 {
        self.refresh_live_metrics();
        self.value_live
    }

    pub fn compute_pnl_live(&mut self) -> f64 {
        self.refresh_live_metrics();
        self.pnl_live
    }

    pub fn elapsed_secs(&self, now: DateTime<Utc>) -> f64 {
        now.signed_duration_since(self.start_time)
            .num_milliseconds()
            .max(0) as f64
            / 1000.0
    }

    pub fn time_left_secs(&self, config: &StrategyConfig, now: DateTime<Utc>) -> f64 {
        (config.t_event - self.elapsed_secs(now)).max(0.0)
    }

    pub fn seconds_to_start(&self, now: DateTime<Utc>) -> f64 {
        self.start_time
            .signed_duration_since(now)
            .num_milliseconds() as f64
            / 1000.0
    }

    pub fn active_order_age_secs(&self, now: DateTime<Utc>) -> Option<f64> {
        self.active_batch.as_ref().map(|batch| {
            now.signed_duration_since(batch.sent_at)
                .num_milliseconds()
                .max(0) as f64
                / 1000.0
        })
    }

    pub fn update_entry_gate(&mut self, config: &StrategyConfig, now: DateTime<Utc>) {
        if self.entry_gate_passed {
            return;
        }

        let elapsed_secs = now
            .signed_duration_since(self.start_time)
            .num_milliseconds() as f64
            / 1000.0;
        if elapsed_secs >= 0.0 && elapsed_secs <= config.t_pre_entry_window.max(0.0) {
            self.entry_gate_window_seen = true;
            if self.entry_gate_price_ok(config) {
                self.entry_gate_passed = true;
                self.entry_gate_failed_final = false;
            }
        } else if elapsed_secs > config.t_pre_entry_window.max(0.0) {
            self.entry_gate_failed_final = true;
        }
    }

    pub fn entry_gate_price_ok(&self, config: &StrategyConfig) -> bool {
        config.up_gate_price_ok(self.up_bid) || config.down_gate_price_ok(self.down_bid)
    }

    pub fn current_imbalance_sign(&self) -> Option<ImbalanceSign> {
        ImbalanceSign::from_q_diff(self.q_diff)
    }

    pub fn enter_adjustment(&mut self, sign: ImbalanceSign) {
        if !self.adjustment_active {
            self.adjustment_active = true;
            self.adjustment_started_sign = Some(sign);
        }
    }

    pub fn clear_adjustment(&mut self) {
        self.adjustment_active = false;
        self.adjustment_started_sign = None;
    }

    pub fn mark_double_submission_pending(&mut self, trace_id: u64) {
        self.pending_double_submission_trace_id = Some(trace_id);
    }

    pub fn clear_double_submission_pending(&mut self) {
        self.pending_double_submission_trace_id = None;
    }

    pub fn has_double_submission_pending(&self) -> bool {
        self.pending_double_submission_trace_id.is_some()
    }

    pub fn mark_execution_reconcile_required(&mut self) {
        self.execution_reconcile_required = true;
    }

    pub fn clear_execution_reconcile_required(&mut self) {
        self.execution_reconcile_required = false;
    }

    pub fn execution_reconcile_required(&self) -> bool {
        self.execution_reconcile_required
    }

    pub fn record_double_sided_batch(
        &mut self,
        trace_id: u64,
        sent_at: DateTime<Utc>,
        up_price: f64,
        up_size: f64,
        down_price: f64,
        down_size: f64,
    ) {
        self.active_batch = Some(ActiveOrderBatch {
            kind: ActiveBatchKind::DoubleSided,
            status: ActiveBatchStatus::Open,
            trace_id,
            sent_at,
            orders: vec![
                ActiveOrder {
                    asset_id: self.up_token_id.clone(),
                    outcome_side: OutcomeSide::Up,
                    price: up_price,
                    original_size: up_size,
                    remaining_size: up_size,
                },
                ActiveOrder {
                    asset_id: self.down_token_id.clone(),
                    outcome_side: OutcomeSide::Down,
                    price: down_price,
                    original_size: down_size,
                    remaining_size: down_size,
                },
            ],
        });
    }

    pub fn record_single_sided_batch(
        &mut self,
        trace_id: u64,
        sent_at: DateTime<Utc>,
        outcome_side: OutcomeSide,
        asset_id: &str,
        price: f64,
        size: f64,
    ) {
        self.last_adjustment_side = Some(outcome_side);
        self.active_batch = Some(ActiveOrderBatch {
            kind: ActiveBatchKind::SingleSidedAdjustment,
            status: ActiveBatchStatus::Open,
            trace_id,
            sent_at,
            orders: vec![ActiveOrder {
                asset_id: asset_id.to_string(),
                outcome_side,
                price,
                original_size: size,
                remaining_size: size,
            }],
        });
    }

    pub fn mark_active_batch_cancelling(&mut self) {
        if let Some(batch) = self.active_batch.as_mut() {
            batch.status = ActiveBatchStatus::Cancelling;
        }
    }

    pub fn mark_active_batch_open_again_after_cancel_failure(&mut self) {
        if let Some(batch) = self.active_batch.as_mut() {
            batch.status = ActiveBatchStatus::Open;
        }
    }

    pub fn mark_active_batch_reconcile_required(&mut self) {
        if let Some(batch) = self.active_batch.as_mut() {
            batch.status = ActiveBatchStatus::ReconcileRequired;
        }
    }

    pub fn clear_active_batch(&mut self) {
        self.active_batch = None;
    }

    pub fn has_live_quotes(&self) -> bool {
        self.up_bid > 0.0 && self.up_ask > 0.0 && self.down_bid > 0.0 && self.down_ask > 0.0
    }

    fn refresh_live_metrics(&mut self) {
        self.q_up = self.inventory.compute_q_up();
        self.q_down = self.inventory.compute_q_down();
        self.q_diff = self.inventory.compute_q_diff();
        self.cost_up = self.inventory.compute_cost_up();
        self.cost_down = self.inventory.compute_cost_down();
        self.cost_total = self.inventory.compute_cost_total();
        self.avg_up_cost = self.inventory.compute_avg_up_cost();
        self.avg_down_cost = self.inventory.compute_avg_down_cost();
        self.avg_cost_sum = self.inventory.compute_avg_cost_sum();
        self.profit_if_up = self.inventory.compute_profit_if_up();
        self.profit_if_down = self.inventory.compute_profit_if_down();
        self.profit_min = self.inventory.compute_profit_min();
        self.value_live = self
            .inventory
            .compute_value_live(self.up_bid, self.down_bid);
        self.pnl_live = self.inventory.compute_pnl_live(self.up_bid, self.down_bid);
    }
}

fn apply_fill_to_batch(batch: &mut ActiveOrderBatch, fill: &FillDelta) {
    for order in &mut batch.orders {
        if order.asset_id != fill.asset_id {
            continue;
        }

        if (order.price - fill.price).abs() > 0.02 {
            continue;
        }

        order.remaining_size = (order.remaining_size - fill.size).max(0.0);
        break;
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;
    use crate::engine::core::inventory::{OrderSide, OutcomeSide};

    fn start_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 4, 19, 0, 0, 0).unwrap()
    }

    fn sample_fill(asset_id: &str, side: OutcomeSide, price: f64, size: f64) -> FillDelta {
        FillDelta {
            fill_id: format!("fill-{asset_id}-{price}-{size}"),
            order_id: "order-1".to_string(),
            asset_id: asset_id.to_string(),
            outcome_side: side,
            order_side: OrderSide::Buy,
            price,
            size,
            fill_timestamp: start_time() + Duration::seconds(2),
        }
    }

    #[test]
    fn market_state_recomputes_q_value_and_pnl_after_fill() {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.update_prices_by_asset_id("up", 0.49, 0.50);
        state.update_prices_by_asset_id("down", 0.46, 0.47);

        state
            .apply_fill_delta(sample_fill("up", OutcomeSide::Up, 0.48, 5.0))
            .unwrap();

        assert_eq!(state.q_up, 5.0);
        assert_eq!(state.q_down, 0.0);
        assert_eq!(state.q_diff, 5.0);
        assert!((state.cost_up - 2.4).abs() < 1e-9);
        assert!((state.cost_down - 0.0).abs() < 1e-9);
        assert!((state.cost_total - 2.4).abs() < 1e-9);
        assert!((state.avg_up_cost.unwrap() - 0.48).abs() < 1e-9);
        assert!(state.avg_down_cost.is_none());
        assert!((state.profit_if_up - 2.6).abs() < 1e-9);
        assert!((state.profit_if_down + 2.4).abs() < 1e-9);
        assert!((state.profit_min + 2.4).abs() < 1e-9);
        assert!((state.value_live - 2.45).abs() < 1e-9);
        assert!((state.pnl_live - 0.05).abs() < 1e-9);
    }

    #[test]
    fn active_batch_is_cleared_after_remaining_size_is_fully_filled() {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.record_single_sided_batch(
            42,
            start_time() + Duration::seconds(1),
            OutcomeSide::Up,
            "up",
            0.48,
            5.0,
        );

        state
            .apply_fill_delta(sample_fill("up", OutcomeSide::Up, 0.48, 5.0))
            .unwrap();

        assert!(state.active_batch.is_none());
    }

    #[test]
    fn double_sided_batch_records_two_open_orders() {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.record_double_sided_batch(
            42,
            start_time() + Duration::seconds(1),
            0.28,
            5.0,
            0.69,
            5.0,
        );

        let batch = state.active_batch.as_ref().expect("double batch recorded");
        assert_eq!(batch.kind, ActiveBatchKind::DoubleSided);
        assert_eq!(batch.status, ActiveBatchStatus::Open);
        assert_eq!(batch.orders.len(), 2);
        assert_eq!(batch.orders[0].asset_id, "up");
        assert_eq!(batch.orders[1].asset_id, "down");
    }

    #[test]
    fn batch_status_can_return_to_open_after_cancel_failure() {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.record_single_sided_batch(
            42,
            start_time() + Duration::seconds(1),
            OutcomeSide::Up,
            "up",
            0.48,
            5.0,
        );
        state.mark_active_batch_cancelling();
        state.mark_active_batch_open_again_after_cancel_failure();

        assert_eq!(
            state.active_batch.as_ref().map(|batch| batch.status),
            Some(ActiveBatchStatus::Open)
        );
    }

    #[test]
    fn batch_status_can_enter_reconcile_required() {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.record_single_sided_batch(
            42,
            start_time() + Duration::seconds(1),
            OutcomeSide::Up,
            "up",
            0.48,
            5.0,
        );
        state.mark_active_batch_reconcile_required();

        assert_eq!(
            state.active_batch.as_ref().map(|batch| batch.status),
            Some(ActiveBatchStatus::ReconcileRequired)
        );
    }

    #[test]
    fn entry_gate_passes_when_reference_price_enters_window() {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        let config = StrategyConfig::default();
        state.update_prices_by_asset_id("up", 0.25, 0.27);
        state.update_prices_by_asset_id("down", 0.76, 0.78);

        state.update_entry_gate(&config, start_time() + Duration::seconds(60));

        assert!(state.entry_gate_window_seen);
        assert!(state.entry_gate_passed);
        assert!(!state.entry_gate_failed_final);
    }

    #[test]
    fn entry_gate_fails_after_entry_window_when_no_price_entered_window() {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        let config = StrategyConfig::default();
        state.update_prices_by_asset_id("up", 0.50, 0.52);
        state.update_prices_by_asset_id("down", 0.47, 0.49);

        state.update_entry_gate(&config, start_time() + Duration::seconds(121));

        assert!(!state.entry_gate_passed);
        assert!(state.entry_gate_failed_final);
    }

    #[test]
    fn entry_gate_does_not_pass_before_event_start() {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        let config = StrategyConfig::default();
        state.update_prices_by_asset_id("up", 0.25, 0.27);
        state.update_prices_by_asset_id("down", 0.76, 0.78);

        state.update_entry_gate(&config, start_time() - Duration::seconds(60));

        assert!(!state.entry_gate_window_seen);
        assert!(!state.entry_gate_passed);
        assert!(!state.entry_gate_failed_final);
    }

    #[test]
    fn single_sided_batch_records_last_adjustment_side() {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");
        state.record_single_sided_batch(
            42,
            start_time() + Duration::seconds(1),
            OutcomeSide::Down,
            "down",
            0.48,
            5.0,
        );

        assert_eq!(state.last_adjustment_side, Some(OutcomeSide::Down));
    }

    #[test]
    fn double_submission_pending_state_tracks_lifecycle() {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");

        assert!(!state.has_double_submission_pending());
        assert_eq!(state.pending_double_submission_trace_id, None);

        state.mark_double_submission_pending(77);

        assert!(state.has_double_submission_pending());
        assert_eq!(state.pending_double_submission_trace_id, Some(77));

        state.clear_double_submission_pending();

        assert!(!state.has_double_submission_pending());
        assert_eq!(state.pending_double_submission_trace_id, None);
    }

    #[test]
    fn execution_reconcile_flag_tracks_lifecycle() {
        let mut state = MarketState::new(1000.0, start_time(), "up", "down");

        assert!(!state.execution_reconcile_required());

        state.mark_execution_reconcile_required();
        assert!(state.execution_reconcile_required());

        state.clear_execution_reconcile_required();
        assert!(!state.execution_reconcile_required());
    }

    #[test]
    fn elapsed_and_time_left_follow_market_start_time() {
        let state = MarketState::new(1000.0, start_time(), "up", "down");
        let config = StrategyConfig::default();
        let now = start_time() + Duration::seconds(30);

        assert!((state.elapsed_secs(now) - 30.0).abs() < 1e-9);
        assert!((state.time_left_secs(&config, now) - 270.0).abs() < 1e-9);
    }

    #[test]
    fn resolver_uses_asset_id_as_primary_and_outcome_as_validation() {
        let resolver = OutcomeResolver::new("asset-up", "asset-down", "YES", "NO");

        assert_eq!(
            resolver
                .resolve_outcome_side("asset-up", Some("YES"))
                .unwrap(),
            OutcomeSide::Up
        );
        assert_eq!(
            resolver
                .resolve_outcome_side("asset-down", Some("NO"))
                .unwrap(),
            OutcomeSide::Down
        );
        assert!(
            resolver
                .resolve_outcome_side("asset-up", Some("NO"))
                .is_err()
        );
    }
}
