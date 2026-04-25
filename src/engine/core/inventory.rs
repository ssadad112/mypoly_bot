use chrono::{DateTime, Utc};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeSide {
    Up,
    Down,
}

impl OutcomeSide {
    pub fn opposite(self) -> Self {
        match self {
            Self::Up => Self::Down,
            Self::Down => Self::Up,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FillDelta {
    pub fill_id: String,
    pub order_id: String,
    pub asset_id: String,
    pub outcome_side: OutcomeSide,
    pub order_side: OrderSide,
    pub price: f64,
    pub size: f64,
    pub fill_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct InventoryLedger {
    q_up: f64,
    q_down: f64,
    cost_up: f64,
    cost_down: f64,
    cost_total: f64,
    processed_fill_ids: HashSet<String>,
}

impl InventoryLedger {
    pub fn apply_fill_delta(&mut self, fill: &FillDelta) -> Result<bool, String> {
        if fill.size <= 0.0 {
            return Err("fill size must be positive".to_string());
        }

        if fill.price <= 0.0 {
            return Err("fill price must be positive".to_string());
        }

        if self.processed_fill_ids.contains(&fill.fill_id) {
            return Ok(false);
        }

        match (fill.outcome_side, fill.order_side) {
            (OutcomeSide::Up, OrderSide::Buy) => {
                self.q_up += fill.size;
                self.cost_up += fill.size * fill.price;
            }
            (OutcomeSide::Down, OrderSide::Buy) => {
                self.q_down += fill.size;
                self.cost_down += fill.size * fill.price;
            }
            (_, OrderSide::Sell) => {
                return Err("sell-side fill deltas are outside the current strategy model".into());
            }
        }

        self.cost_total = self.cost_up + self.cost_down;
        self.processed_fill_ids.insert(fill.fill_id.clone());
        Ok(true)
    }

    pub fn compute_q_up(&self) -> f64 {
        self.q_up
    }

    pub fn compute_q_down(&self) -> f64 {
        self.q_down
    }

    pub fn compute_q_diff(&self) -> f64 {
        self.q_up - self.q_down
    }

    pub fn compute_cost_total(&self) -> f64 {
        self.cost_total
    }

    pub fn compute_cost_up(&self) -> f64 {
        self.cost_up
    }

    pub fn compute_cost_down(&self) -> f64 {
        self.cost_down
    }

    pub fn compute_avg_up_cost(&self) -> Option<f64> {
        (self.q_up > 0.0).then_some(self.cost_up / self.q_up)
    }

    pub fn compute_avg_down_cost(&self) -> Option<f64> {
        (self.q_down > 0.0).then_some(self.cost_down / self.q_down)
    }

    pub fn compute_avg_cost_sum(&self) -> Option<f64> {
        Some(self.compute_avg_up_cost()? + self.compute_avg_down_cost()?)
    }

    pub fn compute_profit_if_up(&self) -> f64 {
        self.q_up - self.cost_total
    }

    pub fn compute_profit_if_down(&self) -> f64 {
        self.q_down - self.cost_total
    }

    pub fn compute_profit_min(&self) -> f64 {
        self.compute_profit_if_up()
            .min(self.compute_profit_if_down())
    }

    pub fn compute_value_live(&self, up_bid: f64, down_bid: f64) -> f64 {
        self.q_up * up_bid.max(0.0) + self.q_down * down_bid.max(0.0)
    }

    pub fn compute_pnl_live(&self, up_bid: f64, down_bid: f64) -> f64 {
        self.compute_value_live(up_bid, down_bid) - self.cost_total
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;

    fn sample_fill(fill_id: &str, outcome_side: OutcomeSide, price: f64, size: f64) -> FillDelta {
        FillDelta {
            fill_id: fill_id.to_string(),
            order_id: "order-1".to_string(),
            asset_id: "asset-1".to_string(),
            outcome_side,
            order_side: OrderSide::Buy,
            price,
            size,
            fill_timestamp: Utc.with_ymd_and_hms(2026, 4, 19, 0, 0, 0).unwrap()
                + Duration::seconds(1),
        }
    }

    #[test]
    fn buy_fills_accumulate_up_down_and_cost() {
        let mut ledger = InventoryLedger::default();

        ledger
            .apply_fill_delta(&sample_fill("up-1", OutcomeSide::Up, 0.48, 5.0))
            .unwrap();
        ledger
            .apply_fill_delta(&sample_fill("down-1", OutcomeSide::Down, 0.44, 3.0))
            .unwrap();

        assert_eq!(ledger.compute_q_up(), 5.0);
        assert_eq!(ledger.compute_q_down(), 3.0);
        assert_eq!(ledger.compute_q_diff(), 2.0);
        assert!((ledger.compute_cost_up() - 2.4).abs() < 1e-9);
        assert!((ledger.compute_cost_down() - 1.32).abs() < 1e-9);
        assert!((ledger.compute_cost_total() - 3.72).abs() < 1e-9);
    }

    #[test]
    fn duplicate_fill_is_ignored() {
        let mut ledger = InventoryLedger::default();
        let fill = sample_fill("dup", OutcomeSide::Up, 0.50, 5.0);

        assert!(ledger.apply_fill_delta(&fill).unwrap());
        assert!(!ledger.apply_fill_delta(&fill).unwrap());
        assert_eq!(ledger.compute_q_up(), 5.0);
    }

    #[test]
    fn live_value_and_pnl_use_current_best_bids() {
        let mut ledger = InventoryLedger::default();
        ledger
            .apply_fill_delta(&sample_fill("up-1", OutcomeSide::Up, 0.48, 5.0))
            .unwrap();
        ledger
            .apply_fill_delta(&sample_fill("down-1", OutcomeSide::Down, 0.47, 5.0))
            .unwrap();

        let value = ledger.compute_value_live(0.49, 0.46);
        let pnl = ledger.compute_pnl_live(0.49, 0.46);

        assert!((value - 4.75).abs() < 1e-9);
        assert!((pnl - 0.0).abs() < 1e-9);
    }

    #[test]
    fn realized_average_cost_and_terminal_profit_are_computed() {
        let mut ledger = InventoryLedger::default();
        ledger
            .apply_fill_delta(&sample_fill("up-1", OutcomeSide::Up, 0.55, 5.0))
            .unwrap();
        ledger
            .apply_fill_delta(&sample_fill("down-1", OutcomeSide::Down, 0.42, 5.0))
            .unwrap();

        assert!((ledger.compute_avg_up_cost().unwrap() - 0.55).abs() < 1e-9);
        assert!((ledger.compute_avg_down_cost().unwrap() - 0.42).abs() < 1e-9);
        assert!((ledger.compute_avg_cost_sum().unwrap() - 0.97).abs() < 1e-9);
        assert!((ledger.compute_profit_if_up() - 0.15).abs() < 1e-9);
        assert!((ledger.compute_profit_if_down() - 0.15).abs() < 1e-9);
        assert!((ledger.compute_profit_min() - 0.15).abs() < 1e-9);
    }

    #[test]
    fn sell_fill_is_rejected() {
        let mut ledger = InventoryLedger::default();
        let mut fill = sample_fill("sell-1", OutcomeSide::Up, 0.48, 5.0);
        fill.order_side = OrderSide::Sell;

        assert!(ledger.apply_fill_delta(&fill).is_err());
    }
}
