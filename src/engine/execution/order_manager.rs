use std::collections::HashMap;
use std::time::Instant;

use crate::engine::core::inventory::OutcomeSide;
use crate::telemetry::TelemetryTx;

#[derive(Debug, Clone, PartialEq)]
pub enum InternalOrderStatus {
    Created,
    Live,
    PartiallyFilled,
    FullyFilled,
    Cancelling,
    Terminated,
}

pub struct OrderTracker {
    pub order_id: String,
    pub status: InternalOrderStatus,
    pub price: f64,
    pub size: f64,
    pub trace_id: u64,
    pub local_intent_id: u64,
    // Last already-accounted cumulative size_matched observed for this order.
    pub matched_amount: f64,
    pub inventory_side: OutcomeSide,
    pub order_side: String,
    pub asset_id: String,
    pub created_at: Instant,
}

#[derive(Debug, Clone)]
pub struct AccountOrderTracker {
    pub order_id: String,
    pub market: Option<String>,
    pub status: String,
    pub matched_amount: f64,
    pub original_size: Option<f64>,
    pub side: Option<String>,
    pub asset_id: Option<String>,
    pub outcome: Option<String>,
    pub price: Option<f64>,
    pub trace_id: u64,
    pub local_intent_id: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct AccountOrderDelta {
    pub previous_size_matched: f64,
    pub current_size_matched: f64,
    pub delta: f64,
}

pub struct OrderManager {
    pub orders: HashMap<String, OrderTracker>,
    pub account_orders: HashMap<String, AccountOrderTracker>,
    pub telemetry: TelemetryTx,
}

impl OrderManager {
    pub fn new(telemetry: TelemetryTx) -> Self {
        Self {
            orders: HashMap::new(),
            account_orders: HashMap::new(),
            telemetry,
        }
    }

    pub fn on_sent(
        &mut self,
        order_id: String,
        price: f64,
        size: f64,
        inventory_side: OutcomeSide,
        order_side: &str,
        asset_id: &str,
        trace_id: u64,
        local_intent_id: u64,
    ) {
        let tracker = OrderTracker {
            order_id: order_id.clone(),
            trace_id,
            local_intent_id,
            status: InternalOrderStatus::Created,
            price,
            size,
            matched_amount: 0.0,
            inventory_side,
            order_side: order_side.to_string(),
            asset_id: asset_id.to_string(),
            created_at: Instant::now(),
        };
        self.orders.insert(order_id.clone(), tracker);
        if let Some(account) = self.account_orders.get_mut(&order_id) {
            account.trace_id = trace_id;
            account.local_intent_id = Some(local_intent_id);
            if account.asset_id.is_none() {
                account.asset_id = Some(asset_id.to_string());
            }
            if account.price.is_none() {
                account.price = Some(price);
            }
            if account.original_size.is_none() {
                account.original_size = Some(size);
            }
            if account.side.is_none() {
                account.side = Some(order_side.to_string());
            }
        }

        self.telemetry.log_order(format!(
            "[t4 SENT] order={} local_intent={} leg={} side={} asset={} price={} size={} tid={}",
            order_id,
            local_intent_id,
            outcome_side_label(inventory_side),
            order_side,
            asset_id,
            price,
            size,
            trace_id
        ));
    }

    pub fn on_acked(&mut self, order_id: &str, status_msg: &str) {
        if let Some(order) = self.orders.get_mut(order_id) {
            order.status = match status_msg {
                "live" => InternalOrderStatus::Live,
                "matched" => InternalOrderStatus::FullyFilled,
                _ => InternalOrderStatus::Live,
            };

            self.telemetry.log_order(format!(
                "[t5 ACKED] order={} status={} latency_us={}",
                order_id,
                status_msg,
                order.created_at.elapsed().as_micros()
            ));
        }
    }

    pub fn on_fill(
        &mut self,
        order_id: &str,
        current_size_matched: f64,
        is_fully_matched: bool,
    ) -> Option<f64> {
        if let Some(order) = self.orders.get_mut(order_id) {
            // current_size_matched comes from the official user-channel order report and
            // is treated as the cumulative matched size for this order.
            let last_size_matched = order.matched_amount;
            let delta = current_size_matched - last_size_matched;

            if delta < 0.0 {
                self.telemetry.log_order(format!(
                    "[ERROR] order={} negative_delta current={} last={}",
                    order_id, current_size_matched, last_size_matched
                ));
                return None;
            }

            if delta == 0.0 {
                return None;
            }

            order.matched_amount = current_size_matched;

            if is_fully_matched {
                order.status = InternalOrderStatus::FullyFilled;
                self.telemetry.log_order(format!(
                    "[t6 FILLED] order={} matched={}/{}",
                    order_id, order.matched_amount, order.size
                ));
            } else {
                order.status = InternalOrderStatus::PartiallyFilled;
                self.telemetry.log_order(format!(
                    "[t6 PARTIAL] order={} matched={}/{}",
                    order_id, order.matched_amount, order.size
                ));
            }

            if delta > 0.0 {
                return Some(delta);
            }
        }

        None
    }

    pub fn observe_order_event(
        &mut self,
        order_id: &str,
        market: Option<&str>,
        status: &str,
        side: Option<&str>,
        asset_id: Option<&str>,
        outcome: Option<&str>,
        price: Option<f64>,
        original_size: Option<f64>,
        size_matched: Option<f64>,
    ) -> Option<AccountOrderDelta> {
        let trace_id = self
            .orders
            .get(order_id)
            .map(|tracker| tracker.trace_id)
            .unwrap_or(0);
        let local_intent_id = self
            .orders
            .get(order_id)
            .map(|tracker| tracker.local_intent_id);

        let tracker = self
            .account_orders
            .entry(order_id.to_string())
            .or_insert_with(|| AccountOrderTracker {
                order_id: order_id.to_string(),
                market: market.map(str::to_string),
                status: status.to_string(),
                matched_amount: 0.0,
                original_size,
                side: side.map(str::to_string),
                asset_id: asset_id.map(str::to_string),
                outcome: outcome.map(str::to_string),
                price,
                trace_id,
                local_intent_id,
            });

        tracker.status = status.to_string();
        if let Some(value) = market {
            tracker.market = Some(value.to_string());
        }
        if let Some(value) = side {
            tracker.side = Some(value.to_string());
        }
        if let Some(value) = asset_id {
            tracker.asset_id = Some(value.to_string());
        }
        if let Some(value) = outcome {
            tracker.outcome = Some(value.to_string());
        }
        if let Some(value) = price {
            tracker.price = Some(value);
        }
        if let Some(value) = original_size {
            tracker.original_size = Some(value);
        }
        if trace_id != 0 {
            tracker.trace_id = trace_id;
        }
        if local_intent_id.is_some() {
            tracker.local_intent_id = local_intent_id;
        }

        let Some(current_size_matched) = size_matched else {
            return None;
        };

        let previous_size_matched = tracker.matched_amount;
        let delta = current_size_matched - previous_size_matched;
        if delta < 0.0 {
            self.telemetry.log_order(format!(
                "[ERROR] account_order={} negative_delta current={} last={}",
                order_id, current_size_matched, previous_size_matched
            ));
            return None;
        }
        if delta == 0.0 {
            return None;
        }

        tracker.matched_amount = current_size_matched;
        Some(AccountOrderDelta {
            previous_size_matched,
            current_size_matched,
            delta,
        })
    }

    pub fn account_order(&self, order_id: &str) -> Option<&AccountOrderTracker> {
        self.account_orders.get(order_id)
    }

    pub fn account_trace_id(&self, order_id: &str) -> u64 {
        self.orders
            .get(order_id)
            .map(|tracker| tracker.trace_id)
            .or_else(|| {
                self.account_orders
                    .get(order_id)
                    .map(|tracker| tracker.trace_id)
            })
            .unwrap_or(0)
    }

    pub fn on_cancelling(&mut self, order_id: &str) {
        if let Some(order) = self.orders.get_mut(order_id) {
            order.status = InternalOrderStatus::Cancelling;
            self.telemetry
                .log_order(format!("[t7 CANCELING] order={}", order_id));
        }
        if let Some(order) = self.account_orders.get_mut(order_id) {
            order.status = "cancelling".to_string();
        }
    }

    pub fn on_terminated(&mut self, order_id: &str, reason: &str) {
        if let Some(order) = self.orders.get_mut(order_id) {
            order.status = InternalOrderStatus::Terminated;
            self.telemetry.log_order(format!(
                "[t8 DONE] order={} reason={} matched={}",
                order_id, reason, order.matched_amount
            ));
        }
        if let Some(order) = self.account_orders.get_mut(order_id) {
            order.status = "terminated".to_string();
        }
    }

    pub fn on_error(&mut self, order_id: &str, error_msg: &str) {
        self.telemetry
            .log_order(format!("[ERROR] order={} reason={}", order_id, error_msg));
    }
}

fn outcome_side_label(side: OutcomeSide) -> &'static str {
    match side {
        OutcomeSide::Up => "UP",
        OutcomeSide::Down => "DOWN",
    }
}
