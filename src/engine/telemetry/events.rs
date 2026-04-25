use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize)]
pub struct MarketDynamicEvent {
    pub trace_id: u64,
    pub event_type: &'static str,
    pub market_ts_ms: Option<u64>,
    pub local_ts: DateTime<Utc>,
    pub eth_price: Option<f64>,
    pub ptb_price: Option<f64>,
    pub eth_ptb_spread: Option<f64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub asset_id: Option<String>,
    pub entry_gate_passed: Option<bool>,
    pub elapsed_secs: Option<f64>,
    pub seconds_to_start: Option<f64>,
    pub q_up: Option<f64>,
    pub q_down: Option<f64>,
    pub q_diff: Option<f64>,
    pub q_allow_abs: Option<f64>,
    pub q_balance_band: Option<f64>,
    pub q_flip_stop_band: Option<f64>,
    pub adjustment_active: Option<bool>,
    pub cost_up: Option<f64>,
    pub cost_down: Option<f64>,
    pub cost_total: Option<f64>,
    pub avg_up_cost: Option<f64>,
    pub avg_down_cost: Option<f64>,
    pub avg_cost_sum: Option<f64>,
    pub k_cost_sum: Option<f64>,
    pub profit_if_up: Option<f64>,
    pub profit_if_down: Option<f64>,
    pub profit_min: Option<f64>,
    pub g_target: Option<f64>,
    pub active_order_age_secs: Option<f64>,
    pub t_cancel: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UserChannelDynamicEvent {
    pub trace_id: u64,
    pub local_ts: DateTime<Utc>,
    pub source: String,
    pub market: Option<String>,
    pub summary: Option<String>,
    pub order_id: String,
    pub status: String,
    pub batch_trace_id: Option<u64>,
    pub blocking: Option<bool>,
    pub blocking_reason: Option<String>,
    pub pending_orders: Option<usize>,
    pub blocked_since: Option<DateTime<Utc>>,
    pub side: Option<String>,
    pub outcome: Option<String>,
    pub asset_id: Option<String>,
    pub fill_id: Option<String>,
    pub matched_amount_cumulative: Option<f64>,
    pub fill_delta: Option<f64>,
    pub filled_amount: Option<f64>,
    pub notional: Option<f64>,
    pub price: Option<f64>,
    pub official_order_type: Option<String>,
    pub official_status: Option<String>,
    pub raw_event_type: Option<String>,
    pub raw_type: Option<String>,
    pub raw_status: Option<String>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawUserChannelEvent {
    pub local_ts: DateTime<Utc>,
    pub stream: String,
    pub market: Option<String>,
    pub order_id: Option<String>,
    pub asset_id: Option<String>,
    pub outcome: Option<String>,
    pub side: Option<String>,
    pub price: Option<f64>,
    pub size_matched: Option<f64>,
    pub original_size: Option<f64>,
    pub raw_event_type: Option<String>,
    pub raw_type: Option<String>,
    pub raw_status: Option<String>,
    pub detail: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct LowFrequencyEvent {
    pub local_ts: DateTime<Utc>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencySummaryEvent {
    pub local_ts: DateTime<Utc>,
    pub source: &'static str,
    pub completed_traces: usize,
    pub best_bid_ask: BTreeMap<String, crate::engine::telemetry::stats::LatencySummary>,
    pub executor: BTreeMap<String, crate::engine::telemetry::stats::LatencySummary>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", content = "payload")]
pub enum DynamicEvent {
    LowFrequency(LowFrequencyEvent),
    Market(MarketDynamicEvent),
    Latency(crate::engine::telemetry::latency::LatencySnapshot),
    LatencySummary(LatencySummaryEvent),
    UserChannel(UserChannelDynamicEvent),
    RawUserChannel(RawUserChannelEvent),
}
