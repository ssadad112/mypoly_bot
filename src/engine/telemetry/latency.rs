use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum BestBidAskStage {
    OfficialExchangeTimestamp,
    LocalWsReceived,
    PayloadReady,
    ParseCompleted,
    MarketStateUpdated,
    StrategyStarted,
    StrategyCompleted,
    TradeActionCreated,
    ActionSent,
    ExecutorReceived,
    RequestSent,
    UserChannelReported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum ExecutorPrepStage {
    ActionChannelSent,
    ExecutorWakeup,
    ExecutorReceived,
    PreprocessCompleted,
    ParametersNormalized,
    RequestConstructed,
    Signed,
    RequestSent,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct BestBidAskTimestamps {
    pub official_exchange_ts_ms: Option<u64>,
    pub local_ws_received_at: Option<DateTime<Utc>>,
    pub payload_ready_at: Option<DateTime<Utc>>,
    pub parse_completed_at: Option<DateTime<Utc>>,
    pub market_state_updated_at: Option<DateTime<Utc>>,
    pub strategy_started_at: Option<DateTime<Utc>>,
    pub strategy_completed_at: Option<DateTime<Utc>>,
    pub trade_action_created_at: Option<DateTime<Utc>>,
    pub action_sent_at: Option<DateTime<Utc>>,
    pub executor_received_at: Option<DateTime<Utc>>,
    pub request_sent_at: Option<DateTime<Utc>>,
    pub user_channel_reported_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ExecutorPrepTimestamps {
    pub action_channel_sent_at: Option<DateTime<Utc>>,
    pub executor_wakeup_at: Option<DateTime<Utc>>,
    pub executor_received_at: Option<DateTime<Utc>>,
    pub preprocess_completed_at: Option<DateTime<Utc>>,
    pub parameters_normalized_at: Option<DateTime<Utc>>,
    pub request_constructed_at: Option<DateTime<Utc>>,
    pub signed_at: Option<DateTime<Utc>>,
    pub request_sent_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct BestBidAskLatency {
    pub exchange_to_local_ws_ms: Option<i64>,
    pub local_ws_to_payload_ready_ms: Option<i64>,
    pub payload_ready_to_parse_ms: Option<i64>,
    pub parse_to_state_update_ms: Option<i64>,
    pub state_update_to_strategy_start_ms: Option<i64>,
    pub strategy_runtime_ms: Option<i64>,
    pub strategy_to_action_ms: Option<i64>,
    pub action_to_executor_received_ms: Option<i64>,
    pub executor_to_request_sent_ms: Option<i64>,
    pub request_to_user_channel_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ExecutorPrepLatency {
    pub action_channel_ms: Option<i64>,
    pub wakeup_ms: Option<i64>,
    pub preprocess_ms: Option<i64>,
    pub normalize_params_ms: Option<i64>,
    pub construct_request_ms: Option<i64>,
    pub signing_ms: Option<i64>,
    pub final_prepare_to_send_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencySnapshot {
    pub trace_id: u64,
    pub source: &'static str,
    pub timestamps: BestBidAskTimestamps,
    pub best_bid_ask_latency: BestBidAskLatency,
    pub executor_timestamps: ExecutorPrepTimestamps,
    pub executor_latency: ExecutorPrepLatency,
}

impl LatencySnapshot {
    pub fn from_parts(
        trace_id: u64,
        source: &'static str,
        timestamps: &BestBidAskTimestamps,
        executor: &ExecutorPrepTimestamps,
    ) -> Self {
        Self {
            trace_id,
            source,
            timestamps: timestamps.clone(),
            best_bid_ask_latency: BestBidAskLatency::from_timestamps(timestamps, executor),
            executor_timestamps: executor.clone(),
            executor_latency: ExecutorPrepLatency::from_timestamps(executor),
        }
    }
}

impl BestBidAskLatency {
    pub fn from_timestamps(
        timestamps: &BestBidAskTimestamps,
        executor: &ExecutorPrepTimestamps,
    ) -> Self {
        Self {
            exchange_to_local_ws_ms: timestamps
                .official_exchange_ts_ms
                .zip(timestamps.local_ws_received_at)
                .map(|(exchange_ms, local)| local.timestamp_millis() - exchange_ms as i64),
            local_ws_to_payload_ready_ms: diff_ms(
                timestamps.local_ws_received_at,
                timestamps.payload_ready_at,
            ),
            payload_ready_to_parse_ms: diff_ms(
                timestamps.payload_ready_at,
                timestamps.parse_completed_at,
            ),
            parse_to_state_update_ms: diff_ms(
                timestamps.parse_completed_at,
                timestamps.market_state_updated_at,
            ),
            state_update_to_strategy_start_ms: diff_ms(
                timestamps.market_state_updated_at,
                timestamps.strategy_started_at,
            ),
            strategy_runtime_ms: diff_ms(
                timestamps.strategy_started_at,
                timestamps.strategy_completed_at,
            ),
            strategy_to_action_ms: diff_ms(
                timestamps.strategy_completed_at,
                timestamps.trade_action_created_at,
            ),
            action_to_executor_received_ms: diff_ms(
                timestamps.action_sent_at,
                executor
                    .executor_received_at
                    .or(timestamps.executor_received_at),
            ),
            executor_to_request_sent_ms: diff_ms(
                executor
                    .executor_received_at
                    .or(timestamps.executor_received_at),
                executor.request_sent_at.or(timestamps.request_sent_at),
            ),
            request_to_user_channel_ms: diff_ms(
                timestamps.request_sent_at.or(executor.request_sent_at),
                timestamps.user_channel_reported_at,
            ),
        }
    }
}

impl ExecutorPrepLatency {
    pub fn from_timestamps(timestamps: &ExecutorPrepTimestamps) -> Self {
        Self {
            action_channel_ms: diff_ms(
                timestamps.action_channel_sent_at,
                timestamps
                    .executor_wakeup_at
                    .or(timestamps.executor_received_at),
            ),
            wakeup_ms: diff_ms(
                timestamps.executor_wakeup_at,
                timestamps.executor_received_at,
            ),
            preprocess_ms: diff_ms(
                timestamps.executor_received_at,
                timestamps.preprocess_completed_at,
            ),
            normalize_params_ms: diff_ms(
                timestamps.preprocess_completed_at,
                timestamps.parameters_normalized_at,
            ),
            construct_request_ms: diff_ms(
                timestamps.parameters_normalized_at,
                timestamps.request_constructed_at,
            ),
            signing_ms: diff_ms(timestamps.request_constructed_at, timestamps.signed_at),
            final_prepare_to_send_ms: diff_ms(timestamps.signed_at, timestamps.request_sent_at),
        }
    }
}

pub fn diff_ms(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Option<i64> {
    start
        .zip(end)
        .map(|(lhs, rhs)| (rhs - lhs).num_milliseconds())
}
