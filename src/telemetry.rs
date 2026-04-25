use chrono::Utc;
use crossbeam_channel::{Receiver, Sender, bounded};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::thread;

use crate::engine::telemetry::events::{
    DynamicEvent, LatencySummaryEvent, LowFrequencyEvent, MarketDynamicEvent, RawUserChannelEvent,
    UserChannelDynamicEvent,
};
use crate::engine::telemetry::latency::{
    BestBidAskStage, BestBidAskTimestamps, ExecutorPrepLatency, ExecutorPrepStage,
    ExecutorPrepTimestamps, LatencySnapshot,
};
use crate::engine::telemetry::sinks::{
    CompositeSink, JsonLineFileSink, PrettyTextFileSink, SinkKind, SinkSet,
};
use crate::engine::telemetry::stats::LatencyStatsAccumulator;

pub enum TelemetryEvent {
    BestBidAskStage {
        trace_id: u64,
        stage: BestBidAskStage,
        local_ts: chrono::DateTime<Utc>,
    },
    BestBidAskAnchor {
        trace_id: u64,
        exchange_ts_ms: u64,
        local_received_at: chrono::DateTime<Utc>,
    },
    ExecutorStage {
        trace_id: u64,
        stage: ExecutorPrepStage,
        local_ts: chrono::DateTime<Utc>,
    },
    Market(DynamicEvent),
    UserChannel(DynamicEvent),
    RawUserChannel(DynamicEvent),
    LowFrequency(String),
    OrderDetail(String),
}

#[derive(Clone)]
pub struct TelemetryTx {
    tx: Sender<TelemetryEvent>,
}

impl TelemetryTx {
    pub fn mark_best_bid_ask_stage(&self, trace_id: u64, stage: BestBidAskStage) {
        let _ = self.tx.try_send(TelemetryEvent::BestBidAskStage {
            trace_id,
            stage,
            local_ts: Utc::now(),
        });
    }

    pub fn mark_t1(&self, trace_id: u64, latency_ms: u64) {
        let now = Utc::now();
        let exchange_ts_ms = now.timestamp_millis().saturating_sub(latency_ms as i64) as u64;
        let _ = self.tx.try_send(TelemetryEvent::BestBidAskAnchor {
            trace_id,
            exchange_ts_ms,
            local_received_at: now,
        });
    }

    pub fn mark_best_bid_ask_anchor(&self, trace_id: u64, exchange_ts_ms: u64) {
        let _ = self.tx.try_send(TelemetryEvent::BestBidAskAnchor {
            trace_id,
            exchange_ts_ms,
            local_received_at: Utc::now(),
        });
    }

    pub fn mark_executor_stage(&self, trace_id: u64, stage: ExecutorPrepStage) {
        let _ = self.tx.try_send(TelemetryEvent::ExecutorStage {
            trace_id,
            stage,
            local_ts: Utc::now(),
        });
    }

    pub fn emit_market_event(&self, event: MarketDynamicEvent) {
        let _ = self
            .tx
            .try_send(TelemetryEvent::Market(DynamicEvent::Market(event)));
    }

    pub fn emit_user_channel_event(&self, event: UserChannelDynamicEvent) {
        let _ = self
            .tx
            .try_send(TelemetryEvent::UserChannel(DynamicEvent::UserChannel(
                event,
            )));
    }

    pub fn emit_raw_user_channel_event(&self, event: RawUserChannelEvent) {
        let _ = self.tx.try_send(TelemetryEvent::RawUserChannel(
            DynamicEvent::RawUserChannel(event),
        ));
    }

    pub fn log(&self, msg: String) {
        let _ = self.tx.try_send(TelemetryEvent::LowFrequency(msg));
    }

    pub fn log_order(&self, msg: String) {
        let _ = self
            .tx
            .try_send(TelemetryEvent::OrderDetail(format!("[order-log] {msg}")));
    }
}

struct NullSink;

impl crate::engine::telemetry::sinks::EventSink for NullSink {
    fn emit(&mut self, _event: &DynamicEvent) {}
}

fn resolve_log_dir() -> PathBuf {
    if let Ok(value) = std::env::var("LOG_DIR") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            return exe_dir.to_path_buf();
        }
    }

    PathBuf::from(".")
}

fn open_json_sink(file_name: &str) -> Box<dyn crate::engine::telemetry::sinks::EventSink> {
    let path = resolve_log_dir().join(file_name);
    match JsonLineFileSink::new(path.clone()) {
        Ok(sink) => Box::new(sink),
        Err(err) => {
            eprintln!(
                "[telemetry] json sink disabled for {}: {}",
                path.display(),
                err
            );
            Box::new(NullSink)
        }
    }
}

fn open_pretty_sink(
    file_name: &str,
    kind: SinkKind,
) -> Box<dyn crate::engine::telemetry::sinks::EventSink> {
    let path = resolve_log_dir().join(file_name);
    match PrettyTextFileSink::new(path.clone(), kind) {
        Ok(sink) => Box::new(sink),
        Err(err) => {
            eprintln!(
                "[telemetry] pretty sink disabled for {}: {}",
                path.display(),
                err
            );
            Box::new(NullSink)
        }
    }
}

pub fn start_telemetry_thread() -> TelemetryTx {
    let (tx, rx): (Sender<TelemetryEvent>, Receiver<TelemetryEvent>) = bounded(10000);

    thread::spawn(move || {
        let mut low_freq_sinks: Vec<Box<dyn crate::engine::telemetry::sinks::EventSink>> =
            vec![open_pretty_sink("runtime_pane.log", SinkKind::LowFrequency)];
        low_freq_sinks.push(open_json_sink("main_lowfreq.log"));

        let mut sinks = SinkSet {
            low_frequency: Box::new(CompositeSink::new(low_freq_sinks)),
            market_dynamic: Box::new(CompositeSink::new(vec![open_pretty_sink(
                "market_pane.log",
                SinkKind::MarketDynamic,
            )])),
            latency_dynamic: Box::new(CompositeSink::new(vec![open_pretty_sink(
                "latency_pane.log",
                SinkKind::LatencyDynamic,
            )])),
            user_channel_dynamic: Box::new(CompositeSink::new(vec![open_pretty_sink(
                "user_channel_pane.log",
                SinkKind::UserChannelDynamic,
            )])),
            raw_user_channel: open_json_sink("user_channel_raw.log"),
            order_detail: Box::new(NullSink),
        };

        let mut best_bid_ask_traces: HashMap<u64, BestBidAskTimestamps> = HashMap::new();
        let mut executor_traces: HashMap<u64, ExecutorPrepTimestamps> = HashMap::new();
        let mut best_bid_ask_stats = LatencyStatsBook::default();
        let mut executor_stats = LatencyStatsBook::default();
        let mut completed_traces = HashSet::new();

        while let Ok(event) = rx.recv() {
            match event {
                TelemetryEvent::LowFrequency(message) => {
                    let low = DynamicEvent::LowFrequency(LowFrequencyEvent {
                        local_ts: Utc::now(),
                        message,
                    });
                    sinks.emit(SinkKind::LowFrequency, &low);
                }
                TelemetryEvent::OrderDetail(message) => {
                    let detail = DynamicEvent::LowFrequency(LowFrequencyEvent {
                        local_ts: Utc::now(),
                        message,
                    });
                    sinks.emit(SinkKind::OrderDetail, &detail);
                }
                TelemetryEvent::Market(event) => {
                    sinks.emit(SinkKind::MarketDynamic, &event);
                }
                TelemetryEvent::UserChannel(event) => {
                    sinks.emit(SinkKind::UserChannelDynamic, &event);
                }
                TelemetryEvent::RawUserChannel(event) => {
                    sinks.emit(SinkKind::RawUserChannel, &event);
                }
                TelemetryEvent::BestBidAskAnchor {
                    trace_id,
                    exchange_ts_ms,
                    local_received_at,
                } => {
                    let trace = best_bid_ask_traces.entry(trace_id).or_default();
                    trace.official_exchange_ts_ms = Some(exchange_ts_ms);
                    trace.local_ws_received_at = Some(local_received_at);
                    emit_latency_snapshot(
                        trace_id,
                        &best_bid_ask_traces,
                        &executor_traces,
                        &mut sinks,
                        &mut best_bid_ask_stats,
                        &mut executor_stats,
                        &mut completed_traces,
                    );
                }
                TelemetryEvent::ExecutorStage {
                    trace_id,
                    stage,
                    local_ts,
                } => {
                    let trace = executor_traces.entry(trace_id).or_default();
                    match stage {
                        ExecutorPrepStage::ActionChannelSent => {
                            trace.action_channel_sent_at = Some(local_ts)
                        }
                        ExecutorPrepStage::ExecutorWakeup => {
                            trace.executor_wakeup_at = Some(local_ts)
                        }
                        ExecutorPrepStage::ExecutorReceived => {
                            trace.executor_received_at = Some(local_ts)
                        }
                        ExecutorPrepStage::PreprocessCompleted => {
                            trace.preprocess_completed_at = Some(local_ts)
                        }
                        ExecutorPrepStage::ParametersNormalized => {
                            trace.parameters_normalized_at = Some(local_ts)
                        }
                        ExecutorPrepStage::RequestConstructed => {
                            trace.request_constructed_at = Some(local_ts)
                        }
                        ExecutorPrepStage::Signed => trace.signed_at = Some(local_ts),
                        ExecutorPrepStage::RequestSent => {
                            trace.request_sent_at = Some(local_ts);
                            best_bid_ask_traces
                                .entry(trace_id)
                                .or_default()
                                .request_sent_at = Some(local_ts);
                        }
                    }
                    emit_latency_snapshot(
                        trace_id,
                        &best_bid_ask_traces,
                        &executor_traces,
                        &mut sinks,
                        &mut best_bid_ask_stats,
                        &mut executor_stats,
                        &mut completed_traces,
                    );
                }
                TelemetryEvent::BestBidAskStage {
                    trace_id,
                    stage,
                    local_ts,
                } => {
                    let trace = best_bid_ask_traces.entry(trace_id).or_default();
                    match stage {
                        BestBidAskStage::OfficialExchangeTimestamp => {
                            trace.official_exchange_ts_ms =
                                Some(local_ts.timestamp_millis() as u64);
                        }
                        BestBidAskStage::LocalWsReceived => {
                            trace.local_ws_received_at = Some(local_ts)
                        }
                        BestBidAskStage::PayloadReady => trace.payload_ready_at = Some(local_ts),
                        BestBidAskStage::ParseCompleted => {
                            trace.parse_completed_at = Some(local_ts)
                        }
                        BestBidAskStage::MarketStateUpdated => {
                            trace.market_state_updated_at = Some(local_ts)
                        }
                        BestBidAskStage::StrategyStarted => {
                            trace.strategy_started_at = Some(local_ts)
                        }
                        BestBidAskStage::StrategyCompleted => {
                            trace.strategy_completed_at = Some(local_ts)
                        }
                        BestBidAskStage::TradeActionCreated => {
                            trace.trade_action_created_at = Some(local_ts)
                        }
                        BestBidAskStage::ActionSent => trace.action_sent_at = Some(local_ts),
                        BestBidAskStage::ExecutorReceived => {
                            trace.executor_received_at = Some(local_ts)
                        }
                        BestBidAskStage::RequestSent => trace.request_sent_at = Some(local_ts),
                        BestBidAskStage::UserChannelReported => {
                            trace.user_channel_reported_at = Some(local_ts)
                        }
                    }
                    emit_latency_snapshot(
                        trace_id,
                        &best_bid_ask_traces,
                        &executor_traces,
                        &mut sinks,
                        &mut best_bid_ask_stats,
                        &mut executor_stats,
                        &mut completed_traces,
                    );
                }
            }
        }
    });

    TelemetryTx { tx }
}

fn emit_latency_snapshot(
    trace_id: u64,
    best_bid_ask_traces: &HashMap<u64, BestBidAskTimestamps>,
    executor_traces: &HashMap<u64, ExecutorPrepTimestamps>,
    sinks: &mut SinkSet,
    best_bid_ask_stats: &mut LatencyStatsBook,
    executor_stats: &mut LatencyStatsBook,
    completed_traces: &mut HashSet<u64>,
) {
    let timestamps = best_bid_ask_traces
        .get(&trace_id)
        .cloned()
        .unwrap_or_default();
    let executor = executor_traces.get(&trace_id).cloned().unwrap_or_default();
    let snapshot = LatencySnapshot::from_parts(trace_id, "best_bid_ask", &timestamps, &executor);
    sinks.emit(
        SinkKind::LatencyDynamic,
        &DynamicEvent::Latency(snapshot.clone()),
    );

    if completed_traces.insert(trace_id) && snapshot.executor_timestamps.request_sent_at.is_some() {
        best_bid_ask_stats.record_best_bid_ask(&snapshot.best_bid_ask_latency);
        executor_stats.record_executor(&snapshot.executor_latency);
        sinks.emit(
            SinkKind::LatencyDynamic,
            &DynamicEvent::LatencySummary(LatencySummaryEvent {
                local_ts: Utc::now(),
                source: "best_bid_ask",
                completed_traces: completed_traces.len(),
                best_bid_ask: best_bid_ask_stats.snapshot(),
                executor: executor_stats.snapshot(),
            }),
        );
    }
}

#[derive(Default)]
struct LatencyStatsBook {
    per_segment: BTreeMap<String, LatencyStatsAccumulator>,
}

impl LatencyStatsBook {
    fn record(&mut self, segment: &str, value: Option<i64>) {
        if let Some(value_ms) = value {
            self.per_segment
                .entry(segment.to_string())
                .or_default()
                .record(value_ms as f64);
        }
    }

    fn record_best_bid_ask(
        &mut self,
        latency: &crate::engine::telemetry::latency::BestBidAskLatency,
    ) {
        self.record("exchange_to_local_ws_ms", latency.exchange_to_local_ws_ms);
        self.record(
            "local_ws_to_payload_ready_ms",
            latency.local_ws_to_payload_ready_ms,
        );
        self.record(
            "payload_ready_to_parse_ms",
            latency.payload_ready_to_parse_ms,
        );
        self.record("parse_to_state_update_ms", latency.parse_to_state_update_ms);
        self.record(
            "state_update_to_strategy_start_ms",
            latency.state_update_to_strategy_start_ms,
        );
        self.record("strategy_runtime_ms", latency.strategy_runtime_ms);
        self.record("strategy_to_action_ms", latency.strategy_to_action_ms);
        self.record(
            "action_to_executor_received_ms",
            latency.action_to_executor_received_ms,
        );
        self.record(
            "executor_to_request_sent_ms",
            latency.executor_to_request_sent_ms,
        );
        self.record(
            "request_to_user_channel_ms",
            latency.request_to_user_channel_ms,
        );
    }

    fn record_executor(&mut self, latency: &ExecutorPrepLatency) {
        self.record("action_channel_ms", latency.action_channel_ms);
        self.record("wakeup_ms", latency.wakeup_ms);
        self.record("preprocess_ms", latency.preprocess_ms);
        self.record("normalize_params_ms", latency.normalize_params_ms);
        self.record("construct_request_ms", latency.construct_request_ms);
        self.record("signing_ms", latency.signing_ms);
        self.record("final_prepare_to_send_ms", latency.final_prepare_to_send_ms);
    }

    fn snapshot(&self) -> BTreeMap<String, crate::engine::telemetry::stats::LatencySummary> {
        self.per_segment
            .iter()
            .map(|(segment, stats)| (segment.clone(), stats.summary()))
            .collect()
    }
}
