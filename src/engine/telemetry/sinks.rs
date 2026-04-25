use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use serde_json::Value;

use crate::engine::telemetry::events::DynamicEvent;
use crate::engine::telemetry::latency::LatencySnapshot;

#[derive(Debug, Clone, Copy)]
pub enum SinkKind {
    LowFrequency,
    MarketDynamic,
    LatencyDynamic,
    UserChannelDynamic,
    RawUserChannel,
    OrderDetail,
}

pub trait EventSink: Send {
    fn emit(&mut self, event: &DynamicEvent);
}

pub struct CompositeSink {
    sinks: Vec<Box<dyn EventSink>>,
}

impl CompositeSink {
    pub fn new(sinks: Vec<Box<dyn EventSink>>) -> Self {
        Self { sinks }
    }
}

impl EventSink for CompositeSink {
    fn emit(&mut self, event: &DynamicEvent) {
        for sink in &mut self.sinks {
            sink.emit(event);
        }
    }
}

pub struct JsonLineFileSink {
    file: BufWriter<std::fs::File>,
}

impl JsonLineFileSink {
    pub fn new(path: PathBuf) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: BufWriter::new(file),
        })
    }
}

impl EventSink for JsonLineFileSink {
    fn emit(&mut self, event: &DynamicEvent) {
        if let Ok(mut value) = serde_json::to_value(event) {
            normalize_timestamps(&mut value);
            if let Ok(line) = serde_json::to_string(&value) {
                let _ = self.file.write_all(line.as_bytes());
                let _ = self.file.write_all(b"\n");
            }
        }
    }
}

pub struct PrettyTextFileSink {
    kind: SinkKind,
    file: BufWriter<std::fs::File>,
    pending_lines: usize,
    flush_every: usize,
}

impl PrettyTextFileSink {
    pub fn new(path: PathBuf, kind: SinkKind) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            kind,
            file: BufWriter::new(file),
            pending_lines: 0,
            flush_every: 16,
        })
    }
}

impl EventSink for PrettyTextFileSink {
    fn emit(&mut self, event: &DynamicEvent) {
        let mut line = format_pretty_line(self.kind, event, false);
        if !line.is_empty() && use_bright_green_file_ansi(self.kind) {
            line = format!("\u{1b}[1;92m{}\u{1b}[0m", line);
        }
        if !line.is_empty() {
            let _ = self.file.write_all(line.as_bytes());
            let _ = self.file.write_all(b"\n");
            self.pending_lines += 1;
            if should_flush_immediately(event) || self.pending_lines >= self.flush_every {
                let _ = self.file.flush();
                self.pending_lines = 0;
            }
        }
    }
}

pub struct TerminalPrettySink {
    kind: SinkKind,
    pending_lines: usize,
    flush_every: usize,
}

impl TerminalPrettySink {
    pub fn new(kind: SinkKind) -> Self {
        Self {
            kind,
            pending_lines: 0,
            flush_every: 16,
        }
    }
}

impl EventSink for TerminalPrettySink {
    fn emit(&mut self, event: &DynamicEvent) {
        let line = format_pretty_line(self.kind, event, true);
        if !line.is_empty() {
            let mut stdout = std::io::stdout().lock();
            let _ = stdout.write_all(line.as_bytes());
            let _ = stdout.write_all(b"\n");
            self.pending_lines += 1;
            if should_flush_immediately(event) || self.pending_lines >= self.flush_every {
                let _ = stdout.flush();
                self.pending_lines = 0;
            }
        }
    }
}

pub struct SinkSet {
    pub low_frequency: Box<dyn EventSink>,
    pub market_dynamic: Box<dyn EventSink>,
    pub latency_dynamic: Box<dyn EventSink>,
    pub user_channel_dynamic: Box<dyn EventSink>,
    pub raw_user_channel: Box<dyn EventSink>,
    pub order_detail: Box<dyn EventSink>,
}

impl SinkSet {
    pub fn emit(&mut self, kind: SinkKind, event: &DynamicEvent) {
        match kind {
            SinkKind::LowFrequency => self.low_frequency.emit(event),
            SinkKind::MarketDynamic => self.market_dynamic.emit(event),
            SinkKind::LatencyDynamic => self.latency_dynamic.emit(event),
            SinkKind::UserChannelDynamic => self.user_channel_dynamic.emit(event),
            SinkKind::RawUserChannel => self.raw_user_channel.emit(event),
            SinkKind::OrderDetail => self.order_detail.emit(event),
        }
    }
}

fn use_bright_green_file_ansi(kind: SinkKind) -> bool {
    matches!(
        kind,
        SinkKind::MarketDynamic | SinkKind::LatencyDynamic | SinkKind::UserChannelDynamic
    )
}

fn should_flush_immediately(event: &DynamicEvent) -> bool {
    match event {
        DynamicEvent::UserChannel(payload) => {
            payload.status.contains("fill") || payload.blocking.unwrap_or(false)
        }
        DynamicEvent::LowFrequency(_) | DynamicEvent::LatencySummary(_) => true,
        DynamicEvent::Market(_) | DynamicEvent::Latency(_) | DynamicEvent::RawUserChannel(_) => {
            false
        }
    }
}

fn normalize_timestamps(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (_, inner) in map.iter_mut() {
                normalize_timestamps(inner);
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_timestamps(item);
            }
        }
        Value::String(text) => {
            if let Ok(parsed) = DateTime::parse_from_rfc3339(text) {
                *text = parsed
                    .with_timezone(&Utc)
                    .to_rfc3339_opts(SecondsFormat::Millis, true);
            }
        }
        _ => {}
    }
}

fn format_pretty_line(_kind: SinkKind, event: &DynamicEvent, ansi: bool) -> String {
    match event {
        DynamicEvent::LowFrequency(payload) => format!(
            "{} {} {}",
            colorize(Color::Dim, "[LOW]", ansi),
            fmt_ts(payload.local_ts),
            emphasize_message(&payload.message, ansi)
        ),
        DynamicEvent::Market(payload) => {
            let market_ts = payload
                .market_ts_ms
                .and_then(|ts| Utc.timestamp_millis_opt(ts as i64).single())
                .map(fmt_ts)
                .unwrap_or_else(|| "N/A".to_string());
            format!(
                "{} {} event={} market_ts={} best_bid={} best_ask={} eth={} ptb={} spread={} asset_id={} entry_gate={} elapsed_secs={} seconds_to_start={} q_up={} q_down={} q_diff={} q_allow_abs={} q_balance_band={} q_flip_stop_band={} adjustment_active={} cost_up={} cost_down={} cost_total={} avg_up={} avg_down={} avg_sum={} k_cost_sum={} profit_up={} profit_down={} profit_min={} g_target={} active_order_age={} t_cancel={}",
                colorize(Color::Green, "[MARKET]", ansi),
                fmt_ts(payload.local_ts),
                payload.event_type,
                colorize(Color::Green, &market_ts, ansi),
                highlight_opt_f64(payload.best_bid, ansi),
                highlight_opt_f64(payload.best_ask, ansi),
                highlight_opt_f64(payload.eth_price, ansi),
                highlight_opt_f64(payload.ptb_price, ansi),
                highlight_opt_f64(payload.eth_ptb_spread, ansi),
                highlight_opt_str(payload.asset_id.as_deref(), ansi),
                highlight_opt_bool(payload.entry_gate_passed, ansi),
                highlight_opt_f64(payload.elapsed_secs, ansi),
                highlight_opt_f64(payload.seconds_to_start, ansi),
                highlight_opt_f64(payload.q_up, ansi),
                highlight_opt_f64(payload.q_down, ansi),
                highlight_opt_f64(payload.q_diff, ansi),
                highlight_opt_f64(payload.q_allow_abs, ansi),
                highlight_opt_f64(payload.q_balance_band, ansi),
                highlight_opt_f64(payload.q_flip_stop_band, ansi),
                highlight_opt_bool(payload.adjustment_active, ansi),
                highlight_opt_f64(payload.cost_up, ansi),
                highlight_opt_f64(payload.cost_down, ansi),
                highlight_opt_f64(payload.cost_total, ansi),
                highlight_opt_f64(payload.avg_up_cost, ansi),
                highlight_opt_f64(payload.avg_down_cost, ansi),
                highlight_opt_f64(payload.avg_cost_sum, ansi),
                highlight_opt_f64(payload.k_cost_sum, ansi),
                highlight_opt_f64(payload.profit_if_up, ansi),
                highlight_opt_f64(payload.profit_if_down, ansi),
                highlight_opt_f64(payload.profit_min, ansi),
                highlight_opt_f64(payload.g_target, ansi),
                highlight_opt_f64(payload.active_order_age_secs, ansi),
                highlight_opt_f64(payload.t_cancel, ansi),
            )
        }
        DynamicEvent::UserChannel(payload) => {
            let prefix_color = if payload.blocking.unwrap_or(false) {
                Color::Yellow
            } else if payload.status.contains("fill") {
                Color::Green
            } else {
                Color::Cyan
            };
            format!(
                "{} {} source={} market={} status={} blocking={} blocking_reason={} pending_orders={} blocked_since={} batch_trace_id={} outcome={} side={} asset_id={} price={} matched_amount_cumulative={} fill_delta={} filled_amount={} notional={} fill_id={} order_id={} official_order_type={} official_status={} raw_event_type={} detail={}",
                colorize(prefix_color, "[USER]", ansi),
                fmt_ts(payload.local_ts),
                colorize(prefix_color, &payload.source, ansi),
                highlight_opt_str(payload.market.as_deref(), ansi),
                colorize(prefix_color, &payload.status, ansi),
                highlight_opt_bool(payload.blocking, ansi),
                highlight_opt_str(payload.blocking_reason.as_deref(), ansi),
                highlight_opt_usize(payload.pending_orders, ansi),
                highlight_opt_ts(payload.blocked_since, ansi),
                highlight_opt_u64(payload.batch_trace_id, ansi),
                highlight_opt_str(payload.outcome.as_deref(), ansi),
                highlight_opt_str(payload.side.as_deref(), ansi),
                highlight_opt_str(payload.asset_id.as_deref(), ansi),
                highlight_opt_f64(payload.price, ansi),
                highlight_opt_f64(payload.matched_amount_cumulative, ansi),
                highlight_opt_f64(payload.fill_delta, ansi),
                highlight_opt_f64(payload.filled_amount, ansi),
                highlight_opt_f64(payload.notional, ansi),
                highlight_opt_str(payload.fill_id.as_deref(), ansi),
                colorize(Color::Green, &payload.order_id, ansi),
                highlight_opt_str(payload.official_order_type.as_deref(), ansi),
                highlight_opt_str(payload.official_status.as_deref(), ansi),
                highlight_opt_str(payload.raw_event_type.as_deref(), ansi),
                highlight_opt_str(payload.detail.as_deref(), ansi),
            )
        }
        DynamicEvent::RawUserChannel(payload) => format!(
            "{} {} stream={} market={} order_id={} outcome={} side={} asset_id={} price={} size_matched={} original_size={} raw_type={} raw_status={} detail={}",
            colorize(Color::Dim, "[USER-RAW]", ansi),
            fmt_ts(payload.local_ts),
            colorize(Color::Dim, &payload.stream, ansi),
            highlight_opt_str(payload.market.as_deref(), ansi),
            highlight_opt_str(payload.order_id.as_deref(), ansi),
            highlight_opt_str(payload.outcome.as_deref(), ansi),
            highlight_opt_str(payload.side.as_deref(), ansi),
            highlight_opt_str(payload.asset_id.as_deref(), ansi),
            highlight_opt_f64(payload.price, ansi),
            highlight_opt_f64(payload.size_matched, ansi),
            highlight_opt_f64(payload.original_size, ansi),
            highlight_opt_str(payload.raw_type.as_deref(), ansi),
            highlight_opt_str(payload.raw_status.as_deref(), ansi),
            highlight_opt_str(payload.detail.as_deref(), ansi),
        ),
        DynamicEvent::Latency(snapshot) => format_latency_line(snapshot, ansi),
        DynamicEvent::LatencySummary(payload) => {
            let best_bid_ask_segments = payload.best_bid_ask.len();
            let executor_segments = payload.executor.len();
            format!(
                "{} {} source={} completed_traces={} best_bid_ask_segments={} executor_segments={}",
                colorize(Color::Yellow, "[LATENCY-SUMMARY]", ansi),
                fmt_ts(payload.local_ts),
                payload.source,
                colorize(Color::Green, &payload.completed_traces.to_string(), ansi),
                colorize(Color::Green, &best_bid_ask_segments.to_string(), ansi),
                colorize(Color::Green, &executor_segments.to_string(), ansi),
            )
        }
    }
}

fn highlight_opt_usize(value: Option<usize>, ansi: bool) -> String {
    value
        .map(|entry| colorize(Color::Yellow, &entry.to_string(), ansi))
        .unwrap_or_else(|| "N/A".to_string())
}

fn highlight_opt_u64(value: Option<u64>, ansi: bool) -> String {
    value
        .map(|entry| colorize(Color::Yellow, &entry.to_string(), ansi))
        .unwrap_or_else(|| "N/A".to_string())
}

fn highlight_opt_ts(value: Option<DateTime<Utc>>, ansi: bool) -> String {
    value
        .map(|entry| colorize(Color::Yellow, &fmt_ts(entry), ansi))
        .unwrap_or_else(|| "N/A".to_string())
}

fn format_latency_line(snapshot: &LatencySnapshot, ansi: bool) -> String {
    let local_ts = snapshot
        .timestamps
        .local_ws_received_at
        .map(fmt_ts)
        .or_else(|| snapshot.timestamps.request_sent_at.map(fmt_ts))
        .or_else(|| snapshot.timestamps.user_channel_reported_at.map(fmt_ts))
        .unwrap_or_else(|| "N/A".to_string());

    let left_header = format!(
        "{} {}  {}={}",
        colorize(Color::Green, "[主链路延迟]", ansi),
        local_ts,
        highlight_latency_label("trace", Color::Green, ansi),
        highlight_latency_value(&snapshot.trace_id.to_string(), Color::Green, ansi),
    );
    let right_header = colorize(Color::Cyan, "[执行器细分]", ansi);

    let left_lines = [
        latency_cell(
            "初始数据获取",
            format_duration_with_fallback_us(
                snapshot.best_bid_ask_latency.exchange_to_local_ws_ms,
                snapshot
                    .timestamps
                    .official_exchange_ts_ms
                    .zip(snapshot.timestamps.local_ws_received_at)
                    .map(|(exchange_ms, local)| {
                        local.timestamp_micros() - (exchange_ms as i64 * 1_000)
                    }),
            ),
            Color::Green,
            ansi,
        ),
        latency_cell(
            "本地载荷准备",
            format_duration_us(diff_us(
                snapshot.timestamps.local_ws_received_at,
                snapshot.timestamps.payload_ready_at,
            )),
            Color::Green,
            ansi,
        ),
        latency_cell(
            "解析完成",
            format_duration_us(diff_us(
                snapshot.timestamps.payload_ready_at,
                snapshot.timestamps.parse_completed_at,
            )),
            Color::Green,
            ansi,
        ),
        latency_cell(
            "状态更新",
            format_duration_us(diff_us(
                snapshot.timestamps.parse_completed_at,
                snapshot.timestamps.market_state_updated_at,
            )),
            Color::Green,
            ansi,
        ),
        latency_cell(
            "策略启动等待",
            format_duration_us(diff_us(
                snapshot.timestamps.market_state_updated_at,
                snapshot.timestamps.strategy_started_at,
            )),
            Color::Green,
            ansi,
        ),
        latency_cell(
            "策略计算",
            format_duration_us(diff_us(
                snapshot.timestamps.strategy_started_at,
                snapshot.timestamps.strategy_completed_at,
            )),
            Color::Green,
            ansi,
        ),
        latency_cell(
            "动作生成",
            format_duration_us(diff_us(
                snapshot.timestamps.strategy_completed_at,
                snapshot.timestamps.trade_action_created_at,
            )),
            Color::Green,
            ansi,
        ),
        latency_cell(
            "动作到执行器",
            format_duration_us(diff_us(
                snapshot.timestamps.action_sent_at,
                snapshot
                    .executor_timestamps
                    .executor_received_at
                    .or(snapshot.timestamps.executor_received_at),
            )),
            Color::Green,
            ansi,
        ),
        latency_cell(
            "执行到发单",
            format_duration_us(diff_us(
                snapshot
                    .executor_timestamps
                    .executor_received_at
                    .or(snapshot.timestamps.executor_received_at),
                snapshot
                    .executor_timestamps
                    .request_sent_at
                    .or(snapshot.timestamps.request_sent_at),
            )),
            Color::Green,
            ansi,
        ),
        latency_cell(
            "发单到回报",
            format_duration_us(diff_us(
                snapshot
                    .timestamps
                    .request_sent_at
                    .or(snapshot.executor_timestamps.request_sent_at),
                snapshot.timestamps.user_channel_reported_at,
            )),
            Color::Green,
            ansi,
        ),
    ];

    let right_lines = [
        latency_cell(
            "动作通道传递",
            format_duration_us(diff_us(
                snapshot.executor_timestamps.action_channel_sent_at,
                snapshot
                    .executor_timestamps
                    .executor_wakeup_at
                    .or(snapshot.executor_timestamps.executor_received_at),
            )),
            Color::Cyan,
            ansi,
        ),
        latency_cell(
            "执行器唤醒",
            format_duration_us(diff_us(
                snapshot.executor_timestamps.executor_wakeup_at,
                snapshot.executor_timestamps.executor_received_at,
            )),
            Color::Cyan,
            ansi,
        ),
        latency_cell(
            "前置处理",
            format_duration_us(diff_us(
                snapshot.executor_timestamps.executor_received_at,
                snapshot.executor_timestamps.preprocess_completed_at,
            )),
            Color::Cyan,
            ansi,
        ),
        latency_cell(
            "参数规整",
            format_duration_us(diff_us(
                snapshot.executor_timestamps.preprocess_completed_at,
                snapshot.executor_timestamps.parameters_normalized_at,
            )),
            Color::Cyan,
            ansi,
        ),
        latency_cell(
            "请求构造",
            format_duration_us(diff_us(
                snapshot.executor_timestamps.parameters_normalized_at,
                snapshot.executor_timestamps.request_constructed_at,
            )),
            Color::Cyan,
            ansi,
        ),
        latency_cell(
            "签名",
            format_duration_us(diff_us(
                snapshot.executor_timestamps.request_constructed_at,
                snapshot.executor_timestamps.signed_at,
            )),
            Color::Cyan,
            ansi,
        ),
        latency_cell(
            "最终发出前准备",
            format_duration_us(diff_us(
                snapshot.executor_timestamps.signed_at,
                snapshot.executor_timestamps.request_sent_at,
            )),
            Color::Cyan,
            ansi,
        ),
    ];

    let body = side_by_side_lines(&left_lines, &right_lines, 52);
    format!("{left_header}    {right_header}\n{body}")
}

fn fmt_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn highlight_opt_f64(value: Option<f64>, ansi: bool) -> String {
    match value {
        Some(number) => colorize(Color::Green, &format!("{number:.6}"), ansi),
        None => colorize(Color::Yellow, "N/A", ansi),
    }
}

fn highlight_opt_bool(value: Option<bool>, ansi: bool) -> String {
    match value {
        Some(number) => colorize(Color::Green, &number.to_string(), ansi),
        None => colorize(Color::Yellow, "N/A", ansi),
    }
}

fn highlight_opt_str(value: Option<&str>, ansi: bool) -> String {
    match value {
        Some(text) if !text.is_empty() => colorize(Color::Green, text, ansi),
        _ => colorize(Color::Yellow, "N/A", ansi),
    }
}

fn diff_us(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Option<i64> {
    start
        .zip(end)
        .map(|(lhs, rhs)| (rhs - lhs).num_microseconds().unwrap_or(0))
}

fn format_duration_with_fallback_us(ms: Option<i64>, us: Option<i64>) -> String {
    if let Some(us) = us {
        return format_duration_us(Some(us));
    }
    match ms {
        Some(value) => format!("{value}ms"),
        None => "N/A".to_string(),
    }
}

fn format_duration_us(us: Option<i64>) -> String {
    match us {
        Some(value) if value >= 1_000 => format!("{}ms", value / 1_000),
        Some(value) if value >= 0 => format!("{value}us"),
        _ => "N/A".to_string(),
    }
}

fn latency_cell(label: &str, value: String, color: Color, ansi: bool) -> String {
    format!(
        "{} {}",
        highlight_latency_label(label, color, ansi),
        highlight_latency_value(&value, color, ansi),
    )
}

fn side_by_side_lines(left: &[String], right: &[String], left_width: usize) -> String {
    let rows = left.len().max(right.len());
    let mut rendered = Vec::with_capacity(rows);
    for idx in 0..rows {
        let left_text = left.get(idx).map(String::as_str).unwrap_or("");
        let right_text = right.get(idx).map(String::as_str).unwrap_or("");
        rendered.push(format!(
            "{left_text:<left_width$}    {right_text}",
            left_width = left_width
        ));
    }
    rendered.join("\n")
}

fn highlight_latency_label(label: &str, color: Color, ansi: bool) -> String {
    colorize_bold(color, label, ansi)
}

fn highlight_latency_value(value: &str, color: Color, ansi: bool) -> String {
    colorize_bold(color, value, ansi)
}

fn emphasize_message(message: &str, ansi: bool) -> String {
    let important = [
        "[MARKET]",
        "escape",
        "round",
        "started",
        "ended",
        "state",
        "view command",
        "dynamic log path",
    ];
    if important.iter().any(|needle| message.contains(needle)) {
        colorize(Color::Green, message, ansi)
    } else {
        colorize(Color::Dim, message, ansi)
    }
}

#[derive(Clone, Copy)]
enum Color {
    Green,
    Yellow,
    Cyan,
    Dim,
}

fn colorize(color: Color, text: &str, ansi: bool) -> String {
    if !ansi {
        return text.to_string();
    }
    let code = match color {
        Color::Green => "32",
        Color::Yellow => "33",
        Color::Cyan => "36",
        Color::Dim => "2",
    };
    format!("\u{1b}[{code}m{text}\u{1b}[0m")
}

fn colorize_bold(color: Color, text: &str, ansi: bool) -> String {
    if !ansi {
        return text.to_string();
    }
    let code = match color {
        Color::Green => "1;32",
        Color::Yellow => "1;33",
        Color::Cyan => "1;36",
        Color::Dim => "1;2",
    };
    format!("\u{1b}[{code}m{text}\u{1b}[0m")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::telemetry::events::{DynamicEvent, UserChannelDynamicEvent};

    fn sample_user_event(source: &str, status: &str) -> DynamicEvent {
        DynamicEvent::UserChannel(UserChannelDynamicEvent {
            trace_id: 7,
            local_ts: Utc::now(),
            source: source.to_string(),
            market: Some("market-1".to_string()),
            summary: None,
            order_id: "order-1".to_string(),
            status: status.to_string(),
            batch_trace_id: None,
            blocking: None,
            blocking_reason: None,
            pending_orders: None,
            blocked_since: None,
            side: Some("BUY".to_string()),
            outcome: Some("YES".to_string()),
            asset_id: Some("asset-up".to_string()),
            fill_id: Some("fill-1".to_string()),
            matched_amount_cumulative: Some(3.0),
            fill_delta: Some(1.0),
            filled_amount: Some(1.0),
            notional: Some(0.48),
            price: Some(0.48),
            official_order_type: Some("UPDATE".to_string()),
            official_status: Some("LIVE".to_string()),
            raw_event_type: Some("order".to_string()),
            raw_type: Some("UPDATE".to_string()),
            raw_status: Some("LIVE".to_string()),
            detail: Some("user_channel_fill".to_string()),
        })
    }

    #[test]
    fn user_channel_pretty_line_includes_source_cumulative_and_delta_fields() {
        let line = format_pretty_line(
            SinkKind::UserChannelDynamic,
            &sample_user_event("order", "partial_fill"),
            false,
        );

        assert!(line.contains("source=order"));
        assert!(line.contains("market=market-1"));
        assert!(line.contains("matched_amount_cumulative=3.000000"));
        assert!(line.contains("fill_delta=1.000000"));
        assert!(line.contains("official_order_type=UPDATE"));
    }

    #[test]
    fn local_cancel_pretty_line_marks_local_source() {
        let line = format_pretty_line(
            SinkKind::UserChannelDynamic,
            &sample_user_event("local", "cancel_acked"),
            false,
        );

        assert!(line.contains("source=local"));
        assert!(line.contains("status=cancel_acked"));
    }

    #[test]
    fn blocking_user_line_includes_reason_and_pending_orders() {
        let line = format_pretty_line(
            SinkKind::UserChannelDynamic,
            &DynamicEvent::UserChannel(UserChannelDynamicEvent {
                trace_id: 9,
                local_ts: Utc::now(),
                source: "reconcile".to_string(),
                market: Some("market-2".to_string()),
                summary: None,
                order_id: "N/A".to_string(),
                status: "required".to_string(),
                batch_trace_id: Some(42),
                blocking: Some(true),
                blocking_reason: Some("cancel_confirmation_timeout".to_string()),
                pending_orders: Some(1),
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
                raw_type: Some("RECONNECT_GAP".to_string()),
                raw_status: Some("BACKFILL_REQUIRED".to_string()),
                detail: Some("scope_blocked".to_string()),
            }),
            false,
        );

        assert!(line.contains("blocking=true"));
        assert!(line.contains("blocking_reason=cancel_confirmation_timeout"));
        assert!(line.contains("pending_orders=1"));
    }
}
