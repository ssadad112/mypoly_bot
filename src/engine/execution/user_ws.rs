use alloy::primitives::B256;
use chrono::{DateTime, TimeZone, Utc};
use futures::StreamExt;
use polymarket_client_sdk::auth::Kind;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::ws::Client as WebSocketClient;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::engine::telemetry::events::RawUserChannelEvent;
use crate::telemetry::TelemetryTx;

#[derive(Debug)]
pub enum UserChannelMsg {
    TradeObserved {
        event_type: String,
        market: String,
        status: String,
        asset_id: String,
        outcome: Option<String>,
        side: String,
        order_id: String,
    },
    OrderFilled {
        order_id: String,
        market: String,
        fill_id: Option<String>,
        size_matched: f64,
        original_size: Option<f64>,
        side: Option<String>,
        asset_id: Option<String>,
        outcome: Option<String>,
        is_full: bool,
        fill_price: Option<f64>,
        fill_timestamp: DateTime<Utc>,
        raw_event_type: Option<String>,
        raw_type: Option<String>,
        raw_status: Option<String>,
    },
    ObservedEvent {
        order_id: String,
        market: String,
        status: String,
        fill_id: Option<String>,
        size_matched: Option<f64>,
        original_size: Option<f64>,
        side: Option<String>,
        asset_id: Option<String>,
        outcome: Option<String>,
        fill_price: Option<f64>,
        fill_timestamp: DateTime<Utc>,
        raw_event_type: Option<String>,
        raw_type: Option<String>,
        raw_status: Option<String>,
        detail: String,
    },
    OrderTerminated {
        order_id: String,
        market: String,
        reason: String,
        raw_event_type: Option<String>,
        raw_type: Option<String>,
        raw_status: Option<String>,
    },
    ReconnectGap {
        market: String,
        reason: String,
    },
}

pub async fn run_user_events_loop<K: Kind + Send + Sync + 'static>(
    ws_client: Arc<WebSocketClient<Authenticated<K>>>,
    market_id: B256,
    tx: mpsc::UnboundedSender<UserChannelMsg>,
    telemetry: TelemetryTx,
) {
    let mut retry_count = 0;
    let max_retries = 10;

    loop {
        telemetry.log(format!(
            "[user-channel] attempting subscription, retry={} ",
            retry_count + 1
        ));

        let market = market_id.to_string();
        match (
            ws_client.subscribe_orders(vec![market.clone()]),
            ws_client.subscribe_trades(vec![market]),
        ) {
            (Ok(order_stream), Ok(trade_stream)) => {
                telemetry.log("[user-channel] orders+trades subscription active".to_string());
                telemetry.emit_raw_user_channel_event(RawUserChannelEvent {
                    local_ts: Utc::now(),
                    stream: "lifecycle".to_string(),
                    market: Some(market_id.to_string()),
                    order_id: None,
                    asset_id: None,
                    outcome: None,
                    side: None,
                    price: None,
                    size_matched: None,
                    original_size: None,
                    raw_event_type: None,
                    raw_type: Some("SUBSCRIBED".to_string()),
                    raw_status: Some("ACTIVE".to_string()),
                    detail: Some("user_channel_subscription_active".to_string()),
                    payload: serde_json::Value::Null,
                });
                let startup_or_reconnect_reason = if retry_count == 0 {
                    "user_channel_started_without_historical_backfill"
                } else {
                    "user_channel_resubscribed_backfill_required"
                };
                let _ = tx.send(UserChannelMsg::ReconnectGap {
                    market: market_id.to_string(),
                    reason: startup_or_reconnect_reason.to_string(),
                });
                retry_count = 0;
                let mut order_stream = Box::pin(order_stream);
                let mut trade_stream = Box::pin(trade_stream);
                // TradeObserved is telemetry-only, so lightweight pre-executor dedup is acceptable.
                // Order updates must always reach executor; final duplicate accounting is decided
                // later by OrderManager::on_fill() from size_matched deltas.
                let mut recent_trade_keys = RecentFillKeys::default();
                let mut order_open = true;
                let mut trade_open = true;

                while order_open || trade_open {
                    tokio::select! {
                        order_item = order_stream.next(), if order_open => {
                            match order_item {
                                Some(Ok(order_msg)) => {
                                    let val = serde_json::to_value(&order_msg)
                                        .unwrap_or(serde_json::Value::Null);
                                    telemetry.emit_raw_user_channel_event(
                                        build_raw_user_channel_event(
                                            "order",
                                            Some(market_id.to_string()),
                                            Some("user_channel_order_raw"),
                                            &val,
                                        ),
                                    );
                                    if let Some(msg) = classify_user_channel_msg(
                                        UserStreamKind::Orders,
                                        Some(order_msg.id.as_str()),
                                        &val,
                                    ) {
                                        let _ = tx.send(msg);
                                    }
                                }
                                Some(Err(e)) => {
                                    telemetry.log(format!("[user-channel] order message parse error: {:?}", e));
                                }
                                None => {
                                    order_open = false;
                                }
                            }
                        }
                        trade_item = trade_stream.next(), if trade_open => {
                            match trade_item {
                                Some(Ok(trade_msg)) => {
                                    let val = serde_json::to_value(&trade_msg)
                                        .unwrap_or(serde_json::Value::Null);
                                    telemetry.emit_raw_user_channel_event(
                                        build_raw_user_channel_event(
                                            "trade",
                                            Some(market_id.to_string()),
                                            Some("user_channel_trade_raw"),
                                            &val,
                                        ),
                                    );
                                    if let Some(msg) = classify_user_channel_msg(
                                        UserStreamKind::Trades,
                                        None,
                                        &val,
                                    ) {
                                        if !recent_trade_keys.should_skip_trade(&msg) {
                                            let _ = tx.send(msg);
                                        }
                                    }
                                }
                                Some(Err(e)) => {
                                    telemetry.log(format!("[user-channel] trade message parse error: {:?}", e));
                                }
                                None => {
                                    trade_open = false;
                                }
                            }
                        }
                    }
                }

                telemetry.log("[user-channel] websocket disconnected, retrying".to_string());
                telemetry.emit_raw_user_channel_event(RawUserChannelEvent {
                    local_ts: Utc::now(),
                    stream: "lifecycle".to_string(),
                    market: Some(market_id.to_string()),
                    order_id: None,
                    asset_id: None,
                    outcome: None,
                    side: None,
                    price: None,
                    size_matched: None,
                    original_size: None,
                    raw_event_type: None,
                    raw_type: Some("DISCONNECTED".to_string()),
                    raw_status: Some("RECONNECT_REQUIRED".to_string()),
                    detail: Some("user_channel_disconnect_gap".to_string()),
                    payload: serde_json::Value::Null,
                });
                let _ = tx.send(UserChannelMsg::ReconnectGap {
                    market: market_id.to_string(),
                    reason: "user_channel_disconnected_backfill_required".to_string(),
                });
            }
            (Err(e), _) => {
                telemetry.log(format!(
                    "[user-channel] orders subscription failed: {:?}",
                    e
                ));
                telemetry.emit_raw_user_channel_event(RawUserChannelEvent {
                    local_ts: Utc::now(),
                    stream: "lifecycle".to_string(),
                    market: Some(market_id.to_string()),
                    order_id: None,
                    asset_id: None,
                    outcome: None,
                    side: None,
                    price: None,
                    size_matched: None,
                    original_size: None,
                    raw_event_type: None,
                    raw_type: Some("SUBSCRIBE_ORDERS_FAILED".to_string()),
                    raw_status: Some("RETRYING".to_string()),
                    detail: Some(format!("orders_subscription_failed:{e:?}")),
                    payload: serde_json::Value::Null,
                });
            }
            (_, Err(e)) => {
                telemetry.log(format!(
                    "[user-channel] trades subscription failed: {:?}",
                    e
                ));
                telemetry.emit_raw_user_channel_event(RawUserChannelEvent {
                    local_ts: Utc::now(),
                    stream: "lifecycle".to_string(),
                    market: Some(market_id.to_string()),
                    order_id: None,
                    asset_id: None,
                    outcome: None,
                    side: None,
                    price: None,
                    size_matched: None,
                    original_size: None,
                    raw_event_type: None,
                    raw_type: Some("SUBSCRIBE_TRADES_FAILED".to_string()),
                    raw_status: Some("RETRYING".to_string()),
                    detail: Some(format!("trades_subscription_failed:{e:?}")),
                    payload: serde_json::Value::Null,
                });
            }
        }

        retry_count += 1;
        if retry_count > max_retries {
            telemetry.log("[user-channel] too many retries, giving up".to_string());
            break;
        }

        let sleep_sec = std::cmp::min(2_u64.pow(retry_count), 30);
        tokio::time::sleep(Duration::from_secs(sleep_sec)).await;
    }
}

fn build_raw_user_channel_event(
    stream: &str,
    fallback_market: Option<String>,
    detail: Option<&str>,
    payload: &serde_json::Value,
) -> RawUserChannelEvent {
    RawUserChannelEvent {
        local_ts: Utc::now(),
        stream: stream.to_string(),
        market: parse_string_field(payload, "market").or(fallback_market),
        order_id: parse_string_field(payload, "id")
            .or_else(|| parse_string_field(payload, "order_id")),
        asset_id: parse_string_field(payload, "asset_id")
            .or_else(|| first_maker_string_field(payload, "asset_id")),
        outcome: parse_string_field(payload, "outcome")
            .or_else(|| first_maker_string_field(payload, "outcome")),
        side: parse_string_field(payload, "side"),
        price: parse_f64_field(payload, "price"),
        size_matched: parse_f64_field(payload, "size_matched"),
        original_size: parse_f64_field(payload, "original_size"),
        raw_event_type: parse_string_field(payload, "event_type"),
        raw_type: parse_string_field(payload, "type"),
        raw_status: parse_string_field(payload, "status"),
        detail: detail.map(str::to_string),
        payload: payload.clone(),
    }
}

#[derive(Clone, Copy)]
enum UserStreamKind {
    Orders,
    Trades,
}

#[derive(Default)]
struct RecentFillKeys {
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl RecentFillKeys {
    const MAX_KEYS: usize = 2048;

    fn should_skip_trade(&mut self, msg: &UserChannelMsg) -> bool {
        let Some(key) = dedup_key(msg) else {
            return false;
        };

        if self.seen.contains(&key) {
            return true;
        }

        self.seen.insert(key.clone());
        self.order.push_back(key);

        if self.order.len() > Self::MAX_KEYS {
            if let Some(oldest) = self.order.pop_front() {
                self.seen.remove(&oldest);
            }
        }

        false
    }
}

fn parse_f64_field(value: &serde_json::Value, key: &str) -> Option<f64> {
    if let Some(raw) = value[key].as_f64() {
        return Some(raw);
    }
    if let Some(raw) = value[key].as_i64() {
        return Some(raw as f64);
    }
    value[key].as_str().and_then(|s| s.parse::<f64>().ok())
}

fn parse_string_field(value: &serde_json::Value, key: &str) -> Option<String> {
    value[key].as_str().map(str::to_string)
}

fn first_maker_string_field(value: &serde_json::Value, key: &str) -> Option<String> {
    value["maker_orders"]
        .as_array()?
        .iter()
        .find_map(|entry| entry[key].as_str().map(str::to_string))
}

fn classify_user_channel_msg(
    stream_kind: UserStreamKind,
    fallback_order_id: Option<&str>,
    val: &serde_json::Value,
) -> Option<UserChannelMsg> {
    match stream_kind {
        UserStreamKind::Trades => classify_trade_msg(val),
        UserStreamKind::Orders => classify_order_msg(val, fallback_order_id),
    }
}

fn classify_trade_msg(val: &serde_json::Value) -> Option<UserChannelMsg> {
    let event_type = parse_string_field(val, "event_type").unwrap_or_else(|| "trade".to_string());
    let market = parse_string_field(val, "market")?;
    let status = parse_string_field(val, "status")
        .or_else(|| parse_string_field(val, "type"))
        .unwrap_or_else(|| "unknown".to_string());
    let side = parse_string_field(val, "side")?;

    let maker_order = val["maker_orders"].as_array()?.iter().find(|entry| {
        entry["order_id"].as_str().is_some() && entry["asset_id"].as_str().is_some()
    })?;

    Some(UserChannelMsg::TradeObserved {
        event_type,
        market,
        status,
        asset_id: maker_order["asset_id"].as_str()?.to_string(),
        outcome: parse_string_field(maker_order, "outcome"),
        side,
        order_id: maker_order["order_id"].as_str()?.to_string(),
    })
}

fn classify_order_msg(
    val: &serde_json::Value,
    fallback_order_id: Option<&str>,
) -> Option<UserChannelMsg> {
    let market = parse_string_field(val, "market")?;
    let order_id = parse_string_field(val, "id")
        .or_else(|| fallback_order_id.map(str::to_string))
        .unwrap_or_default();
    if order_id.is_empty() {
        return None;
    }

    let raw_event_type =
        parse_string_field(val, "event_type").or_else(|| Some("order".to_string()));
    let raw_type = parse_string_field(val, "type");
    let raw_status = parse_string_field(val, "status");
    let event_type_upper = uppercase_opt(raw_event_type.as_deref());
    let type_upper = uppercase_opt(raw_type.as_deref());
    let status_upper = uppercase_opt(raw_status.as_deref());

    let size_matched = parse_f64_field(val, "size_matched");
    let matched_size = size_matched.unwrap_or(0.0);
    let original_size = parse_f64_field(val, "original_size");
    let fill_price = parse_f64_field(val, "price");
    let side = parse_string_field(val, "side");
    let asset_id = parse_string_field(val, "asset_id");
    let outcome = parse_string_field(val, "outcome");
    let fill_timestamp = parse_timestamp_field(val, "timestamp").unwrap_or_else(Utc::now);

    if type_upper == "CANCELLATION" || matches!(status_upper.as_str(), "CANCELED" | "CANCELLED") {
        return Some(UserChannelMsg::OrderTerminated {
            order_id,
            market,
            reason: "user cancelled order".to_string(),
            raw_event_type,
            raw_type,
            raw_status,
        });
    }

    if matches!(status_upper.as_str(), "REJECTED" | "EXPIRED" | "FAILED") {
        return Some(UserChannelMsg::OrderTerminated {
            order_id,
            market,
            reason: format!("system terminated: {}", status_upper),
            raw_event_type,
            raw_type,
            raw_status,
        });
    }

    if matched_size > 0.0 {
        let is_full = match original_size {
            Some(size) if size > 0.0 => matched_size >= size,
            _ => matches!(status_upper.as_str(), "FILLED" | "CONFIRMED"),
        };
        return Some(UserChannelMsg::OrderFilled {
            order_id,
            market,
            fill_id: None,
            size_matched: matched_size,
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
        });
    }

    if type_upper == "PLACEMENT" {
        return Some(UserChannelMsg::ObservedEvent {
            order_id,
            market,
            status: "placement".to_string(),
            fill_id: None,
            size_matched,
            original_size,
            side,
            asset_id,
            outcome,
            fill_price,
            fill_timestamp,
            raw_event_type,
            raw_type,
            raw_status,
            detail: "user_channel_order_placement".to_string(),
        });
    }

    if type_upper == "UPDATE" {
        return Some(UserChannelMsg::ObservedEvent {
            order_id,
            market,
            status: "update".to_string(),
            fill_id: None,
            size_matched,
            original_size,
            side,
            asset_id,
            outcome,
            fill_price,
            fill_timestamp,
            raw_event_type,
            raw_type,
            raw_status,
            detail: "user_channel_order_update".to_string(),
        });
    }

    if !event_type_upper.is_empty() || raw_type.is_some() || raw_status.is_some() {
        return Some(UserChannelMsg::ObservedEvent {
            order_id,
            market,
            status: normalize_observed_status(raw_type.as_deref(), raw_status.as_deref()),
            fill_id: None,
            size_matched,
            original_size,
            side,
            asset_id,
            outcome,
            fill_price,
            fill_timestamp,
            raw_event_type,
            raw_type,
            raw_status,
            detail: "user_channel_order_observed".to_string(),
        });
    }

    None
}

fn normalize_observed_status(raw_type: Option<&str>, raw_status: Option<&str>) -> String {
    let type_upper = uppercase_opt(raw_type);
    let status_upper = uppercase_opt(raw_status);

    match type_upper.as_str() {
        "PLACEMENT" => "placement".to_string(),
        "UPDATE" => "update".to_string(),
        "CANCELLATION" => "cancellation".to_string(),
        _ => match status_upper.as_str() {
            "FILLED" | "CONFIRMED" => "full_fill".to_string(),
            "PARTIAL_FILL" | "PARTIALLY_FILLED" => "partial_fill".to_string(),
            "MATCHED" | "MINED" | "EXECUTED" => "matched".to_string(),
            "CANCELED" | "CANCELLED" => "cancellation".to_string(),
            "PLACED" => "placement".to_string(),
            "LIVE" | "OPEN" => "update".to_string(),
            _ => "unknown".to_string(),
        },
    }
}

fn uppercase_opt(value: Option<&str>) -> String {
    value.unwrap_or("").trim().to_ascii_uppercase()
}

fn dedup_key(msg: &UserChannelMsg) -> Option<String> {
    match msg {
        UserChannelMsg::TradeObserved {
            event_type,
            market,
            status,
            asset_id,
            outcome,
            side,
            order_id,
        } => Some(format!(
            "trade:{}|{}|{}|{}|{}|{}|{}",
            event_type,
            market,
            status,
            outcome.as_deref().unwrap_or("missing"),
            side,
            order_id,
            asset_id,
        )),
        UserChannelMsg::OrderFilled { .. }
        | UserChannelMsg::ObservedEvent { .. }
        | UserChannelMsg::OrderTerminated { .. }
        | UserChannelMsg::ReconnectGap { .. } => None,
    }
}

fn parse_timestamp_field(value: &serde_json::Value, key: &str) -> Option<DateTime<Utc>> {
    if let Some(raw) = value[key].as_i64() {
        if raw > 1_000_000_000_000 {
            return Utc.timestamp_millis_opt(raw).single();
        }
        return Utc.timestamp_opt(raw, 0).single();
    }
    if let Some(raw) = value[key].as_str() {
        if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
            return Some(parsed.with_timezone(&Utc));
        }
        if let Ok(raw_num) = raw.parse::<i64>() {
            if raw_num > 1_000_000_000_000 {
                return Utc.timestamp_millis_opt(raw_num).single();
            }
            return Utc.timestamp_opt(raw_num, 0).single();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trade_stream_only_extracts_whitelisted_fields() {
        let payload = json!({
            "event_type": "trade",
            "market": "market-1",
            "type": "TRADE",
            "status": "MATCHED",
            "side": "BUY",
            "maker_orders": [
                {
                    "order_id": "order-1",
                    "asset_id": "asset-up",
                    "owner": "owner-1",
                    "outcome": "YES"
                }
            ],
            "timestamp": "2026-04-07T01:02:03Z",
            "id": "trade-123"
        });

        let msg = classify_user_channel_msg(UserStreamKind::Trades, None, &payload).unwrap();
        match msg {
            UserChannelMsg::TradeObserved {
                event_type,
                market,
                status,
                outcome,
                side,
                order_id,
                asset_id,
            } => {
                assert_eq!(event_type, "trade");
                assert_eq!(market, "market-1");
                assert_eq!(status, "MATCHED");
                assert_eq!(outcome.as_deref(), Some("YES"));
                assert_eq!(side, "BUY");
                assert_eq!(order_id, "order-1");
                assert_eq!(asset_id, "asset-up");
            }
            _ => panic!("expected TradeObserved"),
        }
    }

    #[test]
    fn official_order_placement_without_status_stays_a_placement() {
        let payload = json!({
            "asset_id": "asset-up",
            "associate_trades": null,
            "id": "order-placement",
            "market": "market-1",
            "original_size": "10",
            "outcome": "YES",
            "price": "0.57",
            "side": "SELL",
            "size_matched": "0",
            "timestamp": "1672290687",
            "type": "PLACEMENT"
        });

        let msg =
            classify_user_channel_msg(UserStreamKind::Orders, Some("fallback"), &payload).unwrap();
        match msg {
            UserChannelMsg::ObservedEvent {
                order_id,
                status,
                size_matched,
                raw_event_type,
                raw_type,
                raw_status,
                ..
            } => {
                assert_eq!(order_id, "order-placement");
                assert_eq!(status, "placement");
                assert_eq!(size_matched, Some(0.0));
                assert_eq!(raw_event_type.as_deref(), Some("order"));
                assert_eq!(raw_type.as_deref(), Some("PLACEMENT"));
                assert_eq!(raw_status, None);
            }
            _ => panic!("expected ObservedEvent"),
        }
    }

    #[test]
    fn order_update_without_matched_size_stays_an_update() {
        let payload = json!({
            "event_type": "order",
            "type": "UPDATE",
            "id": "order-update",
            "market": "market-2",
            "asset_id": "asset-down",
            "outcome": "NO",
            "side": "BUY",
            "price": "0.47",
            "original_size": "10",
            "size_matched": "0",
            "timestamp": "1672290687"
        });

        let msg =
            classify_user_channel_msg(UserStreamKind::Orders, Some("fallback"), &payload).unwrap();
        match msg {
            UserChannelMsg::ObservedEvent {
                status,
                size_matched,
                detail,
                ..
            } => {
                assert_eq!(status, "update");
                assert_eq!(size_matched, Some(0.0));
                assert_eq!(detail, "user_channel_order_update");
            }
            _ => panic!("expected ObservedEvent"),
        }
    }

    #[test]
    fn order_update_with_matched_size_is_treated_as_fill() {
        let payload = json!({
            "event_type": "order",
            "type": "UPDATE",
            "status": "LIVE",
            "id": "order-2",
            "market": "market-2",
            "associate_trades": null,
            "original_size": "10.0",
            "size_matched": "3.0",
            "side": "SELL",
            "asset_id": "asset-down",
            "outcome": "NO",
            "price": "0.52",
            "timestamp": 1_775_210_000_i64
        });

        let msg =
            classify_user_channel_msg(UserStreamKind::Orders, Some("fallback"), &payload).unwrap();
        match msg {
            UserChannelMsg::OrderFilled {
                order_id,
                market,
                size_matched,
                side,
                asset_id,
                outcome,
                raw_event_type,
                raw_type,
                raw_status,
                ..
            } => {
                assert_eq!(order_id, "order-2");
                assert_eq!(market, "market-2");
                assert_eq!(size_matched, 3.0);
                assert_eq!(side.as_deref(), Some("SELL"));
                assert_eq!(asset_id.as_deref(), Some("asset-down"));
                assert_eq!(outcome.as_deref(), Some("NO"));
                assert_eq!(raw_event_type.as_deref(), Some("order"));
                assert_eq!(raw_type.as_deref(), Some("UPDATE"));
                assert_eq!(raw_status.as_deref(), Some("LIVE"));
            }
            _ => panic!("expected OrderFilled"),
        }
    }

    #[test]
    fn order_cancellation_is_recognized_without_event_type() {
        let payload = json!({
            "id": "order-cancel",
            "market": "market-3",
            "type": "CANCELLATION",
            "timestamp": "1672290687"
        });

        let msg = classify_user_channel_msg(UserStreamKind::Orders, Some("order-cancel"), &payload)
            .unwrap();
        match msg {
            UserChannelMsg::OrderTerminated {
                market,
                reason,
                raw_event_type,
                raw_type,
                ..
            } => {
                assert_eq!(market, "market-3");
                assert_eq!(reason, "user cancelled order");
                assert_eq!(raw_event_type.as_deref(), Some("order"));
                assert_eq!(raw_type.as_deref(), Some("CANCELLATION"));
            }
            _ => panic!("expected OrderTerminated"),
        }
    }

    #[test]
    fn cancelled_status_without_cancellation_type_is_still_terminated() {
        let payload = json!({
            "event_type": "order",
            "id": "order-cancelled",
            "market": "market-3",
            "status": "CANCELLED",
            "timestamp": "1672290687"
        });

        let msg =
            classify_user_channel_msg(UserStreamKind::Orders, Some("order-cancelled"), &payload)
                .unwrap();
        match msg {
            UserChannelMsg::OrderTerminated {
                market,
                reason,
                raw_event_type,
                raw_type,
                raw_status,
                ..
            } => {
                assert_eq!(market, "market-3");
                assert_eq!(reason, "user cancelled order");
                assert_eq!(raw_event_type.as_deref(), Some("order"));
                assert_eq!(raw_type, None);
                assert_eq!(raw_status.as_deref(), Some("CANCELLED"));
            }
            _ => panic!("expected OrderTerminated"),
        }
    }

    #[test]
    fn placement_with_price_is_not_misclassified_as_fill() {
        let payload = json!({
            "event_type": "order",
            "type": "PLACEMENT",
            "id": "order-4",
            "market": "market-4",
            "asset_id": "asset-up",
            "outcome": "YES",
            "side": "BUY",
            "price": "0.47",
            "original_size": "10",
            "size_matched": "0",
            "timestamp": "1672290687"
        });

        let msg = classify_user_channel_msg(UserStreamKind::Orders, None, &payload).unwrap();
        assert!(matches!(msg, UserChannelMsg::ObservedEvent { .. }));
    }

    #[test]
    fn trade_stream_requires_whitelisted_fields() {
        let payload = json!({
            "event_type": "trade",
            "market": "market-2",
            "type": "TRADE",
            "status": "MATCHED",
            "side": "BUY",
            "maker_orders": [
                {
                    "asset_id": "asset-up"
                }
            ]
        });

        assert!(classify_user_channel_msg(UserStreamKind::Trades, None, &payload).is_none());
    }

    #[test]
    fn trade_stream_ignores_top_level_fill_like_fields() {
        let payload = json!({
            "event_type": "trade",
            "market": "market-7",
            "type": "TRADE",
            "status": "MATCHED",
            "side": "BUY",
            "maker_orders": [
                {
                    "order_id": "order-7",
                    "asset_id": "maker-asset",
                    "matched_amount": "7.0",
                    "price": "0.44"
                }
            ]
        });

        let msg = classify_user_channel_msg(UserStreamKind::Trades, None, &payload).unwrap();
        match msg {
            UserChannelMsg::TradeObserved {
                order_id, asset_id, ..
            } => {
                assert_eq!(order_id, "order-7");
                assert_eq!(asset_id, "maker-asset");
            }
            _ => panic!("expected TradeObserved"),
        }
    }

    #[test]
    fn dedup_only_applies_to_trade_observed_messages() {
        let msg = UserChannelMsg::TradeObserved {
            event_type: "trade".to_string(),
            market: "market-1".to_string(),
            status: "MATCHED".to_string(),
            asset_id: "asset-up".to_string(),
            outcome: Some("YES".to_string()),
            side: "BUY".to_string(),
            order_id: "order-1".to_string(),
        };

        let mut recent = RecentFillKeys::default();
        assert!(!recent.should_skip_trade(&msg));
        assert!(recent.should_skip_trade(&msg));
    }

    #[test]
    fn order_messages_have_no_pre_executor_dedup_key() {
        let msg = UserChannelMsg::OrderFilled {
            order_id: "order-1".to_string(),
            market: "market-1".to_string(),
            fill_id: None,
            size_matched: 5.0,
            original_size: Some(10.0),
            side: Some("BUY".to_string()),
            asset_id: Some("asset-up".to_string()),
            outcome: Some("YES".to_string()),
            is_full: false,
            fill_price: Some(0.41),
            fill_timestamp: Utc.with_ymd_and_hms(2026, 4, 8, 0, 0, 0).unwrap(),
            raw_event_type: Some("order".to_string()),
            raw_type: Some("UPDATE".to_string()),
            raw_status: Some("LIVE".to_string()),
        };

        assert!(dedup_key(&msg).is_none());
    }
}
