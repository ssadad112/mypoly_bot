use log::warn;
use polymarket_client_sdk::auth::state::State;
use polymarket_client_sdk::clob::Client;
use serde_json::Value;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct MarketIds {
    pub up_token_id: String,
    pub down_token_id: String,
    pub up_raw_outcome: String,
    pub down_raw_outcome: String,
    pub condition_id: String,
    pub official_strike: f64,
}

impl MarketIds {
    pub fn active_asset_ids(&self) -> Vec<String> {
        vec![self.up_token_id.clone(), self.down_token_id.clone()]
    }
}

pub async fn find_eth_5m_ids<S: State>(
    _client: &Client<S>,
    timestamp: u64,
) -> Result<Option<MarketIds>> {
    let target_slug = format!("eth-updown-5m-{}", timestamp);
    let url = format!(
        "https://gamma-api.polymarket.com/markets/slug/{}",
        target_slug
    );

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "Accept",
        reqwest::header::HeaderValue::from_static("application/json"),
    );

    let http = reqwest::Client::builder()
        .user_agent("Mozilla/5.0")
        .default_headers(headers)
        .build()?;

    let resp = http.get(&url).send().await?;
    if !resp.status().is_success() {
        return Ok(None);
    }

    let json: Value = resp.json().await?;
    let condition_id = json["conditionId"].as_str().map(|s| s.to_string());
    let asset_ids = parse_string_array_field(&json["clobTokenIds"]);

    if asset_ids.len() < 2 {
        warn!("[discovery] market {} missing two token ids", target_slug);
        return Ok(None);
    }

    let raw_outcomes = parse_string_array_field(&json["outcomes"]);
    let up_raw_outcome = raw_outcomes
        .first()
        .cloned()
        .unwrap_or_else(|| "YES".to_string());
    let down_raw_outcome = raw_outcomes
        .get(1)
        .cloned()
        .unwrap_or_else(|| "NO".to_string());

    let Some(cid) = condition_id else {
        warn!("[discovery] market {} missing condition id", target_slug);
        return Ok(None);
    };

    Ok(Some(MarketIds {
        // Polymarket documents clobTokenIds in [Yes token ID, No token ID] order.
        // For the ETH up/down market family, YES is the UP leg and NO is the DOWN leg.
        up_token_id: asset_ids[0].clone(),
        down_token_id: asset_ids[1].clone(),
        up_raw_outcome,
        down_raw_outcome,
        condition_id: cid,
        official_strike: 0.0,
    }))
}

fn parse_string_array_field(value: &Value) -> Vec<String> {
    match value {
        Value::Array(items) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_string))
            .collect(),
        Value::String(raw) => match serde_json::from_str::<Value>(raw) {
            Ok(Value::Array(items)) => items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}
