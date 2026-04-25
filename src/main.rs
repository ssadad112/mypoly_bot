#![allow(dead_code, unused_variables, unused_imports, unreachable_code)]

use alloy::primitives::B256;
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner as LocalSigner;
use chrono::SecondsFormat;
use chrono::Utc;
use log::info;
use polymarket_client_sdk::auth::Credentials;
use polymarket_client_sdk::clob::Client as ClobClient;
use polymarket_client_sdk::clob::Config;
use polymarket_client_sdk::clob::types::SignatureType;
use polymarket_client_sdk::clob::ws::Client as WebSocketClient;
use polymarket_client_sdk::ws::config::Config as WsConfig;
use serde_json::json;
use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

// 本地模块
mod engine;
mod telemetry;
use telemetry::start_telemetry_thread;

use engine::core::state::OutcomeResolver;
use engine::core::{MarketState, StrategyConfig, TradeAction};
use engine::data::discovery::find_eth_5m_ids;
use engine::data::price_feeder::PriceFeeder;
use engine::execution::executor::Executor;
use engine::execution::user_ws::{UserChannelMsg, run_user_events_loop};
use engine::execution::ws_handler::run_market_loop;
use engine::redeem::{RedeemConfig, redeem_log_path, start_redeem_supervisor};

fn env_flag_is_enabled(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn load_env_non_cwd() {
    if let Ok(explicit_path) = env::var("DOTENV_PATH") {
        let trimmed = explicit_path.trim();
        if !trimmed.is_empty() {
            if dotenvy::from_path_override(trimmed).is_ok() {
                return;
            }
        }
    }

    if let Ok(exe_path) = env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            if dotenvy::from_path(exe_dir.join(".env")).is_ok() {
                return;
            }
        }
    }

    let _ = dotenvy::from_path(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".env"));
}

fn resolve_log_dir() -> PathBuf {
    if let Ok(value) = env::var("LOG_DIR") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    if let Ok(exe_path) = env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            return exe_dir.to_path_buf();
        }
    }

    PathBuf::from(".")
}

#[tokio::main]
async fn main() {
    load_env_non_cwd();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            writeln!(
                buf,
                "\x1b[1;92m{} [{}] {}\x1b[0m",
                Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
                record.level(),
                record.args()
            )
        })
        .init();

    let telemetry = start_telemetry_thread();
    let runtime_log_path = resolve_log_dir().join("runtime_pane.log");
    let user_channel_log_path = resolve_log_dir().join("user_channel_pane.log");
    let user_channel_raw_log_path = resolve_log_dir().join("user_channel_raw.log");

    info!("🚀 启动 Polymarket 高频量化交易引擎...");
    info!("[runtime] dynamic log path: {}", runtime_log_path.display());
    info!(
        "[runtime] view command: tail -f {}",
        runtime_log_path.display()
    );
    info!(
        "[user-channel] dynamic log path: {}",
        user_channel_log_path.display()
    );
    info!(
        "[user-channel] view command: tail -f {}",
        user_channel_log_path.display()
    );
    info!(
        "[user-channel-raw] audit log path: {}",
        user_channel_raw_log_path.display()
    );
    telemetry.log(format!(
        "[runtime] dynamic log path: {}",
        runtime_log_path.display()
    ));
    telemetry.log(format!(
        "[runtime] view command: tail -f {}",
        runtime_log_path.display()
    ));
    telemetry.log(format!(
        "[user-channel] dynamic log path: {}",
        user_channel_log_path.display()
    ));
    telemetry.log(format!(
        "[user-channel] view command: tail -f {}",
        user_channel_log_path.display()
    ));
    telemetry.log(format!(
        "[user-channel-raw] audit log path: {}",
        user_channel_raw_log_path.display()
    ));

    let pk_hex = env::var("PRIVATE_KEY").expect("🔑 缺少 PRIVATE_KEY");
    let signer = LocalSigner::from_str(&pk_hex)
        .expect("私钥格式错误")
        .with_chain_id(Some(137));

    let proxy_hex = env::var("PROXY_ADDRESS").expect("🔑 缺少 PROXY_ADDRESS");
    let proxy_address = alloy::primitives::Address::from_str(&proxy_hex).expect("地址格式错误");

    let ws_client = WebSocketClient::new(
        "wss://ws-subscriptions-clob.polymarket.com",
        WsConfig::default(),
    )
    .expect("WS init failed");

    let config = Config::builder().use_server_time(true).build();

    let auth_client = ClobClient::new("https://clob.polymarket.com", config)
        .expect("REST init failed")
        .authentication_builder(&signer)
        .funder(proxy_address)
        .signature_type(SignatureType::GnosisSafe)
        .authenticate()
        .await
        .expect("REST 鉴权失败");

    let api_key = env::var("POLYMARKET_API_KEY").unwrap();
    let api_secret = env::var("POLYMARKET_API_SECRET").unwrap();
    let api_passphrase = env::var("POLYMARKET_API_PASSPHRASE").unwrap();

    let creds_json = json!({
        "key": api_key,
        "secret": api_secret,
        "passphrase": api_passphrase,
    });

    let static_credentials: Credentials = serde_json::from_value(creds_json).unwrap();

    let auth_ws_client = WebSocketClient::new(
        "wss://ws-subscriptions-clob.polymarket.com",
        WsConfig::default(),
    )
    .unwrap()
    .authenticate(static_credentials, proxy_address)
    .unwrap();

    let clob_client = Arc::new(auth_client);
    let auth_ws_client = Arc::new(auth_ws_client);

    info!("signer={:?}", signer.address());
    info!("funder={:?}", proxy_address);
    telemetry.log(format!("signer={:?}", signer.address()));
    telemetry.log(format!("funder={:?}", proxy_address));

    if let Ok(redeem_config) = RedeemConfig::from_env() {
        if redeem_config.enabled() {
            let redeem_log_path = redeem_log_path();
            info!("[redeem] enabled");
            info!("[redeem] dynamic log path: {}", redeem_log_path.display());
            telemetry.log("[redeem] enabled".to_string());
            telemetry.log(format!(
                "[redeem] dynamic log path: {}",
                redeem_log_path.display()
            ));
        } else {
            info!("[redeem] disabled");
            telemetry.log("[redeem] disabled".to_string());
        }
        start_redeem_supervisor(proxy_address, redeem_config);
    } else {
        info!("[redeem] config parse failed, redeem worker disabled");
        telemetry.log("[redeem] config parse failed, redeem worker disabled".to_string());
    }

    // 全局通道
    let (user_ws_tx, user_ws_rx) = mpsc::unbounded_channel::<UserChannelMsg>();
    let (action_tx, action_rx) = tokio::sync::watch::channel((TradeAction::DoNothing, 0u64));

    // ==========================================
    // Executor（全局唯一）
    // ==========================================
    let strategy_config = StrategyConfig::default();
    let executor = Arc::new(Executor::new(
        clob_client.clone(),
        signer.clone(),
        &strategy_config,
    ));
    let telemetry_for_exec = telemetry.clone();
    let executor_clone = executor.clone();
    tokio::spawn(async move {
        executor_clone
            .run(
                // 只用 clone，不要直接用 executor
                action_rx,
                user_ws_rx,
                telemetry_for_exec,
            )
            .await;
    });

    // Price feeder
    let (price_tx, price_rx) = watch::channel(0.0);
    let telemetry_for_price = telemetry.clone();
    tokio::spawn(async move {
        PriceFeeder::start_feeding(price_tx, telemetry_for_price).await;
    });

    let mut is_first_run = true;

    // ==========================================
    // 主循环
    // ==========================================
    loop {
        // 每一场新的 inventory 通道
        let (inventory_tx, mut inventory_rx) = mpsc::unbounded_channel();

        // 关键：把 tx 注入 executor（闭环成立）
        executor.set_inventory_tx(inventory_tx).await;

        let now_ts = Utc::now().timestamp() as u64;
        let current_subscribed_ts = (now_ts / 300) * 300;
        let next_ts = current_subscribed_ts + 300;

        if is_first_run {
            let sleep_secs = next_ts - now_ts;
            info!(
                "[startup-guard] current slot {} in progress, sleep {}s then start next slot",
                current_subscribed_ts, sleep_secs
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(sleep_secs)).await;
            is_first_run = false;
            continue;
        }

        let market_ids = loop {
            match find_eth_5m_ids(&*clob_client, current_subscribed_ts).await {
                Ok(Some(ids)) => break ids,
                _ => tokio::time::sleep(tokio::time::Duration::from_secs(2)).await,
            }
        };

        let up_token_id = market_ids.up_token_id.clone();
        let down_token_id = market_ids.down_token_id.clone();
        let strike_price = market_ids.official_strike;
        info!(
            "[MARKET] ts={} strike={:.2} up_token={} down_token={}",
            current_subscribed_ts, strike_price, up_token_id, down_token_id
        );
        telemetry.log(format!(
            "[MARKET] ts={} strike={:.2} up_token={} down_token={}",
            current_subscribed_ts, strike_price, up_token_id, down_token_id
        ));

        let outcome_resolver = OutcomeResolver::new(
            up_token_id.clone(),
            down_token_id.clone(),
            market_ids.up_raw_outcome.clone(),
            market_ids.down_raw_outcome.clone(),
        );
        let active_asset_ids = market_ids.active_asset_ids();

        let market_start_time =
            chrono::DateTime::<Utc>::from_timestamp(current_subscribed_ts as i64, 0)
                .unwrap_or_else(Utc::now);
        let state = MarketState::new_with_outcome_resolver(
            strike_price,
            market_start_time,
            outcome_resolver.clone(),
        );

        let (death_tx, death_rx) = mpsc::channel::<()>(1);
        let config_strategy = StrategyConfig::default();

        let mut cid_hex = market_ids.condition_id.clone();
        if !cid_hex.starts_with("0x") {
            cid_hex = format!("0x{}", cid_hex);
        }
        executor
            .set_active_market_context(cid_hex.clone(), outcome_resolver)
            .await;

        if let Ok(target_market_b256_id) = B256::from_str(&cid_hex) {
            let auth_ws_clone = auth_ws_client.clone();
            let telemetry_for_ws = telemetry.clone();
            let tx_clone = user_ws_tx.clone();

            tokio::spawn(async move {
                run_user_events_loop(
                    auth_ws_clone,
                    target_market_b256_id,
                    tx_clone,
                    telemetry_for_ws,
                )
                .await;

                let _ = death_tx.send(()).await;
            });
        }

        // inventory 闭环输入策略
        if let Err(e) = run_market_loop(
            &ws_client,
            state,
            active_asset_ids,
            current_subscribed_ts,
            price_rx.clone(),
            action_tx.clone(),
            config_strategy,
            telemetry.clone(),
            death_rx,
            &mut inventory_rx,
        )
        .await
        {
            telemetry.log(format!("[market] session error: {:?}", e));
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
    }
}
