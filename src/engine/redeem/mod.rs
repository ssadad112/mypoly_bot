use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, B256};
use chrono::{SecondsFormat, Utc};
use ethers::signers::LocalWallet;
use polymarket_client_sdk::data::Client as DataClient;
use polymarket_client_sdk::data::types::request::PositionsRequest;
use polymarket_relayer::{
    AuthMethod, DirectExecutor, RelayClient, RelayerTxType, Transaction, operations,
};
use rust_decimal::Decimal;
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore};

const DEFAULT_RPC_URL: &str = "https://polygon-rpc.com";

#[derive(Clone)]
pub struct RedeemConfig {
    enabled: bool,
    scan_interval: Duration,
    max_concurrency: usize,
    batch_size: usize,
    retry_base: Duration,
    retry_max: Duration,
    chain_id: u64,
    builder_key: String,
    builder_secret: String,
    builder_passphrase: String,
    relayer_tx_type: String,
}

impl RedeemConfig {
    pub fn from_env() -> Result<Self, String> {
        let enabled = env_bool("REDEEM_ENABLED").unwrap_or(false);
        let scan_interval =
            Duration::from_millis(env_u64("REDEEM_SCAN_INTERVAL_MS").unwrap_or(5_000));
        let max_concurrency = env_usize("REDEEM_MAX_CONCURRENCY").unwrap_or(2).max(1);
        let batch_size = env_usize("REDEEM_BATCH_SIZE").unwrap_or(20).max(1);
        let retry_base = Duration::from_millis(env_u64("REDEEM_RETRY_BASE_MS").unwrap_or(1_000));
        let retry_max = Duration::from_millis(env_u64("REDEEM_RETRY_MAX_MS").unwrap_or(60_000));
        let chain_id = env_u64("REDEEM_CHAIN_ID").unwrap_or(137);

        let builder_key = env_string("BUILDER_KEY")
            .or_else(|| env_string("POLY_BUILDER_API_KEY"))
            .unwrap_or_default();
        let builder_secret = env_string("BUILDER_SECRET")
            .or_else(|| env_string("POLY_BUILDER_SECRET"))
            .unwrap_or_default();
        let builder_passphrase = env_string("BUILDER_PASSPHRASE")
            .or_else(|| env_string("POLY_BUILDER_PASSPHRASE"))
            .unwrap_or_default();

        let relayer_tx_type =
            env_string("REDEEM_RELAYER_TX_TYPE").unwrap_or_else(|| "SAFE".to_string());

        Ok(Self {
            enabled,
            scan_interval,
            max_concurrency,
            batch_size,
            retry_base,
            retry_max,
            chain_id,
            builder_key,
            builder_secret,
            builder_passphrase,
            relayer_tx_type,
        })
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Debug, Clone)]
enum JobStatus {
    InProgress,
    Confirmed,
    Failed {
        next_retry_at: Instant,
        attempts: u32,
    },
}

pub fn start_redeem_supervisor(wallet: Address, config: RedeemConfig) {
    if !config.enabled() {
        return;
    }

    tokio::spawn(async move {
        if let Err(err) = run_supervisor(wallet, config).await {
            append_redeem_log(&format!("[redeem] supervisor exited: {err}"));
        }
    });
}

async fn run_supervisor(wallet: Address, config: RedeemConfig) -> Result<(), String> {
    if config.builder_key.is_empty()
        || config.builder_secret.is_empty()
        || config.builder_passphrase.is_empty()
    {
        return Err("BUILDER_KEY/BUILDER_SECRET/BUILDER_PASSPHRASE missing".to_string());
    }

    let data_client = DataClient::default();
    let relayer = Arc::new(RelayerClient::new(&config, wallet).await?);
    let jobs: Arc<Mutex<HashMap<String, JobStatus>>> = Arc::new(Mutex::new(HashMap::new()));
    let semaphore = Arc::new(Semaphore::new(config.max_concurrency));

    append_redeem_log(&format!(
        "[redeem] started: interval_ms={} concurrency={} batch_size={}",
        config.scan_interval.as_millis(),
        config.max_concurrency,
        config.batch_size
    ));

    loop {
        let req = PositionsRequest::builder()
            .user(wallet)
            .redeemable(true)
            .build();

        match data_client.positions(&req).await {
            Ok(positions) => {
                let mut launched = 0usize;
                for position in positions {
                    if launched >= config.batch_size {
                        break;
                    }
                    if !position.redeemable
                        || position.size.is_zero()
                        || position.current_value <= Decimal::ZERO
                    {
                        continue;
                    }

                    let Some(condition_id) = parse_b256(&position.condition_id) else {
                        append_redeem_log(&format!(
                            "[redeem] skip invalid condition_id={}",
                            position.condition_id
                        ));
                        continue;
                    };

                    let key = format!("{wallet:?}:{}", position.condition_id);
                    {
                        let mut guard = jobs.lock().await;
                        let should_skip = match guard.get(&key) {
                            Some(JobStatus::InProgress) => true,
                            Some(JobStatus::Confirmed) => true,
                            Some(JobStatus::Failed { next_retry_at, .. }) => {
                                Instant::now() < *next_retry_at
                            }
                            None => false,
                        };
                        if should_skip {
                            continue;
                        }
                        guard.insert(key.clone(), JobStatus::InProgress);
                    }

                    let permit = match semaphore.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => break,
                    };

                    launched += 1;
                    let jobs_clone = jobs.clone();
                    let relayer_clone = relayer.clone();
                    let config_clone = config.clone();

                    tokio::spawn(async move {
                        let _permit = permit;
                        let result = relayer_clone.redeem(condition_id).await;

                        let mut guard = jobs_clone.lock().await;
                        match result {
                            Ok(tx_hash) => {
                                append_redeem_log(&format!(
                                    "[redeem] confirmed condition_id={} index_sets=[1,2] tx_hash={}",
                                    condition_id, tx_hash
                                ));
                                guard.insert(key, JobStatus::Confirmed);
                            }
                            Err(err) => {
                                let attempts = match guard.remove(&key) {
                                    Some(JobStatus::Failed { attempts, .. }) => attempts + 1,
                                    _ => 1,
                                };
                                let backoff = exponential_backoff(
                                    config_clone.retry_base,
                                    config_clone.retry_max,
                                    attempts,
                                );
                                guard.insert(
                                    key,
                                    JobStatus::Failed {
                                        next_retry_at: Instant::now() + backoff,
                                        attempts,
                                    },
                                );
                                append_redeem_log(&format!("[redeem] job failed: {err}"));
                            }
                        }
                    });
                }
            }
            Err(err) => {
                append_redeem_log(&format!("[redeem] scan failed: {err}"));
            }
        }

        tokio::time::sleep(config.scan_interval).await;
    }
}

struct RelayerClient {
    client: RelayClient,
    direct: DirectExecutor,
    http: reqwest::Client,
    base_url: String,
}

impl RelayerClient {
    async fn new(config: &RedeemConfig, target_wallet: Address) -> Result<Self, String> {
        let private_key =
            env_string("PRIVATE_KEY").ok_or_else(|| "PRIVATE_KEY missing".to_string())?;
        let wallet: LocalWallet = private_key
            .parse()
            .map_err(|e| format!("invalid PRIVATE_KEY: {e}"))?;

        let rpc_url = env_string("POLYGON_RPC_URL").unwrap_or_else(|| DEFAULT_RPC_URL.to_string());
        let tx_type = parse_tx_type(&config.relayer_tx_type)?;
        let client = RelayClient::new(
            config.chain_id,
            wallet.clone(),
            AuthMethod::builder(
                &config.builder_key,
                &config.builder_secret,
                &config.builder_passphrase,
            ),
            tx_type,
        )
        .await
        .map_err(|e| format!("init relay client failed: {e}"))?;
        let direct = DirectExecutor::new(&rpc_url, wallet, config.chain_id)
            .map_err(|e| format!("init direct executor failed: {e}"))?;

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| format!("init redeem http client failed: {e}"))?;

        let signer_address = client.signer_address();
        let wallet_address = client
            .wallet_address()
            .map_err(|e| format!("read relayer wallet address failed: {e}"))?;
        let expected_wallet = format!("{target_wallet:?}").to_ascii_lowercase();
        let actual_wallet = format!("{wallet_address:?}").to_ascii_lowercase();
        if actual_wallet != expected_wallet {
            return Err(format!(
                "relayer wallet mismatch: expected_safe={} actual_safe={} signer={:?}",
                target_wallet, wallet_address, signer_address
            ));
        }
        append_redeem_log(&format!(
            "[redeem] signer={:?} safe={:?} rpc_url={}",
            signer_address, wallet_address, rpc_url
        ));

        Ok(Self {
            client,
            direct,
            http,
            base_url: "https://relayer-v2.polymarket.com".to_string(),
        })
    }

    async fn redeem(&self, condition_id: B256) -> Result<String, String> {
        self.redeem_once(condition_id).await
    }

    async fn redeem_once(&self, condition_id: B256) -> Result<String, String> {
        let mut cid = [0u8; 32];
        cid.copy_from_slice(condition_id.as_slice());
        let tx = operations::redeem_regular(cid, &[1, 2]);
        match self.try_relayer(&tx).await {
            Ok(tx_hash) => Ok(tx_hash),
            Err(relayer_err) => {
                append_redeem_log(&format!(
                    "[redeem] relayer failed for condition_id={}, trying direct fallback: {}",
                    condition_id, relayer_err
                ));
                self.try_direct(&tx).await
            }
        }
    }

    async fn try_relayer(&self, tx: &Transaction) -> Result<String, String> {
        let handle = self
            .client
            .execute(vec![tx.clone()], "Redeem positions")
            .await
            .map_err(|e| format!("relayer execute failed: {e}"))?;
        let tx_id = handle.id().to_string();
        match handle.wait().await {
            Ok(result) => Ok(format!("{:?}", result.tx_hash)),
            Err(err) => {
                let err_msg = err.to_string();
                if err_msg.contains("error decoding response body") {
                    append_redeem_log(&format!(
                        "[redeem] wait decode failed, fallback to compat poll tx_id={} index_sets=[1,2]: {}",
                        tx_id, err_msg
                    ));
                    self.wait_tx_compat(&tx_id).await
                } else {
                    Err(format!("relayer wait failed: {err}"))
                }
            }
        }
    }

    async fn try_direct(&self, tx: &Transaction) -> Result<String, String> {
        match self.direct.execute(tx).await {
            Ok(result) if result.success => Ok(result.tx_hash),
            Ok(result) => Err(format!(
                "direct execute reverted: tx_hash={}",
                result.tx_hash
            )),
            Err(err) => Err(format!("direct execute failed: {err}")),
        }
    }

    async fn wait_tx_compat(&self, tx_id: &str) -> Result<String, String> {
        const POLL_INTERVAL: Duration = Duration::from_secs(2);
        const MAX_ATTEMPTS: u32 = 100;

        for _ in 0..MAX_ATTEMPTS {
            tokio::time::sleep(POLL_INTERVAL).await;
            let url = format!("{}/transaction?id={}", self.base_url, tx_id);
            let resp = self
                .http
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("compat poll http failed: {e}"))?;

            let status = resp.status();
            let text = resp
                .text()
                .await
                .map_err(|e| format!("compat poll read body failed: {e}"))?;

            if !status.is_success() {
                continue;
            }

            let Ok(v) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            let payload = if let Some(first) = v.as_array().and_then(|arr| arr.first()) {
                first
            } else {
                &v
            };

            let state = payload
                .get("state")
                .and_then(|x| x.as_str())
                .unwrap_or_default()
                .to_ascii_uppercase();
            let error_msg = payload
                .get("errorMsg")
                .and_then(|x| x.as_str())
                .or_else(|| payload.get("error").and_then(|x| x.as_str()))
                .unwrap_or("")
                .to_string();

            let tx_hash = payload
                .get("transactionHash")
                .and_then(|x| x.as_str())
                .or_else(|| payload.get("hash").and_then(|x| x.as_str()))
                .unwrap_or("")
                .to_string();

            if state.contains("FAILED") {
                return Err(format!(
                    "relayer compat poll failed: state={state} tx_id={tx_id} error_msg={}",
                    error_msg
                ));
            }
            if state.contains("INVALID") {
                return Err(format!(
                    "relayer compat poll invalid: state={state} tx_id={tx_id} error_msg={}",
                    error_msg
                ));
            }
            if state.contains("CONFIRMED") || state.contains("MINED") {
                if tx_hash.is_empty() {
                    return Ok(format!("tx_id={tx_id}"));
                }
                return Ok(tx_hash);
            }
        }

        Err(format!("relayer compat poll timeout: tx_id={tx_id}"))
    }
}

fn parse_tx_type(raw: &str) -> Result<RelayerTxType, String> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "SAFE" => Ok(RelayerTxType::Safe),
        "PROXY" => Ok(RelayerTxType::Proxy),
        other => Err(format!("invalid REDEEM_RELAYER_TX_TYPE: {other}")),
    }
}

fn parse_b256(raw: &str) -> Option<B256> {
    if raw.starts_with("0x") {
        B256::from_str(raw).ok()
    } else {
        B256::from_str(&format!("0x{raw}")).ok()
    }
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn env_u64(name: &str) -> Option<u64> {
    env_string(name).and_then(|v| v.parse::<u64>().ok())
}

fn env_usize(name: &str) -> Option<usize> {
    env_string(name).and_then(|v| v.parse::<usize>().ok())
}

fn env_bool(name: &str) -> Option<bool> {
    env_string(name).map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

fn exponential_backoff(base: Duration, max: Duration, attempts: u32) -> Duration {
    let factor = 2u32.saturating_pow(attempts.saturating_sub(1)).max(1);
    let ms = base.as_millis().saturating_mul(factor as u128);
    Duration::from_millis(ms.min(max.as_millis()) as u64)
}

pub fn redeem_log_path() -> PathBuf {
    if let Some(value) = env_string("LOG_DIR") {
        return PathBuf::from(value).join("redeem_dynamic.log");
    }

    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            return exe_dir.join("redeem_dynamic.log");
        }
    }

    PathBuf::from("redeem_dynamic.log")
}

fn append_redeem_log(line: &str) {
    let path = redeem_log_path();
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) else {
        return;
    };
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let _ = writeln!(file, "{} {}", timestamp, line);
}
