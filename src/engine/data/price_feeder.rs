use futures_util::{SinkExt, StreamExt};
use log::info;
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::telemetry::TelemetryTx;

#[derive(Debug, Deserialize)]
pub struct ChainlinkPriceMessage {
    pub topic: String,
    pub payload: PricePayload,
}

#[derive(Debug, Deserialize)]
pub struct PricePayload {
    pub symbol: String,
    pub value: f64,
    pub timestamp: u64,
}

pub struct PriceFeeder;

impl PriceFeeder {
    pub async fn start_feeding(tx: watch::Sender<f64>, telemetry: TelemetryTx) {
        let url = "wss://ws-live-data.polymarket.com";
        let mut logged_ready_once = false;

        loop {
            telemetry.log("[price-feeder] connecting polymarket chainlink feed".to_string());

            match connect_async(url).await {
                Ok((mut ws_stream, _)) => {
                    if !logged_ready_once {
                        info!("[price-feeder] connected, sending subscription");
                        logged_ready_once = true;
                    }
                    telemetry.log("[price-feeder] connected, sending subscription".to_string());

                    let subscribe_msg = json!({
                        "action": "subscribe",
                        "subscriptions": [
                            {
                                "topic": "crypto_prices_chainlink",
                                "type": "*",
                                "filters": "{\"symbol\":\"eth/usd\"}"
                            }
                        ]
                    });

                    if let Err(err) = ws_stream
                        .send(Message::Text(subscribe_msg.to_string()))
                        .await
                    {
                        telemetry.log(format!(
                            "[price-feeder] subscribe request failed: {:?}",
                            err
                        ));
                        continue;
                    }

                    while let Some(Ok(msg)) = ws_stream.next().await {
                        if let Message::Text(text) = msg {
                            if let Ok(parsed) = serde_json::from_str::<ChainlinkPriceMessage>(&text)
                            {
                                let price = parsed.payload.value;
                                let _ = tx.send(price);
                            }
                        }
                    }
                }
                Err(err) => {
                    telemetry.log(format!(
                        "[price-feeder] connection failed: {:?}, retry in 5s",
                        err
                    ));
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
