use poly_bot::engine::data::price_feeder::PriceFeeder;
use poly_bot::telemetry::start_telemetry_thread;

#[tokio::main]
async fn main() {
    println!("starting standalone ETH price watcher...");
    let (tx, mut rx) = tokio::sync::watch::channel(0.0);
    let telemetry = start_telemetry_thread();

    tokio::spawn(async move {
        PriceFeeder::start_feeding(tx, telemetry).await;
    });

    loop {
        if rx.changed().await.is_err() {
            eprintln!("ETH price channel closed");
            break;
        }

        let eth = *rx.borrow_and_update();
        println!("[ETH] realtime={:.2}", eth);
    }
}
