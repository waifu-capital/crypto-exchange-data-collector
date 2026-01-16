use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;

use crate::models::SnapshotData;
use crate::utils::increment_dropped_snapshots;
use crate::println_with_timestamp;

/// WebSocket worker that connects to Binance and streams orderbook data
pub async fn websocket_worker(db_tx: Sender<SnapshotData>, market_symbol: String) {
    let url = format!(
        "wss://stream.binance.com:9443/ws/{}@depth20@100ms",
        market_symbol.to_lowercase()
    );
    let retry_delay = Duration::from_secs(5);

    loop {
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                println_with_timestamp!("Connected to Binance WebSocket for {}", market_symbol);
                let (mut write, mut read) = ws_stream.split();

                while let Some(message) = read.next().await {
                    match message {
                        Ok(msg) => {
                            if msg.is_text() {
                                // Sample log ~0.1% of messages
                                if rand::random::<u32>() % 1000 == 0 {
                                    if let Ok(preview) = msg.to_text() {
                                        let preview_len = preview.len().min(50);
                                        println_with_timestamp!(
                                            "Received orderbook message: {:?}...",
                                            &preview[..preview_len]
                                        );
                                    }
                                }

                                let text = match msg.into_text() {
                                    Ok(t) => t,
                                    Err(e) => {
                                        println_with_timestamp!(
                                            "ERROR: Failed to extract text from message: {}",
                                            e
                                        );
                                        continue;
                                    }
                                };

                                let snapshot: Value = match serde_json::from_str(&text) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        println_with_timestamp!("ERROR: Failed to parse JSON: {}", e);
                                        continue;
                                    }
                                };

                                let exchange_sequence_id = snapshot["lastUpdateId"].to_string();
                                save_snapshot(
                                    &db_tx,
                                    "binance",
                                    &market_symbol,
                                    "orderbook",
                                    &exchange_sequence_id,
                                    &text,
                                );
                            } else if msg.is_ping() {
                                println_with_timestamp!("Received ping frame: {:?}", msg);
                                let pong =
                                    tokio_tungstenite::tungstenite::Message::Pong(msg.into_data());
                                if let Err(e) = write.send(pong).await {
                                    println_with_timestamp!("Failed to send pong frame: {}", e);
                                    break;
                                }
                                println_with_timestamp!("Sent pong frame in response to ping.");
                            } else if msg.is_pong() {
                                println_with_timestamp!("Received pong frame: {:?}", msg);
                            } else {
                                println_with_timestamp!("Received other message: {:?}", msg);
                            }
                        }
                        Err(e) => {
                            println_with_timestamp!("WebSocket error: {}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                println_with_timestamp!("Failed to connect to WebSocket: {}", e);
            }
        }

        println_with_timestamp!("Reconnecting in {} seconds...", retry_delay.as_secs());
        sleep(retry_delay).await;
    }
}

/// Save a snapshot to the channel for database processing
fn save_snapshot(
    db_tx: &Sender<SnapshotData>,
    exchange: &str,
    symbol: &str,
    data_type: &str,
    exchange_sequence_id: &str,
    raw_data: &str,
) {
    let data = SnapshotData {
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        data_type: data_type.to_string(),
        exchange_sequence_id: exchange_sequence_id.to_string(),
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64,
        data: raw_data.to_string(),
    };

    // Non-blocking send - never stall WebSocket even under backpressure
    match db_tx.try_send(data) {
        Ok(_) => {}
        Err(_) => {
            let count = increment_dropped_snapshots();
            println_with_timestamp!(
                "WARNING: Channel full, dropped snapshot (total dropped: {})",
                count
            );
        }
    }
}
