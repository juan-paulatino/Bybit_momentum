// bybit_trade_aggressor_detector.rs
// Single-file skeleton for a Bybit publicTrade aggressive buyer/seller detector in Rust.

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use url::Url;

// Configuration: edit these symbols / window sizes as you like
const WS_ENDPOINT: &str = "wss://stream.bybit.com/v5/public/linear"; // Bybit public v5
const SYMBOLS: &[&str] = &["POPCATUSDT"];
const AGG_WINDOW_SECONDS: u64 = 5; // rolling window in seconds for short-term metrics

#[derive(Debug, Deserialize)]
struct BybitMsg {
    topic: Option<String>,
    _op: Option<String>,
    _req_id: Option<String>,
    _ret_code: Option<i64>,
    _ret_msg: Option<String>,
    data: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
struct Trade {
    ts_ms: i64,
    symbol: String,
    side: String, // "Buy" or "Sell"
    price: f64,
    size: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Channel: websocket reader -> processor
    let (tx, mut rx) = mpsc::unbounded_channel::<Trade>();

    // Spawn processor task
    tokio::spawn(async move {
        if let Err(e) = processor_task(&mut rx).await {
            eprintln!("processor error: {:#}", e);
        }
    });

    // Connect & subscribe loop (simple reconnect on failure)
    loop {
        if let Err(e) = ws_connect_and_run(tx.clone()).await {
            eprintln!("ws error: {:#}, reconnecting in 2s...", e);
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        } else {
            // graceful exit if function returns Ok (unlikely)
            break;
        }
    }

    Ok(())
}

async fn ws_connect_and_run(tx: mpsc::UnboundedSender<Trade>) -> anyhow::Result<()> {
    let url = Url::parse(WS_ENDPOINT)?;
    println!("Connecting to {}", url);
    let (ws_stream, _resp) = connect_async(url).await.context("connect failed")?;
    println!("Connected to Bybit websocket");

    let (mut write, mut read) = ws_stream.split();

    // Subscribe to publicTrade topics for configured symbols
    let args: Vec<String> = SYMBOLS
        .iter()
        .map(|s| format!("publicTrade.{}", s))
        .collect();

    let sub = json!({
        "op": "subscribe",
        "args": args,
    });

    write
        .send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string()))
        .await
        .context("sending subscribe failed")?;

    println!("Subscribed to trades: {:?}", SYMBOLS);

    // read loop
    while let Some(msg) = read.next().await {
        let msg = msg?;
        if msg.is_text() {
            let txt = msg.into_text()?;
            // quick parse
            match serde_json::from_str::<BybitMsg>(&txt) {
                Ok(bmsg) => {
                    if let Some(topic) = bmsg.topic {
                        if topic.starts_with("publicTrade.") {
                            if let Some(data) = bmsg.data {
                                // data is typically an array of trades
                                if let Some(arr) = data.as_array() {
                                    for item in arr.iter() {
                                        // fields: T (timestamp ms), s (symbol), S (side), p (price), v (size)
                                        let ts_ms = item["T"].as_i64().or_else(|| item["t"].as_i64()).unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                                        let symbol = item["s"].as_str().or_else(|| item["S"].as_str()).unwrap_or("").to_string();
                                        let side = item["S"].as_str().or_else(|| item["s"].as_str()).unwrap_or("").to_string();
                                        // Bybit sometimes uses lowercase p/v or strings
                                        let price = item["p"].as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| item["price"].as_f64()).unwrap_or(0.0);
                                        let size = item["v"].as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| item["size"].as_f64()).unwrap_or(0.0);

                                        // normalize side: try to capture both "Buy"/"Sell" and other flags
                                        let side_norm = match side.to_lowercase().as_str() {
                                            "buy" | "b" | "taker_buy" => "Buy".to_string(),
                                            "sell" | "s" | "taker_sell" => "Sell".to_string(),
                                            _ => side,
                                        };

                                        let trade = Trade {
                                            ts_ms,
                                            symbol: symbol.clone(),
                                            side: side_norm,
                                            price,
                                            size,
                                        };

                                        // send to processor (ignore if receiver closed)
                                        let _ = tx.send(trade);
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("failed to parse incoming message: {}\nerr: {}", txt, e);
                }
            }
        }
    }

    Ok(())
}

async fn processor_task(rx: &mut mpsc::UnboundedReceiver<Trade>) -> anyhow::Result<()> {
    // We'll maintain a sliding window (VecDeque) per symbol storing trades within AGG_WINDOW_SECONDS
    use std::collections::HashMap;

    let mut windows: HashMap<String, VecDeque<Trade>> = HashMap::new();
    let mut cumulative_delta: HashMap<String, f64> = HashMap::new();

    // periodic reporter
    let mut ticker = tokio::time::interval(Duration::from_millis(500));

    loop {
        tokio::select! {
            Some(trade) = rx.recv() => {
                let sym_win = windows.entry(trade.symbol.clone()).or_insert_with(VecDeque::new);
                sym_win.push_back(trade.clone());

                // update cumulative delta
                let delta = match trade.side.as_str() {
                    "Buy" => trade.size,
                    "Sell" => -trade.size,
                    _ => 0.0,
                };
                *cumulative_delta.entry(trade.symbol.clone()).or_insert(0.0) += delta;
            }
            _ = ticker.tick() => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                // compute metrics per symbol
                for (symbol, deque) in windows.iter_mut() {
                    // pop old trades outside window
                    while let Some(front) = deque.front() {
                        if (now_ms - front.ts_ms) as u64 > AGG_WINDOW_SECONDS * 1000 {
                            deque.pop_front();
                        } else {
                            break;
                        }
                    }

                    let mut buy_vol = 0.0_f64;
                    let mut sell_vol = 0.0_f64;
                    let mut vwap_num = 0.0_f64;
                    let mut vwap_den = 0.0_f64;
                    for t in deque.iter() {
                        if t.side == "Buy" { buy_vol += t.size; }
                        else if t.side == "Sell" { sell_vol += t.size; }
                        vwap_num += t.price * t.size;
                        vwap_den += t.size;
                    }
                    let delta = buy_vol - sell_vol;
                    let cum = *cumulative_delta.get(symbol).unwrap_or(&0.0);
                    let aggression_ratio = if buy_vol + sell_vol > 0.0 { buy_vol / (buy_vol + sell_vol) } else { 0.5 };
                    let vwap = if vwap_den > 0.0 { vwap_num / vwap_den } else { 0.0 };

                    // Simple alerting heuristic (adjust thresholds to taste)
                    if delta.abs() > 5.0 { // example threshold in base units (e.g. 5 BTC)
                        println!("ALERT {}: strong imbalance in last {}s — delta={:.6} buy_vol={:.6} sell_vol={:.6} cum_delta={:.6} vwap={:.2} ratio={:.2}", symbol, AGG_WINDOW_SECONDS, delta, buy_vol, sell_vol, cum, vwap, aggression_ratio);
                    } else {
                        println!("{} | window={}s | delta={:.6} buy={:.6} sell={:.6} cum={:.6} vwap={:.2} ratio={:.2}", symbol, AGG_WINDOW_SECONDS, delta, buy_vol, sell_vol, cum, vwap, aggression_ratio);
                    }
                }
            }
        }
    }
}

// End of file