use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;

use crate::price_sources::PriceSource;

const SYMBOLS: &[&str] = &["XLM", "BTC", "ETH", "SOL"];

pub async fn price_ingestion_loop(state: crate::AppState) {
    let symbols = SYMBOLS
        .iter()
        .map(|symbol| (*symbol).to_string())
        .collect::<Vec<_>>();
    loop {
        match state.price_source.fetch(&symbols).await {
            Ok(quotes) => {
                let mut prices = state.spot_prices.lock().unwrap();
                let mut ticks = state.price_ticks.lock().unwrap();
                for (symbol, tick) in quotes {
                    if !tick.price.is_finite() || tick.price <= 0.0 {
                        tracing::warn!(symbol, "ignoring invalid observed market price");
                        continue;
                    }
                    prices.insert(symbol.clone(), tick.price);
                    ticks.insert(symbol, tick);
                }
                let payload = crate::SpotResponse {
                    prices: prices.clone(),
                    vols: state.vol_surface.lock().unwrap().clone(),
                    sources: ticks
                        .iter()
                        .map(|(symbol, tick)| (symbol.clone(), tick.source.clone()))
                        .collect(),
                };
                match serde_json::to_string(&payload) {
                    Ok(encoded) => {
                        let _ = state.spot_tx.send(encoded);
                    }
                    Err(error) => {
                        tracing::error!(%error, "could not serialize market-price update");
                    }
                }
            }
            Err(error) => {
                tracing::error!(%error, "market price ingestion failed; retaining last verified prices");
            }
        }
        tokio::time::sleep(Duration::from_secs(15)).await;
    }
}

pub async fn ws_spot(
    State(state): State<crate::AppState>,
    websocket: WebSocketUpgrade,
) -> impl IntoResponse {
    websocket.on_upgrade(move |socket| stream_prices(socket, state))
}

async fn stream_prices(mut socket: WebSocket, state: crate::AppState) {
    let mut updates = state.spot_tx.subscribe();
    loop {
        tokio::select! {
            message = socket.recv() => {
                match message {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => {}
                }
            }
            update = updates.recv() => {
                match update {
                    Ok(payload) => {
                        if socket.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}
