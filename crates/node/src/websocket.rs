//! Dialing a hub over Vox WebSocket links.

use tokio_tungstenite::MaybeTlsStream;
use vox::transport::websocket::WsLink;

use crate::config::{ConfigError, HubEndpoint};

pub type WebSocketLink = WsLink<MaybeTlsStream<tokio::net::TcpStream>>;

pub async fn dial(endpoint: &HubEndpoint) -> Result<Option<WebSocketLink>, ConfigError> {
    let _ = endpoint;
    Err(ConfigError::UnsupportedScheme(
        "websocket dialing is not implemented".into(),
    ))
}
