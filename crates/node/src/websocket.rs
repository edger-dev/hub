//! Dialing a hub over Vox WebSocket links.
//!
//! implements: hub::spec::node-websocket-dialing@48b41b726a48a78ab316b83935f2e27276ea4c2d3238f8d5a29549aa83dc8053

use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use vox::transport::websocket::WsLink;

use crate::config::{ConfigError, HubEndpoint};

pub type WebSocketLink = WsLink<MaybeTlsStream<tokio::net::TcpStream>>;

/// Dial `ws://` or `wss://`, returning no link for a temporarily unreachable hub.
pub async fn dial(endpoint: &HubEndpoint) -> Result<Option<WebSocketLink>, ConfigError> {
    match endpoint.0.split_once("://") {
        Some(("ws" | "wss", _)) => {}
        Some((scheme, _)) => return Err(ConfigError::UnsupportedScheme(scheme.to_string())),
        None => return Err(ConfigError::UnsupportedScheme("tcp".into())),
    }
    let request = endpoint
        .0
        .as_str()
        .into_client_request()
        .map_err(|error| ConfigError::InvalidWebSocketAddress(error.to_string()))?;
    match tokio_tungstenite::connect_async(request).await {
        Ok((stream, _)) => Ok(Some(WsLink::new(stream))),
        Err(_) => Ok(None),
    }
}
