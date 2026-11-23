//! Accepting Vox connections over WebSocket beside TCP.
//!
//! implements: hub::spec::websocket-transport@c8c8feec188821c5d915875ffe406573ee9392b6fb537f9ddfa052bed3557c84

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;

use crate::serve::{ServedHub, serve_connection_with};
use crate::tcp::LiveConnections;

/// A bound plain-WebSocket hub listener.
pub struct WsHubListener {
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl WsHubListener {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        Ok(Self {
            listener,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept and serve WebSocket connections until the listener errors.
    pub async fn serve(self, hub: ServedHub) -> io::Result<()> {
        let live = LiveConnections::default();
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = self.listener.accept() => {
                    let (socket, _peer) = accepted?;
                    let hub = hub.clone();
                    let handles = Arc::clone(&live.handles);
                    connections.spawn(async move {
                        let Ok(link) = vox::transport::websocket::WsLink::server(socket).await else {
                            return;
                        };
                        let _ = serve_connection_with(hub, link, move |handle| {
                            if let Ok(mut handles) = handles.lock() {
                                handles.push(handle);
                            }
                        })
                        .await;
                    });
                }
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    }
}
