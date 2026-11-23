//! Accepting Vox connections over WebSocket beside TCP.

use std::io;
use std::net::SocketAddr;

use tokio::net::TcpListener;

use crate::serve::ServedHub;

/// A bound plain-WebSocket hub listener.
pub struct WsHubListener {
    _listener: TcpListener,
    local_addr: SocketAddr,
}

impl WsHubListener {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        Ok(Self {
            _listener: listener,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn serve(self, _hub: ServedHub) -> io::Result<()> {
        Err(io::Error::other("WebSocket serving is not implemented"))
    }
}
