//! Accepting nodes and consumers over TCP.
//!
//! The hub is the one component that *listens* — nodes and consumers all dial in
//! (§2.1, §2.4). Each accepted connection is served on its own task, so one
//! peer's disconnect never disturbs another's.

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use vox::ConnectionHandle;

use crate::serve::{ServedHub, serve_connection_with};

/// Live connections, shut down when the serving future is dropped.
///
/// vox runs each connection on driver tasks that own the socket, so simply
/// dropping the serving task would leave peers holding a socket to a hub that no
/// longer serves. Shutting down explicitly makes "the hub went away" observable,
/// which is what lets a node start reconnecting (§2.1).
#[derive(Default)]
struct LiveConnections {
    handles: Arc<Mutex<Vec<ConnectionHandle>>>,
}

impl Drop for LiveConnections {
    fn drop(&mut self) {
        if let Ok(handles) = self.handles.lock() {
            for handle in handles.iter() {
                let _ = handle.shutdown();
            }
        }
    }
}

/// A bound hub listener, before it starts accepting.
///
/// Binding is separate from accepting so a caller (or a test using port 0) can
/// learn the real address first.
pub struct HubListener {
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl HubListener {
    /// Bind the hub to `addr` (e.g. `127.0.0.1:15400`, or `127.0.0.1:0` to let
    /// the OS choose).
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        Ok(Self {
            listener,
            local_addr,
        })
    }

    /// The address actually bound — the one nodes should dial.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Accept and serve connections until the listener errors.
    ///
    /// Each connection gets its own task: a node dropping (or a consumer closing
    /// a tab) must not interrupt anyone else. Per-connection failures are
    /// contained in that task; only a listener-level error ends this loop.
    ///
    /// The tasks are owned by this future, not detached. Dropping it — stopping
    /// the hub — therefore closes every live connection, so peers see the hub go
    /// away and start reconnecting (§2.1) instead of holding a socket to a hub
    /// that is no longer serving.
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
                        // A connection that fails to establish, or ends, is
                        // ordinary — the peer reconnects on its own (§2.1).
                        let _ = serve_connection_with(
                            hub,
                            vox::transport::tcp::StreamLink::tcp(socket),
                            move |handle| {
                                if let Ok(mut handles) = handles.lock() {
                                    handles.push(handle);
                                }
                            },
                        )
                        .await;
                    });
                }
                // Reap finished connections so the set cannot grow without bound.
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    }
}

/// Bind and serve in one call, for a hub that never needs its own address.
pub async fn serve_tcp(hub: ServedHub, addr: impl tokio::net::ToSocketAddrs) -> io::Result<()> {
    HubListener::bind(addr).await?.serve(hub).await
}
