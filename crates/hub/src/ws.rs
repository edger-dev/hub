//! Accepting Vox connections over WebSocket beside TCP.
//!
//! implements: hub::spec::websocket-transport@c8c8feec188821c5d915875ffe406573ee9392b6fb537f9ddfa052bed3557c84

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};

use crate::serve::{ServedHub, serve_connection_with};
use crate::tcp::LiveConnections;

enum Request {
    Health,
    WebSocket(Vec<u8>),
    Other,
}

struct PrefixedStream {
    prefix: Vec<u8>,
    position: usize,
    stream: TcpStream,
}

impl AsyncRead for PrefixedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.position < self.prefix.len() {
            let count = (self.prefix.len() - self.position).min(buf.remaining());
            buf.put_slice(&self.prefix[self.position..self.position + count]);
            self.position += count;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for PrefixedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

// implements: 476776853392a6a97d0831ba5991341bb2f68f91e3a2cfc9adb0ca375a83146c@476776853392a6a97d0831ba5991341bb2f68f91e3a2cfc9adb0ca375a83146c
async fn request(socket: &mut TcpStream) -> io::Result<Request> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut bytes = Vec::new();
        let mut chunk = [0; 1024];
        for _ in 0..64 {
            let count = socket.read(&mut chunk).await?;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete HTTP request",
                ));
            }
            if bytes.len() + count > 8192 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP request headers too large",
                ));
            }
            bytes.extend_from_slice(&chunk[..count]);
            if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        if !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "too many HTTP request packets",
            ));
        }
        let text = String::from_utf8_lossy(&bytes);
        let mut lines = text.split("\r\n");
        let mut parts = lines.next().unwrap_or_default().split_whitespace();
        let method = parts.next().unwrap_or_default();
        let path = parts.next().unwrap_or_default();
        let upgrade = lines.any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.eq_ignore_ascii_case("upgrade")
                    && value.trim().eq_ignore_ascii_case("websocket")
            })
        });
        if upgrade {
            Ok(Request::WebSocket(bytes))
        } else if method == "GET" && path == "/healthz" {
            Ok(Request::Health)
        } else {
            Ok(Request::Other)
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "HTTP request timed out"))?
}

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
                    let (mut socket, _peer) = accepted?;
                    let hub = hub.clone();
                    let handles = Arc::clone(&live.handles);
                    connections.spawn(async move {
                        match request(&mut socket).await {
                            Ok(Request::Health) => {
                                let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nok\n").await;
                            }
                            Ok(Request::Other) | Err(_) => {
                                let _ = socket.write_all(b"HTTP/1.1 426 Upgrade Required\r\nContent-Length: 19\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nwebsocket endpoint\n").await;
                            }
                            Ok(Request::WebSocket(prefix)) => {
                                let stream = PrefixedStream { prefix, position: 0, stream: socket };
                                let Ok(link) = vox::transport::websocket::WsLink::server(stream).await else {
                                    return;
                                };
                                let _ = serve_connection_with(hub, link, move |handle| {
                                    if let Ok(mut handles) = handles.lock() {
                                        handles.push(handle);
                                    }
                                })
                                .await;
                            }
                        }
                    });
                }
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    }
}
