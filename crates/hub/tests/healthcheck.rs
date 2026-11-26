#![allow(clippy::unwrap_used, clippy::expect_used)]

use hub::{ServedHub, WsHubListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn request(addr: std::net::SocketAddr, request: &[u8]) -> String {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(request).await.unwrap();
    socket.shutdown().await.unwrap();
    let mut response = Vec::new();
    socket.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
}

#[tokio::test]
async fn healthcheck_returns_ok_and_other_http_requires_websocket_upgrade() {
    let listener = WsHubListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr();
    let _server = tokio::spawn(listener.serve(ServedHub::new()));

    let healthy = request(addr, b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
    assert!(healthy.starts_with("HTTP/1.1 200"), "{healthy:?}");
    assert!(healthy.ends_with("ok\n"), "{healthy:?}");

    let upgrade = request(addr, b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
    assert!(upgrade.starts_with("HTTP/1.1 426"), "{upgrade:?}");
}
