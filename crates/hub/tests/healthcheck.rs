#![allow(clippy::unwrap_used, clippy::expect_used)]

use hub::{ServedHub, WsHubListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn request(addr: std::net::SocketAddr, request: &[u8]) -> String {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(request).await.unwrap();
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

    let lookalike = request(
        addr,
        b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Upgrade: websocket\r\n\r\n",
    )
    .await;
    assert!(lookalike.starts_with("HTTP/1.1 426"), "{lookalike:?}");

    let body = request(
        addr,
        b"GET / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 20\r\n\r\nUpgrade: websocket\r\n",
    )
    .await;
    assert!(body.starts_with("HTTP/1.1 426"), "{body:?}");

    let pipelined = request(
        addr,
        b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\nGET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n\r\n",
    )
    .await;
    assert!(pipelined.starts_with("HTTP/1.1 426"), "{pipelined:?}");

    let large = format!(
        "GET /healthz HTTP/1.1\r\nHost: localhost\r\nX-Large: {}\r\n\r\n",
        "x".repeat(9000)
    );
    let large = request(addr, large.as_bytes()).await;
    assert!(large.starts_with("HTTP/1.1 426"), "{large:?}");
}

async fn request_status(addr: std::net::SocketAddr, request: &[u8]) -> String {
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(request).await.unwrap();
    let mut response = [0; 12];
    socket.read_exact(&mut response).await.unwrap();
    String::from_utf8(response.to_vec()).unwrap()
}

#[tokio::test]
async fn valid_upgrades_reach_vox_even_on_the_health_path() {
    let listener = WsHubListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr();
    let _server = tokio::spawn(listener.serve(ServedHub::new()));

    let response = request_status(
        addr,
        b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nUpgrade:websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 101"), "{response:?}");
}
