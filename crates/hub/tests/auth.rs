#![allow(clippy::unwrap_used, clippy::expect_used)]

use hub::{HubListener, ServedHub};
use hub_protocol::service::{ConsumerApiClient, PeerIngressClient, SessionIngressClient};
use hub_protocol::{LinkLabel, NodeId, SessionInfo};
use node::{HubEndpoint, NodeConfig};

async fn consumer(addr: &str) -> ConsumerApiClient {
    let socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let connection = vox::initiator_on(vox::transport::tcp::StreamLink::tcp(socket))
        .establish_connection()
        .await
        .unwrap();
    connection.open_lane().await.unwrap()
}

fn config() -> NodeConfig {
    NodeConfig::new(
        NodeId("alpha".into()),
        LinkLabel::stable(),
        Some(HubEndpoint("unused".into())),
        vec![],
        "test".into(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn consumers_authenticate_before_reading_or_routing() {
    let listener = HubListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().to_string();
    let _server = tokio::spawn(listener.serve(ServedHub::new().with_auth_token("secret")));
    let consumer = consumer(&addr).await;

    assert!(consumer.topology().await.is_err());
    assert!(consumer.authenticate("wrong".into()).await.is_err());
    consumer.authenticate("secret".into()).await.unwrap();
    consumer.topology().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn peers_authenticate_before_registering() {
    let listener = HubListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().to_string();
    let hub = ServedHub::new()
        .with_auth_token("secret")
        .with_peers(hub::Peers::new("hub"));
    let _server = tokio::spawn(listener.serve(hub.clone()));

    let socket = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let connection = vox::initiator_on(vox::transport::tcp::StreamLink::tcp(socket))
        .establish_connection()
        .await
        .unwrap();
    let peer: PeerIngressClient = connection.open_lane().await.unwrap();
    assert!(peer.register("edger".into()).await.is_err());
    assert!(peer.authenticate("wrong".into()).await.is_err());
    assert!(hub.peers().names().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn sessions_authenticate_before_registering() {
    let listener = HubListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().to_string();
    let _server = tokio::spawn(listener.serve(ServedHub::new().with_auth_token("secret")));

    let socket = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let connection = vox::initiator_on(vox::transport::tcp::StreamLink::tcp(socket))
        .establish_connection()
        .await
        .unwrap();
    let session: SessionIngressClient = connection.open_lane().await.unwrap();
    let info = SessionInfo {
        id: "session-a".into(),
        node: "yolo".into(),
        harness: "test".into(),
        workspace: "/work".into(),
        context: None,
    };
    assert!(session.register(info).await.is_err());
    assert!(session.authenticate("wrong".into()).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn nodes_authenticate_before_registering() {
    let listener = HubListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().to_string();
    let _server = tokio::spawn(listener.serve(ServedHub::new().with_auth_token("secret")));

    let socket = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let result =
        node::serve_and_register(vox::transport::tcp::StreamLink::tcp(socket), &config()).await;
    assert!(result.is_err());

    let socket = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let connection = node::serve_and_register(
        vox::transport::tcp::StreamLink::tcp(socket),
        &config().with_auth_token("secret"),
    )
    .await
    .unwrap();
    drop(connection);
}
