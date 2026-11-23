#![allow(clippy::unwrap_used, clippy::expect_used)]

use hub::{ServedHub, WsHubListener};
use hub_protocol::service::ConsumerApiClient;

#[tokio::test(flavor = "multi_thread")]
async fn websocket_clients_use_the_same_authenticated_hub() {
    let listener = WsHubListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr();
    let _server = tokio::spawn(listener.serve(ServedHub::new().with_auth_token("secret")));
    let connection = vox::connect(format!("ws://{addr}")).await.unwrap();
    let consumer: ConsumerApiClient = connection.open_lane().await.unwrap();
    consumer.authenticate("secret".into()).await.unwrap();
    assert!(consumer.topology().await.unwrap().nodes.is_empty());
}
