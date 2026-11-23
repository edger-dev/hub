#![allow(clippy::unwrap_used, clippy::expect_used)]

use hub::{ServedHub, WsHubListener};
use hub_protocol::service::ConsumerApiClient;
use hub_protocol::{LinkLabel, NodeId};
use node::{Connectors, HubEndpoint, NodeConfig};

#[tokio::test(flavor = "multi_thread")]
async fn a_node_registers_and_serves_over_websocket() {
    let listener = WsHubListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().to_string();
    let _server = tokio::spawn(listener.serve(ServedHub::new().with_auth_token("secret")));
    let config = NodeConfig::new(
        NodeId("home".into()),
        LinkLabel::stable(),
        Some(HubEndpoint(format!("ws://{addr}"))),
        vec![],
        "test".into(),
    )
    .unwrap()
    .with_auth_token("secret");
    let link = node::websocket::dial(&config.hub)
        .await
        .unwrap()
        .expect("hub is listening");
    let node_connection = node::serve_and_register_with(
        link,
        &config,
        std::sync::Arc::new(Connectors::new()),
    )
    .await
    .unwrap();

    let consumer_connection = vox::connect(format!("ws://{addr}")).await.unwrap();
    let consumer: ConsumerApiClient = consumer_connection.open_lane().await.unwrap();
    consumer.authenticate("secret".into()).await.unwrap();
    assert_eq!(consumer.topology().await.unwrap().nodes.len(), 1);
    drop(node_connection);
}
