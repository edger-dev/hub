#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use hub::{ServedHub, WsHubListener, peer_dialer};
use hub_protocol::service::ConsumerApiClient;
use hub_protocol::{LinkLabel, LinkSelector, NodeId, RoutedCall};
use node::files::{FileContents, PathRequest};
use node::{Connectors, FilesConnector};

async fn eventually<F, Fut>(mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..100 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("never held");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_carries_topology_and_routed_calls_over_websocket() {
    let listener = WsHubListener::bind("127.0.0.1:0").await.unwrap();
    let edger_addr = listener.local_addr().to_string();
    let edger = ServedHub::new()
        .with_auth_token("secret")
        .with_peers(hub::Peers::new("edger").with_auth_token("secret"));
    let _edger_server = tokio::spawn(listener.serve(edger.clone()));

    let dir = tempdir::TempDir::new("peer-websocket").unwrap();
    std::fs::write(dir.path().join("motd.txt"), "hello over wss").unwrap();
    let mut connectors = Connectors::new();
    connectors.register(Box::new(FilesConnector::new("files-0", dir.path())));
    let node_config = node::NodeConfig::new(
        NodeId("yolo".into()),
        LinkLabel::stable(),
        Some(node::HubEndpoint(format!("ws://{edger_addr}"))),
        vec![],
        "test".into(),
    )
    .unwrap()
    .with_auth_token("secret");
    let link = node::websocket::dial(&node_config.hub)
        .await
        .unwrap()
        .expect("edger is listening");
    let _node_connection = node::serve_and_register_with(link, &node_config, Arc::new(connectors))
        .await
        .unwrap();

    let listener = WsHubListener::bind("127.0.0.1:0").await.unwrap();
    let pc_addr = listener.local_addr().to_string();
    let pc = ServedHub::new()
        .with_auth_token("secret")
        .with_peers(hub::Peers::new("pc").with_auth_token("secret"));
    pc.peers()
        .add(
            "edger",
            peer_dialer(&pc, "edger", format!("ws://{edger_addr}"), None),
        )
        .unwrap();
    let _pc_server = tokio::spawn(listener.serve(pc.clone()));

    let connection = vox::connect(format!("ws://{pc_addr}")).await.unwrap();
    let consumer: ConsumerApiClient = connection.open_lane().await.unwrap();
    consumer.authenticate("secret".into()).await.unwrap();
    eventually(|| {
        let consumer = consumer.clone();
        async move {
            consumer.topology().await.is_ok_and(|snapshot| {
                snapshot
                    .nodes
                    .iter()
                    .any(|node| node.node.0 == "edger::yolo")
            })
        }
    })
    .await;

    let reply = consumer
        .route(
            LinkSelector::node(NodeId("edger::yolo".into())),
            RoutedCall {
                connector: "files-0".into(),
                method: "read".into(),
                payload: facet_json::to_string(&PathRequest {
                    path: "motd.txt".into(),
                })
                .unwrap()
                .into_bytes(),
            },
        )
        .await
        .unwrap();
    let contents: FileContents =
        facet_json::from_str(std::str::from_utf8(&reply.payload).unwrap()).unwrap();
    assert_eq!(contents.contents, "hello over wss");

    let pc_dir = tempdir::TempDir::new("peer-websocket-reverse").unwrap();
    std::fs::write(pc_dir.path().join("motd.txt"), "hello from pc").unwrap();
    let mut connectors = Connectors::new();
    connectors.register(Box::new(FilesConnector::new("files-0", pc_dir.path())));
    let node_config = node::NodeConfig::new(
        NodeId("pc-node".into()),
        LinkLabel::stable(),
        Some(node::HubEndpoint(format!("ws://{pc_addr}"))),
        vec![],
        "test".into(),
    )
    .unwrap()
    .with_auth_token("secret");
    let link = node::websocket::dial(&node_config.hub)
        .await
        .unwrap()
        .expect("pc is listening");
    let _pc_node_connection =
        node::serve_and_register_with(link, &node_config, Arc::new(connectors))
            .await
            .unwrap();

    let connection = vox::connect(format!("ws://{edger_addr}")).await.unwrap();
    let on_edger: ConsumerApiClient = connection.open_lane().await.unwrap();
    on_edger.authenticate("secret".into()).await.unwrap();
    eventually(|| {
        let on_edger = on_edger.clone();
        async move {
            on_edger.topology().await.is_ok_and(|snapshot| {
                snapshot
                    .nodes
                    .iter()
                    .any(|node| node.node.0 == "pc::pc-node")
            })
        }
    })
    .await;
    let reply = on_edger
        .route(
            LinkSelector::node(NodeId("pc::pc-node".into())),
            RoutedCall {
                connector: "files-0".into(),
                method: "read".into(),
                payload: facet_json::to_string(&PathRequest {
                    path: "motd.txt".into(),
                })
                .unwrap()
                .into_bytes(),
            },
        )
        .await
        .unwrap();
    let contents: FileContents =
        facet_json::from_str(std::str::from_utf8(&reply.payload).unwrap()).unwrap();
    assert_eq!(contents.contents, "hello from pc");
}
