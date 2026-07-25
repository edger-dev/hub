#![allow(clippy::unwrap_used, clippy::expect_used)]
//! The whole mesh doing real work: a consumer routes a `files.read` through the
//! hub to a node's connector and gets actual file contents back (§1).

use std::sync::Arc;
use std::time::Duration;

use hub::{ServedHub, serve_connection};
use hub_protocol::service::ConsumerApiClient;
use hub_protocol::{LinkLabel, LinkSelector, NodeId, RoutedCall};
use node::files::{FileContents, Listing, PathRequest};
use node::{Connectors, FilesConnector};

fn attach(hub: &ServedHub) -> vox::MemoryLink {
    let (peer, hub_link) = vox::memory_link_pair(16);
    let hub = hub.clone();
    tokio::spawn(async move {
        let _ = serve_connection(hub, hub_link).await;
    });
    peer
}

fn payload(path: &str) -> Vec<u8> {
    facet_json::to_string(&PathRequest {
        path: path.to_string(),
    })
    .expect("encode")
    .into_bytes()
}

#[tokio::test]
async fn a_consumer_reads_a_file_on_a_node_through_the_hub() {
    let dir = tempdir::TempDir::new("connector-e2e").expect("tempdir");
    std::fs::write(dir.path().join("motd.txt"), "hello from the node").expect("write");

    let hub: ServedHub = ServedHub::new();

    // A node serving a real files connector.
    let mut connectors = Connectors::new();
    connectors.register(Box::new(FilesConnector::new("files-0", dir.path())));

    let config = node::NodeConfig::new(
        NodeId("alpha".to_string()),
        LinkLabel::stable(),
        Some(node::HubEndpoint("memory://e2e".to_string())),
        Vec::new(),
        "1.2.3".to_string(),
    )
    .expect("config");

    let _node_conn = tokio::time::timeout(
        Duration::from_secs(10),
        node::serve_and_register_with(attach(&hub), &config, Arc::new(connectors)),
    )
    .await
    .expect("no timeout")
    .expect("node registers");

    let consumer: ConsumerApiClient = vox::initiator_on(attach(&hub))
        .establish_connection()
        .await
        .expect("establish")
        .open_lane()
        .await
        .expect("lane");

    // Discovery advertises what the link can actually do (§2.6).
    let snapshot = consumer.topology().await.expect("topology");
    let connectors = &snapshot.nodes[0].links[0].connectors;
    assert_eq!(connectors.len(), 1);
    assert_eq!(connectors[0].id, "files-0");
    assert_eq!(connectors[0].kind, hub_protocol::ConnectorKind::Files);

    // Read a real file, through the hub, from the node's connector.
    let reply = consumer
        .route(
            LinkSelector::node(NodeId("alpha".to_string())),
            RoutedCall {
                connector: "files-0".to_string(),
                method: "read".to_string(),
                payload: payload("motd.txt"),
            },
        )
        .await
        .expect("route read");
    let contents: FileContents =
        facet_json::from_str(std::str::from_utf8(&reply.payload).unwrap()).expect("decode");
    assert_eq!(contents.contents, "hello from the node");

    // And a listing.
    let reply = consumer
        .route(
            LinkSelector::node(NodeId("alpha".to_string())),
            RoutedCall {
                connector: "files-0".to_string(),
                method: "list".to_string(),
                payload: payload("."),
            },
        )
        .await
        .expect("route list");
    let listing: Listing =
        facet_json::from_str(std::str::from_utf8(&reply.payload).unwrap()).expect("decode");
    assert_eq!(listing.entries.len(), 1);
    assert_eq!(listing.entries[0].name, "motd.txt");

    // The connector's own refusal still comes back as the node's error.
    let err = consumer
        .route(
            LinkSelector::node(NodeId("alpha".to_string())),
            RoutedCall {
                connector: "files-0".to_string(),
                method: "read".to_string(),
                payload: payload("../escape"),
            },
        )
        .await
        .expect_err("traversal must be refused");
    match err {
        vox::VoxError::User(route_error) => match *route_error {
            hub_protocol::RouteError::Node(hub_protocol::NodeError::Rejected(m)) => {
                assert!(m.contains("escapes"), "got {m}");
            }
            other => panic!("expected the node's refusal, got {other:?}"),
        },
        other => panic!("expected a domain error, got {other:?}"),
    }
}
