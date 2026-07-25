#![allow(clippy::unwrap_used, clippy::expect_used)]
//! The real `node` crate against the real hub: reverse-dial, register, appear in
//! live discovery, and answer a consumer's routed call (§2.1, §2.5, §2.6).

use std::time::Duration;

use hub_protocol::service::ConsumerApiClient;
use hub_protocol::{Connector, ConnectorKind, LinkLabel, LinkSelector, NodeId, RoutedCall};
use hub::{ServedHub, serve_connection};

fn node_config(link: LinkLabel) -> node::NodeConfig {
    node::NodeConfig::new(
        NodeId("alpha".to_string()),
        link,
        Some(node::HubEndpoint("memory://e2e".to_string())),
        vec![Connector {
            id: "pm-0".to_string(),
            kind: ConnectorKind::ProcessManager,
        }],
        "1.2.3".to_string(),
    )
    .expect("config")
}

/// Attach a peer to the hub over a fresh memory link; returns the peer's side.
fn attach(hub: &ServedHub) -> vox::MemoryLink {
    let (peer_link, hub_link) = vox::memory_link_pair(16);
    let hub = hub.clone();
    tokio::spawn(async move {
        let _ = serve_connection(hub, hub_link).await;
    });
    peer_link
}

#[tokio::test]
async fn node_registers_appears_in_discovery_and_answers_a_routed_call() {
    let hub: ServedHub = ServedHub::new();

    // 1. The real node daemon reverse-dials the real hub and registers.
    let node_conn = tokio::time::timeout(
        Duration::from_secs(10),
        node::serve_and_register(attach(&hub), &node_config(LinkLabel::stable())),
    )
    .await
    .expect("no timeout")
    .expect("node should register with the hub");

    // 2. A consumer sees it in live discovery (§2.6).
    let consumer: ConsumerApiClient = vox::initiator_on(attach(&hub))
        .establish_connection()
        .await
        .expect("consumer establish")
        .open_lane()
        .await
        .expect("open ConsumerApi lane");

    let snapshot = consumer.topology().await.expect("topology");
    assert_eq!(snapshot.nodes.len(), 1, "the node is discoverable");
    assert_eq!(snapshot.nodes[0].node, NodeId("alpha".to_string()));
    assert_eq!(snapshot.nodes[0].links.len(), 1);
    assert!(snapshot.nodes[0].links[0].link.is_stable());
    assert_eq!(snapshot.nodes[0].links[0].connectors[0].id, "pm-0");

    // 3. A routed call reaches the node. The stub refuses every call, so the
    //    error must come back as the *node's* — proving the hub delivered it and
    //    preserved which side failed.
    let result = consumer
        .route(
            LinkSelector::node(NodeId("alpha".to_string())),
            RoutedCall {
                connector: "pm-0".to_string(),
                method: "spawn".to_string(),
                payload: vec![1, 2, 3],
            },
        )
        .await;
    // Structurally the node's own refusal — the hub delivered it and the error
    // says which side failed (§ route-error spec).
    match result.expect_err("the stub refuses every call") {
        vox::VoxError::User(route_error) => match *route_error {
            hub_protocol::RouteError::Node(hub_protocol::NodeError::UnknownConnector) => {}
            other => panic!("expected the node's UnknownConnector, got {other:?}"),
        },
        other => panic!("expected a domain error, got {other:?}"),
    }

    // 4. Routing to an unknown node is the hub's failure, not a node's (§2.5).
    let result = consumer
        .route(
            LinkSelector::node(NodeId("ghost".to_string())),
            RoutedCall {
                connector: "pm-0".to_string(),
                method: "spawn".to_string(),
                payload: vec![],
            },
        )
        .await;
    match result.expect_err("routing to a ghost must fail") {
        vox::VoxError::User(route_error) => match *route_error {
            hub_protocol::RouteError::Hub(hub_protocol::HubError::UnknownNode) => {}
            other => panic!("expected the hub's UnknownNode, got {other:?}"),
        },
        other => panic!("expected a domain error, got {other:?}"),
    }

    drop(node_conn);
}
