#![allow(clippy::unwrap_used, clippy::expect_used)]
//! The mesh over a real TCP socket: register, discover, route.
//!
//! The hub-restart proof lives in `restart.rs`, which kills a real hub *process*:
//! in-process, vox's driver tasks own the socket and neither dropping nor
//! `shutdown()`-ing a connection closes it, so a peer never observes the hub
//! going away (see the inbox finding).

use std::time::Duration;

use hub::{HubListener, ServedHub};
use hub_protocol::service::ConsumerApiClient;
use hub_protocol::{Connector, ConnectorKind, LinkLabel, LinkSelector, NodeId, RoutedCall};
use node::reconnect::{Backoff, DialOutcome};

fn config(hub_addr: &str) -> node::NodeConfig {
    node::NodeConfig::new(
        NodeId("alpha".to_string()),
        LinkLabel::stable(),
        Some(node::HubEndpoint(format!("tcp://{hub_addr}"))),
        vec![Connector {
            id: "pm-0".to_string(),
            kind: ConnectorKind::ProcessManager,
        }],
        "1.2.3".to_string(),
    )
    .expect("config")
}

async fn consumer_for(addr: &str) -> ConsumerApiClient {
    let socket = tokio::net::TcpStream::connect(addr).await.expect("connect");
    vox::initiator_on(vox::transport::tcp::StreamLink::tcp(socket))
        .establish_connection()
        .await
        .expect("consumer establish")
        .open_lane()
        .await
        .expect("open ConsumerApi lane")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_mesh_registers_discovers_and_routes_over_tcp() {
    // ── the hub binds a real port ────────────────────────────────────────────
    let hub_state: ServedHub = ServedHub::new();
    let listener = HubListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().to_string();
    let serving = tokio::spawn({
        let hub_state = hub_state.clone();
        async move { listener.serve(hub_state).await }
    });

    // ── the node dials it and reconnects forever ─────────────────────────────
    let cfg = config(&addr);
    let node_task = tokio::spawn({
        let cfg = cfg.clone();
        async move {
            node::reconnect::run(
                &cfg,
                &Backoff {
                    initial: Duration::from_millis(20),
                    max: Duration::from_millis(100),
                },
                || {
                    let hub = cfg.hub.clone();
                    async move { node::tcp::dial(&hub).await.expect("valid address") }
                },
                tokio::time::sleep,
                |_: DialOutcome| true, // run forever
            )
            .await;
        }
    });

    // ── it shows up in live discovery ────────────────────────────────────────
    let consumer = consumer_for(&addr).await;
    let mut seen = false;
    for _ in 0..100 {
        if consumer.topology().await.expect("topology").nodes.len() == 1 {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(seen, "the node should register over TCP and be discoverable");

    // ── a routed call reaches it (the stub refuses, which proves delivery) ───
    let err = consumer
        .route(
            LinkSelector::node(NodeId("alpha".to_string())),
            RoutedCall {
                connector: "pm-0".to_string(),
                method: "spawn".to_string(),
                payload: vec![7],
            },
        )
        .await
        .expect_err("the stub refuses every call");
    match err {
        vox::VoxError::User(route_error) => match *route_error {
            hub_protocol::RouteError::Node(hub_protocol::NodeError::UnknownConnector) => {}
            other => panic!("expected the node's refusal, got {other:?}"),
        },
        other => panic!("expected a domain error, got {other:?}"),
    }

    node_task.abort();
    serving.abort();
}
