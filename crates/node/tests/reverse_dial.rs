//! End-to-end: the node reverse-dials a fake hub over a vox memory link,
//! registers, and answers a ping the hub calls back (§2.1).

// This whole file is test code — panicking on a setup failure *is* the failure
// signal. The `allow-*-in-tests` clippy config only reaches `#[test]` bodies,
// not the top-level helpers/impls here, so allow the restriction lints file-wide.
// rule: jig::rust::no-unwrap-in-lib (test carve-out)
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use hub_protocol::service::{NodeIngressDispatcher, NodeServiceClient};
use hub_protocol::{HubError, LinkLabel, NodeId, NodeIngress, Registration};
use node::{HubEndpoint, NodeConfig};

/// A stand-in hub that records the registration it receives.
#[derive(Clone)]
struct FakeHub {
    seen: Arc<Mutex<Option<Registration>>>,
}

impl NodeIngress for FakeHub {
    async fn register(&self, registration: Registration) -> Result<(), HubError> {
        *self.seen.lock().expect("registration mutex poisoned") = Some(registration);
        Ok(())
    }
}

fn config() -> NodeConfig {
    NodeConfig::new(
        NodeId("alpha".to_string()),
        LinkLabel::stable(),
        Some(HubEndpoint("memory://test".to_string())),
        vec![],
        "0.0.0".to_string(),
    )
    .expect("config")
}

#[tokio::test]
async fn node_reverse_dials_registers_and_answers_ping() {
    let (node_link, hub_link) = vox::memory_link_pair(16);
    let seen = Arc::new(Mutex::new(None));

    // Fake hub: accept the NodeIngress lane and record what registers.
    let factory = {
        let seen = Arc::clone(&seen);
        vox::lane_acceptor_fn(
            move |request: &vox::LaneRequest,
                  lane: vox::PendingLane|
                  -> Result<(), vox::LaneRejection> {
                match request.service() {
                    "NodeIngress" => {
                        lane.handle_with(NodeIngressDispatcher::new(FakeHub {
                            seen: Arc::clone(&seen),
                        }));
                        Ok(())
                    }
                    _ => Err(vox::LaneRejection::new(vox::LaneRejectReason::UnknownService)),
                }
            },
        )
    };

    let hub = tokio::spawn(async move {
        vox::acceptor_on(hub_link)
            .on_lane(factory)
            .establish_connection()
            .await
            .expect("hub establish")
    });

    // The node dials out, serves NodeService, and registers.
    let node_conn = node::serve_and_register(node_link, &config())
        .await
        .expect("node reverse-dial + register");
    let hub_conn = hub.await.expect("hub task");

    // The registration reached the hub, carrying the node's identity.
    assert_eq!(
        seen.lock().expect("mutex").as_ref().expect("registered").node,
        NodeId("alpha".to_string()),
    );

    // The hub can now call the node back over NodeService.
    let node_client: NodeServiceClient = hub_conn.open_lane().await.expect("open NodeService lane");
    let pong = node_client.ping().await.expect("ping");
    assert_eq!(pong.node, NodeId("alpha".to_string()));
    assert_eq!(pong.agent_version, "0.0.0");

    drop(node_conn);
}
