//! End-to-end: after the hub drops the connection, the node re-dials and
//! re-registers — reconnect forever (§2.1).

// Test code: panicking on a setup failure *is* the failure signal, and the
// `allow-*-in-tests` clippy config only reaches `#[test]` bodies, not the
// top-level helpers/impls here.
// rule: jig::rust::no-unwrap-in-lib (test carve-out)
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use hub_protocol::service::NodeIngressDispatcher;
use hub_protocol::{HubError, LinkLabel, NodeId, NodeIngress, Registration};
use node::reconnect::{Backoff, DialOutcome};
use node::{HubEndpoint, NodeConfig};

/// A stand-in hub that records every registration it receives.
#[derive(Clone)]
struct CountingHub {
    registrations: Arc<Mutex<Vec<Registration>>>,
}

impl NodeIngress for CountingHub {
    async fn register(&self, registration: Registration) -> Result<(), HubError> {
        self.registrations
            .lock()
            .expect("registrations mutex poisoned")
            .push(registration);
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
async fn node_re_registers_after_the_hub_drops_the_connection() {
    let registrations = Arc::new(Mutex::new(Vec::new()));
    let drops = Arc::new(Mutex::new(0usize));

    // Every dial attempt gets a fresh link and a fresh fake hub — the hub that
    // "comes back" after a blink. Each hub holds its connection briefly, then
    // shuts it down, which is the disconnect the node must recover from.
    let dial = {
        let registrations = Arc::clone(&registrations);
        move || {
            let registrations = Arc::clone(&registrations);
            async move {
                let (node_link, hub_link) = vox::memory_link_pair(16);
                tokio::spawn(async move {
                    let factory = vox::lane_acceptor_fn(
                        move |request: &vox::LaneRequest,
                              lane: vox::PendingLane|
                              -> Result<(), vox::LaneRejection> {
                            match request.service() {
                                "NodeIngress" => {
                                    lane.handle_with(NodeIngressDispatcher::new(CountingHub {
                                        registrations: Arc::clone(&registrations),
                                    }));
                                    Ok(())
                                }
                                _ => Err(vox::LaneRejection::new(
                                    vox::LaneRejectReason::UnknownService,
                                )),
                            }
                        },
                    );
                    let connection = vox::acceptor_on(hub_link)
                        .on_lane(factory)
                        .establish_connection()
                        .await
                        .expect("hub establish");
                    // Stay up long enough for the node to register, then blink.
                    // `shutdown()` (not a bare drop) is what signals the peer: a
                    // memory link keeps its driver tasks alive when a handle is
                    // dropped, so nothing would close. Over a real transport a
                    // dying hub sends FIN/RST and has the same effect.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    connection.shutdown().expect("hub shutdown");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    drop(connection);
                });
                Some(node_link)
            }
        }
    };

    // Stop the loop once the node has survived two disconnects.
    let observe = {
        let drops = Arc::clone(&drops);
        move |outcome: DialOutcome| {
            if outcome == DialOutcome::ConnectedThenDropped {
                let mut seen = drops.lock().expect("drops mutex poisoned");
                *seen += 1;
                return *seen < 2;
            }
            true
        }
    };

    let backoff = Backoff {
        initial: Duration::from_millis(5),
        max: Duration::from_millis(20),
    };

    tokio::time::timeout(
        Duration::from_secs(20),
        node::reconnect::run(&config(), &backoff, dial, tokio::time::sleep, observe),
    )
    .await
    .expect("reconnect loop should finish once the observer stops it");

    // Reconnect forever (§2.1): the node came back and registered again.
    let count = registrations.lock().expect("registrations mutex").len();
    assert!(
        count >= 2,
        "node should have re-registered after a disconnect, saw {count} registration(s)"
    );
    assert_eq!(*drops.lock().expect("drops mutex"), 2, "survived two blinks");
}
