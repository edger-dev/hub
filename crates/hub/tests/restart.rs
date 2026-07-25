#![allow(clippy::unwrap_used, clippy::expect_used)]
//! The reconnect proof (§2.1): kill a real hub **process** and bring it back —
//! the node re-registers on its own.
//!
//! The hub runs as a separate process on purpose. In-process, vox's driver tasks
//! own the socket and neither dropping nor `shutdown()`-ing a connection closes
//! it, so a peer never observes the hub going away. A killed process closes its
//! sockets for real, which is exactly what production does.

use std::process::{Child, Command};
use std::time::Duration;

use hub_protocol::service::ConsumerApiClient;
use hub_protocol::{LinkLabel, NodeId};
use node::reconnect::{Backoff, DialOutcome};

fn start_hub(addr: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_hub"))
        .arg(addr)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn hub binary")
}

async fn registered_nodes(addr: &str) -> Option<usize> {
    let socket = tokio::net::TcpStream::connect(addr).await.ok()?;
    let consumer: ConsumerApiClient = vox::initiator_on(vox::transport::tcp::StreamLink::tcp(socket))
        .establish_connection()
        .await
        .ok()?
        .open_lane()
        .await
        .ok()?;
    Some(consumer.topology().await.ok()?.nodes.len())
}

/// Poll until the hub reports `want` nodes, or give up.
async fn wait_for_nodes(addr: &str, want: usize, tries: u32) -> bool {
    for _ in 0..tries {
        if registered_nodes(addr).await == Some(want) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_re_registers_after_the_hub_process_restarts() {
    // A fixed high port: the restarted hub must reuse the same address.
    let addr = "127.0.0.1:15496".to_string();
    let mut hub = start_hub(&addr);

    let config = node::NodeConfig::new(
        NodeId("alpha".to_string()),
        LinkLabel::stable(),
        Some(node::HubEndpoint(addr.clone())),
        Vec::new(),
        "1.2.3".to_string(),
    )
    .expect("config");

    // The node runs in-process, dialing the hub process; short backoff so the
    // recovery is quick to observe.
    let node_task = tokio::spawn({
        let config = config.clone();
        async move {
            node::reconnect::run(
                &config,
                &Backoff {
                    initial: Duration::from_millis(50),
                    max: Duration::from_millis(300),
                },
                || {
                    let hub = config.hub.clone();
                    async move { node::tcp::dial(&hub).await.ok().flatten() }
                },
                tokio::time::sleep,
                |_: DialOutcome| true,
            )
            .await;
        }
    });

    assert!(
        wait_for_nodes(&addr, 1, 50).await,
        "the node should register with the hub process"
    );

    // ── the hub dies ─────────────────────────────────────────────────────────
    hub.kill().expect("kill hub");
    hub.wait().expect("reap hub");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── and comes back on the same address ───────────────────────────────────
    let mut hub = start_hub(&addr);

    // The node re-registers unaided — the hub merely blinked (§2.1).
    let recovered = wait_for_nodes(&addr, 1, 100).await;

    node_task.abort();
    let _ = hub.kill();
    let _ = hub.wait();

    assert!(
        recovered,
        "the node should re-register itself after the hub restarts"
    );
}
