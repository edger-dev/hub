//! The node daemon: reverse-dial the hub and reconnect forever.
//!
//! Reads `NODE_ID`, `NODE_LINK` (default `stable`), and `HUB_ADDR` (or argv[1]).
//! A missing hub is a hard startup error — the hub is mandatory (§2.4).
use std::sync::Arc;

use hub_protocol::{LinkLabel, NodeId};
use node::reconnect::{Backoff, DialOutcome};
use node::{Connectors, FilesConnector, HubEndpoint, NodeConfig};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = std::env::var("NODE_ID").unwrap_or_else(|_| {
        hostname_fallback()
    });
    let link = std::env::var("NODE_LINK").unwrap_or_else(|_| LinkLabel::STABLE.to_string());
    let hub = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("HUB_ADDR").ok())
        .map(HubEndpoint);
    let config = NodeConfig::new(
        NodeId(node),
        LinkLabel(link),
        hub,
        Vec::new(),
        env!("CARGO_PKG_VERSION").to_string(),
    )?;
    // Fail fast on an address we could never dial, rather than retrying forever.
    config.hub.dial_target()?;
    // Capabilities this node offers. A files root is optional; with none
    // registered the node still connects and simply refuses routed calls (§1).
    let mut connectors = Connectors::new();
    if let Ok(root) = std::env::var("NODE_FILES_ROOT") {
        connectors.register(Box::new(FilesConnector::new("files-0", &root)));
        println!("serving files connector `files-0` rooted at {root}");
    }
    let connectors = Arc::new(connectors);

    println!(
        "node {:?} link {:?} dialing hub at {} ({} connector(s))",
        config.node,
        config.link,
        config.hub.0,
        connectors.descriptors().len()
    );
    node::reconnect::run_with(
        &config,
        Arc::clone(&connectors),
        &Backoff::default(),
        || {
            let hub = config.hub.clone();
            async move { node::tcp::dial(&hub).await.ok().flatten() }
        },
        tokio::time::sleep,
        |outcome: DialOutcome| {
            match outcome {
                DialOutcome::ConnectedThenDropped => println!("hub connection closed; re-dialing"),
                DialOutcome::Failed(e) => println!("dial failed ({e}); retrying"),
            }
            true
        },
    )
    .await;
    Ok(())
}
/// Fall back to the machine's hostname as the node name (§2.4: explicit → env →
/// hostname).
fn hostname_fallback() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "unknown-node".to_string())
}
