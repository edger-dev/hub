//! The node daemon: reverse-dial the hub and reconnect forever.
//!
//! Reads `NODE_ID`, `NODE_LINK` (default `stable`), and `HUB_ADDR` (or argv[1]).
//! A missing hub is a hard startup error — the hub is mandatory (§2.4).
use hub_protocol::{LinkLabel, NodeId};
use node::reconnect::{Backoff, DialOutcome};
use node::{HubEndpoint, NodeConfig};
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
    println!(
        "node {:?} link {:?} dialing hub at {}",
        config.node, config.link, config.hub.0
    );
    node::reconnect::run(
        &config,
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
