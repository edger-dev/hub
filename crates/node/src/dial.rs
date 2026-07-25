//! Reverse-dialing the hub (§2.1).
//!
//! The node dials *out* over a vox [`Link`], serves [`hub_protocol::NodeService`]
//! inbound (so the hub can call `ping`/`deliver` back), and registers its link.
//! It never listens for the hub to reach in (§2.4).

// implements: d661c899202c18f1d5a968f14d5c87a9d6b422aa62e450dd2134a1c1bfdec4cc@d661c899202c18f1d5a968f14d5c87a9d6b422aa62e450dd2134a1c1bfdec4cc

use std::fmt;

use hub_protocol::service::{NodeIngressClient, NodeServiceDispatcher};
use vox::{ConnectionHandle, Link};

use crate::config::NodeConfig;
use crate::stub::StubNode;

/// Why a reverse-dial attempt failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialError {
    /// The connection handshake did not complete.
    Establish(String),
    /// The `NodeIngress` lane could not be opened.
    OpenIngress(String),
    /// The `register` call failed — the hub was unreachable or refused it. (vox
    /// folds the domain `HubError` into the call error, so it arrives as text.)
    Register(String),
}

impl fmt::Display for DialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DialError::Establish(e) => write!(f, "hub connection handshake failed: {e}"),
            DialError::OpenIngress(e) => write!(f, "could not open the NodeIngress lane: {e}"),
            DialError::Register(e) => write!(f, "register failed: {e}"),
        }
    }
}

impl std::error::Error for DialError {}

/// Reverse-dial the hub over `link`, serve `NodeService`, and register (§2.1).
///
/// On success the returned [`ConnectionHandle`] owns the live connection: hold it
/// to stay connected (the hub can now call this node back), drop it to disconnect.
pub async fn serve_and_register<L>(
    link: L,
    config: &NodeConfig,
) -> Result<ConnectionHandle, DialError>
where
    L: Link + Send + 'static,
    L::Tx: Send + 'static,
    L::Rx: Send + 'static,
{
    let stub = StubNode::from_config(config);

    let connection = vox::initiator_on(link)
        .on_lane(NodeServiceDispatcher::new(stub))
        .establish_connection()
        .await
        .map_err(|e| DialError::Establish(format!("{e:?}")))?;

    let hub: NodeIngressClient = connection
        .open_lane()
        .await
        .map_err(|e| DialError::OpenIngress(format!("{e:?}")))?;

    hub.register(config.registration())
        .await
        .map_err(|e| DialError::Register(format!("{e:?}")))?;

    Ok(connection)
}
