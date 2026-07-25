//! Reverse-dialing the hub (§2.1).
//!
//! The node dials *out* over a vox [`Link`], serves [`hub_protocol::NodeService`]
//! inbound (so the hub can call `ping`/`deliver` back), and registers its link.
//! It never listens for the hub to reach in (§2.4).

// implements: d661c899202c18f1d5a968f14d5c87a9d6b422aa62e450dd2134a1c1bfdec4cc@d661c899202c18f1d5a968f14d5c87a9d6b422aa62e450dd2134a1c1bfdec4cc

use std::fmt;
use std::sync::Arc;

use hub_protocol::HubError;
use hub_protocol::service::{NodeIngressClient, NodeServiceDispatcher};
use vox::{ConnectionHandle, Link};

use crate::config::NodeConfig;
use crate::connector::Connectors;
use crate::daemon::NodeDaemon;

/// Why a reverse-dial attempt failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialError {
    /// The connection handshake did not complete.
    Establish(String),
    /// The `NodeIngress` lane could not be opened.
    OpenIngress(String),
    /// The hub received the registration and refused it.
    Rejected(HubError),
    /// The `register` call never got an answer (transport-level failure).
    RegisterTransport(String),
}

impl fmt::Display for DialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DialError::Establish(e) => write!(f, "hub connection handshake failed: {e}"),
            DialError::OpenIngress(e) => write!(f, "could not open the NodeIngress lane: {e}"),
            DialError::Rejected(e) => write!(f, "hub refused registration: {e:?}"),
            DialError::RegisterTransport(e) => write!(f, "register call failed: {e}"),
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
    serve_and_register_with(link, config, Arc::new(Connectors::new())).await
}

/// As [`serve_and_register`], but serving `connectors` (§1).
///
/// The registration announces exactly what is registered, so discovery reports
/// what the link can actually do (§2.6).
pub async fn serve_and_register_with<L>(
    link: L,
    config: &NodeConfig,
    connectors: Arc<Connectors>,
) -> Result<ConnectionHandle, DialError>
where
    L: Link + Send + 'static,
    L::Tx: Send + 'static,
    L::Rx: Send + 'static,
{
    let daemon = NodeDaemon::new(config, Arc::clone(&connectors));

    let connection = vox::initiator_on(link)
        .on_lane(NodeServiceDispatcher::new(daemon))
        .establish_connection()
        .await
        .map_err(|e| DialError::Establish(format!("{e:?}")))?;

    let hub: NodeIngressClient = connection
        .open_lane()
        .await
        .map_err(|e| DialError::OpenIngress(format!("{e:?}")))?;

    // vox carries the callee's domain error in `VoxError::User`, so the hub's
    // structured `HubError` is recovered intact rather than stringified.
    let mut registration = config.registration();
    if !connectors.is_empty() {
        registration.connectors = connectors.descriptors();
    }
    hub.register(registration)
        .await
        .map_err(|e| match e {
            vox::VoxError::User(hub_error) => DialError::Rejected(*hub_error),
            other => DialError::RegisterTransport(format!("{other:?}")),
        })?;

    Ok(connection)
}
