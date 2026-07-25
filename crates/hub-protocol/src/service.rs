//! The vox service traits — the RPC surface every role dials.
//!
//! Two services, mirroring the two directions of the mesh:
//!
//! - [`HubService`] is dialed *into* the hub, by both **nodes** (to register) and
//!   **consumers** (to discover and to route calls). Every party binds to this;
//!   it is the stable boundary (§1).
//! - [`NodeService`] is what the hub dials back *toward a node*. The node answers
//!   here — it never binds a local listener (§2.4).
//!
//! The `#[vox::service]` macro generates a `…Client` and a `…Dispatcher` for each
//! trait; this crate ships the *contract only*, so no implementations live here.

use vox::Tx;

use crate::discovery::{TopologyEvent, TopologySnapshot};
use crate::errors::{HubError, NodeError};
use crate::identity::NodeId;
use crate::registration::Registration;
use crate::routing::{RoutedCall, RoutedReply};
use crate::targeting::LinkSelector;

/// A node's liveness answer — also the out-of-band deploy check (§5).
///
/// After a self-restarting deploy the honest success signal is "the node answers
/// a trivial command again"; [`NodeService::ping`] is that command, and `Pong`
/// carries enough to confirm *which* version answered.
#[derive(facet::Facet, Debug, Clone, PartialEq, Eq)]
pub struct Pong {
    pub node: NodeId,
    pub agent_version: String,
}

/// Dialed into the hub by nodes and consumers alike (§1).
#[vox::service]
pub trait HubService {
    /// A reverse-dialing link announces itself and its connectors (§2.1, §2.4).
    ///
    /// Re-registration by a known `(node, link)` is an expected reconnect, not an
    /// error (§2.4).
    async fn register(&self, registration: Registration) -> Result<(), HubError>;

    /// A point-in-time view of the connected mesh (§2.6).
    ///
    /// Reads are link-agnostic (§2.5): any connected link can back the snapshot.
    async fn topology(&self) -> TopologySnapshot;

    /// Subscribe to live topology changes; the hub sends each change into
    /// `events` (§2.6). Pair a first [`HubService::topology`] snapshot with this
    /// tail to hold a current view.
    async fn subscribe_topology(&self, events: Tx<TopologyEvent>) -> Result<(), HubError>;

    /// Route one call to a connector on the selected node/link (§1, §2.5).
    ///
    /// The hub picks the link per `target` — defaulting to and falling back on
    /// `stable` — then relays `call` to the node without inspecting its payload.
    async fn route(
        &self,
        target: LinkSelector,
        call: RoutedCall,
    ) -> Result<RoutedReply, HubError>;
}

/// Dialed by the hub toward a node; the node answers here (§2.4).
#[vox::service]
pub trait NodeService {
    /// Liveness probe. Trivial by design — it is the out-of-band signal that a
    /// node survived a deploy restart (§5).
    async fn ping(&self) -> Pong;

    /// Deliver a routed consumer call to one of this node's connectors (§1).
    ///
    /// `start`-type calls are link-scoped (this daemon's toolchain); reads are
    /// link-agnostic (§2.5). The connector owns the payload's meaning.
    async fn deliver(&self, call: RoutedCall) -> Result<RoutedReply, NodeError>;
}

#[cfg(test)]
mod tests {
    // The `#[vox::service]` macro generates a client + dispatcher per trait.
    // Naming them here is a compile-time assertion that the macro accepted our
    // trait shapes (args, streaming `Tx`, and `Result` returns).
    #[allow(unused_imports)]
    use super::{
        HubServiceClient, HubServiceDispatcher, NodeServiceClient, NodeServiceDispatcher,
    };

    #[test]
    fn generated_client_and_dispatcher_types_exist() {
        // If this test module compiles, the imports above resolved — the vox
        // service macro produced the expected surface for both traits.
    }

    #[test]
    fn pong_round_trips() {
        use super::Pong;
        use crate::identity::NodeId;
        let v = Pong {
            node: NodeId("alpha".to_string()),
            agent_version: "0.0.0".to_string(),
        };
        let json = facet_json::to_string(&v).unwrap();
        let back: Pong = facet_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }
}
