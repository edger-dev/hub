//! The vox service traits — the RPC surface each role dials.
//!
//! Three services, split by *who dials whom* so a peer only ever sees the
//! methods its role should call:
//!
//! - [`NodeIngress`] is dialed *into* the hub by a **node** that reverse-dials to
//!   offer itself (§2.1, §2.4). Its one job is to register.
//! - [`ConsumerApi`] is dialed *into* the hub by a **consumer** (the console or
//!   the agent shim) to discover the mesh and route calls (§1, §2.6).
//! - [`NodeService`] is what the hub dials back *toward a node*. The node answers
//!   here — it never binds a local listener (§2.4).
//!
//! The `#[vox::service]` macro generates a `…Client` and a `…Dispatcher` for each
//! trait; this crate ships the *contract only*, so no implementations live here.

// implements: 14de2dfd6b1bbd978bfb44e8a3bbe718702ddf5c46cc59aa754d002932002d3f@14de2dfd6b1bbd978bfb44e8a3bbe718702ddf5c46cc59aa754d002932002d3f

use vox::Tx;

use crate::discovery::{TopologyEvent, TopologySnapshot};
use crate::errors::{HubError, NodeError, RouteError};
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

/// Dialed into the hub by a reverse-dialing node (§2.1, §2.4).
#[vox::service]
pub trait NodeIngress {
    /// A reverse-dialing link announces itself and its connectors (§2.1, §2.4).
    ///
    /// Re-registration by a known `(node, link)` is an expected reconnect, not an
    /// error (§2.4).
    async fn register(&self, registration: Registration) -> Result<(), HubError>;
}

/// Dialed into the hub by a consumer — the console or the agent shim (§1, §2.6).
#[vox::service]
pub trait ConsumerApi {
    /// A point-in-time view of the connected mesh (§2.6).
    ///
    /// Reads are link-agnostic (§2.5): any connected link can back the snapshot.
    async fn topology(&self) -> TopologySnapshot;

    /// Subscribe to live topology changes; the hub sends each change into
    /// `events` (§2.6). Pair a first [`ConsumerApi::topology`] snapshot with this
    /// tail to hold a current view.
    async fn subscribe_topology(&self, events: Tx<TopologyEvent>) -> Result<(), HubError>;

    /// Route one call to a connector on the selected node/link (§1, §2.5).
    ///
    /// The hub picks the link per `target` — defaulting to and falling back on
    /// `stable` — then relays `call` to the node without inspecting its payload.
    /// The error preserves *where* it failed: [`RouteError::Hub`] if the hub could
    /// not deliver, [`RouteError::Node`] if the connector refused.
    async fn route(
        &self,
        target: LinkSelector,
        call: RoutedCall,
    ) -> Result<RoutedReply, RouteError>;
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
        ConsumerApiClient, ConsumerApiDispatcher, NodeIngressClient, NodeIngressDispatcher,
        NodeServiceClient, NodeServiceDispatcher,
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
