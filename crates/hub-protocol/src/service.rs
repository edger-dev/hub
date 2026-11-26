//! The vox service traits — the RPC surface each role dials.
//!
//! Six services, split by *who dials whom* so a peer only ever sees the
//! methods its role should call:
//!
//! - [`NodeIngress`] is dialed *into* the hub by a **node** that reverse-dials to
//!   offer itself (§2.1, §2.4). Its one job is to register.
//! - [`PeerIngress`] is dialed *into* the hub by a **peer hub** to register its
//!   name and offer a reverse consumer lane over the same connection.
//! - [`SessionIngress`] registers a live agent session; [`SessionService`] is
//!   what the hub calls back to deliver an acknowledged direct message.
//! - [`ConsumerApi`] is dialed *into* the hub by a **consumer** (the console or
//!   the agent shim) to discover the mesh and route calls (§1, §2.6).
//! - [`NodeService`] is what the hub dials back *toward a node*. The node answers
//!   here — it never binds a local listener (§2.4).
//!
//! The `#[vox::service]` macro generates a `…Client` and a `…Dispatcher` for each
//! trait; this crate ships the *contract only*, so no implementations live here.

// implements: 14de2dfd6b1bbd978bfb44e8a3bbe718702ddf5c46cc59aa754d002932002d3f@14de2dfd6b1bbd978bfb44e8a3bbe718702ddf5c46cc59aa754d002932002d3f
// implements: ba47c87abc0f961967d5ccfa9254945075c16c1e9a3b8eaed1e8195e1663c241@ba47c87abc0f961967d5ccfa9254945075c16c1e9a3b8eaed1e8195e1663c241

use vox::Tx;

use crate::discovery::{TopologyEvent, TopologySnapshot};
use crate::errors::{HubError, NodeError, RouteError};
use crate::events::HubEvent;
use crate::identity::NodeId;
use crate::registration::Registration;
use crate::routing::{RoutedCall, RoutedReply};
use crate::session::{SessionInfo, SessionMessage};
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
    /// Authenticate this connection before any other operation.
    async fn authenticate(&self, token: String) -> Result<(), HubError>;

    /// A reverse-dialing link announces itself and its connectors (§2.1, §2.4).
    ///
    /// Re-registration by a known `(node, link)` is an expected reconnect, not an
    /// error (§2.4).
    async fn register(&self, registration: Registration) -> Result<(), HubError>;
}

/// Dialed into the hub by a reverse-dialing peer hub.
// implements: 586748b7b572d2285f4747e555105426f76892c08966826018bfc00ae8d48efa@701163e19fa9e575f1e75f0b347c08ef3b640403f42e6282fab59a6917561c1a
#[vox::service]
pub trait PeerIngress {
    /// Authenticate this connection before any other operation.
    async fn authenticate(&self, token: String) -> Result<(), HubError>;

    /// Announce the dialing hub's name and offer a reverse consumer lane.
    async fn register(&self, peer: String) -> Result<(), HubError>;
}

/// Dialed into a hub by a live agent session.
// implements: 396fba0cab2ac48f5fdf59a3258f377fc04aa9c6d7f42d27f4be9b1eafcb8b38@396fba0cab2ac48f5fdf59a3258f377fc04aa9c6d7f42d27f4be9b1eafcb8b38
#[vox::service]
pub trait SessionIngress {
    /// Authenticate this connection before any other operation.
    async fn authenticate(&self, token: String) -> Result<(), HubError>;

    /// Register this connection's one session and its reverse delivery lane.
    async fn register(&self, session: SessionInfo) -> Result<(), HubError>;
}

/// Dialed into the hub by a consumer — the console or the agent shim (§1, §2.6).
#[vox::service]
pub trait ConsumerApi {
    /// Authenticate this connection before any other operation.
    async fn authenticate(&self, token: String) -> Result<(), HubError>;

    /// A point-in-time view of the connected mesh (§2.6).
    ///
    /// Reads are link-agnostic (§2.5): any connected link can back the snapshot.
    async fn topology(&self) -> Result<TopologySnapshot, HubError>;

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

    /// Publish one event; the hub queues it for every subscriber whose prefix
    /// matches its topic, never reading the payload and never waiting on a
    /// subscriber, and answers how many it was queued for. Live only: nobody
    /// listening means nobody receives it. `hub/` topics and payloads over
    /// [`crate::MAX_EVENT_PAYLOAD`] are refused.
    async fn publish(&self, event: HubEvent) -> Result<u32, HubError>;

    /// Subscribe to every event whose topic starts with `prefix` — a plain
    /// string prefix, so end it with `/` (`kudo/context/c1/`, not
    /// `kudo/context/c1`, which would also match `c10`); an empty prefix is
    /// everything. The hub sends each into `events`, in the order it received
    /// them. A channel streams within its call, so this call stays open while
    /// the subscription lives — run it on a task of its own; it holds one of
    /// the connection's concurrent-call slots, and the same connection may
    /// publish meanwhile. The first event is [`crate::SUBSCRIBED_TOPIC`]: from
    /// then on nothing published is missed. A subscriber that falls too far
    /// behind is dropped (its call returns); it re-reads what it follows.
    async fn subscribe_events(&self, prefix: String, events: Tx<HubEvent>) -> Result<(), HubError>;

    /// This hub's own nodes, with no peer's — what a peer hub reads, so two
    /// hubs asking each other never recurse.
    // implements: a297d880b240fa86f9872753ebc3009c0d3af0dd572ec9eaa2a17a3f815f8d45@55ffeceb39795324eb405923fdba976d6b9a2e959425e4dee7511dc681794e87
    async fn local_topology(&self) -> Result<TopologySnapshot, HubError>;

    /// Live sessions on this hub and its one-hop peers, with qualified names.
    async fn sessions(&self) -> Result<Vec<SessionInfo>, HubError>;

    /// This hub's own live sessions, with no peer's — the peer-safe view.
    async fn local_sessions(&self) -> Result<Vec<SessionInfo>, HubError>;

    /// Send from this connection's registered session to a local or one-hop peer.
    async fn send_session(
        &self,
        target: String,
        message_id: String,
        body: String,
    ) -> Result<(), HubError>;

    /// Forward an origin-qualified message from this connection's registered peer.
    async fn forward_session(
        &self,
        target: String,
        message: SessionMessage,
    ) -> Result<(), HubError>;

    /// As [`ConsumerApi::subscribe_events`], but only events published to
    /// this hub itself, never one it carries from a peer — what a peer hub
    /// follows, so an event crosses one hop and never echoes back.
    // implements: a297d880b240fa86f9872753ebc3009c0d3af0dd572ec9eaa2a17a3f815f8d45@55ffeceb39795324eb405923fdba976d6b9a2e959425e4dee7511dc681794e87
    async fn subscribe_local_events(
        &self,
        prefix: String,
        events: Tx<HubEvent>,
    ) -> Result<(), HubError>;
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

/// Dialed by a hub toward its registered agent session.
// implements: 396fba0cab2ac48f5fdf59a3258f377fc04aa9c6d7f42d27f4be9b1eafcb8b38@396fba0cab2ac48f5fdf59a3258f377fc04aa9c6d7f42d27f4be9b1eafcb8b38
#[vox::service]
pub trait SessionService {
    /// Accept one message into the session's local mailbox before returning.
    async fn deliver(&self, message: SessionMessage) -> Result<(), HubError>;
}

#[cfg(test)]
mod tests {
    // The `#[vox::service]` macro generates a client + dispatcher per trait.
    // Naming them here is a compile-time assertion that the macro accepted our
    // trait shapes (args, streaming `Tx`, and `Result` returns).
    #[allow(unused_imports)]
    use super::{
        ConsumerApiClient, ConsumerApiDispatcher, NodeIngressClient, NodeIngressDispatcher,
        NodeServiceClient, NodeServiceDispatcher, PeerIngressClient, PeerIngressDispatcher,
        SessionIngressClient, SessionIngressDispatcher, SessionServiceClient,
        SessionServiceDispatcher,
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
