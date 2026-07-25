//! Serving the hub's two inbound services over vox (§1, §2.4).
//!
//! One accept path handles both audiences, routed by service name — nodes arrive
//! on `NodeIngress`, consumers on `ConsumerApi`. A node's connection is also the
//! hub's route *back* to it: on register the hub opens a `NodeService` lane to
//! the caller and stores it, so a later `route` can reach that link.

// implements: ba47c87abc0f961967d5ccfa9254945075c16c1e9a3b8eaed1e8195e1663c241@ba47c87abc0f961967d5ccfa9254945075c16c1e9a3b8eaed1e8195e1663c241
// implements: 7c924d1c2994326c261f907f81c39f0fa676381b53f7898556893bb4f750d85c@7c924d1c2994326c261f907f81c39f0fa676381b53f7898556893bb4f750d85c

use std::sync::Arc;

use hub_protocol::service::{NodeServiceClient, NodeIngressDispatcher, ConsumerApiDispatcher};
use hub_protocol::{
    ConsumerApi, HubError, LinkSelector, NodeIngress, NodeLink, Registration, RouteError,
    RoutedCall, RoutedReply, TopologyEvent, TopologySnapshot,
};
use tokio::sync::{Mutex, watch};
use vox::{ConnectionHandle, Link, Tx};

use crate::hub::Hub;

/// The hub state as served over vox: each link is reachable by a `NodeService`
/// client opened back over its own connection.
pub type ServedHub = Hub<NodeServiceClient>;

/// Handles `NodeIngress` for **one** node connection.
///
/// The dispatcher must exist before `establish_connection()` returns the handle,
/// so the accept path publishes the handle into `connection` immediately after —
/// `register` waits for it, then opens the callback lane.
#[derive(Clone)]
struct NodeIngressHandler {
    hub: ServedHub,
    connection: watch::Receiver<Option<ConnectionHandle>>,
    /// What this connection registered, so the accept path can retire it on close.
    registered: Arc<Mutex<Option<NodeLink>>>,
}

impl NodeIngressHandler {
    /// Wait for the accept path to publish this connection's handle.
    async fn connection(&self) -> Result<ConnectionHandle, HubError> {
        let mut rx = self.connection.clone();
        loop {
            if let Some(handle) = rx.borrow().clone() {
                return Ok(handle);
            }
            if rx.changed().await.is_err() {
                return Err(HubError::Internal(
                    "connection closed before it was published".to_string(),
                ));
            }
        }
    }
}

impl NodeIngress for NodeIngressHandler {
    async fn register(&self, registration: Registration) -> Result<(), HubError> {
        let connection = self.connection().await?;

        // The node's own connection is the route back to it (§2.4): no local
        // listener, so the hub reaches the link the same way it was dialed.
        let client: NodeServiceClient = connection
            .open_lane()
            .await
            .map_err(|e| HubError::Internal(format!("could not open NodeService lane: {e:?}")))?;

        let target = NodeLink {
            node: registration.node.clone(),
            link: registration.link.clone(),
        };
        self.hub.register(registration, client).await;
        *self.registered.lock().await = Some(target);
        Ok(())
    }
}

/// Handles `ConsumerApi` for one consumer connection.
#[derive(Clone)]
struct ConsumerApiHandler {
    hub: ServedHub,
}

impl ConsumerApi for ConsumerApiHandler {
    async fn topology(&self) -> TopologySnapshot {
        self.hub.snapshot().await
    }

    async fn subscribe_topology(&self, events: Tx<TopologyEvent>) -> Result<(), HubError> {
        self.hub.subscribe(events).await;
        Ok(())
    }

    async fn route(
        &self,
        target: LinkSelector,
        call: RoutedCall,
    ) -> Result<RoutedReply, RouteError> {
        // Which link answers: requested, else stable, else an error (§2.5).
        let client = self
            .hub
            .resolve(&target)
            .await
            .map_err(RouteError::Hub)?;

        // Delivered — so a failure here is the node's, not the hub's. vox carries
        // the callee's domain error in `VoxError::User`, so the node's structured
        // `NodeError` is recovered intact; anything else is a transport-level
        // failure to reach a link the registry believed was connected, which is
        // the hub's problem to report.
        client.deliver(call).await.map_err(|e| match e {
            vox::VoxError::User(node_error) => RouteError::Node(*node_error),
            other => RouteError::Hub(HubError::Rejected(format!("{other:?}"))),
        })
    }
}

/// Serve one accepted connection until it closes, then retire what it registered.
///
/// Lanes are routed by service name, which is exactly the audience split: a node
/// may only reach `NodeIngress`, a consumer only `ConsumerApi`.
pub async fn serve_connection<L>(hub: ServedHub, link: L) -> Result<(), vox::ConnectionError>
where
    L: Link + Send + 'static,
    L::Tx: Send + 'static,
    L::Rx: Send + 'static,
{
    let (publish, connection) = watch::channel(None);
    let registered = Arc::new(Mutex::new(None));

    let node_handler = NodeIngressHandler {
        hub: hub.clone(),
        connection,
        registered: Arc::clone(&registered),
    };
    let consumer_handler = ConsumerApiHandler { hub: hub.clone() };

    let acceptor = vox::lane_acceptor_fn(
        move |request: &vox::LaneRequest, lane: vox::PendingLane| -> Result<(), vox::LaneRejection> {
            match request.service() {
                "NodeIngress" => {
                    lane.handle_with(NodeIngressDispatcher::new(node_handler.clone()));
                    Ok(())
                }
                "ConsumerApi" => {
                    lane.handle_with(ConsumerApiDispatcher::new(consumer_handler.clone()));
                    Ok(())
                }
                _ => Err(vox::LaneRejection::new(vox::LaneRejectReason::UnknownService)),
            }
        },
    );

    let handle = vox::acceptor_on(link)
        .on_lane(acceptor)
        .establish_connection()
        .await?;

    // Publish the handle so `register` can open its callback lane.
    let _ = publish.send(Some(handle.clone()));

    handle.closed().await;

    // The link is gone — discovery must reflect that (§2.6).
    if let Some(target) = registered.lock().await.take() {
        hub.disconnect(&target).await;
    }
    Ok(())
}
