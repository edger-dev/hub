//! Serving the hub's two inbound services over vox (§1, §2.4).
//!
//! One accept path handles both audiences, routed by service name — nodes arrive
//! on `NodeIngress`, consumers on `ConsumerApi`. A node's connection is also the
//! hub's route *back* to it: on register the hub opens a `NodeService` lane to
//! the caller and stores it, so a later `route` can reach that link.

// implements: ba47c87abc0f961967d5ccfa9254945075c16c1e9a3b8eaed1e8195e1663c241@ba47c87abc0f961967d5ccfa9254945075c16c1e9a3b8eaed1e8195e1663c241
// implements: 7c924d1c2994326c261f907f81c39f0fa676381b53f7898556893bb4f750d85c@7c924d1c2994326c261f907f81c39f0fa676381b53f7898556893bb4f750d85c

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use hub_protocol::service::{ConsumerApiDispatcher, NodeIngressDispatcher, NodeServiceClient};
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

/// Connection-scoped authentication, shared by both inbound audiences.
// implements: hub::spec::connection-auth@59fc2cad98775fd2aeb0ecdd9f22ebf59a290cb37f106b3ab1f0bfcc932e4d2b
#[derive(Clone)]
struct ConnectionAuth {
    required: Option<Arc<str>>,
    authenticated: Arc<AtomicBool>,
}

impl ConnectionAuth {
    fn new(required: Option<Arc<str>>) -> Self {
        Self {
            authenticated: Arc::new(AtomicBool::new(required.is_none())),
            required,
        }
    }

    fn authenticate(&self, token: String) -> Result<(), HubError> {
        if self.required.as_deref() == Some(token.as_str()) {
            self.authenticated.store(true, Ordering::Release);
            Ok(())
        } else if self.required.is_none() {
            Ok(())
        } else {
            Err(HubError::Rejected("invalid authentication token".into()))
        }
    }

    fn check(&self) -> Result<(), HubError> {
        if self.authenticated.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(HubError::Rejected("authentication required".into()))
        }
    }
}

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
    auth: ConnectionAuth,
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
    async fn authenticate(&self, token: String) -> Result<(), HubError> {
        self.auth.authenticate(token)
    }

    async fn register(&self, registration: Registration) -> Result<(), HubError> {
        self.auth.check()?;
        // `::` names a peer hub's node: no node may take it.
        if registration.node.0.contains(hub_protocol::PEER_SEPARATOR) {
            return Err(HubError::Rejected(format!(
                "node {:?}: `{}` is reserved for peer hubs' nodes",
                registration.node.0,
                hub_protocol::PEER_SEPARATOR
            )));
        }
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
    auth: ConnectionAuth,
}

impl ConsumerApi for ConsumerApiHandler {
    async fn authenticate(&self, token: String) -> Result<(), HubError> {
        self.auth.authenticate(token)
    }

    async fn topology(&self) -> Result<TopologySnapshot, HubError> {
        self.auth.check()?;
        let mut snapshot = self.hub.snapshot().await;
        snapshot.nodes.extend(self.hub.peers().nodes().await);
        Ok(snapshot)
    }

    async fn subscribe_topology(&self, events: Tx<TopologyEvent>) -> Result<(), HubError> {
        self.auth.check()?;
        self.hub.subscribe(events).await;
        Ok(())
    }

    async fn publish(&self, event: hub_protocol::HubEvent) -> Result<u32, HubError> {
        self.auth.check()?;
        // `hub/` topics are the hub's own word to its subscribers.
        if event.topic.starts_with("hub/") {
            return Err(HubError::Rejected(format!(
                "topic {:?}: `hub/` topics are the hub's own",
                event.topic
            )));
        }
        if event.payload.len() > hub_protocol::MAX_EVENT_PAYLOAD {
            return Err(HubError::Rejected(format!(
                "a {}-byte payload is over the {}-byte ceiling: publish a pointer, not the thing",
                event.payload.len(),
                hub_protocol::MAX_EVENT_PAYLOAD
            )));
        }
        Ok(self.hub.publish(event).await)
    }

    async fn subscribe_events(
        &self,
        prefix: String,
        events: Tx<hub_protocol::HubEvent>,
    ) -> Result<(), HubError> {
        self.auth.check()?;
        // The channel streams within this call: hold the call open until the
        // subscriber is gone. While it lives, peers' own events are carried
        // here (one subscription per peer, shared by every subscriber).
        let _watching = self.hub.peers().watch();
        let gone = self.hub.subscribe_events(prefix, events).await;
        let _ = gone.await;
        Ok(())
    }

    async fn local_topology(&self) -> Result<TopologySnapshot, HubError> {
        self.auth.check()?;
        Ok(self.hub.snapshot().await)
    }

    async fn subscribe_local_events(
        &self,
        prefix: String,
        events: Tx<hub_protocol::HubEvent>,
    ) -> Result<(), HubError> {
        self.auth.check()?;
        let gone = self.hub.subscribe_local_events(prefix, events).await;
        let _ = gone.await;
        Ok(())
    }

    async fn route(
        &self,
        target: LinkSelector,
        call: RoutedCall,
    ) -> Result<RoutedReply, RouteError> {
        self.auth.check().map_err(RouteError::Hub)?;
        // A peer hub's node goes one hop, its name stripped; this hub's own
        // name is just a longer way to say a local node.
        let target = match target.node.split_peer() {
            Some((hub, node)) if Some(hub) == self.hub.peers().name() => LinkSelector {
                node,
                link: target.link,
            },
            // One hop only: a name still qualified after it is refused.
            Some((_, node)) if node.split_peer().is_some() => {
                return Err(RouteError::Hub(HubError::UnknownNode));
            }
            Some((hub, node)) => {
                let peer = self
                    .hub
                    .peers()
                    .client(hub)
                    .await
                    .ok_or(RouteError::Hub(HubError::UnknownNode))?;
                let target = LinkSelector {
                    node,
                    link: target.link,
                };
                return peer.route(target, call).await.map_err(|e| match e {
                    vox::VoxError::User(route_error) => *route_error,
                    other => {
                        RouteError::Hub(HubError::Rejected(format!("peer hub {hub}: {other:?}")))
                    }
                });
            }
            None => target,
        };
        // Which link answers: requested, else stable, else an error (§2.5).
        let client = self.hub.resolve(&target).await.map_err(RouteError::Hub)?;

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
    serve_connection_with(hub, link, |_| {}).await
}

/// As [`serve_connection`], but hands the established [`ConnectionHandle`] to
/// `on_handle` first.
///
/// The caller needs the handle to shut the connection down deliberately: vox
/// runs a connection on its own driver tasks, which own the socket, so dropping
/// or aborting the serving task alone does **not** close it.
pub async fn serve_connection_with<L>(
    hub: ServedHub,
    link: L,
    on_handle: impl FnOnce(ConnectionHandle),
) -> Result<(), vox::ConnectionError>
where
    L: Link + Send + 'static,
    L::Tx: Send + 'static,
    L::Rx: Send + 'static,
{
    let (publish, connection) = watch::channel(None);
    let registered = Arc::new(Mutex::new(None));
    let auth = ConnectionAuth::new(hub.auth_token());

    let node_handler = NodeIngressHandler {
        hub: hub.clone(),
        connection,
        registered: Arc::clone(&registered),
        auth: auth.clone(),
    };
    let consumer_handler = ConsumerApiHandler {
        hub: hub.clone(),
        auth,
    };

    let acceptor = vox::lane_acceptor_fn(
        move |request: &vox::LaneRequest,
              lane: vox::PendingLane|
              -> Result<(), vox::LaneRejection> {
            match request.service() {
                "NodeIngress" => {
                    lane.handle_with(NodeIngressDispatcher::new(node_handler.clone()));
                    Ok(())
                }
                "ConsumerApi" => {
                    lane.handle_with(ConsumerApiDispatcher::new(consumer_handler.clone()));
                    Ok(())
                }
                _ => Err(vox::LaneRejection::new(
                    vox::LaneRejectReason::UnknownService,
                )),
            }
        },
    );

    let handle = vox::acceptor_on(link)
        .on_lane(acceptor)
        .establish_connection()
        .await?;

    // Publish the handle so `register` can open its callback lane.
    let _ = publish.send(Some(handle.clone()));
    on_handle(handle.clone());

    handle.closed().await;

    // The link is gone — discovery must reflect that (§2.6).
    if let Some(target) = registered.lock().await.take() {
        hub.disconnect(&target).await;
    }
    Ok(())
}
