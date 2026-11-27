//! Serving the hub's inbound services over vox (§1, §2.4).
//!
//! One accept path handles every audience, routed by service name — nodes arrive
//! on `NodeIngress`, peer hubs on `PeerIngress`, and consumers on `ConsumerApi`.
//! A node's connection is also the hub's route *back* to it: on register the hub
//! opens a `NodeService` lane to the caller and stores it. Peer registration
//! similarly opens a reverse `ConsumerApi` lane over the same connection.

// implements: ba47c87abc0f961967d5ccfa9254945075c16c1e9a3b8eaed1e8195e1663c241@ba47c87abc0f961967d5ccfa9254945075c16c1e9a3b8eaed1e8195e1663c241
// implements: 7c924d1c2994326c261f907f81c39f0fa676381b53f7898556893bb4f750d85c@7c924d1c2994326c261f907f81c39f0fa676381b53f7898556893bb4f750d85c

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use hub_protocol::service::{
    ConsumerApiClient, ConsumerApiDispatcher, DiagnosticsApiDispatcher, NodeIngressDispatcher,
    NodeServiceClient, PeerIngressDispatcher, SessionIngressDispatcher, SessionServiceClient,
};
use hub_protocol::{
    AuthChallenge, AuthProof, ConsumerApi, HubError, LinkSelector, NodeIngress, NodeLink,
    PeerIngress, Registration, RouteError, RoutedCall, RoutedReply, SessionInfo, SessionIngress,
    SessionMessage, TopologyEvent, TopologySnapshot,
};
use tokio::sync::{Mutex, watch};
use vox::{ConnectionHandle, Link, Tx};

use crate::hub::Hub;

/// The hub state as served over vox: each link is reachable by a `NodeService`
/// client opened back over its own connection.
pub type ServedHub = Hub<NodeServiceClient>;

/// Connection-scoped authentication, shared by both inbound audiences.
// implements: hub::spec::connection-auth@bd6ac42fe5e75451450c5183ae1892b19f92c718138de69e222e428197d940bd
#[derive(Clone)]
pub(crate) struct ConnectionAuth {
    required: Option<Arc<str>>,
    trust: hub_protocol::TrustStore,
    authenticated: Arc<AtomicBool>,
    peer: Arc<Mutex<Option<String>>>,
    challenges: Arc<Mutex<std::collections::BTreeMap<String, (std::time::Instant, AuthChallenge)>>>,
}

impl ConnectionAuth {
    pub(crate) fn new(required: Option<Arc<str>>, trust: hub_protocol::TrustStore) -> Self {
        Self {
            authenticated: Arc::new(AtomicBool::new(required.is_none() && trust.is_empty())),
            required,
            trust,
            peer: Arc::new(Mutex::new(None)),
            challenges: Arc::default(),
        }
    }

    pub(crate) fn authenticate(&self, token: String) -> Result<(), HubError> {
        if self.required.as_deref() == Some(token.as_str()) {
            self.authenticated.store(true, Ordering::Release);
            Ok(())
        } else if self.required.is_none() && self.trust.is_empty() {
            Ok(())
        } else {
            Err(HubError::Rejected("invalid authentication token".into()))
        }
    }

    pub(crate) async fn challenge(&self, hub: &str, role: &str) -> Result<AuthChallenge, HubError> {
        if self.trust.is_empty() {
            return Err(HubError::Rejected(
                "key authentication is not configured".into(),
            ));
        }
        let challenge = AuthChallenge::new(hub, role).map_err(HubError::Rejected)?;
        self.challenges.lock().await.insert(
            role.to_string(),
            (std::time::Instant::now(), challenge.clone()),
        );
        Ok(challenge)
    }

    pub(crate) async fn authenticate_key(
        &self,
        role: &str,
        proof: AuthProof,
    ) -> Result<(), HubError> {
        let Some((issued, challenge)) = self.challenges.lock().await.remove(role) else {
            return Err(HubError::Rejected(
                "authentication challenge required".into(),
            ));
        };
        if issued.elapsed() > std::time::Duration::from_secs(60) {
            return Err(HubError::Rejected(
                "authentication challenge expired".into(),
            ));
        }
        let peer = self
            .trust
            .verify(&challenge, &proof)
            .map_err(HubError::Rejected)?;
        *self.peer.lock().await = Some(peer);
        self.authenticated.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) async fn check(&self) -> Result<Option<String>, HubError> {
        if self.authenticated.load(Ordering::Acquire) {
            Ok(self.peer.lock().await.clone())
        } else {
            Err(HubError::Rejected("authentication required".into()))
        }
    }

    pub(crate) async fn check_identity(&self, expected: &str) -> Result<(), HubError> {
        if self.check().await?.is_some_and(|peer| peer != expected) {
            return Err(HubError::Rejected(format!(
                "authenticated peer cannot register {expected:?}"
            )));
        }
        Ok(())
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

    async fn authentication_challenge(&self) -> Result<AuthChallenge, HubError> {
        self.auth
            .challenge(self.hub.peers().name().unwrap_or("unnamed"), "node")
            .await
    }

    async fn authenticate_key(&self, proof: AuthProof) -> Result<(), HubError> {
        self.auth.authenticate_key("node", proof).await
    }

    async fn register(&self, registration: Registration) -> Result<(), HubError> {
        self.auth.check_identity(&registration.node.0).await?;
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

enum ReverseRegistration {
    Open(Option<(String, u64)>),
    Closed,
}

enum SessionRegistration {
    Open(Option<(SessionInfo, u64)>),
    Closed,
}

#[derive(Clone)]
enum ConsumerPeer {
    Registered(Arc<Mutex<ReverseRegistration>>),
    Configured(String),
}

/// Handles `PeerIngress` for one peer connection.
#[derive(Clone)]
struct PeerIngressHandler {
    hub: ServedHub,
    connection: watch::Receiver<Option<ConnectionHandle>>,
    registered: Arc<Mutex<ReverseRegistration>>,
    auth: ConnectionAuth,
}

impl PeerIngressHandler {
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

// implements: 586748b7b572d2285f4747e555105426f76892c08966826018bfc00ae8d48efa@701163e19fa9e575f1e75f0b347c08ef3b640403f42e6282fab59a6917561c1a
impl PeerIngress for PeerIngressHandler {
    async fn authenticate(&self, token: String) -> Result<(), HubError> {
        self.auth.authenticate(token)
    }

    async fn authentication_challenge(&self) -> Result<AuthChallenge, HubError> {
        self.auth
            .challenge(self.hub.peers().name().unwrap_or("unnamed"), "peer")
            .await
    }

    async fn authenticate_key(&self, proof: AuthProof) -> Result<(), HubError> {
        self.auth.authenticate_key("peer", proof).await
    }

    async fn register(&self, peer: String) -> Result<(), HubError> {
        self.auth.check_identity(&peer).await?;
        let mut registered = self.registered.lock().await;
        match &*registered {
            ReverseRegistration::Closed => {
                return Err(HubError::Rejected("peer connection is closed".into()));
            }
            ReverseRegistration::Open(Some(_)) => {
                return Err(HubError::Rejected(
                    "peer connection already registered a name".into(),
                ));
            }
            ReverseRegistration::Open(None) => {}
        }
        let connection = self.connection().await?;
        let client: ConsumerApiClient = connection.open_lane().await.map_err(|error| {
            HubError::Internal(format!(
                "could not open reverse ConsumerApi lane: {error:?}"
            ))
        })?;
        if let Some(key) = self.hub.peers().signing_key() {
            let challenge = client.authentication_challenge().await.map_err(|error| {
                HubError::Rejected(format!(
                    "reverse authentication challenge failed: {error:?}"
                ))
            })?;
            let proof = key.sign(&challenge).map_err(HubError::Rejected)?;
            client.authenticate_key(proof).await.map_err(|error| {
                HubError::Rejected(format!(
                    "could not authenticate reverse ConsumerApi lane: {error:?}"
                ))
            })?;
        } else if let Some(token) = self.hub.auth_token() {
            client
                .authenticate(token.to_string())
                .await
                .map_err(|error| {
                    HubError::Rejected(format!(
                        "could not authenticate reverse ConsumerApi lane: {error:?}"
                    ))
                })?;
        }
        let registration = self
            .hub
            .peers()
            .register_reverse(&peer, client, connection.clone())
            .map_err(HubError::Rejected)?;
        *registered = ReverseRegistration::Open(Some((peer, registration)));
        Ok(())
    }
}

/// Handles `SessionIngress` for one session connection.
#[derive(Clone)]
struct SessionIngressHandler {
    hub: ServedHub,
    connection: watch::Receiver<Option<ConnectionHandle>>,
    registered: Arc<Mutex<SessionRegistration>>,
    auth: ConnectionAuth,
}

impl SessionIngressHandler {
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

// implements: 396fba0cab2ac48f5fdf59a3258f377fc04aa9c6d7f42d27f4be9b1eafcb8b38@b6936c5b4bf599c582c8c986e716b9a763d5df72b5a8594423bdc25a94092501
impl SessionIngress for SessionIngressHandler {
    async fn authenticate(&self, token: String) -> Result<(), HubError> {
        self.auth.authenticate(token)
    }

    async fn authentication_challenge(&self) -> Result<AuthChallenge, HubError> {
        self.auth
            .challenge(self.hub.peers().name().unwrap_or("unnamed"), "session")
            .await
    }

    async fn authenticate_key(&self, proof: AuthProof) -> Result<(), HubError> {
        self.auth.authenticate_key("session", proof).await
    }

    async fn register(&self, session: SessionInfo) -> Result<(), HubError> {
        self.auth.check_identity(&session.node).await?;
        let mut registered = self.registered.lock().await;
        match &*registered {
            SessionRegistration::Closed => {
                return Err(HubError::Rejected("session connection is closed".into()));
            }
            SessionRegistration::Open(Some(_)) => {
                return Err(HubError::Rejected(
                    "session connection already registered a session".into(),
                ));
            }
            SessionRegistration::Open(None) => {}
        }
        let connection = self.connection().await?;
        let client: SessionServiceClient = connection.open_lane().await.map_err(|error| {
            HubError::Internal(format!("could not open SessionService lane: {error:?}"))
        })?;
        let registration = self.hub.register_session(session.clone(), client)?;
        *registered = SessionRegistration::Open(Some((session, registration)));
        Ok(())
    }
}

/// Handles `ConsumerApi` for one consumer connection.
#[derive(Clone)]
struct ConsumerApiHandler {
    hub: ServedHub,
    auth: ConnectionAuth,
    session: Option<Arc<Mutex<SessionRegistration>>>,
    peer: Option<ConsumerPeer>,
}

impl ConsumerApiHandler {
    async fn session_sender(&self) -> Result<SessionInfo, HubError> {
        let Some(session) = &self.session else {
            return Err(HubError::Rejected(
                "this connection has no registered session".into(),
            ));
        };
        match &*session.lock().await {
            SessionRegistration::Open(Some((info, registration)))
                if self.hub.session_is_current(&info.id, *registration) =>
            {
                Ok(info.clone())
            }
            _ => Err(HubError::Rejected(
                "this connection has no live registered session".into(),
            )),
        }
    }

    async fn peer_name(&self) -> Result<String, HubError> {
        match self.peer.as_ref() {
            Some(ConsumerPeer::Configured(name)) => Ok(name.clone()),
            Some(ConsumerPeer::Registered(registration)) => match &*registration.lock().await {
                ReverseRegistration::Open(Some((name, generation)))
                    if self.hub.peers().reverse_is_current(name, *generation) =>
                {
                    Ok(name.clone())
                }
                _ => Err(HubError::Rejected("peer registration is not live".into())),
            },
            None => Err(HubError::Rejected(
                "this connection has no registered peer".into(),
            )),
        }
    }

    fn validate_message(message: &SessionMessage) -> Result<(), HubError> {
        if message.id.is_empty() {
            return Err(HubError::Rejected("message id must not be empty".into()));
        }
        if message.body.len() > hub_protocol::MAX_SESSION_MESSAGE_BYTES {
            return Err(HubError::Rejected(format!(
                "message body is over {} bytes",
                hub_protocol::MAX_SESSION_MESSAGE_BYTES
            )));
        }
        Ok(())
    }

    async fn deliver_local(&self, target: String, message: SessionMessage) -> Result<(), HubError> {
        let Some((_, client)) = self.hub.session_client(&target) else {
            return Err(HubError::UnknownSession);
        };
        client.deliver(message).await.map_err(|error| match error {
            vox::VoxError::User(error) => *error,
            other => HubError::Rejected(format!("session {target}: {other:?}")),
        })
    }

    async fn send_to_peer(
        &self,
        peer: String,
        target: String,
        mut message: SessionMessage,
    ) -> Result<(), HubError> {
        let Some(origin) = self.hub.peers().name().map(str::to_owned) else {
            return Err(HubError::Rejected(
                "cross-hub session messages need this hub to have a name".into(),
            ));
        };
        message.from.qualify(&origin);
        let client = self
            .hub
            .peers()
            .client(&peer)
            .await
            .ok_or(HubError::UnknownSession)?;
        client
            .forward_session(target, message)
            .await
            .map_err(|error| match error {
                vox::VoxError::User(error) => *error,
                other => HubError::Rejected(format!("peer hub {peer}: {other:?}")),
            })
    }
}

impl ConsumerApi for ConsumerApiHandler {
    async fn authenticate(&self, token: String) -> Result<(), HubError> {
        self.auth.authenticate(token)
    }

    async fn authentication_challenge(&self) -> Result<AuthChallenge, HubError> {
        self.auth
            .challenge(self.hub.peers().name().unwrap_or("unnamed"), "consumer")
            .await
    }

    async fn authenticate_key(&self, proof: AuthProof) -> Result<(), HubError> {
        self.auth.authenticate_key("consumer", proof).await
    }

    async fn topology(&self) -> Result<TopologySnapshot, HubError> {
        self.auth.check().await?;
        let mut snapshot = self.hub.snapshot().await;
        snapshot.nodes.extend(self.hub.peers().nodes().await);
        Ok(snapshot)
    }

    async fn subscribe_topology(&self, events: Tx<TopologyEvent>) -> Result<(), HubError> {
        self.auth.check().await?;
        self.hub.subscribe(events).await;
        Ok(())
    }

    async fn publish(&self, event: hub_protocol::HubEvent) -> Result<u32, HubError> {
        self.auth.check().await?;
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
        self.auth.check().await?;
        // The channel streams within this call: hold the call open until the
        // subscriber is gone. While it lives, peers' own events are carried
        // here (one subscription per peer, shared by every subscriber).
        let _watching = self.hub.peers().watch();
        let gone = self.hub.subscribe_events(prefix, events).await;
        let _ = gone.await;
        Ok(())
    }

    async fn local_topology(&self) -> Result<TopologySnapshot, HubError> {
        self.auth.check().await?;
        Ok(self.hub.snapshot().await)
    }

    async fn sessions(&self) -> Result<Vec<hub_protocol::SessionInfo>, HubError> {
        self.auth.check().await?;
        let mut sessions = self.hub.local_sessions();
        sessions.extend(self.hub.peers().sessions().await);
        sessions.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(sessions)
    }

    async fn local_sessions(&self) -> Result<Vec<hub_protocol::SessionInfo>, HubError> {
        self.auth.check().await?;
        Ok(self.hub.local_sessions())
    }

    // implements: 396fba0cab2ac48f5fdf59a3258f377fc04aa9c6d7f42d27f4be9b1eafcb8b38@b6936c5b4bf599c582c8c986e716b9a763d5df72b5a8594423bdc25a94092501
    async fn send_session(
        &self,
        target: String,
        message_id: String,
        body: String,
    ) -> Result<(), HubError> {
        self.auth.check().await?;
        let message = SessionMessage {
            id: message_id,
            from: self.session_sender().await?,
            body,
        };
        Self::validate_message(&message)?;
        if let Some((peer, local)) = target.split_once(hub_protocol::PEER_SEPARATOR) {
            if Some(peer) == self.hub.peers().name() {
                return self.deliver_local(local.to_string(), message).await;
            }
            if local.contains(hub_protocol::PEER_SEPARATOR) {
                return Err(HubError::UnknownSession);
            }
            self.send_to_peer(peer.to_string(), local.to_string(), message)
                .await
        } else {
            self.deliver_local(target, message).await
        }
    }

    async fn forward_session(
        &self,
        target: String,
        message: hub_protocol::SessionMessage,
    ) -> Result<(), HubError> {
        self.auth.check().await?;
        Self::validate_message(&message)?;
        let peer = self.peer_name().await?;
        let prefix = format!("{peer}{}", hub_protocol::PEER_SEPARATOR);
        let valid_sender = [&message.from.id, &message.from.node]
            .into_iter()
            .all(|value| {
                value.strip_prefix(&prefix).is_some_and(|rest| {
                    !rest.is_empty() && !rest.contains(hub_protocol::PEER_SEPARATOR)
                })
            });
        if !valid_sender {
            return Err(HubError::Rejected(
                "forwarded sender does not belong to the registered peer".into(),
            ));
        }
        if target.contains(hub_protocol::PEER_SEPARATOR) {
            return Err(HubError::UnknownSession);
        }
        self.deliver_local(target, message).await
    }

    async fn subscribe_local_events(
        &self,
        prefix: String,
        events: Tx<hub_protocol::HubEvent>,
    ) -> Result<(), HubError> {
        self.auth.check().await?;
        let gone = self.hub.subscribe_local_events(prefix, events).await;
        let _ = gone.await;
        Ok(())
    }

    async fn route(
        &self,
        target: LinkSelector,
        call: RoutedCall,
    ) -> Result<RoutedReply, RouteError> {
        self.auth.check().await.map_err(RouteError::Hub)?;
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

/// Serve this hub's `ConsumerApi` on an outbound peer connection, so the peer
/// can open its reverse lane immediately after `PeerIngress` registration.
pub fn peer_consumer_acceptor(hub: &ServedHub, peer: &str) -> impl vox::LaneAcceptor {
    let handler = ConsumerApiHandler {
        hub: hub.clone(),
        auth: ConnectionAuth::new(hub.auth_token(), hub.auth_trust()),
        session: None,
        peer: Some(ConsumerPeer::Configured(peer.to_string())),
    };
    vox::lane_acceptor_fn(move |request: &vox::LaneRequest, lane: vox::PendingLane| {
        if request.service() != "ConsumerApi" {
            return Err(vox::LaneRejection::new(
                vox::LaneRejectReason::UnknownService,
            ));
        }
        lane.handle_with(ConsumerApiDispatcher::new(handler.clone()));
        Ok(())
    })
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
    let registered_peer = Arc::new(Mutex::new(ReverseRegistration::Open(None)));
    let registered_session = Arc::new(Mutex::new(SessionRegistration::Open(None)));
    let auth = ConnectionAuth::new(hub.auth_token(), hub.auth_trust());

    let node_handler = NodeIngressHandler {
        hub: hub.clone(),
        connection: connection.clone(),
        registered: Arc::clone(&registered),
        auth: auth.clone(),
    };
    let peer_handler = PeerIngressHandler {
        hub: hub.clone(),
        connection: connection.clone(),
        registered: Arc::clone(&registered_peer),
        auth: auth.clone(),
    };
    let session_handler = SessionIngressHandler {
        hub: hub.clone(),
        connection: connection.clone(),
        registered: Arc::clone(&registered_session),
        auth: auth.clone(),
    };
    let consumer_handler = ConsumerApiHandler {
        hub: hub.clone(),
        auth,
        session: Some(Arc::clone(&registered_session)),
        peer: Some(ConsumerPeer::Registered(Arc::clone(&registered_peer))),
    };
    let diagnostics_handler = crate::diagnostics::DiagnosticsApiHandler::new(hub.clone());

    let acceptor = vox::lane_acceptor_fn(
        move |request: &vox::LaneRequest,
              lane: vox::PendingLane|
              -> Result<(), vox::LaneRejection> {
            match request.service() {
                "NodeIngress" => {
                    lane.handle_with(NodeIngressDispatcher::new(node_handler.clone()));
                    Ok(())
                }
                "PeerIngress" => {
                    lane.handle_with(PeerIngressDispatcher::new(peer_handler.clone()));
                    Ok(())
                }
                "SessionIngress" => {
                    lane.handle_with(SessionIngressDispatcher::new(session_handler.clone()));
                    Ok(())
                }
                "ConsumerApi" => {
                    lane.handle_with(ConsumerApiDispatcher::new(consumer_handler.clone()));
                    Ok(())
                }
                "DiagnosticsApi" => {
                    lane.handle_with(DiagnosticsApiDispatcher::new(diagnostics_handler.clone()));
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
    let reverse = std::mem::replace(
        &mut *registered_peer.lock().await,
        ReverseRegistration::Closed,
    );
    if let ReverseRegistration::Open(Some((peer, registration))) = reverse {
        hub.peers().unregister_reverse(&peer, registration);
    }
    let session = std::mem::replace(
        &mut *registered_session.lock().await,
        SessionRegistration::Closed,
    );
    if let SessionRegistration::Open(Some((session, registration))) = session {
        hub.unregister_session(&session.id, registration);
    }
    Ok(())
}
