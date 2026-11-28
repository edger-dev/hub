//! Peer hubs: each hub dials a static list of named peers and reaches their
//! nodes one hop away, as `<peer>::<node>`.
//!
//! A peer is dialled as a consumer of the other hub and registers its own name
//! for a reverse consumer lane on the same connection. Its own nodes come from
//! `local_topology`, calls go through `route` with the name stripped (and
//! refused if still qualified), and events through one `subscribe_local_events`
//! per peer, held while anyone here subscribes. The two `local` methods keep
//! everything one hop. Older peers can remain forward-only.
//!
//! implements: a297d880b240fa86f9872753ebc3009c0d3af0dd572ec9eaa2a17a3f815f8d45@55ffeceb39795324eb405923fdba976d6b9a2e959425e4dee7511dc681794e87
//! implements: 586748b7b572d2285f4747e555105426f76892c08966826018bfc00ae8d48efa@701163e19fa9e575f1e75f0b347c08ef3b640403f42e6282fab59a6917561c1a

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hub_protocol::service::{ConsumerApiClient, ForwardedDiagnosticsApiClient, PeerIngressClient};
use hub_protocol::{
    DiagnosticStatus, HubEvent, NodeInfo, PEER_SEPARATOR, PeerDiagnostic, SessionInfo,
    TopologySnapshot,
};
use tokio::sync::{mpsc, watch};
use vox::ConnectionHandle;

/// Opens a connection to one peer hub.
pub type Dialer = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<ConnectionHandle, String>> + Send>>
        + Send
        + Sync,
>;

/// How long a peer's topology may take before its nodes are left out.
const TOPOLOGY_WAIT: Duration = Duration::from_secs(2);
/// The longest wait between redials (or resubscribes) of a peer that is down.
const REDIAL_MAX: Duration = Duration::from_secs(30);
/// A peer that stops answering pings is dropped within about this twice.
const KEEPALIVE: Duration = Duration::from_secs(10);

struct Peer {
    name: String,
    client: Option<ConsumerApiClient>,
    handle: Option<ConnectionHandle>,
    configured: bool,
    registration: u64,
}

/// This hub's name and its peers, each redialled forever.
#[derive(Clone)]
pub struct Peers {
    name: Option<String>,
    peers: Arc<Mutex<Vec<Peer>>>,
    /// How many subscribers here follow events: peers' events are carried
    /// only while this is above zero.
    watchers: Arc<watch::Sender<usize>>,
    /// Where carried events go: the hub's fan-out, set by the hub.
    sink: Arc<Mutex<Option<mpsc::Sender<Carried>>>>,
    auth_token: Option<String>,
    signing_key: Option<Arc<hub_protocol::SigningKey>>,
    next_registration: Arc<AtomicU64>,
}

/// What a follower hands the hub's fan-out.
pub(crate) enum Carried {
    /// A peer's own event.
    Event(HubEvent),
    /// A peer's subscription ended while followed: events may be lost.
    Lost,
}

impl Default for Peers {
    fn default() -> Self {
        Self {
            name: None,
            peers: Arc::default(),
            watchers: Arc::new(watch::channel(0).0),
            sink: Arc::default(),
            auth_token: None,
            signing_key: None,
            next_registration: Arc::new(AtomicU64::new(1)),
        }
    }
}

/// Held while a subscriber here follows events; dropping it lets the peers'
/// subscriptions go once nobody follows.
pub struct Watching(Arc<watch::Sender<usize>>);

impl Drop for Watching {
    fn drop(&mut self) {
        self.0.send_modify(|n| *n = n.saturating_sub(1));
    }
}

impl Peers {
    /// A hub named `name`, with no peers yet.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }

    /// Authenticate outbound peer connections with this token.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Sign outbound and reverse peer authentication with this key.
    pub fn with_signing_key(mut self, key: hub_protocol::SigningKey) -> Self {
        self.signing_key = Some(Arc::new(key));
        self
    }

    pub fn signing_key(&self) -> Option<Arc<hub_protocol::SigningKey>> {
        self.signing_key.clone()
    }

    /// This hub's name, when it has one.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn with<R>(&self, f: impl FnOnce(&mut Vec<Peer>) -> R) -> R {
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut peers)
    }

    fn set(&self, peer: &str, client: Option<ConsumerApiClient>, handle: Option<ConnectionHandle>) {
        self.with(|peers| {
            if let Some(p) = peers.iter_mut().find(|p| p.name == peer) {
                p.client = client;
                p.handle = handle;
            }
        });
    }

    /// Send carried events into `sink` (the hub's fan-out).
    pub(crate) fn set_sink(&self, sink: mpsc::Sender<Carried>) {
        *self
            .sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sink);
    }

    fn sink(&self) -> Option<mpsc::Sender<Carried>> {
        self.sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Count a subscriber here as following events while the guard lives.
    pub fn watch(&self) -> Watching {
        self.watchers.send_modify(|n| *n += 1);
        Watching(Arc::clone(&self.watchers))
    }

    /// Attach a consumer opened back over an authenticated peer connection.
    pub(crate) fn register_reverse(
        &self,
        peer: &str,
        client: ConsumerApiClient,
        handle: ConnectionHandle,
    ) -> Result<u64, String> {
        self.validate_name(peer)?;
        let registration = self.next_registration.fetch_add(1, Ordering::Relaxed);
        let follow = self.with(|peers| {
            if let Some(existing) = peers.iter_mut().find(|p| p.name == peer) {
                if existing.configured {
                    return Err(format!("peer {peer:?} is already configured"));
                }
                existing.client = Some(client);
                existing.handle = Some(handle);
                existing.registration = registration;
                return Ok(false);
            }
            peers.push(Peer {
                name: peer.to_string(),
                client: Some(client),
                handle: Some(handle),
                configured: false,
                registration,
            });
            Ok(true)
        })?;
        if follow {
            let peers = self.clone();
            let peer = peer.to_string();
            tokio::spawn(async move { peers.follow(peer).await });
        }
        Ok(registration)
    }

    pub(crate) fn diagnostics(&self) -> Vec<PeerDiagnostic> {
        self.with(|peers| {
            peers
                .iter()
                .map(|peer| PeerDiagnostic {
                    name: peer.name.clone(),
                    direction: if peer.configured {
                        "outbound"
                    } else {
                        "reverse"
                    }
                    .into(),
                    registration: peer.registration,
                    current: peer.client.is_some() && peer.handle.is_some(),
                    last_error: None,
                })
                .collect()
        })
    }

    pub(crate) async fn diagnostics_client(
        &self,
        peer: &str,
    ) -> Result<ForwardedDiagnosticsApiClient, (DiagnosticStatus, String)> {
        let Some(handle) = self.with(|peers| {
            peers
                .iter()
                .find(|existing| existing.name == peer)
                .and_then(|existing| existing.handle.clone())
        }) else {
            return Err((
                DiagnosticStatus::Failed,
                format!("peer {peer:?} is not live"),
            ));
        };
        let client: ForwardedDiagnosticsApiClient = handle.open_lane().await.map_err(|error| {
            (
                DiagnosticStatus::Unsupported,
                format!("DiagnosticsApi is unsupported: {error:?}"),
            )
        })?;
        if let Some(key) = &self.signing_key {
            let challenge = client.authentication_challenge().await.map_err(|error| {
                (
                    DiagnosticStatus::Unsupported,
                    format!("diagnostic authentication is unsupported: {error:?}"),
                )
            })?;
            let proof = key
                .sign(&challenge)
                .map_err(|error| (DiagnosticStatus::Failed, error))?;
            client.authenticate_key(proof).await.map_err(|error| {
                (
                    DiagnosticStatus::Failed,
                    format!("diagnostic authentication failed: {error:?}"),
                )
            })?;
        } else if let Some(token) = &self.auth_token {
            client.authenticate(token.clone()).await.map_err(|error| {
                (
                    DiagnosticStatus::Failed,
                    format!("diagnostic authentication failed: {error:?}"),
                )
            })?;
        }
        Ok(client)
    }

    /// Clear a reverse registration only if its connection is still current.
    pub(crate) fn unregister_reverse(&self, peer: &str, registration: u64) {
        self.with(|peers| {
            if let Some(existing) = peers.iter_mut().find(|p| p.name == peer)
                && !existing.configured
                && existing.registration == registration
            {
                existing.client = None;
                existing.handle = None;
            }
        });
    }

    pub(crate) fn reverse_is_current(&self, peer: &str, registration: u64) -> bool {
        self.with(|peers| {
            peers.iter().any(|existing| {
                existing.name == peer
                    && !existing.configured
                    && existing.client.is_some()
                    && existing.registration == registration
            })
        })
    }

    fn validate_name(&self, peer: &str) -> Result<(), String> {
        if Some(peer) == self.name() {
            return Err(format!("peer {peer:?} is this hub's own name"));
        }
        if peer.is_empty() || peer.contains(PEER_SEPARATOR) {
            return Err(format!("peer {peer:?}: a name without {PEER_SEPARATOR}"));
        }
        Ok(())
    }

    /// Dial `peer` with `dialer`, and redial whenever it drops, backing off
    /// up to thirty seconds while it is down; follow its events while anyone
    /// here does. Refuses this hub's own name, a name already added, and a
    /// name holding the peer separator. Needs a tokio runtime.
    pub fn add(&self, peer: impl Into<String>, dialer: Dialer) -> Result<(), String> {
        let peer = peer.into();
        self.validate_name(&peer)?;
        let added = self.with(|peers| {
            if peers.iter().any(|p| p.name == peer) {
                return false;
            }
            peers.push(Peer {
                name: peer.clone(),
                client: None,
                handle: None,
                configured: true,
                registration: 0,
            });
            true
        });
        if !added {
            return Err(format!("peer {peer:?} is already added"));
        }
        let (peers, name) = (self.clone(), peer.clone());
        tokio::spawn(async move {
            let mut wait = Duration::from_secs(1);
            loop {
                if let Ok(handle) = dialer().await {
                    let lane: Result<ConsumerApiClient, _> = handle.open_lane().await;
                    match lane {
                        Ok(client) => {
                            let authenticated = match &peers.signing_key {
                                Some(key) => match client.authentication_challenge().await {
                                    Ok(challenge) => match key.sign(&challenge) {
                                        Ok(proof) => client.authenticate_key(proof).await.is_ok(),
                                        Err(_) => false,
                                    },
                                    Err(_) => false,
                                },
                                None => match &peers.auth_token {
                                    Some(token) => client.authenticate(token.clone()).await.is_ok(),
                                    None => true,
                                },
                            };
                            if authenticated {
                                if let Some(local_name) = peers.name().map(str::to_owned)
                                    && let Ok(ingress) =
                                        handle.open_lane::<PeerIngressClient>().await
                                {
                                    let authenticated = match &peers.signing_key {
                                        Some(key) => match ingress.authentication_challenge().await
                                        {
                                            Ok(challenge) => match key.sign(&challenge) {
                                                Ok(proof) => {
                                                    ingress.authenticate_key(proof).await.is_ok()
                                                }
                                                Err(_) => false,
                                            },
                                            Err(_) => false,
                                        },
                                        None => match &peers.auth_token {
                                            Some(token) => {
                                                ingress.authenticate(token.clone()).await.is_ok()
                                            }
                                            None => true,
                                        },
                                    };
                                    if authenticated {
                                        // Older peers and existing mutual configurations
                                        // remain useful in the forward direction.
                                        let _ = ingress.register(local_name).await;
                                    }
                                }
                                peers.set(&name, Some(client), Some(handle.clone()));
                                wait = Duration::from_secs(1);
                                handle.closed().await;
                                peers.set(&name, None, None);
                            } else {
                                let _ = handle.shutdown();
                            }
                        }
                        // Dropping a handle does not close its connection.
                        Err(_) => {
                            let _ = handle.shutdown();
                        }
                    }
                }
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(REDIAL_MAX);
            }
        });
        let peers = self.clone();
        tokio::spawn(async move { peers.follow(peer).await });
        Ok(())
    }

    pub(crate) fn is_configured(&self, peer: &str) -> bool {
        self.with(|peers| {
            peers
                .iter()
                .any(|existing| existing.name == peer && existing.configured)
        })
    }

    /// The peers' names, in the order they were added.
    pub fn names(&self) -> Vec<String> {
        self.with(|peers| peers.iter().map(|p| p.name.clone()).collect())
    }

    /// The live connection to `peer`, if it is up.
    pub async fn client(&self, peer: &str) -> Option<ConsumerApiClient> {
        self.with(|peers| {
            peers
                .iter()
                .find(|p| p.name == peer)
                .and_then(|p| p.client.clone())
        })
    }

    /// Every reachable peer's own nodes, qualified by the peer's name, asked
    /// of all peers at once; one down or slower than two seconds is left out.
    pub async fn nodes(&self) -> Vec<NodeInfo> {
        let mut asks = tokio::task::JoinSet::new();
        for peer in self.names() {
            let Some(client) = self.client(&peer).await else {
                continue;
            };
            asks.spawn(async move {
                match tokio::time::timeout(TOPOLOGY_WAIT, client.local_topology()).await {
                    Ok(Ok(TopologySnapshot { nodes })) => nodes
                        .into_iter()
                        .map(|n| NodeInfo {
                            node: hub_protocol::NodeId::qualified(&peer, &n.node),
                            links: n.links,
                        })
                        .collect(),
                    _ => Vec::new(),
                }
            });
        }
        let mut nodes = Vec::new();
        while let Some(Ok(theirs)) = asks.join_next().await {
            nodes.extend(theirs);
        }
        nodes.sort_by(|a, b| a.node.cmp(&b.node));
        nodes
    }

    /// Every reachable peer's live local sessions, qualified one hop away.
    pub async fn sessions(&self) -> Vec<SessionInfo> {
        let mut asks = tokio::task::JoinSet::new();
        for peer in self.names() {
            let Some(client) = self.client(&peer).await else {
                continue;
            };
            asks.spawn(async move {
                match tokio::time::timeout(TOPOLOGY_WAIT, client.local_sessions()).await {
                    Ok(Ok(sessions)) => sessions
                        .into_iter()
                        .map(|mut session| {
                            session.qualify(&peer);
                            session
                        })
                        .collect(),
                    _ => Vec::new(),
                }
            });
        }
        let mut sessions = Vec::new();
        while let Some(Ok(theirs)) = asks.join_next().await {
            sessions.extend(theirs);
        }
        sessions.sort_by(|a, b| a.id.cmp(&b.id));
        sessions
    }

    /// Carry `peer`'s own events into the hub's fan-out while anyone here
    /// follows events: one subscription per peer, whatever the number of
    /// subscribers, resubscribed (with backoff) whenever it ends.
    async fn follow(&self, peer: String) {
        let mut watching = self.watchers.subscribe();
        let mut wait = Duration::from_secs(1);
        loop {
            if !wait_until(&mut watching, |n| n > 0).await {
                return;
            }
            let (Some(client), Some(sink)) = (self.client(&peer).await, self.sink()) else {
                // The peer is down (its redial runs apart): look again soon,
                // so it is followed as soon as it is back.
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };
            let mut live = false;
            let mut released = false;
            {
                let (tx, mut rx) = vox::channel::<HubEvent>();
                let call = client.subscribe_local_events(String::new(), tx);
                tokio::pin!(call);
                loop {
                    tokio::select! {
                        _ = &mut call => break,
                        got = rx.recv() => match got {
                            Ok(Some(event)) => {
                                let mut owned = None;
                                let _ = event.map(|e| owned = Some(e));
                                let Some(event) = owned else { continue };
                                // The hub's own word: the first says live.
                                if event.topic.starts_with("hub/") {
                                    live = true;
                                    continue;
                                }
                                if sink.send(Carried::Event(event)).await.is_err() {
                                    return;
                                }
                            }
                            _ => break,
                        },
                        _ = wait_until(&mut watching, |n| n == 0) => {
                            released = true;
                            break;
                        }
                    }
                }
            }
            // Ended while followed: whatever the peer published meanwhile is
            // lost, so this hub's followers re-read.
            if live && !released && sink.send(Carried::Lost).await.is_err() {
                return;
            }
            if live {
                wait = Duration::from_secs(1);
            } else {
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(REDIAL_MAX);
            }
        }
    }
}

/// Until the watcher count satisfies `want`; false once its sender is gone.
async fn wait_until(watching: &mut watch::Receiver<usize>, want: impl Fn(usize) -> bool) -> bool {
    watching.wait_for(|n| want(*n)).await.is_ok()
}

/// A Cloudflare Access service token used during a peer WebSocket handshake.
#[derive(Clone)]
pub struct AccessServiceToken {
    pub id: String,
    pub secret: String,
}

/// Select a peer transport from the address scheme.
// implements: 8976aead0fb36e8b16db9819a9d18b7be09cc5cc6944143a063bd3e2c247b69f@3e9c1d8f725914bc1b2f86f165ff7badbf38f05f153e0505f11b0cc7da37eede
pub fn peer_dialer(
    hub: &crate::ServedHub,
    peer: &str,
    addr: String,
    access: Option<AccessServiceToken>,
) -> Dialer {
    if !matches!(addr.split_once("://"), Some(("ws" | "wss", _))) {
        return tcp_dialer(hub, peer, addr);
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (hub, peer) = (hub.clone(), peer.to_string());
    Arc::new(move || {
        let (addr, access, hub, peer) = (addr.clone(), access.clone(), hub.clone(), peer.clone());
        Box::pin(async move {
            let acceptor = crate::serve::peer_consumer_acceptor(&hub, &peer);
            let request = websocket_request(&addr, access.as_ref())?;
            let (stream, _) = tokio::time::timeout(
                Duration::from_secs(3),
                tokio_tungstenite::connect_async(request),
            )
            .await
            .map_err(|_| format!("{addr} did not answer"))?
            .map_err(|error| format!("{addr}: {error}"))?;
            vox::initiator_on(vox::transport::websocket::WsLink::new(stream))
                .on_lane(acceptor)
                .keepalive(vox::ConnectionKeepaliveConfig {
                    ping_interval: KEEPALIVE,
                    pong_timeout: KEEPALIVE,
                })
                .establish_connection()
                .await
                .map_err(|error| format!("{addr}: {error:?}"))
        })
    })
}

fn websocket_request(
    addr: &str,
    access: Option<&AccessServiceToken>,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, String> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};

    let mut request = addr
        .into_client_request()
        .map_err(|error| format!("invalid WebSocket URL: {error}"))?;
    if let Some(access) = access {
        for (name, value) in [
            (
                HeaderName::from_static("cf-access-client-id"),
                access.id.as_str(),
            ),
            (
                HeaderName::from_static("cf-access-client-secret"),
                access.secret.as_str(),
            ),
        ] {
            let value = HeaderValue::from_str(value)
                .map_err(|error| format!("invalid Cloudflare Access credential: {error}"))?;
            request.headers_mut().insert(name, value);
        }
    }
    Ok(request)
}

/// A dialer for a peer hub at `addr` over TCP: three seconds to connect, and
/// a keepalive so a peer that vanishes (power, network) is noticed.
pub fn tcp_dialer(hub: &crate::ServedHub, peer: &str, addr: String) -> Dialer {
    let (hub, peer) = (hub.clone(), peer.to_string());
    Arc::new(move || {
        let (addr, hub, peer) = (addr.clone(), hub.clone(), peer.clone());
        Box::pin(async move {
            let acceptor = crate::serve::peer_consumer_acceptor(&hub, &peer);
            let socket = tokio::time::timeout(
                Duration::from_secs(3),
                tokio::net::TcpStream::connect(addr.as_str()),
            )
            .await
            .map_err(|_| format!("{addr} did not answer"))?
            .map_err(|e| format!("{addr}: {e}"))?;
            vox::initiator_on(vox::transport::tcp::StreamLink::tcp(socket))
                .on_lane(acceptor)
                .keepalive(vox::ConnectionKeepaliveConfig {
                    ping_interval: KEEPALIVE,
                    pong_timeout: KEEPALIVE,
                })
                .establish_connection()
                .await
                .map_err(|e| format!("{addr}: {e:?}"))
        })
    })
}

/// `HUB_PEERS`: `name=addr,…`. A name may not hold the peer separator, nor
/// appear twice.
pub fn parse_peers(spec: &str) -> Result<Vec<(String, String)>, String> {
    let mut peers: Vec<(String, String)> = Vec::new();
    for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (name, addr) = entry
            .split_once('=')
            .ok_or_else(|| format!("peer {entry:?}: want name=addr"))?;
        let (name, addr) = (name.trim(), addr.trim());
        if name.is_empty() || addr.is_empty() || name.contains(PEER_SEPARATOR) {
            return Err(format!(
                "peer {entry:?}: want name=addr, a name without {PEER_SEPARATOR}"
            ));
        }
        if peers.iter().any(|(n, _)| n == name) {
            return Err(format!("peer {name:?} is named twice"));
        }
        peers.push((name.to_string(), addr.to_string()));
    }
    Ok(peers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloudflare_service_tokens_are_websocket_headers() {
        let access = AccessServiceToken {
            id: "client-id".into(),
            secret: "client-secret".into(),
        };
        let request = websocket_request("wss://hub.example.com", Some(&access)).unwrap();
        assert_eq!(request.headers()["cf-access-client-id"], "client-id");
        assert_eq!(
            request.headers()["cf-access-client-secret"],
            "client-secret"
        );

        let plain = websocket_request("ws://127.0.0.1:15400", None).unwrap();
        assert!(!plain.headers().contains_key("cf-access-client-id"));
        assert!(!plain.headers().contains_key("cf-access-client-secret"));
    }
}
