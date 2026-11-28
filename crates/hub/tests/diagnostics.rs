#![allow(clippy::unwrap_used, clippy::expect_used)]

use hub::{Peers, ServedHub, serve_connection};
use hub_protocol::service::{
    ConsumerApiClient, DiagnosticsApiClient, PeerIngressClient, SessionIngressClient,
    SessionServiceDispatcher,
};
use hub_protocol::{
    DiagnosticStatus, HubError, SessionInfo, SessionMessage, SessionService, SigningKey, TrustStore,
};

#[derive(Clone, Copy)]
enum Behavior {
    Ok,
    Unsupported,
    Hang,
}

#[derive(Clone)]
struct Mailbox {
    info: SessionInfo,
    behavior: Behavior,
}

impl SessionService for Mailbox {
    async fn ping(&self) -> Result<SessionInfo, HubError> {
        match self.behavior {
            Behavior::Ok => Ok(self.info.clone()),
            Behavior::Unsupported => Err(HubError::Rejected("unknown-method".into())),
            Behavior::Hang => std::future::pending().await,
        }
    }

    async fn deliver(&self, _message: SessionMessage) -> Result<(), HubError> {
        Ok(())
    }
}

fn attach(hub: &ServedHub) -> vox::MemoryLink {
    let (client, hub_link) = vox::memory_link_pair(64);
    let hub = hub.clone();
    tokio::spawn(async move {
        let _ = serve_connection(hub, hub_link).await;
    });
    client
}

fn info(id: &str) -> SessionInfo {
    SessionInfo {
        id: id.into(),
        node: "yolo".into(),
        harness: "test".into(),
        workspace: "/work".into(),
        context: None,
    }
}

async fn session(
    hub: &ServedHub,
    info: SessionInfo,
) -> (vox::ConnectionHandle, DiagnosticsApiClient) {
    session_with(hub, info, Behavior::Ok).await
}

async fn session_with(
    hub: &ServedHub,
    info: SessionInfo,
    behavior: Behavior,
) -> (vox::ConnectionHandle, DiagnosticsApiClient) {
    let mailbox = Mailbox {
        info: info.clone(),
        behavior,
    };
    let connection = vox::initiator_on(attach(hub))
        .on_lane(SessionServiceDispatcher::new(mailbox))
        .establish_connection()
        .await
        .unwrap();
    let ingress: SessionIngressClient = connection.open_lane().await.unwrap();
    ingress.register(info).await.unwrap();
    let diagnostics = connection.open_lane().await.unwrap();
    (connection, diagnostics)
}

async fn signed_session(
    hub: &ServedHub,
    info: SessionInfo,
    key: &SigningKey,
) -> (vox::ConnectionHandle, DiagnosticsApiClient) {
    let mailbox = Mailbox {
        info: info.clone(),
        behavior: Behavior::Ok,
    };
    let connection = vox::initiator_on(attach(hub))
        .on_lane(SessionServiceDispatcher::new(mailbox))
        .establish_connection()
        .await
        .unwrap();
    let ingress: SessionIngressClient = connection.open_lane().await.unwrap();
    let challenge = ingress.authentication_challenge().await.unwrap();
    ingress
        .authenticate_key(key.sign(&challenge).unwrap())
        .await
        .unwrap();
    ingress.register(info).await.unwrap();
    let diagnostics: DiagnosticsApiClient = connection.open_lane().await.unwrap();
    let challenge = diagnostics.authentication_challenge().await.unwrap();
    diagnostics
        .authenticate_key(key.sign(&challenge).unwrap())
        .await
        .unwrap();
    (connection, diagnostics)
}

#[tokio::test(flavor = "multi_thread")]
async fn diagnostics_probe_local_unknown_and_disconnected_targets() {
    let public = ServedHub::new().with_peers(Peers::new("hub"));
    let (_a, diagnostics) = session(&public, info("a")).await;
    let capabilities = diagnostics.capabilities().await.unwrap();
    assert_eq!(capabilities.version, 1);
    assert!(capabilities.session_ping);

    let report = diagnostics
        .session_probe("a".into(), "local-ok".into())
        .await
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Ok, "{report:?}");
    assert_eq!(report.probe_id, "local-ok");
    assert!(report.observed_at_ms > 0);
    assert!(report.checks.iter().any(|check| check.stage == "callback"));
    assert!(diagnostics.peer_diagnostics().await.unwrap().is_empty());

    let report = diagnostics
        .session_probe("missing".into(), "missing".into())
        .await
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Failed, "{report:?}");
    assert!(report.checks.iter().any(|check| check.stage == "target"));

    let edger = ServedHub::new().with_peers(Peers::new("edger"));
    let connection = vox::initiator_on(attach(&public))
        .on_lane(hub::serve::peer_consumer_acceptor(&edger, "edger"))
        .establish_connection()
        .await
        .unwrap();
    let ingress: PeerIngressClient = connection.open_lane().await.unwrap();
    ingress.register("edger".into()).await.unwrap();
    drop(ingress);
    connection.shutdown().unwrap();
    drop(connection);
    for _ in 0..100 {
        if public.peers().client("edger").await.is_none() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let report = diagnostics
        .session_probe("edger::a".into(), "disconnected".into())
        .await
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Failed, "{report:?}");
    assert!(report.checks.iter().any(|check| check.stage == "peer"));
}

#[tokio::test(flavor = "multi_thread")]
async fn diagnostics_distinguishes_unsupported_and_timeout() {
    let public = ServedHub::new().with_peers(Peers::new("hub"));
    let (_old, old_diagnostics) = session_with(&public, info("old"), Behavior::Unsupported).await;
    let report = old_diagnostics
        .session_probe("old".into(), "old".into())
        .await
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Unsupported, "{report:?}");

    let (_hung, hung_diagnostics) = session_with(&public, info("hung"), Behavior::Hang).await;
    let started = std::time::Instant::now();
    let report = hung_diagnostics
        .session_probe("hung".into(), "hung".into())
        .await
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Unknown, "{report:?}");
    assert!(started.elapsed() >= std::time::Duration::from_secs(2));
    let rendered = format!("{report:?}");
    assert!(!rendered.contains("HUB_AUTH_TOKEN"));
    assert!(!rendered.contains("private_key"));
}

#[tokio::test(flavor = "multi_thread")]
async fn signed_configured_peer_probe_and_delivery_include_forwarding_admission() {
    let hub_key = SigningKey::generate("hub").unwrap();
    let edger_key = SigningKey::generate("edger").unwrap();
    let yolo_key = SigningKey::generate("yolo").unwrap();
    let mut trust = TrustStore::default();
    trust
        .add("edger", &edger_key.public_hex(), &["peer", "consumer"])
        .unwrap();
    trust
        .add("yolo", &yolo_key.public_hex(), &["consumer", "session"])
        .unwrap();
    let public = ServedHub::new()
        .with_auth_trust(trust)
        .with_peers(Peers::new("hub").with_signing_key(hub_key));
    let listener = hub::HubListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().to_string();
    let _server = tokio::spawn(listener.serve(public.clone()));
    let down: hub::Dialer = std::sync::Arc::new(|| Box::pin(async { Err("down".into()) }));
    public.peers().add("edger", down).unwrap();

    let edger = ServedHub::new().with_peers(Peers::new("edger").with_signing_key(edger_key));
    edger
        .peers()
        .add("hub", hub::tcp_dialer(&edger, "hub", addr))
        .unwrap();
    for _ in 0..100 {
        if edger.peers().client("hub").await.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let (_target, _) = signed_session(&public, info("a"), &yolo_key).await;
    let (caller, diagnostics) = session(&edger, info("caller")).await;
    let consumer: ConsumerApiClient = caller.open_lane().await.unwrap();

    let report = diagnostics
        .session_probe("hub::a".into(), "configured-peer".into())
        .await
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Ok, "{report:?}");
    assert!(
        report
            .checks
            .iter()
            .any(|check| check.stage == "forwarding")
    );
    consumer
        .send_session("hub::a".into(), "configured-message".into(), "hello".into())
        .await
        .unwrap();
}
