#![allow(clippy::unwrap_used, clippy::expect_used)]

use hub::{Peers, ServedHub, serve_connection};
use hub_protocol::service::{
    DiagnosticsApiClient, PeerIngressClient, SessionIngressClient,
    SessionServiceDispatcher,
};
use hub_protocol::{
    DiagnosticStatus, HubError, SessionInfo, SessionMessage, SessionService,
};

#[derive(Clone)]
struct Mailbox {
    info: SessionInfo,
}

impl SessionService for Mailbox {
    async fn ping(&self) -> Result<SessionInfo, HubError> {
        Ok(self.info.clone())
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

async fn session(hub: &ServedHub, info: SessionInfo) -> (vox::ConnectionHandle, DiagnosticsApiClient) {
    let mailbox = Mailbox { info: info.clone() };
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

    let connection = vox::initiator_on(attach(&public))
        .establish_connection()
        .await
        .unwrap();
    let ingress: PeerIngressClient = connection.open_lane().await.unwrap();
    ingress.register("edger".into()).await.unwrap();
    drop(connection);
    let report = diagnostics
        .session_probe("edger::a".into(), "disconnected".into())
        .await
        .unwrap();
    assert_eq!(report.status, DiagnosticStatus::Failed, "{report:?}");
    assert!(report.checks.iter().any(|check| check.stage == "peer"));
}
