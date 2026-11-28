//! Authenticated live diagnostics and session probes.

// implements: hub::spec::session-diagnostics@4f1da10173d44802ad14da7601334b7b7ea94d10d69dc30e5ee2a8435e320c4e

use hub_protocol::service::DiagnosticsApi;
use hub_protocol::{
    AuthChallenge, AuthProof, DiagnosticCapabilities, DiagnosticCheck, DiagnosticReport,
    DiagnosticStatus, HubError, PeerDiagnostic,
};

use crate::serve::{ConnectionAuth, ConsumerPeer, ServedHub, authenticated_peer_name};

#[derive(Clone)]
pub(crate) struct DiagnosticsApiHandler {
    hub: ServedHub,
    auth: ConnectionAuth,
    peer: Option<ConsumerPeer>,
}

impl DiagnosticsApiHandler {
    pub(crate) fn new(hub: ServedHub, peer: Option<ConsumerPeer>) -> Self {
        let auth = ConnectionAuth::new(hub.auth_token(), hub.auth_trust());
        Self { hub, auth, peer }
    }

    fn report(&self, probe_id: String, target: String) -> DiagnosticReport {
        DiagnosticReport::new(
            probe_id,
            self.hub.peers().name().unwrap_or("unnamed").to_string(),
            target,
        )
    }

    fn complete(
        &self,
        started: std::time::Instant,
        mut report: DiagnosticReport,
    ) -> DiagnosticReport {
        report.observed_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        report.elapsed_ms = started.elapsed().as_millis() as u64;
        report.finish()
    }
}

impl DiagnosticsApi for DiagnosticsApiHandler {
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

    async fn capabilities(&self) -> Result<DiagnosticCapabilities, HubError> {
        self.auth.check().await?;
        Ok(DiagnosticCapabilities {
            version: 1,
            session_ping: true,
        })
    }

    async fn peer_diagnostics(&self) -> Result<Vec<PeerDiagnostic>, HubError> {
        self.auth.check().await?;
        Ok(self.hub.peers().diagnostics())
    }

    async fn session_probe(
        &self,
        target: String,
        probe_id: String,
    ) -> Result<DiagnosticReport, HubError> {
        self.auth.check().await?;
        if probe_id.is_empty() {
            return Err(HubError::Rejected("probe id must not be empty".into()));
        }
        let started = std::time::Instant::now();
        let mut report = self.report(probe_id.clone(), target.clone());
        if let Some((peer, local)) = target.split_once(hub_protocol::PEER_SEPARATOR) {
            if local.contains(hub_protocol::PEER_SEPARATOR) {
                report.checks.push(DiagnosticCheck {
                    stage: "target".into(),
                    status: DiagnosticStatus::Failed,
                    detail: "multi-hop probes are refused".into(),
                });
                return Ok(self.complete(started, report));
            }
            let client = match self.hub.peers().diagnostics_client(peer).await {
                Ok(client) => client,
                Err((status, detail)) => {
                    report.checks.push(DiagnosticCheck {
                        stage: "peer".into(),
                        status,
                        detail,
                    });
                    return Ok(self.complete(started, report));
                }
            };
            report.checks.push(DiagnosticCheck {
                stage: "peer".into(),
                status: DiagnosticStatus::Ok,
                detail: format!("peer {peer:?} is live"),
            });
            match client
                .forward_session_probe(local.to_string(), probe_id)
                .await
            {
                Ok(mut remote) => {
                    remote.target = target;
                    remote.checks.splice(0..0, report.checks);
                    remote.elapsed_ms = started.elapsed().as_millis() as u64;
                    return Ok(remote.finish());
                }
                Err(error) => {
                    report.checks.push(DiagnosticCheck {
                        stage: "peer".into(),
                        status: DiagnosticStatus::Failed,
                        detail: format!("peer diagnostic call failed: {error:?}"),
                    });
                }
            }
        } else if let Some((_info, client)) = self.hub.session_client(&target) {
            report.checks.push(DiagnosticCheck {
                stage: "target".into(),
                status: DiagnosticStatus::Ok,
                detail: "session is registered".into(),
            });
            match tokio::time::timeout(std::time::Duration::from_secs(2), client.ping()).await {
                Ok(Ok(info)) => report.checks.push(DiagnosticCheck {
                    stage: "callback".into(),
                    status: DiagnosticStatus::Ok,
                    detail: format!("node {} · harness {}", info.node, info.harness),
                }),
                Ok(Err(error)) => {
                    let detail = format!("{error:?}");
                    report.checks.push(DiagnosticCheck {
                        stage: "callback".into(),
                        status: if detail.contains("unknown") {
                            DiagnosticStatus::Unsupported
                        } else {
                            DiagnosticStatus::Failed
                        },
                        detail,
                    });
                }
                Err(_) => report.checks.push(DiagnosticCheck {
                    stage: "callback".into(),
                    status: DiagnosticStatus::Unknown,
                    detail: "callback timed out; reachability is unknown".into(),
                }),
            }
        } else {
            report.checks.push(DiagnosticCheck {
                stage: "target".into(),
                status: DiagnosticStatus::Failed,
                detail: "unknown session".into(),
            });
        }
        Ok(self.complete(started, report))
    }

    async fn forward_session_probe(
        &self,
        target: String,
        probe_id: String,
    ) -> Result<DiagnosticReport, HubError> {
        self.auth.check().await?;
        let started = std::time::Instant::now();
        let mut report = self.report(probe_id.clone(), target.clone());
        let peer = match authenticated_peer_name(&self.hub, &self.auth, self.peer.as_ref()).await {
            Ok(peer) => peer,
            Err(error) => {
                report.checks.push(DiagnosticCheck {
                    stage: "forwarding".into(),
                    status: DiagnosticStatus::Failed,
                    detail: format!("{error:?}"),
                });
                return Ok(self.complete(started, report));
            }
        };
        let mut report = self.session_probe(target, probe_id).await?;
        report.checks.insert(
            0,
            DiagnosticCheck {
                stage: "forwarding".into(),
                status: DiagnosticStatus::Ok,
                detail: format!("origin peer {peer:?} is admitted"),
            },
        );
        Ok(report.finish())
    }
}
