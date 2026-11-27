//! Structured live diagnostic reports.

// implements: hub::spec::session-diagnostics@0ed7d0e7b10814c751b8b3303cb9572a2f0b34b7c0047b49ac805c19d0e55033

#[derive(facet::Facet, Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticCapabilities {
    pub version: u32,
    pub session_ping: bool,
}

#[repr(u8)]
#[derive(facet::Facet, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticStatus {
    Ok,
    Failed,
    Unsupported,
    Unknown,
}

#[derive(facet::Facet, Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticCheck {
    pub stage: String,
    pub status: DiagnosticStatus,
    pub detail: String,
}

#[derive(facet::Facet, Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticReport {
    pub probe_id: String,
    pub observed_at_ms: u64,
    pub source: String,
    pub target: String,
    pub elapsed_ms: u64,
    pub status: DiagnosticStatus,
    pub checks: Vec<DiagnosticCheck>,
}

impl DiagnosticReport {
    pub fn new(probe_id: String, source: String, target: String) -> Self {
        Self {
            probe_id,
            observed_at_ms: 0,
            source,
            target,
            elapsed_ms: 0,
            status: DiagnosticStatus::Unknown,
            checks: Vec::new(),
        }
    }

    pub fn finish(mut self) -> Self {
        self
    }
}

#[derive(facet::Facet, Debug, Clone, PartialEq, Eq)]
pub struct PeerDiagnostic {
    pub name: String,
    pub direction: String,
    pub registration: u64,
    pub current: bool,
    pub last_error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_precedes_unsupported_unknown_and_ok() {
        let report = DiagnosticReport::new("p".into(), "a".into(), "b".into());
        assert_eq!(report.finish().status, DiagnosticStatus::Ok);
        let report = DiagnosticReport {
            checks: vec![DiagnosticCheck {
                stage: "peer".into(),
                status: DiagnosticStatus::Unsupported,
                detail: "old hub".into(),
            }],
            ..DiagnosticReport::new("p".into(), "a".into(), "b".into())
        };
        assert_eq!(report.finish().status, DiagnosticStatus::Unsupported);
        let report = DiagnosticReport {
            checks: vec![
                DiagnosticCheck {
                    stage: "peer".into(),
                    status: DiagnosticStatus::Unsupported,
                    detail: "old hub".into(),
                },
                DiagnosticCheck {
                    stage: "callback".into(),
                    status: DiagnosticStatus::Failed,
                    detail: "closed".into(),
                },
            ],
            ..DiagnosticReport::new("p".into(), "a".into(), "b".into())
        };
        assert_eq!(report.finish().status, DiagnosticStatus::Failed);
    }
}
