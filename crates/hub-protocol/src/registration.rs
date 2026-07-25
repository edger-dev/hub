//! Registration — what a reverse-dialing link announces to the hub.
//!
//! Every node dials *out* to the hub and offers itself there (§2.1, §2.4); it
//! never listens for the hub to reach in. A [`Registration`] is the payload of
//! that announcement: which node, which link, and the connectors it serves.

use facet::Facet;

use crate::connector::Connector;
use crate::identity::{LinkLabel, NodeId};

/// A link's self-announcement to the hub on connect (§2.1).
///
/// The hub keys its registry on `(node, link)`: one node may register several
/// links (§2.5). Re-registration by an already-known `(node, link)` is expected
/// churn (a reconnect), not an error.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    /// The registry identity this link belongs to (§2.4).
    pub node: NodeId,
    /// Which link of that node this is; `stable` by default (§2.5).
    pub link: LinkLabel,
    /// The connectors this link serves.
    pub connectors: Vec<Connector>,
    /// The daemon's version string, for operator visibility during deploys (§5).
    pub agent_version: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::ConnectorKind;

    #[test]
    fn registration_round_trips() {
        let v = Registration {
            node: NodeId("alpha".to_string()),
            link: LinkLabel::stable(),
            connectors: vec![
                Connector {
                    id: "pm-0".to_string(),
                    kind: ConnectorKind::ProcessManager,
                },
                Connector {
                    id: "files-0".to_string(),
                    kind: ConnectorKind::Files,
                },
            ],
            agent_version: "0.0.0".to_string(),
        };
        let json = facet_json::to_string(&v).unwrap();
        let back: Registration = facet_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }
}
