//! Live topology — what the hub reports about who is currently connected.
//!
//! The console's node list is built from **live** topology (§2.6): the hub
//! reports which nodes are connected right now, derived from its reverse-dial
//! registry — not a static list that drifts. These types are that report.

use facet::Facet;

use crate::connector::Connector;
use crate::identity::{LinkLabel, NodeId};

/// One connected link of a node, with the connectors it currently serves.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
pub struct LinkInfo {
    pub link: LinkLabel,
    pub connectors: Vec<Connector>,
}

/// A connected node and all of its currently-connected links (§2.5).
///
/// One node, many links: `links` carries every link presently registered under
/// this `node`.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub node: NodeId,
    pub links: Vec<LinkInfo>,
}

/// A point-in-time view of the whole mesh (§2.6).
///
/// The hub serves this as a snapshot; a live tail of changes rides the same
/// contract (added in the service slice).
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
pub struct TopologySnapshot {
    pub nodes: Vec<NodeInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::ConnectorKind;

    fn sample() -> TopologySnapshot {
        TopologySnapshot {
            nodes: vec![NodeInfo {
                node: NodeId("alpha".to_string()),
                links: vec![
                    LinkInfo {
                        link: LinkLabel::stable(),
                        connectors: vec![Connector {
                            id: "pm-0".to_string(),
                            kind: ConnectorKind::ProcessManager,
                        }],
                    },
                    LinkInfo {
                        link: LinkLabel::dev(),
                        connectors: vec![],
                    },
                ],
            }],
        }
    }

    #[test]
    fn topology_snapshot_round_trips() {
        let v = sample();
        let json = facet_json::to_string(&v).unwrap();
        let back: TopologySnapshot = facet_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn one_node_can_carry_many_links() {
        let snap = sample();
        assert_eq!(snap.nodes[0].links.len(), 2, "a node may carry many links");
    }
}
