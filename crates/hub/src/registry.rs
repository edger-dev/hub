//! The live registry — who is connected, and which link answers (§2.5, §2.6).
//!
//! The registry is the hub's only durable state, and discovery is a *projection*
//! of it: a node exists exactly as long as it has a connected link. It is generic
//! over the per-link value `T` (a way to call that link back) so the routing and
//! discovery rules are testable with no transport in sight.

// implements: 7c924d1c2994326c261f907f81c39f0fa676381b53f7898556893bb4f750d85c@7c924d1c2994326c261f907f81c39f0fa676381b53f7898556893bb4f750d85c
// implements: 08c3ce357742ccf578caddcfb6578a92216b515adcfe1ac9de4543406be52cb0@08c3ce357742ccf578caddcfb6578a92216b515adcfe1ac9de4543406be52cb0

use std::collections::BTreeMap;

use hub_protocol::{
    Connector, HubError, LinkInfo, LinkLabel, LinkSelector, NodeId, NodeInfo, NodeLink,
    Registration, TopologyEvent, TopologySnapshot,
};

/// One registered link: what it offers, plus the handle used to reach it.
#[derive(Debug, Clone)]
pub struct LinkEntry<T> {
    pub connectors: Vec<Connector>,
    pub agent_version: String,
    /// How to call this link back (a `NodeService` client in production).
    pub handle: T,
}

/// The connected mesh, keyed by node then link.
///
/// `BTreeMap` keeps snapshots and events in a deterministic order, which makes
/// the discovery stream reproducible and the tests exact.
#[derive(Debug)]
pub struct Registry<T> {
    nodes: BTreeMap<NodeId, BTreeMap<LinkLabel, LinkEntry<T>>>,
}

impl<T> Default for Registry<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Registry<T> {
    /// An empty registry — no nodes connected.
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
        }
    }

    /// Record a link's registration, returning the topology change it implies.
    ///
    /// Re-registration by an already-known `(node, link)` is expected reconnect
    /// churn, not an error (§2.4): the entry is replaced and no event is emitted,
    /// since nothing about the *topology* changed.
    pub fn register(&mut self, registration: Registration, handle: T) -> Option<TopologyEvent> {
        let Registration {
            node,
            link,
            connectors,
            agent_version,
        } = registration;
        let entry = LinkEntry {
            connectors,
            agent_version,
            handle,
        };
        let link_info = LinkInfo {
            link: link.clone(),
            connectors: entry.connectors.clone(),
        };

        let links = self.nodes.entry(node.clone()).or_default();
        let is_first_link = links.is_empty();
        let replaced = links.insert(link, entry).is_some();

        if is_first_link {
            // The node just became visible (§2.6).
            Some(TopologyEvent::NodeConnected(NodeInfo {
                node,
                links: vec![link_info],
            }))
        } else if replaced {
            // A reconnect of a link we already knew about — no topology change.
            None
        } else {
            Some(TopologyEvent::LinkConnected(node, link_info))
        }
    }

    /// Drop one link, returning the topology change it implies.
    ///
    /// The node disappears with its last link (§2.6).
    pub fn remove(&mut self, target: &NodeLink) -> Option<TopologyEvent> {
        let links = self.nodes.get_mut(&target.node)?;
        links.remove(&target.link)?;
        if links.is_empty() {
            self.nodes.remove(&target.node);
            Some(TopologyEvent::NodeDisconnected(target.node.clone()))
        } else {
            Some(TopologyEvent::LinkDisconnected(target.clone()))
        }
    }

    /// Resolve a consumer's target to the link that should answer (§2.5).
    ///
    /// The requested link if connected; otherwise `stable`; otherwise
    /// [`HubError::UnknownLink`]. A node with no links at all is
    /// [`HubError::UnknownNode`] — discovery is live, so "not connected" and
    /// "does not exist" are the same thing (§2.6).
    pub fn resolve(&self, target: &LinkSelector) -> Result<(&LinkLabel, &LinkEntry<T>), HubError> {
        let links = self
            .nodes
            .get(&target.node)
            .filter(|links| !links.is_empty())
            .ok_or(HubError::UnknownNode)?;

        let wanted = target.preferred_link();
        if let Some((label, entry)) = links.get_key_value(&wanted) {
            return Ok((label, entry));
        }
        // Fall back to `stable` when the requested link is absent (§2.5).
        links
            .get_key_value(&LinkLabel::stable())
            .ok_or(HubError::UnknownLink)
    }

    /// A point-in-time view of everything connected (§2.6).
    pub fn snapshot(&self) -> TopologySnapshot {
        TopologySnapshot {
            nodes: self
                .nodes
                .iter()
                .map(|(node, links)| NodeInfo {
                    node: node.clone(),
                    links: links
                        .iter()
                        .map(|(link, entry)| LinkInfo {
                            link: link.clone(),
                            connectors: entry.connectors.clone(),
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    /// Whether any link is currently connected.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hub_protocol::ConnectorKind;

    fn registration(node: &str, link: LinkLabel) -> Registration {
        Registration {
            node: NodeId(node.to_string()),
            link,
            connectors: vec![Connector {
                id: "pm-0".to_string(),
                kind: ConnectorKind::ProcessManager,
            }],
            agent_version: "0.0.0".to_string(),
        }
    }

    #[test]
    fn first_link_makes_the_node_appear() {
        let mut reg = Registry::new();
        let event = reg.register(registration("alpha", LinkLabel::stable()), 1);
        match event {
            Some(TopologyEvent::NodeConnected(info)) => {
                assert_eq!(info.node, NodeId("alpha".to_string()));
                assert_eq!(info.links.len(), 1);
            }
            other => panic!("expected NodeConnected, got {other:?}"),
        }
    }

    #[test]
    fn a_second_link_is_a_link_event_under_the_same_node() {
        let mut reg = Registry::new();
        reg.register(registration("alpha", LinkLabel::stable()), 1);
        let event = reg.register(registration("alpha", LinkLabel::dev()), 2);
        match event {
            Some(TopologyEvent::LinkConnected(node, info)) => {
                assert_eq!(node, NodeId("alpha".to_string()));
                assert_eq!(info.link, LinkLabel::dev());
            }
            other => panic!("expected LinkConnected, got {other:?}"),
        }
        assert_eq!(reg.snapshot().nodes[0].links.len(), 2);
    }

    #[test]
    fn re_registration_is_accepted_churn_not_an_event() {
        let mut reg = Registry::new();
        reg.register(registration("alpha", LinkLabel::stable()), 1);
        let event = reg.register(registration("alpha", LinkLabel::stable()), 2);
        assert!(event.is_none(), "a reconnect changes no topology");
        // ...but the handle is refreshed to the new connection.
        let entry = reg
            .resolve(&LinkSelector::node(NodeId("alpha".to_string())))
            .unwrap()
            .1;
        assert_eq!(entry.handle, 2);
    }

    #[test]
    fn node_disappears_with_its_last_link() {
        let mut reg = Registry::new();
        reg.register(registration("alpha", LinkLabel::stable()), 1);
        reg.register(registration("alpha", LinkLabel::dev()), 2);

        let event = reg.remove(&NodeLink {
            node: NodeId("alpha".to_string()),
            link: LinkLabel::dev(),
        });
        assert!(matches!(event, Some(TopologyEvent::LinkDisconnected(_))));
        assert!(!reg.is_empty(), "stable link still connected");

        let event = reg.remove(&NodeLink {
            node: NodeId("alpha".to_string()),
            link: LinkLabel::stable(),
        });
        assert!(matches!(event, Some(TopologyEvent::NodeDisconnected(_))));
        assert!(reg.is_empty(), "node vanished with its last link");
    }

    #[test]
    fn removing_an_unknown_link_is_a_no_op() {
        let mut reg: Registry<u8> = Registry::new();
        let event = reg.remove(&NodeLink {
            node: NodeId("ghost".to_string()),
            link: LinkLabel::stable(),
        });
        assert!(event.is_none());
    }

    #[test]
    fn route_defaults_to_stable() {
        let mut reg = Registry::new();
        reg.register(registration("alpha", LinkLabel::stable()), 1);
        reg.register(registration("alpha", LinkLabel::dev()), 2);
        let (label, entry) = reg
            .resolve(&LinkSelector::node(NodeId("alpha".to_string())))
            .unwrap();
        assert!(label.is_stable());
        assert_eq!(entry.handle, 1);
    }

    #[test]
    fn route_honours_an_explicit_link() {
        let mut reg = Registry::new();
        reg.register(registration("alpha", LinkLabel::stable()), 1);
        reg.register(registration("alpha", LinkLabel::dev()), 2);
        let (label, entry) = reg
            .resolve(&LinkSelector::link(
                NodeId("alpha".to_string()),
                LinkLabel::dev(),
            ))
            .unwrap();
        assert_eq!(*label, LinkLabel::dev());
        assert_eq!(entry.handle, 2);
    }

    #[test]
    fn route_falls_back_to_stable_when_the_link_is_absent() {
        let mut reg = Registry::new();
        reg.register(registration("alpha", LinkLabel::stable()), 1);
        let (label, entry) = reg
            .resolve(&LinkSelector::link(
                NodeId("alpha".to_string()),
                LinkLabel::dev(),
            ))
            .unwrap();
        assert!(label.is_stable(), "fell back to stable");
        assert_eq!(entry.handle, 1);
    }

    #[test]
    fn route_without_a_stable_fallback_is_unknown_link() {
        let mut reg = Registry::new();
        reg.register(registration("alpha", LinkLabel::dev()), 2);
        let err = reg
            .resolve(&LinkSelector::link(
                NodeId("alpha".to_string()),
                LinkLabel("build".to_string()),
            ))
            .unwrap_err();
        assert_eq!(err, HubError::UnknownLink);
    }

    #[test]
    fn route_to_an_absent_node_is_unknown_node() {
        let reg: Registry<u8> = Registry::new();
        let err = reg
            .resolve(&LinkSelector::node(NodeId("ghost".to_string())))
            .unwrap_err();
        assert_eq!(err, HubError::UnknownNode);
    }

    #[test]
    fn snapshot_reports_every_connected_link() {
        let mut reg = Registry::new();
        reg.register(registration("alpha", LinkLabel::stable()), 1);
        reg.register(registration("beta", LinkLabel::stable()), 2);
        let snap = reg.snapshot();
        assert_eq!(snap.nodes.len(), 2);
        assert_eq!(snap.nodes[0].node, NodeId("alpha".to_string()));
        assert_eq!(snap.nodes[1].node, NodeId("beta".to_string()));
    }
}
