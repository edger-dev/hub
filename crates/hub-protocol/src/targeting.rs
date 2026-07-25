//! Targeting — how a consumer names the link it wants a request routed to.
//!
//! A consumer request targets a node plus an *optional* link label; the hub
//! routes to the selected link, defaulting to `stable` and falling back to
//! `stable` when the requested link is absent (§2.5).

// implements: 08c3ce357742ccf578caddcfb6578a92216b515adcfe1ac9de4543406be52cb0@08c3ce357742ccf578caddcfb6578a92216b515adcfe1ac9de4543406be52cb0

use facet::Facet;

use crate::identity::{LinkLabel, NodeId};

/// A routing target: a node and, optionally, which of its links to reach.
///
/// `link == None` means "the default link" — `stable` (§2.5). An explicit label
/// requests that link; the hub falls back to `stable` if it is not connected.
/// Read/observe operations are link-agnostic; only `start`-type operations
/// truly care which link answers (§2.5).
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
pub struct LinkSelector {
    pub node: NodeId,
    pub link: Option<LinkLabel>,
}

impl LinkSelector {
    /// Target a node's default (`stable`) link.
    pub fn node(node: NodeId) -> Self {
        Self { node, link: None }
    }

    /// Target a specific link of a node.
    pub fn link(node: NodeId, link: LinkLabel) -> Self {
        Self {
            node,
            link: Some(link),
        }
    }

    /// The link this selector prefers, resolving `None` to the `stable` default.
    ///
    /// This is the *preferred* label only; the hub still falls back to `stable`
    /// if the preferred link is not currently connected (§2.5).
    pub fn preferred_link(&self) -> LinkLabel {
        self.link.clone().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_node_prefers_stable() {
        let sel = LinkSelector::node(NodeId("alpha".to_string()));
        assert!(sel.preferred_link().is_stable());
    }

    #[test]
    fn explicit_link_is_preserved() {
        let sel = LinkSelector::link(NodeId("alpha".to_string()), LinkLabel::dev());
        assert_eq!(sel.preferred_link(), LinkLabel::dev());
    }

    #[test]
    fn selector_round_trips() {
        let v = LinkSelector::link(NodeId("alpha".to_string()), LinkLabel::dev());
        let json = facet_json::to_string(&v).unwrap();
        let back: LinkSelector = facet_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }
}
