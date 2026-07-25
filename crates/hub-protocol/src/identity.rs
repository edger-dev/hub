//! Node and link identity — how the mesh is addressed.
//!
//! Uniform addressing (§2.4): a node is reached by its **name** in the hub's
//! registry, never a local endpoint. One machine is one *node*; a node may offer
//! several *links* (§2.5) — a default `stable` link and, say, a session-scoped
//! `dev` link — told apart by a [`LinkLabel`], not by listen address.

use facet::Facet;

/// A node's stable identity: its name in the hub registry (§2.4).
///
/// This is the *only* way a node is addressed. It is stable across a node's
/// links and across daemon restarts.
#[derive(Facet, Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

/// Which link of a node a request concerns (§2.5).
///
/// `stable` is distinguished: it is the routing default and the fallback when a
/// requested link is absent. Other labels (e.g. `dev`) are conventions carried
/// as-is.
#[derive(Facet, Debug, Clone, PartialEq, Eq, Hash)]
pub struct LinkLabel(pub String);

impl LinkLabel {
    /// The distinguished default/fallback link.
    pub const STABLE: &'static str = "stable";
    /// The conventional development link (its own toolchain environment).
    pub const DEV: &'static str = "dev";

    /// The `stable` link — the routing default.
    pub fn stable() -> Self {
        Self(Self::STABLE.to_string())
    }

    /// The `dev` link.
    pub fn dev() -> Self {
        Self(Self::DEV.to_string())
    }

    /// Whether this is the distinguished `stable` link.
    pub fn is_stable(&self) -> bool {
        self.0 == Self::STABLE
    }
}

impl Default for LinkLabel {
    /// Absent an explicit label, routing targets `stable` (§2.5).
    fn default() -> Self {
        Self::stable()
    }
}

/// A fully-qualified target: a specific link of a specific node.
#[derive(Facet, Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeLink {
    pub node: NodeId,
    pub link: LinkLabel,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_is_the_default_link() {
        assert_eq!(LinkLabel::default(), LinkLabel::stable());
        assert!(LinkLabel::default().is_stable());
        assert!(!LinkLabel::dev().is_stable());
    }

    #[test]
    fn node_id_round_trips() {
        let v = NodeId("alpha".to_string());
        let json = facet_json::to_string(&v).unwrap();
        let back: NodeId = facet_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn node_link_round_trips() {
        let v = NodeLink {
            node: NodeId("alpha".to_string()),
            link: LinkLabel::dev(),
        };
        let json = facet_json::to_string(&v).unwrap();
        let back: NodeLink = facet_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }
}
