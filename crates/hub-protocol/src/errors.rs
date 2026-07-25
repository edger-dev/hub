//! Domain errors carried on the wire.
//!
//! These are *protocol* errors — the failures a peer can report through a reply.
//! Transport-level failures (a dropped connection) are vox's concern, not these;
//! a node reappears on its own after a hub blink (§2.1), so "hub unreachable" is
//! not modelled as a domain error here.

use facet::Facet;

/// Why the hub could not satisfy a consumer request.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum HubError {
    /// No node by that name is currently connected (§2.6 — discovery is live).
    UnknownNode,
    /// The node is connected but the requested link is absent and there is no
    /// `stable` link to fall back to (§2.5).
    UnknownLink,
    /// The selected link's daemon did not accept the routed call.
    Rejected(String),
    /// An unexpected hub-side failure.
    Internal(String),
}

/// Why a node could not satisfy a routed call the hub delivered.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeError {
    /// This link offers no connector with that id.
    UnknownConnector,
    /// The connector refused the call.
    Rejected(String),
}

// implements: 45af8b01ab1929e94682ed82013a4aad0b48af81c61b3dea89d7b8323cf94752@45af8b01ab1929e94682ed82013a4aad0b48af81c61b3dea89d7b8323cf94752
/// Why a consumer's routed call failed — preserving *where* it failed.
///
/// The two arms want different consumer reactions: a [`RouteError::Hub`] often
/// means the consumer's topology is stale (refresh discovery, retry), while a
/// [`RouteError::Node`] means the call reached the connector and was refused
/// (surface it; do not blindly retry).
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum RouteError {
    /// The hub could not deliver the call (no such node/link, node offline).
    Hub(HubError),
    /// The call reached the node, but its connector rejected it.
    Node(NodeError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hub_error_round_trips() {
        for v in [HubError::UnknownNode, HubError::Rejected("busy".to_string())] {
            let json = facet_json::to_string(&v).unwrap();
            let back: HubError = facet_json::from_str(&json).unwrap();
            assert_eq!(v, back);
        }
    }

    #[test]
    fn node_error_round_trips() {
        let v = NodeError::UnknownConnector;
        let json = facet_json::to_string(&v).unwrap();
        let back: NodeError = facet_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn route_error_preserves_which_side_failed() {
        for v in [
            RouteError::Hub(HubError::UnknownNode),
            RouteError::Node(NodeError::Rejected("busy".to_string())),
        ] {
            let json = facet_json::to_string(&v).unwrap();
            let back: RouteError = facet_json::from_str(&json).unwrap();
            assert_eq!(v, back);
        }
    }
}
