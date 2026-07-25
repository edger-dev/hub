//! Routed calls — the opaque envelope the hub relays to a node's connector.
//!
//! The hub is a router, not an interpreter: it delivers a call to the addressed
//! connector without inspecting the payload (§1, and "the hub doesn't inspect
//! message payloads — it just delivers them"). The connector on the far side
//! owns the payload's meaning; this contract only names *where* it goes.

use facet::Facet;

/// One call addressed to a connector on the selected node/link.
///
/// `payload` is opaque to the hub — a connector-defined encoding the far side
/// decodes. The node is chosen out-of-band by a `LinkSelector`; this is the
/// message that rides to it.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
pub struct RoutedCall {
    /// Which connector on the target link this addresses (its descriptor `id`).
    pub connector: String,
    /// A connector-defined method name; opaque to the hub.
    pub method: String,
    /// Connector-defined request bytes; the hub never decodes these.
    pub payload: Vec<u8>,
}

/// The connector's reply to a [`RoutedCall`], relayed back unmodified.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
pub struct RoutedReply {
    /// Connector-defined response bytes; opaque to the hub.
    pub payload: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routed_call_round_trips() {
        let v = RoutedCall {
            connector: "pm-0".to_string(),
            method: "spawn".to_string(),
            payload: vec![1, 2, 3, 4],
        };
        let json = facet_json::to_string(&v).unwrap();
        let back: RoutedCall = facet_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn routed_reply_round_trips() {
        let v = RoutedReply {
            payload: vec![9, 8, 7],
        };
        let json = facet_json::to_string(&v).unwrap();
        let back: RoutedReply = facet_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }
}
