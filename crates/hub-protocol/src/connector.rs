//! Connectors — the capabilities a link offers over the hub.
//!
//! A node's daemon exposes a set of **connectors** (§1): process management,
//! sessions, files, commands, … A [`Connector`] here is only a *descriptor* that
//! identifies one on the wire; the connector's actual behaviour lives behind the
//! hub, out of this contract.

use facet::Facet;

/// The kind of capability a connector provides (§1).
///
/// [`ConnectorKind::Other`] keeps the set open — a node may offer capabilities
/// this protocol version does not name, without a wire break.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectorKind {
    /// Spawn / observe / manage processes.
    ProcessManager,
    /// Interactive sessions (e.g. PTYs).
    Sessions,
    /// File access.
    Files,
    /// One-shot command execution.
    Commands,
    /// A capability not named by this protocol version.
    Other(String),
}

/// A descriptor for one connector a link offers.
///
/// `id` disambiguates multiple connectors of the same [`ConnectorKind`] on one
/// link; it is opaque to the hub.
#[derive(Facet, Debug, Clone, PartialEq, Eq)]
pub struct Connector {
    pub id: String,
    pub kind: ConnectorKind,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_round_trips() {
        let v = Connector {
            id: "pm-0".to_string(),
            kind: ConnectorKind::ProcessManager,
        };
        let json = facet_json::to_string(&v).unwrap();
        let back: Connector = facet_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn open_connector_kind_round_trips() {
        let v = Connector {
            id: "x-0".to_string(),
            kind: ConnectorKind::Other("metrics".to_string()),
        };
        let json = facet_json::to_string(&v).unwrap();
        let back: Connector = facet_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }
}
