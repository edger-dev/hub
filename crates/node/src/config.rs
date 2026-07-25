//! Node configuration — and the rule that a hub is mandatory (§2.4).

// implements: bf406db5b660bd595cccdfcec5921121bb5a1ac9281a0d9a5c28be2d7ccc79b4@bf406db5b660bd595cccdfcec5921121bb5a1ac9281a0d9a5c28be2d7ccc79b4

use std::fmt;

use hub_protocol::{Connector, LinkLabel, NodeId, Registration};

/// Where the node dials to reach the hub (e.g. `tcp://127.0.0.1:15400`).
///
/// This is an address the node dials *out* to; a node never listens for the hub
/// (§2.4). The string is a vox transport URL, validated when the transport wires
/// it — this type only carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubEndpoint(pub String);

/// Why a node could not be configured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// No hub endpoint was configured. The hub is mandatory infrastructure, so
    /// this is a hard error, not an idle wait (§2.4).
    MissingHub,
    /// The hub address names a transport this node cannot dial.
    UnsupportedScheme(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::MissingHub => {
                f.write_str("no hub configured — a node requires a hub to dial (§2.4)")
            }
            ConfigError::UnsupportedScheme(scheme) => {
                write!(f, "unsupported hub address scheme `{scheme}` (expected tcp)")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Static configuration a node daemon starts from.
///
/// A `NodeConfig` cannot exist without a [`HubEndpoint`]; the only constructor
/// that takes an optional hub ([`NodeConfig::new`]) rejects the `None` case, so
/// the hub-required rule (§2.4) holds by construction.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node: NodeId,
    pub link: LinkLabel,
    pub hub: HubEndpoint,
    pub connectors: Vec<Connector>,
    pub agent_version: String,
}

impl NodeConfig {
    /// Build a config from parts, where the hub may be absent (as when read from
    /// the environment). A missing hub is a hard error (§2.4).
    pub fn new(
        node: NodeId,
        link: LinkLabel,
        hub: Option<HubEndpoint>,
        connectors: Vec<Connector>,
        agent_version: String,
    ) -> Result<Self, ConfigError> {
        let hub = hub.ok_or(ConfigError::MissingHub)?;
        Ok(Self {
            node,
            link,
            hub,
            connectors,
            agent_version,
        })
    }

    /// The [`Registration`] this node's link announces to the hub (§2.1).
    pub fn registration(&self) -> Registration {
        Registration {
            node: self.node.clone(),
            link: self.link.clone(),
            connectors: self.connectors.clone(),
            agent_version: self.agent_version.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hub_protocol::{Connector, ConnectorKind};

    fn parts() -> (NodeId, LinkLabel, Vec<Connector>, String) {
        (
            NodeId("alpha".to_string()),
            LinkLabel::stable(),
            vec![Connector {
                id: "pm-0".to_string(),
                kind: ConnectorKind::ProcessManager,
            }],
            "0.0.0".to_string(),
        )
    }

    #[test]
    fn missing_hub_is_a_hard_error() {
        let (node, link, connectors, version) = parts();
        let err = NodeConfig::new(node, link, None, connectors, version).unwrap_err();
        assert_eq!(err, ConfigError::MissingHub);
    }

    #[test]
    fn config_with_hub_is_accepted() {
        let (node, link, connectors, version) = parts();
        let cfg = NodeConfig::new(
            node,
            link,
            Some(HubEndpoint("tcp://127.0.0.1:15400".to_string())),
            connectors,
            version,
        )
        .unwrap();
        assert_eq!(cfg.hub, HubEndpoint("tcp://127.0.0.1:15400".to_string()));
    }

    #[test]
    fn registration_carries_config_identity_and_connectors() {
        let (node, link, connectors, version) = parts();
        let cfg = NodeConfig::new(
            node.clone(),
            link.clone(),
            Some(HubEndpoint("tcp://127.0.0.1:15400".to_string())),
            connectors.clone(),
            version.clone(),
        )
        .unwrap();
        let reg = cfg.registration();
        assert_eq!(reg.node, node);
        assert_eq!(reg.link, link);
        assert_eq!(reg.connectors, connectors);
        assert_eq!(reg.agent_version, version);
    }
}
