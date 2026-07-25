//! [`NodeDaemon`] — the node's `NodeService` side, dispatching to connectors.

use std::sync::Arc;

use hub_protocol::{NodeError, NodeId, NodeService, Pong, RoutedCall, RoutedReply};

use crate::config::NodeConfig;
use crate::connector::Connectors;

/// Answers the hub over `NodeService`, dispatching routed calls to connectors.
///
/// With an empty registry every call is refused — the stub behaviour the mesh
/// was built against, now for the right reason (§1: nothing is registered).
#[derive(Clone)]
pub struct NodeDaemon {
    node: NodeId,
    agent_version: String,
    connectors: Arc<Connectors>,
}

impl NodeDaemon {
    /// A daemon for `config`, serving `connectors`.
    pub fn new(config: &NodeConfig, connectors: Arc<Connectors>) -> Self {
        Self {
            node: config.node.clone(),
            agent_version: config.agent_version.clone(),
            connectors,
        }
    }

    /// A daemon that offers nothing — every routed call is refused.
    pub fn stub(config: &NodeConfig) -> Self {
        Self::new(config, Arc::new(Connectors::new()))
    }
}

impl NodeService for NodeDaemon {
    async fn ping(&self) -> Pong {
        Pong {
            node: self.node.clone(),
            agent_version: self.agent_version.clone(),
        }
    }

    async fn deliver(&self, call: RoutedCall) -> Result<RoutedReply, NodeError> {
        self.connectors.dispatch(&call)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HubEndpoint;
    use crate::connector::Connector;
    use hub_protocol::{Connector as ConnectorDescriptor, ConnectorKind, LinkLabel};

    struct Echo;
    impl Connector for Echo {
        fn descriptor(&self) -> ConnectorDescriptor {
            ConnectorDescriptor {
                id: "echo-0".to_string(),
                kind: ConnectorKind::Other("echo".to_string()),
            }
        }
        fn call(&self, _method: &str, payload: &[u8]) -> Result<Vec<u8>, NodeError> {
            Ok(payload.to_vec())
        }
    }

    fn config() -> NodeConfig {
        NodeConfig::new(
            NodeId("alpha".to_string()),
            LinkLabel::stable(),
            Some(HubEndpoint("127.0.0.1:1".to_string())),
            Vec::new(),
            "0.0.0".to_string(),
        )
        .unwrap()
    }

    fn call() -> RoutedCall {
        RoutedCall {
            connector: "echo-0".to_string(),
            method: "echo".to_string(),
            payload: vec![9],
        }
    }

    #[tokio::test]
    async fn ping_answers_with_identity_and_version() {
        let pong = NodeDaemon::stub(&config()).ping().await;
        assert_eq!(pong.node, NodeId("alpha".to_string()));
        assert_eq!(pong.agent_version, "0.0.0");
    }

    #[tokio::test]
    async fn a_stub_refuses_every_call() {
        let result = NodeDaemon::stub(&config()).deliver(call()).await;
        assert_eq!(result, Err(NodeError::UnknownConnector));
    }

    #[tokio::test]
    async fn a_registered_connector_answers() {
        let mut connectors = Connectors::new();
        connectors.register(Box::new(Echo));
        let daemon = NodeDaemon::new(&config(), Arc::new(connectors));
        let reply = daemon.deliver(call()).await.unwrap();
        assert_eq!(reply.payload, vec![9]);
    }
}
