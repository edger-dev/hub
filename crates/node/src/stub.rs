//! [`StubNode`] — a node that speaks [`NodeService`] but stubs connector work.

use hub_protocol::{NodeError, NodeId, NodeService, Pong, RoutedCall, RoutedReply};

use crate::config::NodeConfig;

/// A node-side [`NodeService`] handler with no real connectors wired.
///
/// It answers the hub's liveness probe honestly (so out-of-band deploy checks
/// work, §5) but refuses every routed call — there is nothing behind the edge
/// yet. Real connector dispatch replaces [`StubNode::deliver`] later.
#[derive(Debug, Clone)]
pub struct StubNode {
    node: NodeId,
    agent_version: String,
}

impl StubNode {
    /// A stub identified by its node name and daemon version.
    pub fn new(node: NodeId, agent_version: String) -> Self {
        Self {
            node,
            agent_version,
        }
    }

    /// A stub for the node described by `config`.
    pub fn from_config(config: &NodeConfig) -> Self {
        Self::new(config.node.clone(), config.agent_version.clone())
    }
}

impl NodeService for StubNode {
    async fn ping(&self) -> Pong {
        Pong {
            node: self.node.clone(),
            agent_version: self.agent_version.clone(),
        }
    }

    async fn deliver(&self, _call: RoutedCall) -> Result<RoutedReply, NodeError> {
        // No connectors are wired in the stub — every call is refused.
        Err(NodeError::UnknownConnector)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stub() -> StubNode {
        StubNode::new(NodeId("alpha".to_string()), "0.0.0".to_string())
    }

    #[tokio::test]
    async fn ping_answers_with_node_identity_and_version() {
        let pong = stub().ping().await;
        assert_eq!(pong.node, NodeId("alpha".to_string()));
        assert_eq!(pong.agent_version, "0.0.0");
    }

    #[tokio::test]
    async fn deliver_refuses_every_call_in_the_stub() {
        let call = RoutedCall {
            connector: "pm-0".to_string(),
            method: "spawn".to_string(),
            payload: vec![],
        };
        let result = stub().deliver(call).await;
        assert_eq!(result, Err(NodeError::UnknownConnector));
    }
}
