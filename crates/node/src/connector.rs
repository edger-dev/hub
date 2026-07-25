//! Connectors — the capabilities a node actually offers (§1).
//!
//! A connector is addressed by id and owns its own payload encoding; the hub
//! routes on the addressing fields and never looks inside (§1). Adding a
//! capability therefore touches this node only — not the hub, not the protocol.

// implements: d78fb95fddc31aa42bc927da1eaed44292529b3671d1ef9ef58ec8cd0858e51c@d78fb95fddc31aa42bc927da1eaed44292529b3671d1ef9ef58ec8cd0858e51c

use std::collections::BTreeMap;

use hub_protocol::{Connector as ConnectorDescriptor, NodeError, RoutedCall, RoutedReply};

/// One capability this node offers.
///
/// `call` receives the method name and the opaque request bytes, and returns
/// opaque response bytes. What those bytes mean is entirely this connector's
/// business.
pub trait Connector: Send + Sync {
    /// How this connector appears in `Registration` and in discovery (§2.6).
    fn descriptor(&self) -> ConnectorDescriptor;

    /// Handle one call. An unrecognised `method` should be refused, not ignored.
    fn call(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>, NodeError>;
}

/// The connectors a node link serves, addressed by descriptor id.
#[derive(Default)]
pub struct Connectors {
    by_id: BTreeMap<String, Box<dyn Connector>>,
}

impl Connectors {
    /// A node offering nothing.
    pub fn new() -> Self {
        Self {
            by_id: BTreeMap::new(),
        }
    }

    /// Offer `connector` under its descriptor id, replacing any previous one.
    pub fn register(&mut self, connector: Box<dyn Connector>) -> &mut Self {
        self.by_id.insert(connector.descriptor().id, connector);
        self
    }

    /// Every connector's descriptor — what this link advertises (§2.6).
    pub fn descriptors(&self) -> Vec<ConnectorDescriptor> {
        self.by_id.values().map(|c| c.descriptor()).collect()
    }

    /// Whether anything is registered.
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Dispatch a routed call to the addressed connector.
    ///
    /// An id this node does not serve is [`NodeError::UnknownConnector`] — the
    /// same answer a node with no connectors at all gives.
    pub fn dispatch(&self, call: &RoutedCall) -> Result<RoutedReply, NodeError> {
        let connector = self
            .by_id
            .get(&call.connector)
            .ok_or(NodeError::UnknownConnector)?;
        connector
            .call(&call.method, &call.payload)
            .map(|payload| RoutedReply { payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hub_protocol::ConnectorKind;

    struct Echo;

    impl Connector for Echo {
        fn descriptor(&self) -> ConnectorDescriptor {
            ConnectorDescriptor {
                id: "echo-0".to_string(),
                kind: ConnectorKind::Other("echo".to_string()),
            }
        }

        fn call(&self, method: &str, payload: &[u8]) -> Result<Vec<u8>, NodeError> {
            match method {
                "echo" => Ok(payload.to_vec()),
                other => Err(NodeError::Rejected(format!("unknown method `{other}`"))),
            }
        }
    }

    fn call(connector: &str, method: &str) -> RoutedCall {
        RoutedCall {
            connector: connector.to_string(),
            method: method.to_string(),
            payload: vec![1, 2, 3],
        }
    }

    fn registry() -> Connectors {
        let mut c = Connectors::new();
        c.register(Box::new(Echo));
        c
    }

    #[test]
    fn dispatch_reaches_the_addressed_connector() {
        let reply = registry().dispatch(&call("echo-0", "echo")).unwrap();
        assert_eq!(reply.payload, vec![1, 2, 3]);
    }

    #[test]
    fn an_unknown_connector_id_is_refused() {
        let err = registry().dispatch(&call("nope-0", "echo")).unwrap_err();
        assert_eq!(err, NodeError::UnknownConnector);
    }

    #[test]
    fn an_unknown_method_is_refused_by_the_connector() {
        let err = registry().dispatch(&call("echo-0", "nope")).unwrap_err();
        assert!(matches!(err, NodeError::Rejected(_)));
    }

    #[test]
    fn a_node_with_no_connectors_refuses_everything() {
        let err = Connectors::new().dispatch(&call("echo-0", "echo")).unwrap_err();
        assert_eq!(err, NodeError::UnknownConnector);
    }

    #[test]
    fn descriptors_advertise_what_is_registered() {
        let descriptors = registry().descriptors();
        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors[0].id, "echo-0");
    }
}
