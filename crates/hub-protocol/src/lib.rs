//! # hub-protocol
//!
//! The hub's **stable protocol boundary** — the facet message + identity types
//! every role exchanges, and (in a following slice) the vox service traits they
//! dial. Nodes, consumers, and the hub itself bind to this contract; what sits
//! behind it (the event store, the connector set, which node hosts what) stays
//! swappable.
//!
//! This crate is the *contract only* — no hub or node implementation lives here.
//!
//! task: c6178132314d68a977b50be334976aa0ab5a22547331573f4dea0ef61027048b

pub mod connector;
pub mod discovery;
pub mod errors;
pub mod identity;
pub mod registration;
pub mod routing;
pub mod service;
pub mod targeting;

pub use connector::{Connector, ConnectorKind};
pub use discovery::{LinkInfo, NodeInfo, TopologyEvent, TopologySnapshot};
pub use errors::{HubError, NodeError, RouteError};
pub use identity::{LinkLabel, NodeId, NodeLink};
pub use registration::Registration;
pub use routing::{RoutedCall, RoutedReply};
pub use service::{ConsumerApi, NodeIngress, NodeService, Pong};
pub use targeting::LinkSelector;
