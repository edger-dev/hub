//! # node
//!
//! The per-machine node daemon (§2.1). A node **reverse-dials** the hub, **serves**
//! [`hub_protocol::NodeService`], and **registers** itself — it never binds a local
//! listener, and it requires a hub to be configured (§2.4).
//!
//! This is the *stub*: it speaks the protocol edge faithfully but stubs the actual
//! connector behaviour ([`StubNode::deliver`] refuses every call). Real connectors,
//! and production transport wiring, come later.
//!
//! task: fab72ddcdfba854a1330679380958961aeb445a93660bf8445dfdf6f6400e75f

pub mod config;
pub mod dial;
pub mod stub;

pub use config::{ConfigError, HubEndpoint, NodeConfig};
pub use dial::{DialError, serve_and_register};
pub use stub::StubNode;
