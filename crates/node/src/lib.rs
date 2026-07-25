//! # node
//!
//! The per-machine node daemon (§2.1). A node **reverse-dials** the hub, **serves**
//! [`hub_protocol::NodeService`], and **registers** itself — it never binds a local
//! listener, and it requires a hub to be configured (§2.4).
//!
//! Capabilities are [`Connector`]s registered on the daemon; a routed call is
//! dispatched to the addressed connector, which owns its own payload encoding.
//! A node with nothing registered refuses every call.
//!
//! task: fab72ddcdfba854a1330679380958961aeb445a93660bf8445dfdf6f6400e75f

pub mod config;
pub mod connector;
pub mod daemon;
pub mod dial;
pub mod files;
pub mod reconnect;
pub mod tcp;

pub use config::{ConfigError, HubEndpoint, NodeConfig};
pub use dial::{DialError, serve_and_register, serve_and_register_with};
pub use reconnect::{Backoff, DialOutcome};
pub use connector::{Connector, Connectors};
pub use daemon::NodeDaemon;
pub use files::FilesConnector;
