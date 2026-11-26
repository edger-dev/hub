//! # hub
//!
//! The hub: the stable boundary of the mesh (§1). It accepts reverse-dialed
//! nodes, maintains the live registry, serves discovery from it, and routes
//! consumer calls to the selected link.
//!
//! The hub owns nothing durable except **routing** — the registry is a projection
//! of who is connected right now (§2.6).
//!
//! task: 6abbf3122736c91bbb0fc2de579e76a2adc4df1837e0577079e37fcc3ea19118

pub mod discovery;
pub mod events;
#[allow(clippy::module_inception)]
pub mod hub;
pub mod peers;
pub mod registry;
pub mod serve;
mod sessions;
pub mod tcp;
pub mod ws;

pub use discovery::Subscribers;
pub use events::EventSubscribers;
pub use hub::Hub;
pub use peers::{AccessServiceToken, Dialer, Peers, parse_peers, peer_dialer, tcp_dialer};
pub use registry::{LinkEntry, Registry};
pub use serve::{ServedHub, serve_connection};
pub use tcp::{HubListener, serve_tcp};
pub use ws::WsHubListener;
