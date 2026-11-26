//! Live agent-session registration and direct messages.

/// The largest session message body, to keep one call from exhausting a hub.
pub const MAX_SESSION_MESSAGE_BYTES: usize = 64 * 1024;

/// One live agent session as registered by its shim.
#[derive(facet::Facet, Debug, Clone, PartialEq, Eq)]
pub struct SessionInfo {
    pub id: String,
    pub node: String,
    pub harness: String,
    pub workspace: String,
    pub context: Option<String>,
}

impl SessionInfo {
    /// Qualify this session's identity when it crosses from its origin hub.
    pub fn qualify(&mut self, hub: &str) {
        self.id = format!("{hub}::{}", self.id);
        self.node = format!("{hub}::{}", self.node);
    }
}

/// One direct message; `from` is established by the authenticated origin.
#[derive(facet::Facet, Debug, Clone, PartialEq, Eq)]
pub struct SessionMessage {
    pub id: String,
    pub from: SessionInfo,
    pub body: String,
}
