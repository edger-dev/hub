//! Live agent sessions, registered by their own connections.
//!
//! implements: 396fba0cab2ac48f5fdf59a3258f377fc04aa9c6d7f42d27f4be9b1eafcb8b38@7ce52c42b71efc012dae8b790d4910cf81f811bdb6354dc6e2a40cde6b511ebc

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use hub_protocol::service::SessionServiceClient;
use hub_protocol::{HubError, PEER_SEPARATOR, SessionInfo};

#[derive(Clone)]
struct Entry {
    info: SessionInfo,
    client: SessionServiceClient,
    registration: u64,
}

/// The live local session registry; peers read it through `local_sessions`.
#[derive(Clone)]
pub(crate) struct Sessions {
    entries: Arc<Mutex<BTreeMap<String, BTreeMap<u64, Entry>>>>,
    next_registration: Arc<AtomicU64>,
}

impl Default for Sessions {
    fn default() -> Self {
        Self {
            entries: Arc::default(),
            next_registration: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl Sessions {
    pub(crate) fn register(
        &self,
        info: SessionInfo,
        client: SessionServiceClient,
    ) -> Result<u64, HubError> {
        if info.id.is_empty() || info.id.contains(PEER_SEPARATOR) {
            return Err(HubError::Rejected(format!(
                "session id {:?}: nonempty, without {PEER_SEPARATOR}",
                info.id
            )));
        }
        if info.node.is_empty() || info.node.contains(PEER_SEPARATOR) {
            return Err(HubError::Rejected(format!(
                "session node {:?}: nonempty, without {PEER_SEPARATOR}",
                info.node
            )));
        }
        if info.harness.is_empty() {
            return Err(HubError::Rejected(
                "session harness must not be empty".into(),
            ));
        }
        let registration = self.next_registration.fetch_add(1, Ordering::Relaxed);
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(info.id.clone())
            .or_default()
            .insert(
                registration,
                Entry {
                    info,
                    client,
                    registration,
                },
            );
        Ok(registration)
    }

    pub(crate) fn unregister(&self, id: &str, registration: u64) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let empty = entries.get_mut(id).is_some_and(|registrations| {
            registrations.remove(&registration);
            registrations.is_empty()
        });
        if empty {
            entries.remove(id);
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<SessionInfo> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter_map(|registrations| registrations.last_key_value())
            .map(|(_, entry)| entry.info.clone())
            .collect()
    }

    pub(crate) fn is_current(&self, id: &str, registration: u64) -> bool {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .and_then(|registrations| registrations.last_key_value())
            .is_some_and(|(_, entry)| entry.registration == registration)
    }

    pub(crate) fn client(&self, id: &str) -> Option<(SessionInfo, SessionServiceClient)> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .and_then(|registrations| registrations.last_key_value())
            .map(|(_, entry)| (entry.info.clone(), entry.client.clone()))
    }
}
