//! The hub's shared state — the registry plus its discovery subscribers.
//!
//! Every mutation goes through here so the topology and the tail can never
//! disagree: the event a change implies is broadcast as part of applying it.

use std::sync::Arc;

use hub_protocol::{HubError, LinkSelector, NodeLink, Registration, TopologySnapshot};
use tokio::sync::Mutex;
use vox::Tx;

use crate::discovery::Subscribers;
use crate::registry::Registry;

/// Shared hub state, cloneable across connection tasks.
///
/// `T` is the per-link handle used to call a node back (a `NodeService` client
/// in production), which keeps this testable without a transport.
pub struct Hub<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

struct Inner<T> {
    registry: Registry<T>,
    subscribers: Subscribers,
}

impl<T> Clone for Hub<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Clone> Default for Hub<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> Hub<T> {
    /// A hub with nothing connected.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                registry: Registry::new(),
                subscribers: Subscribers::new(),
            })),
        }
    }

    /// Register a link and broadcast the topology change it implies (§2.1, §2.6).
    pub async fn register(&self, registration: Registration, handle: T) {
        let mut inner = self.inner.lock().await;
        if let Some(event) = inner.registry.register(registration, handle) {
            inner.subscribers.broadcast(event).await;
        }
    }

    /// Drop a link and broadcast the topology change it implies (§2.6).
    pub async fn disconnect(&self, target: &NodeLink) {
        let mut inner = self.inner.lock().await;
        if let Some(event) = inner.registry.remove(target) {
            inner.subscribers.broadcast(event).await;
        }
    }

    /// The live topology (§2.6).
    pub async fn snapshot(&self) -> TopologySnapshot {
        self.inner.lock().await.registry.snapshot()
    }

    /// Attach a consumer to the live topology tail (§2.6).
    pub async fn subscribe(&self, events: Tx<hub_protocol::TopologyEvent>) {
        self.inner.lock().await.subscribers.add(events);
    }

    /// Resolve a routing target to the handle that should answer it (§2.5).
    pub async fn resolve(&self, target: &LinkSelector) -> Result<T, HubError> {
        let inner = self.inner.lock().await;
        inner.registry.resolve(target).map(|(_, entry)| entry.handle.clone())
    }

    /// How many subscribers are attached (test/observability helper).
    pub async fn subscriber_count(&self) -> usize {
        self.inner.lock().await.subscribers.len()
    }
}
