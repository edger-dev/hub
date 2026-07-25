//! Discovery fan-out — the live topology tail (§2.6).
//!
//! Consumers subscribe with a `Tx<TopologyEvent>`; every registry change is
//! delivered to all of them. A subscriber whose channel has gone away is dropped
//! silently: a console tab closing is ordinary, not an error the hub reports.

// implements: 7c924d1c2994326c261f907f81c39f0fa676381b53f7898556893bb4f750d85c@7c924d1c2994326c261f907f81c39f0fa676381b53f7898556893bb4f750d85c

use hub_protocol::TopologyEvent;
use vox::Tx;

/// The set of live topology subscribers.
#[derive(Default)]
pub struct Subscribers {
    channels: Vec<Tx<TopologyEvent>>,
}

impl Subscribers {
    /// No subscribers yet.
    pub fn new() -> Self {
        Self {
            channels: Vec::new(),
        }
    }

    /// Add a subscriber's channel to the fan-out.
    pub fn add(&mut self, events: Tx<TopologyEvent>) {
        self.channels.push(events);
    }

    /// How many subscribers are currently attached.
    pub fn len(&self) -> usize {
        self.channels.len()
    }

    /// Whether anyone is listening.
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    /// Deliver `event` to every subscriber, dropping those that have gone away.
    ///
    /// Returns the number of subscribers that received it.
    pub async fn broadcast(&mut self, event: TopologyEvent) -> usize {
        let mut live = Vec::with_capacity(self.channels.len());
        let mut delivered = 0;
        for channel in std::mem::take(&mut self.channels) {
            if channel.send(event.clone()).await.is_ok() {
                delivered += 1;
                live.push(channel);
            }
            // A send error means the consumer is gone — drop the channel.
        }
        self.channels = live;
        delivered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: delivery is deliberately *not* unit-tested here. A `Tx` only carries
    // values once the framework binds it, which happens when the handle is passed
    // in an RPC method's arguments (see vox's `channel()` docs); a locally paired
    // Tx/Rx never binds, so `send` would block forever. Fan-out delivery is proven
    // end-to-end against a real `subscribe_topology` call instead.

    #[test]
    fn subscribers_start_empty() {
        let subs = Subscribers::new();
        assert!(subs.is_empty());
        assert_eq!(subs.len(), 0);
    }
}
