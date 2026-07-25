//! Reconnect forever (§2.1).
//!
//! A node keeps retrying until the hub answers, and re-dials whenever an
//! established connection drops. A hub restart, a sleeping hub host, a crash, or
//! a deploy all reduce to the same transient — "the hub blinked" — and the node
//! recovers on its own. The loop never gives up; it only backs off.

// implements: d661c899202c18f1d5a968f14d5c87a9d6b422aa62e450dd2134a1c1bfdec4cc@d661c899202c18f1d5a968f14d5c87a9d6b422aa62e450dd2134a1c1bfdec4cc

use std::future::Future;
use std::time::Duration;

use crate::config::NodeConfig;
use crate::dial::{DialError, serve_and_register};
use vox::Link;

/// Bounded exponential backoff between dial attempts.
///
/// "Forever" is patient, not a hot loop (§2.1 escape hatch): the delay doubles up
/// to `max`, and resets once a connection is established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backoff {
    pub initial: Duration,
    pub max: Duration,
}

impl Backoff {
    /// The delay after `failures` consecutive failed attempts, capped at `max`.
    pub fn delay(&self, failures: u32) -> Duration {
        if failures == 0 {
            return Duration::ZERO;
        }
        let shift = failures.saturating_sub(1).min(32);
        self.initial
            .saturating_mul(1u32.checked_shl(shift).unwrap_or(u32::MAX))
            .min(self.max)
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(250),
            max: Duration::from_secs(30),
        }
    }
}

/// What happened on one pass of the reconnect loop — reported to the observer so
/// a caller (or a test) can watch the loop without draining it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialOutcome {
    /// Dialed, served, and registered; the connection then dropped.
    ConnectedThenDropped,
    /// The attempt failed before a connection was established.
    Failed(DialError),
}

/// Dial the hub and stay connected, re-dialing forever (§2.1).
///
/// `dial` produces a fresh [`Link`] per attempt (the transport is the caller's
/// choice, which keeps this testable over memory links). `sleep` is the delay
/// hook — `tokio::time::sleep` in production. `observe` is called once per pass;
/// return `false` from it to stop the loop, which is how a caller shuts a node
/// down (and how tests bound the run).
pub async fn run<L, Dial, DialFut, Sleep, SleepFut, Observe>(
    config: &NodeConfig,
    backoff: &Backoff,
    mut dial: Dial,
    mut sleep: Sleep,
    mut observe: Observe,
) where
    L: Link + Send + 'static,
    L::Tx: Send + 'static,
    L::Rx: Send + 'static,
    Dial: FnMut() -> DialFut,
    DialFut: Future<Output = Option<L>>,
    Sleep: FnMut(Duration) -> SleepFut,
    SleepFut: Future<Output = ()>,
    Observe: FnMut(DialOutcome) -> bool,
{
    let mut failures: u32 = 0;
    loop {
        let delay = backoff.delay(failures);
        if !delay.is_zero() {
            sleep(delay).await;
        }

        // A dialer that yields no link is itself a failed attempt — the hub host
        // may be down or asleep; keep trying (§2.1).
        let Some(link) = dial().await else {
            failures = failures.saturating_add(1);
            if !observe(DialOutcome::Failed(DialError::Establish(
                "no link from dialer".to_string(),
            ))) {
                return;
            }
            continue;
        };

        match serve_and_register(link, config).await {
            Ok(connection) => {
                failures = 0;
                // Hold the connection until the hub goes away, then re-dial.
                connection.closed().await;
                if !observe(DialOutcome::ConnectedThenDropped) {
                    return;
                }
            }
            Err(error) => {
                failures = failures.saturating_add(1);
                if !observe(DialOutcome::Failed(error)) {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_then_caps() {
        let b = Backoff {
            initial: Duration::from_millis(100),
            max: Duration::from_millis(500),
        };
        assert_eq!(b.delay(0), Duration::ZERO, "first attempt is immediate");
        assert_eq!(b.delay(1), Duration::from_millis(100));
        assert_eq!(b.delay(2), Duration::from_millis(200));
        assert_eq!(b.delay(3), Duration::from_millis(400));
        assert_eq!(b.delay(4), Duration::from_millis(500), "capped");
        assert_eq!(b.delay(99), Duration::from_millis(500), "stays capped");
    }
}
