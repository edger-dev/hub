//! Dialing the hub over TCP (§2.1, §2.4).
//!
//! The node only ever dials *out*. A hub that is down is not an error here — it
//! is the ordinary case the reconnect loop exists for, so a failed dial yields
//! no link rather than propagating a failure.

// implements: d661c899202c18f1d5a968f14d5c87a9d6b422aa62e450dd2134a1c1bfdec4cc@d661c899202c18f1d5a968f14d5c87a9d6b422aa62e450dd2134a1c1bfdec4cc

use tokio::net::TcpStream;
use vox::transport::tcp::StreamLink;

use crate::config::{ConfigError, HubEndpoint};

/// A vox link over a TCP stream — the concrete type `StreamLink::tcp` yields.
pub type TcpLink = StreamLink<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>;

impl HubEndpoint {
    /// The `host:port` this endpoint dials.
    ///
    /// Accepts `tcp://host:port` or a bare `host:port`. Any other scheme is a
    /// config error — the node should fail loudly at startup rather than retry
    /// an address it can never reach (§2.4).
    pub fn dial_target(&self) -> Result<&str, ConfigError> {
        match self.0.split_once("://") {
            None => Ok(&self.0),
            Some(("tcp", rest)) => Ok(rest),
            Some((scheme, _)) => Err(ConfigError::UnsupportedScheme(scheme.to_string())),
        }
    }
}

/// Dial the hub over TCP, or yield `None` if it is not reachable right now.
///
/// `None` is the "hub is down / asleep / restarting" case — the reconnect loop
/// backs off and tries again (§2.1). A misconfigured address is different: it is
/// surfaced as an error so it cannot be silently retried forever.
pub async fn dial(endpoint: &HubEndpoint) -> Result<Option<TcpLink>, ConfigError> {
    let target = endpoint.dial_target()?;
    match TcpStream::connect(target).await {
        Ok(socket) => Ok(Some(StreamLink::tcp(socket))),
        // Connection refused / unreachable / timed out — the hub blinked.
        Err(_) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_bare_host_port() {
        let e = HubEndpoint("127.0.0.1:15400".to_string());
        assert_eq!(e.dial_target().unwrap(), "127.0.0.1:15400");
    }

    #[test]
    fn accepts_a_tcp_url() {
        let e = HubEndpoint("tcp://127.0.0.1:15400".to_string());
        assert_eq!(e.dial_target().unwrap(), "127.0.0.1:15400");
    }

    #[test]
    fn rejects_an_unsupported_scheme() {
        let e = HubEndpoint("ws://127.0.0.1:15400".to_string());
        assert_eq!(
            e.dial_target().unwrap_err(),
            ConfigError::UnsupportedScheme("ws".to_string())
        );
    }

    #[tokio::test]
    async fn a_closed_port_yields_no_link_not_an_error() {
        // Port 1 on loopback is not listening: the hub is "down".
        let e = HubEndpoint("127.0.0.1:1".to_string());
        let link = dial(&e).await.expect("a down hub is not a config error");
        assert!(link.is_none(), "a down hub yields no link, to be retried");
    }
}
