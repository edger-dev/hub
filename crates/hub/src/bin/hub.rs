//! The hub daemon: bind, accept, serve — until killed.
//!
//! Address from the first argument or `HUB_ADDR`, defaulting to 127.0.0.1:15400;
//! several, comma-separated, serve the same hub on each (a bridge for local
//! nodes, a private network for peer hubs).
//! Peer hubs: `HUB_NAME` (this hub's name, defaulting to the hostname) and
//! `HUB_PEERS` (`name=addr,…`); without `HUB_PEERS` the hub stands alone.

/// This machine's hostname, as the kernel has it.
fn hostname() -> Option<String> {
    ["/proc/sys/kernel/hostname", "/etc/hostname"]
        .iter()
        .find_map(|path| std::fs::read_to_string(path).ok())
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("HUB_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:15400".to_string());

    let auth_token = std::env::var("HUB_AUTH_TOKEN")
        .ok()
        .map(|token| {
            if token.is_empty() {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "HUB_AUTH_TOKEN must not be empty",
                ))
            } else {
                Ok(token)
            }
        })
        .transpose()?;

    let peers = std::env::var("HUB_PEERS").unwrap_or_default();
    let peers = hub::parse_peers(&peers)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let mut hub = hub::ServedHub::new();
    if let Some(token) = &auth_token {
        hub = hub.with_auth_token(token.as_str());
    }
    if !peers.is_empty() {
        let name = std::env::var("HUB_NAME")
            .ok()
            .or_else(hostname)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "peer hubs need a name: set HUB_NAME",
                )
            })?;
        if name.contains(hub_protocol::PEER_SEPARATOR) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("HUB_NAME {name:?}: a name without `::`"),
            ));
        }
        let mut set = hub::Peers::new(name.clone());
        if let Some(token) = auth_token {
            set = set.with_auth_token(token);
        }
        for (peer, peer_addr) in peers {
            println!("hub {name}: peer {peer} at {peer_addr}");
            set.add(peer, hub::tcp_dialer(peer_addr))
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        }
        hub = hub.with_peers(set);
    }

    let mut serving = tokio::task::JoinSet::new();
    for addr in addr.split(',').map(str::trim).filter(|a| !a.is_empty()) {
        let listener = hub::HubListener::bind(addr).await?;
        println!("hub listening on {}", listener.local_addr());
        serving.spawn(listener.serve(hub.clone()));
    }
    // Any listener ending ends the hub (its service restarts it).
    match serving.join_next().await {
        Some(Ok(result)) => result,
        Some(Err(e)) => Err(std::io::Error::other(e)),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no address to listen on",
        )),
    }
}
