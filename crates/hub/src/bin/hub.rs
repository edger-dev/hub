//! The hub daemon: bind, accept, serve — until killed.
//!
//! Address from the first argument or `HUB_ADDR`, defaulting to 127.0.0.1:15400;
//! several, comma-separated, serve the same hub on each (a bridge for local
//! nodes, a private network for peer hubs).
//! Peer hubs: `HUB_NAME` (this hub's name, defaulting to the hostname) and
//! `HUB_PEERS` (`name=addr,…`); without `HUB_PEERS` the hub stands alone.
//! WebSocket peers may also use `CF_ACCESS_CLIENT_ID` and
//! `CF_ACCESS_CLIENT_SECRET` as a pair.

use hub::AccessServiceToken;

/// This machine's hostname, as the kernel has it.
fn hostname() -> Option<String> {
    ["/proc/sys/kernel/hostname", "/etc/hostname"]
        .iter()
        .find_map(|path| std::fs::read_to_string(path).ok())
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
}

fn access_service_token(
    id: Option<String>,
    secret: Option<String>,
) -> Result<Option<AccessServiceToken>, String> {
    match (id, secret) {
        (Some(id), Some(secret)) if !id.is_empty() && !secret.is_empty() => {
            Ok(Some(AccessServiceToken { id, secret }))
        }
        (None, None) => Ok(None),
        _ => Err(
            "CF_ACCESS_CLIENT_ID and CF_ACCESS_CLIENT_SECRET must both be nonempty and set together"
                .into(),
        ),
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let first = args.next();
    if first.as_deref() == Some("keygen") {
        let peer = args.next().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "usage: hub keygen PEER PATH",
            )
        })?;
        let path = args.next().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "usage: hub keygen PEER PATH",
            )
        })?;
        let key = hub_protocol::SigningKey::generate(&peer)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
        std::fs::write(&path, format!("{}\n", key.private_hex()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        }
        println!("peer={peer}");
        println!("key_id={}", key.key_id());
        println!("public={}", key.public_hex());
        return Ok(());
    }
    let addr = first
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
    let name = std::env::var("HUB_NAME").ok().or_else(hostname);
    if !peers.is_empty() && name.is_none() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "peer hubs need a name: set HUB_NAME",
        ));
    }
    if let Some(name) = &name
        && name.contains(hub_protocol::PEER_SEPARATOR)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("HUB_NAME {name:?}: a name without `::`"),
        ));
    }
    if let Ok(path) = std::env::var("HUB_TRUST_FILE") {
        let trust = hub_protocol::TrustStore::from_file(path)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
        hub = hub.with_auth_trust(trust);
    }
    let signing_key = std::env::var("HUB_PRIVATE_KEY_FILE")
        .ok()
        .map(|path| {
            let peer = name.clone().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "HUB_PRIVATE_KEY_FILE requires HUB_NAME",
                )
            })?;
            hub_protocol::SigningKey::from_private_file(peer, path)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
        })
        .transpose()?;
    let mut set = name
        .as_ref()
        .map_or_else(hub::Peers::default, |name| hub::Peers::new(name.clone()));
    if let Some(key) = signing_key {
        set = set.with_signing_key(key);
    }
    if let Some(token) = &auth_token {
        set = set.with_auth_token(token.clone());
    }
    hub = hub.with_peers(set);
    if !peers.is_empty() {
        let access = access_service_token(
            std::env::var("CF_ACCESS_CLIENT_ID").ok(),
            std::env::var("CF_ACCESS_CLIENT_SECRET").ok(),
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        for (peer, peer_addr) in peers {
            println!(
                "hub {}: peer {peer} at {peer_addr}",
                name.as_deref().unwrap_or("unnamed")
            );
            hub.peers()
                .add(
                    peer.clone(),
                    hub::peer_dialer(&hub, &peer, peer_addr, access.clone()),
                )
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        }
    }

    let mut serving = tokio::task::JoinSet::new();
    if let Ok(ws_addr) = std::env::var("HUB_WS_ADDR") {
        let listener = hub::WsHubListener::bind(&ws_addr).await?;
        println!("hub listening for WebSocket on {}", listener.local_addr());
        serving.spawn(listener.serve(hub.clone()));
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_service_tokens_must_be_a_nonempty_pair() {
        assert!(access_service_token(None, None).unwrap().is_none());
        assert!(
            access_service_token(Some("id".into()), Some("secret".into()))
                .unwrap()
                .is_some()
        );
        for (id, secret) in [
            (Some("id".into()), None),
            (None, Some("secret".into())),
            (Some(String::new()), Some("secret".into())),
            (Some("id".into()), Some(String::new())),
        ] {
            assert!(access_service_token(id, secret).is_err());
        }
    }
}
