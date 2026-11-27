//! Per-peer Ed25519 challenge authentication.

// implements: hub::spec::connection-auth@bd6ac42fe5e75451450c5183ae1892b19f92c718138de69e222e428197d940bd

use std::collections::{BTreeMap, BTreeSet};

use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};

/// A single-use server challenge for one service role.
#[derive(facet::Facet, Debug, Clone, PartialEq, Eq)]
pub struct AuthChallenge {
    pub hub: String,
    pub role: String,
    pub nonce: Vec<u8>,
}

impl AuthChallenge {
    pub fn new(hub: &str, role: &str) -> Result<Self, String> {
        if hub.is_empty() || role.is_empty() {
            return Err("authentication hub and role must be nonempty".into());
        }
        let mut nonce = vec![0; 32];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| "could not generate an authentication nonce".to_string())?;
        Ok(Self {
            hub: hub.into(),
            role: role.into(),
            nonce,
        })
    }
}

/// Proof of possession of a peer's private key.
#[derive(facet::Facet, Debug, Clone, PartialEq, Eq)]
pub struct AuthProof {
    pub peer: String,
    pub key_id: String,
    pub signature: Vec<u8>,
}

/// A peer's Ed25519 signing identity.
#[derive(Clone)]
pub struct SigningKey {
    peer: String,
    seed: [u8; 32],
    public: [u8; 32],
}

impl std::fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigningKey")
            .field("peer", &self.peer)
            .field("key_id", &self.key_id())
            .finish()
    }
}

impl SigningKey {
    pub fn generate(peer: impl Into<String>) -> Result<Self, String> {
        let mut seed = [0; 32];
        SystemRandom::new()
            .fill(&mut seed)
            .map_err(|_| "could not generate an Ed25519 key".to_string())?;
        Self::from_seed(peer.into(), seed)
    }

    pub fn from_private_hex(peer: impl Into<String>, private: &str) -> Result<Self, String> {
        let seed = decode_hex(private)?;
        let seed: [u8; 32] = seed
            .try_into()
            .map_err(|_| "an Ed25519 private key must contain 32 bytes".to_string())?;
        Self::from_seed(peer.into(), seed)
    }

    fn from_seed(peer: String, seed: [u8; 32]) -> Result<Self, String> {
        if peer.is_empty() || peer.contains(crate::PEER_SEPARATOR) {
            return Err("a peer identity must be nonempty and not contain `::`".into());
        }
        let pair = Ed25519KeyPair::from_seed_unchecked(&seed)
            .map_err(|error| format!("invalid Ed25519 private key: {error}"))?;
        let public: [u8; 32] = pair
            .public_key()
            .as_ref()
            .try_into()
            .map_err(|_| "an Ed25519 public key must contain 32 bytes".to_string())?;
        Ok(Self { peer, seed, public })
    }

    fn pair(&self) -> Result<Ed25519KeyPair, String> {
        Ed25519KeyPair::from_seed_unchecked(&self.seed)
            .map_err(|error| format!("invalid Ed25519 private key: {error}"))
    }

    pub fn from_private_file(
        peer: impl Into<String>,
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, String> {
        let path = path.as_ref();
        let private = std::fs::read_to_string(path)
            .map_err(|error| format!("cannot read private key {}: {error}", path.display()))?;
        Self::from_private_hex(peer, &private)
    }

    pub fn private_hex(&self) -> String {
        encode_hex(&self.seed)
    }

    pub fn public_hex(&self) -> String {
        encode_hex(&self.public)
    }

    pub fn key_id(&self) -> String {
        key_id(&self.public)
    }

    pub fn sign(&self, challenge: &AuthChallenge) -> Result<AuthProof, String> {
        if challenge.nonce.len() != 32 {
            return Err("an authentication nonce must contain 32 bytes".into());
        }
        let key_id = self.key_id();
        let message = challenge_bytes(challenge, &self.peer, &key_id)?;
        let signature = self.pair()?.sign(&message).as_ref().to_vec();
        Ok(AuthProof {
            peer: self.peer.clone(),
            key_id,
            signature,
        })
    }
}

#[derive(Clone, Default)]
struct TrustedPeer {
    keys: BTreeMap<String, Vec<u8>>,
    roles: BTreeSet<String>,
}

/// Public keys and role grants trusted by one hub.
#[derive(Clone, Default)]
pub struct TrustStore {
    peers: BTreeMap<String, TrustedPeer>,
}

impl TrustStore {
    pub fn add(&mut self, peer: &str, public_hex: &str, roles: &[&str]) -> Result<(), String> {
        if peer.is_empty() || peer.contains(crate::PEER_SEPARATOR) {
            return Err("a peer identity must be nonempty and not contain `::`".into());
        }
        if roles.is_empty() {
            return Err(format!("peer {peer:?} needs at least one role"));
        }
        let public = decode_hex(public_hex)?;
        if public.len() != 32 {
            return Err("an Ed25519 public key must contain 32 bytes".into());
        }
        let key_id = key_id(&public);
        let trusted = self.peers.entry(peer.to_string()).or_default();
        for role in roles {
            if !matches!(*role, "node" | "peer" | "session" | "consumer") {
                return Err(format!("unknown authentication role {role:?}"));
            }
            trusted.roles.insert((*role).to_string());
        }
        trusted.keys.insert(key_id, public);
        Ok(())
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let mut trust = Self::default();
        for (index, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let fields: Vec<&str> = line.split_whitespace().collect();
            let [peer, public, roles] = fields.as_slice() else {
                return Err(format!(
                    "trust line {} must be `peer public-key role,…`",
                    index + 1
                ));
            };
            let roles: Vec<&str> = roles.split(',').collect();
            trust
                .add(peer, public, &roles)
                .map_err(|error| format!("trust line {}: {error}", index + 1))?;
        }
        Ok(trust)
    }

    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self, String> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("cannot read trust file {}: {error}", path.display()))?;
        Self::parse(&text)
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    pub fn verify(&self, challenge: &AuthChallenge, proof: &AuthProof) -> Result<String, String> {
        let trusted = self
            .peers
            .get(&proof.peer)
            .ok_or_else(|| format!("unknown authentication peer {:?}", proof.peer))?;
        if !trusted.roles.contains(&challenge.role) {
            return Err(format!(
                "peer {:?} is not allowed the {:?} role",
                proof.peer, challenge.role
            ));
        }
        let public = trusted
            .keys
            .get(&proof.key_id)
            .ok_or_else(|| format!("unknown key for peer {:?}", proof.peer))?;
        let message = challenge_bytes(challenge, &proof.peer, &proof.key_id)?;
        UnparsedPublicKey::new(&ED25519, public)
            .verify(&message, &proof.signature)
            .map_err(|_| format!("invalid signature for peer {:?}", proof.peer))?;
        Ok(proof.peer.clone())
    }
}

fn key_id(public: &[u8]) -> String {
    encode_hex(ring::digest::digest(&ring::digest::SHA256, public).as_ref())
}

fn challenge_bytes(challenge: &AuthChallenge, peer: &str, key_id: &str) -> Result<Vec<u8>, String> {
    if challenge.nonce.len() != 32 {
        return Err("an authentication nonce must contain 32 bytes".into());
    }
    let mut message = b"kudo-hub-auth-v1".to_vec();
    for field in [
        challenge.hub.as_bytes(),
        challenge.role.as_bytes(),
        peer.as_bytes(),
        key_id.as_bytes(),
        &challenge.nonce,
    ] {
        let length = u32::try_from(field.len()).map_err(|_| "authentication field too long")?;
        message.extend_from_slice(&length.to_le_bytes());
        message.extend_from_slice(field);
    }
    Ok(message)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(HEX[(byte >> 4) as usize] as char);
        text.push(HEX[(byte & 15) as usize] as char);
    }
    text
}

fn decode_hex(text: &str) -> Result<Vec<u8>, String> {
    let text = text.trim();
    if !text.len().is_multiple_of(2) {
        return Err("hex key material must have an even length".into());
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = digit(pair[0])?;
            let low = digit(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn digit(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex digit {:?}", byte as char)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_challenges_verify_identity_and_rotation() {
        let old = SigningKey::generate("edger").unwrap();
        let new = SigningKey::generate("edger").unwrap();
        let other = SigningKey::generate("pc").unwrap();
        let challenge = AuthChallenge {
            hub: "hub".into(),
            role: "peer".into(),
            nonce: vec![7; 32],
        };
        let old_proof = old.sign(&challenge).unwrap();
        let new_proof = new.sign(&challenge).unwrap();
        let mut trust = TrustStore::default();
        trust.add("edger", &old.public_hex(), &["peer"]).unwrap();
        trust
            .add("edger", &new.public_hex(), &["peer", "node"])
            .unwrap();
        assert_eq!(trust.verify(&challenge, &old_proof).unwrap(), "edger");
        assert_eq!(trust.verify(&challenge, &new_proof).unwrap(), "edger");

        let wrong_key = other.sign(&challenge).unwrap();
        assert!(trust.verify(&challenge, &wrong_key).is_err());
        let wrong_hub = AuthChallenge {
            hub: "other-hub".into(),
            ..challenge.clone()
        };
        assert!(trust.verify(&wrong_hub, &old_proof).is_err());
        let wrong_role = AuthChallenge {
            role: "consumer".into(),
            ..challenge
        };
        assert!(trust.verify(&wrong_role, &old_proof).is_err());
    }

    #[test]
    fn generated_keys_are_distinct_and_private_material_round_trips() {
        let first = SigningKey::generate("edger").unwrap();
        let second = SigningKey::generate("edger").unwrap();
        assert_ne!(first.public_hex(), second.public_hex());
        assert_eq!(first.private_hex().len(), 64);
        assert_eq!(first.public_hex().len(), 64);
        assert_eq!(first.key_id().len(), 64);
        let restored = SigningKey::from_private_hex("edger", &first.private_hex()).unwrap();
        assert_eq!(restored.public_hex(), first.public_hex());
        assert!(SigningKey::from_private_hex("edger", "not-a-key").is_err());
    }
}
