//! Per-peer Ed25519 challenge authentication.

// implements: hub::spec::connection-auth@bd6ac42fe5e75451450c5183ae1892b19f92c718138de69e222e428197d940bd

/// A single-use server challenge for one service role.
#[derive(facet::Facet, Debug, Clone, PartialEq, Eq)]
pub struct AuthChallenge {
    pub hub: String,
    pub role: String,
    pub nonce: Vec<u8>,
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
}

impl SigningKey {
    pub fn generate(peer: impl Into<String>) -> Result<Self, String> {
        let _ = peer.into();
        Err("Ed25519 key generation is not implemented".into())
    }

    pub fn from_private_hex(peer: impl Into<String>, private: &str) -> Result<Self, String> {
        let _ = (peer.into(), private);
        Err("Ed25519 key parsing is not implemented".into())
    }

    pub fn private_hex(&self) -> String {
        self.peer.clone()
    }

    pub fn public_hex(&self) -> String {
        self.peer.clone()
    }

    pub fn key_id(&self) -> String {
        self.peer.clone()
    }

    pub fn sign(&self, challenge: &AuthChallenge) -> Result<AuthProof, String> {
        let _ = challenge;
        Err("Ed25519 signing is not implemented".into())
    }
}

/// Public keys and role grants trusted by one hub.
#[derive(Clone, Default)]
pub struct TrustStore;

impl TrustStore {
    pub fn add(
        &mut self,
        peer: &str,
        public_hex: &str,
        roles: &[&str],
    ) -> Result<(), String> {
        let _ = (peer, public_hex, roles);
        Err("Ed25519 trust configuration is not implemented".into())
    }

    pub fn verify(&self, challenge: &AuthChallenge, proof: &AuthProof) -> Result<String, String> {
        let _ = (challenge, proof);
        Err("Ed25519 verification is not implemented".into())
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
        trust.add("edger", &new.public_hex(), &["peer", "node"]).unwrap();
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
