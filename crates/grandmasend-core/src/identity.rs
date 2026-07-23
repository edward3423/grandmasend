//! Transfer identity: the sender's per-transfer Ed25519 keypair, derived
//! deterministically from the code.
//!
//! Both sides run the same derivation: the sender to obtain its secret key,
//! the receiver to obtain the public NodeId to dial. The code-to-peer mapping
//! is pure math; no rendezvous infrastructure exists (ADR 0002).

use iroh::{EndpointId, SecretKey};

use crate::code::Code;

/// Domain-separation context for the KDF. Part of the wire-compatibility
/// contract: changing it changes every derived NodeId.
const KDF_CONTEXT: &str = "grandmasend v1 transfer identity";

/// Derive the transfer secret key from a code. Sender side.
pub fn transfer_secret(code: &Code) -> SecretKey {
    let seed = blake3::derive_key(KDF_CONTEXT, code.canonical().as_bytes());
    SecretKey::from_bytes(&seed)
}

/// Derive the transfer NodeId (public half) from a code. Receiver side.
pub fn transfer_id(code: &Code) -> EndpointId {
    transfer_secret(code).public()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derivation_is_deterministic_across_sides() {
        let code: Code = "abacus abdomen abdominal abide".parse().unwrap();
        assert_eq!(transfer_secret(&code).public(), transfer_id(&code));
        assert_eq!(transfer_id(&code), transfer_id(&code.clone()));
    }

    #[test]
    fn different_codes_different_ids() {
        let a: Code = "abacus abdomen abdominal abide".parse().unwrap();
        let b: Code = "abide abdominal abdomen abacus".parse().unwrap();
        assert_ne!(transfer_id(&a), transfer_id(&b));
    }

    /// Pins the derivation output forever. If this test breaks, released
    /// receivers can no longer dial released senders.
    #[test]
    fn derivation_is_frozen() {
        let code: Code = "abacus abdomen abdominal abide".parse().unwrap();
        let seed = blake3::derive_key(KDF_CONTEXT, code.canonical().as_bytes());
        assert_eq!(
            blake3::hash(&seed).to_hex().as_str(),
            // Recorded at first implementation; must never change.
            "b3c844476793c5462521b81b2a417f11385dee0dd7c5460bd9be35aa2433d2c1"
        );
    }
}
