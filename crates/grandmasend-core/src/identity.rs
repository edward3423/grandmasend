//! Transfer identity: the sender's per-transfer Ed25519 keypair, derived
//! deterministically from the code.
//!
//! Both sides run the same derivation: the sender to obtain its secret key,
//! the receiver to obtain the public NodeId to dial. The code-to-peer mapping
//! is pure math; no rendezvous infrastructure exists (ADR 0002).

use std::path::Path;

use anyhow::{Context, Result};
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

/// The receiver's persistent identity: generated once per machine, survives
/// between runs so binding and resume recognize the same receiver.
pub fn load_or_create_receiver_key(data_dir: &Path) -> Result<SecretKey> {
    let path = data_dir.join("receiver.key");
    if let Ok(hex) = std::fs::read_to_string(&path) {
        let bytes: [u8; 32] = parse_hex32(hex.trim())
            .with_context(|| format!("corrupt receiver key at {}", path.display()))?;
        return Ok(SecretKey::from_bytes(&bytes));
    }
    let key = SecretKey::generate();
    std::fs::create_dir_all(data_dir)?;
    let hex: String = key.to_bytes().iter().map(|b| format!("{b:02x}")).collect();
    write_private(&path, hex.as_bytes())?;
    Ok(key)
}

fn parse_hex32(s: &str) -> Result<[u8; 32]> {
    anyhow::ensure!(s.len() == 64, "expected 64 hex chars, got {}", s.len());
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)?;
    }
    Ok(out)
}

#[cfg(unix)]
fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents)?;
    Ok(())
}

#[cfg(not(unix))]
fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    std::fs::write(path, contents)?;
    Ok(())
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
