//! Sender-side send state: what survives a sender process restart.
//!
//! One JSON file per active send, keyed by a hash of the code so the file
//! name leaks nothing. A send disappears from disk exactly when its code is
//! consumed (completion) - revival after ctrl-c reads it back; a consumed
//! code can never be revived because its file is gone.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use iroh::EndpointId;
use serde::{Deserialize, Serialize};

use crate::code::Code;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendState {
    /// Canonical code string; the file key is derived from it.
    pub code: String,
    /// Absolute path of the offered payload.
    pub path: PathBuf,
    /// First receiver NodeId that redeemed the code, hex; the only one the
    /// sender will ever serve for this code.
    pub bound: Option<String>,
    /// Unix seconds when the send was created.
    pub created: u64,
}

impl SendState {
    pub fn bound_id(&self) -> Option<EndpointId> {
        self.bound.as_ref().and_then(|hex| hex.parse().ok())
    }
}

/// Stable file key for a code: first 16 hex chars of BLAKE3 of the canonical
/// string, domain-separated from the identity KDF.
pub fn state_key(code: &Code) -> String {
    let hash = blake3::derive_key("grandmasend v1 send state key", code.canonical().as_bytes());
    data_encoding_hex(&hash[..8])
}

fn data_encoding_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn sends_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("sends")
}

pub fn state_file(data_dir: &Path, code: &Code) -> PathBuf {
    sends_dir(data_dir).join(format!("{}.json", state_key(code)))
}

/// Directory for a send's blob store, next to its state file.
pub fn store_dir(data_dir: &Path, code: &Code) -> PathBuf {
    sends_dir(data_dir).join(state_key(code))
}

pub fn save(data_dir: &Path, state: &SendState) -> Result<()> {
    let code: Code = state.code.parse().context("state holds an invalid code")?;
    let dir = sends_dir(data_dir);
    std::fs::create_dir_all(&dir)?;
    let path = state_file(data_dir, &code);
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(state)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// Remove a send's state and blob store; the code is consumed or abandoned.
pub fn remove(data_dir: &Path, code: &Code) -> Result<()> {
    std::fs::remove_file(state_file(data_dir, code)).ok();
    std::fs::remove_dir_all(store_dir(data_dir, code)).ok();
    Ok(())
}

/// All persisted sends, oldest first.
pub fn list(data_dir: &Path) -> Result<Vec<SendState>> {
    let dir = sends_dir(data_dir);
    let mut states = Vec::new();
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Ok(states);
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let bytes = std::fs::read(&path)
            .with_context(|| format!("reading send state {}", path.display()))?;
        match serde_json::from_slice::<SendState>(&bytes) {
            Ok(state) => states.push(state),
            // A malformed state file must not brick every future send.
            Err(cause) => tracing::warn!("skipping corrupt send state {}: {cause}", path.display()),
        }
    }
    states.sort_by_key(|s| s.created);
    Ok(states)
}

/// The unconsumed send for `path`, if one exists: the revival candidate.
pub fn find_by_path(data_dir: &Path, path: &Path) -> Result<Option<SendState>> {
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    Ok(list(data_dir)?.into_iter().find(|s| s.path == canonical))
}

pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code() -> Code {
        "abacus abdomen abdominal abide".parse().unwrap()
    }

    #[test]
    fn save_list_remove_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let state = SendState {
            code: code().canonical(),
            path: PathBuf::from("/tmp/payload"),
            bound: None,
            created: 123,
        };
        save(dir.path(), &state).unwrap();
        let listed = list(dir.path()).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].code, state.code);
        remove(dir.path(), &code()).unwrap();
        assert!(list(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn find_by_path_matches_only_same_path() {
        let dir = tempfile::tempdir().unwrap();
        let state = SendState {
            code: code().canonical(),
            path: PathBuf::from("/tmp/payload"),
            bound: None,
            created: 1,
        };
        save(dir.path(), &state).unwrap();
        assert!(find_by_path(dir.path(), Path::new("/tmp/payload"))
            .unwrap()
            .is_some());
        assert!(find_by_path(dir.path(), Path::new("/tmp/other"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn corrupt_state_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let sends = dir.path().join("sends");
        std::fs::create_dir_all(&sends).unwrap();
        std::fs::write(sends.join("bad.json"), b"not json").unwrap();
        assert!(list(dir.path()).unwrap().is_empty());
    }
}
