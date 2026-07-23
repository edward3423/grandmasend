//! The receiver engine: dial the code-derived identity, learn the hash over
//! the hello channel, fetch missing ranges, export atomically, ack.
//!
//! Resume is not a mode: partial blobs persist in a hidden directory on the
//! destination volume, and `local.missing()` computes the remaining ranges
//! on every run. The destination name is claimed only at export time.

use std::{
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use anyhow::{Context, Result};
use iroh::{address_lookup::dns::DnsAddressLookup, endpoint::presets, Endpoint, EndpointAddr};
use iroh_blobs::{
    api::{
        blobs::{ExportMode, ExportOptions, ExportProgressItem},
        remote::GetProgressItem,
    },
    format::collection::Collection,
    store::fs::FsStore,
    Hash, HashAndFormat,
};
use iroh_mdns_address_lookup::MdnsAddressLookup;
use n0_future::StreamExt;
use tokio::sync::mpsc;

use crate::{code::Code, events::ReceiverEvent, hello, identity};

pub struct ReceiveConfig {
    pub code: Code,
    /// Final destination directory, typically ~/Downloads.
    pub dest: PathBuf,
    /// App data directory holding the persistent receiver identity.
    pub data_dir: PathBuf,
    /// Version string exchanged in the frozen hello.
    pub version: String,
    /// Dial these addresses instead of discovery. Test/debug hook only.
    pub sender_addr: Option<EndpointAddr>,
}

#[derive(Debug)]
pub struct ReceiveSummary {
    pub payload_size: u64,
    pub file_count: u64,
    pub dest: PathBuf,
}

pub async fn receive(
    config: ReceiveConfig,
    events: mpsc::Sender<ReceiverEvent>,
) -> Result<ReceiveSummary> {
    // The persistent identity is what binding and resume recognize: every
    // run from this machine redeems the code as the same NodeId.
    let secret = identity::load_or_create_receiver_key(&config.data_dir)?;
    // Race DNS (internet) and mDNS (LAN) lookups for the code-derived
    // NodeId; whichever answers first wins (Q6).
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![])
        .secret_key(secret)
        .address_lookup(DnsAddressLookup::n0_dns())
        .address_lookup(MdnsAddressLookup::builder().advertise(false))
        .bind()
        .await?;

    let addr = config
        .sender_addr
        .clone()
        .unwrap_or_else(|| EndpointAddr::from(identity::transfer_id(&config.code)));

    events.send(ReceiverEvent::Connecting).await.ok();

    // Await-retry: the sender may not be online yet; a wrong-but-valid code
    // and a binding rejection both look identical to an offline sender.
    // Keep dialing forever; the CLI layers waiting hints.
    let (control, offer) = loop {
        if let Ok(conn) = endpoint.connect(addr.clone(), hello::ALPN).await {
            if let Ok(offer) = hello::exchange_hello(&conn, &config.version).await {
                break (conn, offer);
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    };
    let hash = Hash::from_str(&offer.hash).context("offer carried an invalid hash")?;
    let content = HashAndFormat::hash_seq(hash);

    // Partial store lives on the destination volume so the final export is
    // an atomic rename and peak disk usage is one payload, never two.
    let partial_root = config.dest.join(".grandmasend-partial");
    let partial_dir = partial_root.join(hash.to_hex().as_str());
    tokio::fs::create_dir_all(&partial_dir).await?;
    let db = FsStore::load(partial_dir.join("store")).await?;

    let result = fetch_and_export(
        &endpoint,
        &control,
        &config,
        &offer,
        content,
        &db,
        &partial_dir,
        &events,
    )
    .await;

    db.shutdown().await.ok();
    endpoint.close().await;

    let dest = result?;

    // Partials are resume state and survive failures indefinitely; only a
    // completed transfer removes them.
    tokio::fs::remove_dir_all(&partial_dir).await.ok();

    events
        .send(ReceiverEvent::Done { dest: dest.clone() })
        .await
        .ok();
    Ok(ReceiveSummary {
        payload_size: offer.payload_size,
        file_count: offer.file_count,
        dest,
    })
}

#[allow(clippy::too_many_arguments)]
async fn fetch_and_export(
    endpoint: &Endpoint,
    control: &iroh::endpoint::Connection,
    config: &ReceiveConfig,
    offer: &hello::Offer,
    content: HashAndFormat,
    db: &FsStore,
    partial_dir: &Path,
    events: &mpsc::Sender<ReceiverEvent>,
) -> Result<PathBuf> {
    let local = db.remote().local(content).await?;

    // Free-space preflight: hard refusal with a plain-language message
    // before a single byte moves. Staged export renames in place, so the
    // remaining fetch is all the space this transfer will take.
    let needed = offer.payload_size.saturating_sub(local.local_bytes());
    if let Ok(free) = fs4::available_space(&config.dest) {
        anyhow::ensure!(
            free > needed,
            "Not enough space on this disk: the transfer needs {} more, \
             but only {} is free. Make room, then run the same command again.",
            human_bytes(needed),
            human_bytes(free),
        );
    }

    events
        .send(ReceiverEvent::OfferReceived {
            name: offer.name.clone(),
            payload_size: offer.payload_size,
            file_count: offer.file_count,
            resumed_bytes: local.local_bytes(),
            sender_version: offer.version.clone(),
        })
        .await
        .ok();

    if !local.is_complete() {
        let addr = config
            .sender_addr
            .clone()
            .unwrap_or_else(|| EndpointAddr::from(identity::transfer_id(&config.code)));
        let conn = endpoint
            .connect(addr, iroh_blobs::protocol::ALPN)
            .await
            .context("connecting for blob fetch")?;
        let get = db.remote().execute_get(conn, local.missing());
        let mut stream = get.stream();
        while let Some(item) = stream.next().await {
            match item {
                GetProgressItem::Progress(offset) => {
                    events.send(ReceiverEvent::Progress { offset }).await.ok();
                }
                GetProgressItem::Done(_stats) => break,
                GetProgressItem::Error(cause) => {
                    return Err(anyhow::Error::from(cause).context("transfer interrupted"));
                }
            }
        }
    }

    events.send(ReceiverEvent::Exporting).await.ok();
    let collection = Collection::load(content.hash, db.as_ref()).await?;

    // Export into a staging dir next to the store, then atomically rename
    // the single top-level entry into the destination.
    let staging = partial_dir.join("export");
    for (name, hash) in collection.iter() {
        let target = entry_path(&staging, name)?;
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if target.exists() {
            // Re-export after an interrupted export: overwrite staging only.
            tokio::fs::remove_file(&target).await?;
        }
        // TryReference moves store-owned files instead of copying: peak disk
        // usage stays one payload, never two.
        let mut stream = db
            .export_with_opts(ExportOptions {
                hash: *hash,
                target,
                mode: ExportMode::TryReference,
            })
            .stream()
            .await;
        while let Some(item) = stream.next().await {
            match item {
                ExportProgressItem::Error(cause) => {
                    return Err(anyhow::Error::from(cause).context(format!("exporting {name}")));
                }
                ExportProgressItem::Done => break,
                _ => {}
            }
        }
    }

    let safe_name = sanitize_component(&offer.name);
    let staged = staging.join(&safe_name);
    anyhow::ensure!(
        staged.exists(),
        "export finished but staged payload {} is missing",
        staged.display()
    );
    let final_dest = claim_dest(&config.dest, &safe_name)?;
    tokio::fs::rename(&staged, &final_dest)
        .await
        .with_context(|| format!("moving payload into {}", final_dest.display()))?;

    hello::exchange_complete(control, &offer.hash)
        .await
        .context("delivering completion ack")?;

    Ok(final_dest)
}

/// Resolve a collection entry name to a path under `root`: traversal is
/// rejected outright, every component is sanitized for the local filesystem.
fn entry_path(root: &Path, name: &str) -> Result<PathBuf> {
    let mut path = root.to_path_buf();
    for part in name.split('/') {
        anyhow::ensure!(
            !part.is_empty() && part != "." && part != ".." && !part.contains('\\'),
            "invalid path component {part:?} in collection"
        );
        path.push(sanitize_component(part));
    }
    Ok(path)
}

/// Windows reserved device names; also refused on other platforms so a
/// folder receives identically everywhere.
const RESERVED_NAMES: [&str; 22] = [
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Make one path component safe on every supported filesystem: control and
/// Windows-illegal characters become '_', trailing dots/spaces are trimmed,
/// reserved device names are prefixed. Never returns an empty string.
fn sanitize_component(part: &str) -> String {
    let mut cleaned: String = part
        .chars()
        .map(|c| match c {
            '\0'..='\x1f' | '<' | '>' | ':' | '"' | '|' | '?' | '*' | '/' | '\\' => '_',
            c => c,
        })
        .collect();
    while cleaned.ends_with('.') || cleaned.ends_with(' ') {
        cleaned.pop();
    }
    if cleaned.is_empty() {
        return "_".to_string();
    }
    let stem = cleaned.split('.').next().unwrap_or("").to_ascii_uppercase();
    if RESERVED_NAMES.contains(&stem.as_str()) {
        cleaned.insert(0, '_');
    }
    cleaned
}

fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Claim the destination name, strict collision policy: if ANY entry with
/// the name exists - even an empty folder - append " (1)"-style suffixes.
/// Never overwrite, never prompt.
fn claim_dest(dest: &Path, name: &str) -> Result<PathBuf> {
    let candidate = dest.join(name);
    if !candidate.exists() && !candidate.is_symlink() {
        return Ok(candidate);
    }
    let (stem, ext) = split_name(name);
    for n in 1..10_000 {
        let numbered = match ext {
            Some(ext) => format!("{stem} ({n}).{ext}"),
            None => format!("{stem} ({n})"),
        };
        let candidate = dest.join(&numbered);
        if !candidate.exists() && !candidate.is_symlink() {
            return Ok(candidate);
        }
    }
    anyhow::bail!(
        "could not find a free name for {name} in {}",
        dest.display()
    );
}

/// Split "report.pdf" into ("report", Some("pdf")); dotfiles and extensionless
/// names keep their whole name as the stem.
fn split_name(name: &str) -> (&str, Option<&str>) {
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() => (stem, Some(ext)),
        _ => (name, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_name_variants() {
        assert_eq!(split_name("report.pdf"), ("report", Some("pdf")));
        assert_eq!(split_name("archive.tar.gz"), ("archive.tar", Some("gz")));
        assert_eq!(split_name("folder"), ("folder", None));
        assert_eq!(split_name(".bashrc"), (".bashrc", None));
        assert_eq!(split_name("trailing."), ("trailing.", None));
    }

    #[test]
    fn claim_dest_suffixes_on_any_entry() {
        let dir = tempfile::tempdir().unwrap();
        // Free name is used as-is.
        assert_eq!(
            claim_dest(dir.path(), "report.pdf").unwrap(),
            dir.path().join("report.pdf")
        );
        // An empty folder with the same name still forces a suffix.
        std::fs::create_dir(dir.path().join("report.pdf")).unwrap();
        assert_eq!(
            claim_dest(dir.path(), "report.pdf").unwrap(),
            dir.path().join("report (1).pdf")
        );
        std::fs::write(dir.path().join("report (1).pdf"), b"x").unwrap();
        assert_eq!(
            claim_dest(dir.path(), "report.pdf").unwrap(),
            dir.path().join("report (2).pdf")
        );
    }

    #[test]
    fn entry_path_rejects_traversal() {
        let root = Path::new("/tmp/x");
        assert!(entry_path(root, "a/../b").is_err());
        assert!(entry_path(root, "a//b").is_err());
        assert!(entry_path(root, "a\\b").is_err());
        assert!(entry_path(root, "ok/name.txt").is_ok());
    }

    #[test]
    fn sanitize_component_cases() {
        assert_eq!(sanitize_component("normal.txt"), "normal.txt");
        assert_eq!(sanitize_component("a<b>c:d.txt"), "a_b_c_d.txt");
        assert_eq!(sanitize_component("trailing. . "), "trailing");
        assert_eq!(sanitize_component("..."), "_");
        assert_eq!(sanitize_component("CON"), "_CON");
        assert_eq!(sanitize_component("con.txt"), "_con.txt");
        assert_eq!(sanitize_component("console.txt"), "console.txt");
        assert_eq!(sanitize_component("tab\there"), "tab_here");
    }

    #[test]
    fn human_bytes_formatting() {
        assert_eq!(human_bytes(999), "999 B");
        assert_eq!(human_bytes(1_500_000), "1.5 MB");
        assert_eq!(human_bytes(2_000_000_000), "2.0 GB");
    }
}
