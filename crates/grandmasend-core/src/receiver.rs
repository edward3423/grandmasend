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
use n0_future::StreamExt;
use tokio::sync::mpsc;

use crate::{code::Code, events::ReceiverEvent, hello, identity};

pub struct ReceiveConfig {
    pub code: Code,
    /// Final destination directory, typically ~/Downloads.
    pub dest: PathBuf,
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
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![])
        .address_lookup(DnsAddressLookup::n0_dns())
        .bind()
        .await?;

    let addr = config
        .sender_addr
        .clone()
        .unwrap_or_else(|| EndpointAddr::from(identity::transfer_id(&config.code)));

    events.send(ReceiverEvent::Connecting).await.ok();

    // Await-retry: the sender may not be online yet; a wrong-but-valid code
    // looks identical. Keep dialing forever; the CLI layers waiting hints.
    let control = loop {
        match endpoint.connect(addr.clone(), hello::ALPN).await {
            Ok(conn) => break conn,
            Err(_) => tokio::time::sleep(Duration::from_secs(5)).await,
        }
    };

    let offer = hello::exchange_hello(&control, &config.version).await?;
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
    events
        .send(ReceiverEvent::OfferReceived {
            name: offer.name.clone(),
            payload_size: offer.payload_size,
            file_count: offer.file_count,
            resumed_bytes: local.local_bytes(),
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
        let mut stream = db
            .export_with_opts(ExportOptions {
                hash: *hash,
                target,
                mode: ExportMode::Copy,
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

    let staged = staging.join(&offer.name);
    anyhow::ensure!(
        staged.exists(),
        "export finished but staged payload {} is missing",
        staged.display()
    );
    let final_dest = claim_dest(&config.dest, &offer.name)?;
    tokio::fs::rename(&staged, &final_dest)
        .await
        .with_context(|| format!("moving payload into {}", final_dest.display()))?;

    hello::exchange_complete(control, &offer.hash)
        .await
        .context("delivering completion ack")?;

    Ok(final_dest)
}

/// Resolve a collection entry name to a path under `root`, rejecting
/// separator tricks.
fn entry_path(root: &Path, name: &str) -> Result<PathBuf> {
    let mut path = root.to_path_buf();
    for part in name.split('/') {
        anyhow::ensure!(
            !part.is_empty() && part != "." && part != ".." && !part.contains('\\'),
            "invalid path component {part:?} in collection"
        );
        path.push(part);
    }
    Ok(path)
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
}
