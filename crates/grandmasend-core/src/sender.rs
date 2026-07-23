//! The sender engine: import a payload, serve it under a code-derived
//! identity, and exit on the receiver's completion ack.
//!
//! The blob serving path is stock iroh-blobs, wired exactly as sendme wires
//! it; grandmasend adds the identity, the hello handler, the binding filter,
//! and the completion signal.

use std::{
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use anyhow::{Context, Result};
use futures_buffered::BufferedStreamExt;
use iroh::{
    address_lookup::pkarr::PkarrPublisher,
    endpoint::presets,
    protocol::{AcceptError, ProtocolHandler, Router},
    Endpoint, EndpointId,
};
use iroh_blobs::{
    api::{
        blobs::{AddPathOptions, AddProgressItem, ImportMode},
        Store, TempTag,
    },
    format::collection::Collection,
    provider::events::{EventMask, EventSender, ProviderMessage, RequestMode, RequestUpdate},
    store::fs::FsStore,
    BlobFormat, BlobsProtocol,
};
use iroh_mdns_address_lookup::MdnsAddressLookup;
use n0_future::StreamExt;
use tokio::sync::{mpsc, oneshot};
use walkdir::WalkDir;

use crate::{
    code::Code,
    events::SenderEvent,
    hello::{self, CompleteAck, ControlMsg, Offer},
    identity,
};

/// QUIC close code for "this code is bound to another receiver".
const CLOSE_NOT_BOUND: u32 = 1;

pub struct SendConfig {
    /// File or folder to offer.
    pub path: PathBuf,
    /// Code to serve under; freshly generated when absent.
    pub code: Option<Code>,
    /// Receiver NodeId bound in an earlier run of this send, if any.
    pub bound: Option<EndpointId>,
    /// Directory for this send's blob store (references, not copies).
    pub data_dir: PathBuf,
    /// Version string exchanged in the frozen hello.
    pub version: String,
}

#[derive(Debug)]
pub struct SendSummary {
    pub payload_size: u64,
    pub file_count: u64,
}

/// First NodeId to redeem the code; shared between the control handler
/// (which sets it) and the blobs accept filter (which enforces it).
type Binding = Arc<Mutex<Option<EndpointId>>>;

/// True when `remote` is the bound receiver. A poisoned lock fails closed:
/// nobody is served rather than anybody.
fn is_bound_to(binding: &Binding, remote: EndpointId) -> bool {
    binding
        .lock()
        .map(|bound| *bound == Some(remote))
        .unwrap_or(false)
}

/// Serve one payload until the receiver acks completion. Never times out;
/// cancellation (ctrl-c) is the caller's job via future drop.
pub async fn send(config: SendConfig, events: mpsc::Sender<SenderEvent>) -> Result<SendSummary> {
    let code = config.code.unwrap_or_else(Code::generate);
    let secret = identity::transfer_secret(&code);
    let binding: Binding = Arc::new(Mutex::new(config.bound));

    tokio::fs::create_dir_all(&config.data_dir).await?;
    let store = FsStore::load(config.data_dir.join("store")).await?;

    let (provider_tx, provider_rx) = mpsc::channel(32);
    let blobs = BlobsProtocol::new(
        &store,
        Some(EventSender::new(
            provider_tx,
            EventMask {
                get: RequestMode::NotifyLog,
                ..EventMask::DEFAULT
            },
        )),
    );
    tokio::spawn(forward_serve_progress(provider_rx, events.clone()));

    let (temp_tag, payload_size, collection) = import(config.path.clone(), blobs.store()).await?;
    let hash = temp_tag.hash();
    let file_count = collection.len() as u64;
    let name = payload_name(&config.path)?;

    // The code-derived NodeId is announced through BOTH channels: pkarr
    // (relay/DNS, internet) and mDNS (LAN). The receiver races them; offline
    // is not a mode, mDNS just wins the race (Q6).
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![
            iroh_blobs::protocol::ALPN.to_vec(),
            hello::ALPN.to_vec(),
        ])
        .secret_key(secret)
        .address_lookup(PkarrPublisher::n0_dns())
        .address_lookup(MdnsAddressLookup::builder())
        .bind()
        .await?;

    let offer = Offer {
        version: config.version.clone(),
        hash: hash.to_hex().to_string(),
        payload_size,
        file_count,
        name: name.clone(),
    };
    let (complete_tx, complete_rx) = oneshot::channel();
    let control = ControlHandler {
        offer,
        events: events.clone(),
        binding: binding.clone(),
        complete: Mutex::new(Some(complete_tx)),
    };
    let bound_blobs = BoundBlobs {
        inner: blobs.clone(),
        binding: binding.clone(),
    };

    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, bound_blobs)
        .accept(hello::ALPN, control)
        .spawn();

    // Wait for relay registration so the published address is dialable.
    let ep = router.endpoint().clone();
    tokio::time::timeout(std::time::Duration::from_secs(30), ep.online())
        .await
        .context("timed out waiting for relay connection")?;

    events
        .send(SenderEvent::Ready {
            code: code.clone(),
            payload_size,
            file_count,
            name,
            hash: hash.to_hex().to_string(),
            addr: ep.addr(),
        })
        .await
        .ok();

    // Forever-serve: block until the receiver's completion ack, however long
    // that takes. No idle timeout, no code expiry.
    complete_rx.await.context("control handler dropped")?;

    events
        .send(SenderEvent::Completed { payload_size })
        .await
        .ok();

    drop(temp_tag);
    tokio::time::timeout(std::time::Duration::from_secs(2), router.shutdown())
        .await
        .ok();
    store.shutdown().await.ok();

    Ok(SendSummary {
        payload_size,
        file_count,
    })
}

/// Translate provider request updates into a cumulative served-bytes count.
async fn forward_serve_progress(
    mut provider_rx: mpsc::Receiver<ProviderMessage>,
    events: mpsc::Sender<SenderEvent>,
) {
    let total = Arc::new(AtomicU64::new(0));
    while let Some(msg) = provider_rx.recv().await {
        if let ProviderMessage::GetRequestReceivedNotify(msg) = msg {
            let total = total.clone();
            let events = events.clone();
            let mut rx = msg.rx;
            tokio::spawn(async move {
                // end_offset is cumulative within one request; convert to
                // deltas so concurrent requests sum correctly.
                let mut last = 0u64;
                while let Ok(Some(update)) = rx.recv().await {
                    match update {
                        RequestUpdate::Started(_) => last = 0,
                        RequestUpdate::Progress(p) => {
                            let delta = p.end_offset.saturating_sub(last);
                            last = p.end_offset;
                            let bytes = total.fetch_add(delta, Ordering::Relaxed) + delta;
                            events.send(SenderEvent::ServeProgress { bytes }).await.ok();
                        }
                        RequestUpdate::Completed(_) | RequestUpdate::Aborted(_) => {}
                    }
                }
            });
        }
    }
}

/// Handles the hello/ack control ALPN on the sender.
#[derive(Debug)]
struct ControlHandler {
    offer: Offer,
    events: mpsc::Sender<SenderEvent>,
    binding: Binding,
    complete: Mutex<Option<oneshot::Sender<()>>>,
}

fn accept_err(e: anyhow::Error) -> AcceptError {
    AcceptError::from_err(std::io::Error::other(format!("{e:#}")))
}

impl ProtocolHandler for ControlHandler {
    async fn accept(&self, conn: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        let remote = conn.remote_id();
        loop {
            let (mut send, mut recv) = match conn.accept_bi().await {
                Ok(streams) => streams,
                // Receiver closed the control connection; nothing to clean up.
                Err(_) => return Ok(()),
            };
            let msg: ControlMsg = hello::read_frame(&mut recv).await.map_err(accept_err)?;
            match msg {
                ControlMsg::Hello { version } => {
                    // Binding: the first NodeId to redeem the code is the
                    // only one this send will ever serve. A rejected
                    // receiver sees a closed connection, indistinguishable
                    // from a sender that is not there.
                    let newly_bound = {
                        let Ok(mut bound) = self.binding.lock() else {
                            conn.close(CLOSE_NOT_BOUND.into(), b"not bound");
                            return Ok(());
                        };
                        match *bound {
                            None => {
                                *bound = Some(remote);
                                true
                            }
                            Some(id) if id == remote => false,
                            Some(_) => {
                                conn.close(CLOSE_NOT_BOUND.into(), b"not bound");
                                return Ok(());
                            }
                        }
                    };
                    self.events
                        .send(SenderEvent::ReceiverConnected {
                            id: remote,
                            version,
                        })
                        .await
                        .ok();
                    if newly_bound {
                        self.events
                            .send(SenderEvent::Bound { id: remote })
                            .await
                            .ok();
                    }
                    hello::write_frame(&mut send, &self.offer)
                        .await
                        .map_err(accept_err)?;
                    send.finish().ok();
                }
                ControlMsg::Complete { hash } => {
                    if !is_bound_to(&self.binding, remote) || hash != self.offer.hash {
                        conn.close(CLOSE_NOT_BOUND.into(), b"not bound");
                        return Ok(());
                    }
                    hello::write_frame(&mut send, &CompleteAck {})
                        .await
                        .map_err(accept_err)?;
                    send.finish().ok();
                    // Flush the ack before the router shuts down.
                    send.stopped().await.ok();
                    if let Ok(mut guard) = self.complete.lock() {
                        if let Some(tx) = guard.take() {
                            tx.send(()).ok();
                        }
                    }
                    return Ok(());
                }
            }
        }
    }
}

/// Accept filter in front of the stock blobs protocol: only the bound
/// receiver may fetch. Data is never served before a hello has bound.
#[derive(Debug, Clone)]
struct BoundBlobs {
    inner: BlobsProtocol,
    binding: Binding,
}

impl ProtocolHandler for BoundBlobs {
    async fn accept(&self, conn: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        let remote = conn.remote_id();
        if !is_bound_to(&self.binding, remote) {
            conn.close(CLOSE_NOT_BOUND.into(), b"not bound");
            return Ok(());
        }
        self.inner.accept(conn).await
    }
}

/// The display name of the payload: the last path component.
fn payload_name(path: &Path) -> Result<String> {
    let canonical = path.canonicalize()?;
    Ok(canonical
        .file_name()
        .context("payload path has no name")?
        .to_string_lossy()
        .into_owned())
}

/// Convert an already canonicalized path to a collection entry name.
/// Verbatim from sendme.
fn canonicalized_path_to_string(path: impl AsRef<Path>, must_be_relative: bool) -> Result<String> {
    let mut path_str = String::new();
    let parts = path
        .as_ref()
        .components()
        .filter_map(|c| match c {
            Component::Normal(x) => {
                let c = match x.to_str() {
                    Some(c) => c,
                    None => return Some(Err(anyhow::anyhow!("invalid character in path"))),
                };
                if !c.contains('/') && !c.contains('\\') {
                    Some(Ok(c))
                } else {
                    Some(Err(anyhow::anyhow!("invalid path component {:?}", c)))
                }
            }
            Component::RootDir => {
                if must_be_relative {
                    Some(Err(anyhow::anyhow!("invalid path component {:?}", c)))
                } else {
                    path_str.push('/');
                    None
                }
            }
            _ => Some(Err(anyhow::anyhow!("invalid path component {:?}", c))),
        })
        .collect::<Result<Vec<_>>>()?;
    let parts = parts.join("/");
    path_str.push_str(&parts);
    Ok(path_str)
}

/// Import a file or directory into the store as a collection, adapted from
/// sendme's import minus the progress bars. `TryReference` means local files
/// are referenced, not copied.
async fn import(path: PathBuf, db: &Store) -> Result<(TempTag, u64, Collection)> {
    let parallelism = num_cpus::get();
    let path = path.canonicalize()?;
    anyhow::ensure!(path.exists(), "path {} does not exist", path.display());
    let root = path.parent().context("get parent of payload path")?;
    let files = WalkDir::new(path.clone()).into_iter();
    let data_sources: Vec<(String, PathBuf)> = files
        .map(|entry| {
            let entry = entry?;
            if !entry.file_type().is_file() {
                // Skip symlinks; directories are handled by WalkDir.
                return Ok(None);
            }
            let path = entry.into_path();
            let relative = path.strip_prefix(root)?;
            let name = canonicalized_path_to_string(relative, true)?;
            anyhow::Ok(Some((name, path)))
        })
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>>>()?;
    let mut names_and_tags = n0_future::stream::iter(data_sources)
        .map(|(name, path)| {
            let db = db.clone();
            async move {
                let import = db.add_path_with_opts(AddPathOptions {
                    path,
                    mode: ImportMode::TryReference,
                    format: BlobFormat::Raw,
                });
                let mut stream = import.stream().await;
                let mut item_size = 0;
                let temp_tag = loop {
                    let item = stream
                        .next()
                        .await
                        .context("import stream ended without a tag")?;
                    match item {
                        AddProgressItem::Size(size) => item_size = size,
                        AddProgressItem::Error(cause) => {
                            anyhow::bail!("error importing {name}: {cause}");
                        }
                        AddProgressItem::Done(tt) => break tt,
                        _ => {}
                    }
                };
                anyhow::Ok((name, temp_tag, item_size))
            }
        })
        .buffered_unordered(parallelism)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    names_and_tags.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
    let size = names_and_tags.iter().map(|(_, _, size)| *size).sum::<u64>();
    let (collection, tags) = names_and_tags
        .into_iter()
        .map(|(name, tag, _)| ((name, tag.hash()), tag))
        .unzip::<_, _, Collection, Vec<_>>();
    let temp_tag = collection.clone().store(db).await?;
    // The collection now protects the data; the per-file tags can go.
    drop(tags);
    Ok((temp_tag, size, collection))
}
