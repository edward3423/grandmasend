//! The frozen hello/ack control protocol.
//!
//! This is grandmasend's only wire protocol of its own; the data plane is
//! stock iroh-blobs. It runs on a dedicated ALPN and carries two exchanges,
//! each on its own bidirectional stream:
//!
//! 1. Hello/Offer: receiver introduces itself, sender answers with the
//!    content hash and payload metadata. This replaces the ticket: the hash
//!    travels over the code-authenticated channel (ADR 0002).
//! 2. Complete/CompleteAck: receiver confirms all bytes are exported; the
//!    sender consumes the code and shuts down.
//!
//! FROZEN: the ALPN, the framing (u32 LE length prefix + JSON), the `type`
//! tag, and the existing field set are version-independent forever. Every
//! future version speaks this exchange first so version mismatches produce
//! clear messages instead of garbage. Fields may be ADDED (unknown JSON keys
//! are ignored); existing fields must never change meaning or type.

use anyhow::{bail, Context, Result};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// ALPN for the control protocol. Never changes.
pub const ALPN: &[u8] = b"grandmasend/hello/0";

/// Frames larger than this are rejected; control messages are tiny.
const MAX_FRAME: u32 = 1024 * 64;

/// Every control message, receiver -> sender. Tagged so the sender can
/// dispatch on stream content rather than stream order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMsg {
    /// First message from a receiver on any new control connection.
    Hello {
        /// Version of the receiving binary, semver.
        version: String,
    },
    /// Sent on a fresh stream once every byte is verified and exported.
    Complete {
        /// Root hash the receiver holds, hex; must match the offer.
        hash: String,
    },
}

/// Sender -> receiver, reply to [`ControlMsg::Hello`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Offer {
    /// Version of the sending binary, semver.
    pub version: String,
    /// Root hash of the HashSeq collection, hex.
    pub hash: String,
    /// Sum of all payload file sizes in bytes.
    pub payload_size: u64,
    /// Number of payload files.
    pub file_count: u64,
    /// Top-level name of the payload (file name or folder name).
    pub name: String,
}

/// Sender -> receiver, reply to [`ControlMsg::Complete`]. Receiving this
/// means the sender has consumed the code and is shutting down.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteAck {}

pub async fn write_frame<T: Serialize>(stream: &mut SendStream, msg: &T) -> Result<()> {
    let bytes = serde_json::to_vec(msg)?;
    let len = u32::try_from(bytes.len()).context("frame too large")?;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&bytes).await?;
    Ok(())
}

pub async fn read_frame<T: DeserializeOwned>(stream: &mut RecvStream) -> Result<T> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await?;
    let len = u32::from_le_bytes(len_bytes);
    if len > MAX_FRAME {
        bail!("control frame of {len} bytes exceeds limit");
    }
    let mut bytes = vec![0u8; len as usize];
    stream.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Receiver side: run the hello exchange on an open control connection.
pub async fn exchange_hello(conn: &Connection, version: &str) -> Result<Offer> {
    let (mut send, mut recv) = conn.open_bi().await?;
    let hello = ControlMsg::Hello {
        version: version.to_string(),
    };
    write_frame(&mut send, &hello).await?;
    send.finish()?;
    let offer: Offer = read_frame(&mut recv).await?;
    Ok(offer)
}

/// Receiver side: deliver the completion message and wait for the ack.
pub async fn exchange_complete(conn: &Connection, hash: &str) -> Result<CompleteAck> {
    let (mut send, mut recv) = conn.open_bi().await?;
    let complete = ControlMsg::Complete {
        hash: hash.to_string(),
    };
    write_frame(&mut send, &complete).await?;
    send.finish()?;
    let ack: CompleteAck = read_frame(&mut recv).await?;
    Ok(ack)
}
