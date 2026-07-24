//! Typed progress events emitted by the engines. The CLI renders them; the
//! core never prints.

use iroh::{EndpointAddr, EndpointId};

use crate::code::Code;

#[derive(Debug, Clone)]
pub enum SenderEvent {
    /// Payload imported into the blob store; the send is ready to serve.
    Ready {
        code: Code,
        payload_size: u64,
        file_count: u64,
        name: String,
        hash: String,
        /// The sender's bound address; test/debug hook for dialing without
        /// discovery infrastructure.
        addr: EndpointAddr,
    },
    /// A receiver completed a hello on the control connection.
    ReceiverConnected {
        id: EndpointId,
        /// The receiver's binary version from the frozen hello.
        version: String,
    },
    /// The first receiver redeemed the code; only this NodeId is served now.
    Bound { id: EndpointId },
    /// Cumulative payload bytes served this session.
    ServeProgress { bytes: u64 },
    /// The bound receiver confirmed completion; the send is over.
    Completed { payload_size: u64 },
}

#[derive(Debug, Clone)]
pub enum ReceiverEvent {
    /// Dialing the sender.
    Connecting,
    /// Hello exchange finished; transfer parameters known.
    OfferReceived {
        name: String,
        payload_size: u64,
        file_count: u64,
        /// Bytes already present locally from an earlier interrupted run.
        resumed_bytes: u64,
        /// The sender's binary version from the frozen hello.
        sender_version: String,
    },
    /// Absolute verified bytes present locally (resumed + fetched).
    Progress { offset: u64 },
    /// The connection to the sender was lost mid-transfer; the receiver
    /// keeps waiting and resumes when the sender comes back.
    Interrupted,
    /// Every byte is exported but the completion ack could not be
    /// delivered; the sender may still show the send as waiting.
    AckUndelivered,
    /// All bytes verified; files are being exported to the destination.
    Exporting,
    /// The sender asked for autoextract; extraction is running.
    Extracting { name: String },
    /// Extraction finished; the folder sits next to the archive.
    Extracted {
        files: u64,
        dest: std::path::PathBuf,
    },
    /// Extraction failed; the archive itself was still delivered.
    ExtractFailed { reason: String },
    /// Export finished and the sender acknowledged completion.
    Done {
        /// Final destination the payload was exported to.
        dest: std::path::PathBuf,
    },
}
