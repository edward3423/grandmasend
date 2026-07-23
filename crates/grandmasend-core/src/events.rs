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
    /// A receiver established a control connection.
    ReceiverConnected { id: EndpointId },
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
    },
    /// Cumulative verified bytes fetched this session (excludes resumed).
    Progress { offset: u64 },
    /// All bytes verified; files are being exported to the destination.
    Exporting,
    /// Export finished and the sender acknowledged completion.
    Done {
        /// Final destination the payload was exported to.
        dest: std::path::PathBuf,
    },
}
