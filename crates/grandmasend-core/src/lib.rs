//! grandmasend core: everything except the CLI surface.
//!
//! The transfer layer (iroh + iroh-blobs) is consumed exactly as sendme
//! consumes it (ADR 0001); this crate adds code-derived identity, the
//! hello/ack control protocol, binding, and persistence around it.

pub mod code;
pub mod events;
pub mod hello;
pub mod identity;
pub mod receiver;
pub mod sender;
