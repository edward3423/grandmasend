//! In-process UDP chaos proxy: the cross-platform, no-root replacement for
//! tc netem. A receiver dials the proxy address instead of the sender; the
//! proxy forwards datagrams both ways while injecting loss, latency, and
//! link cuts.

use std::{
    net::{SocketAddr, UdpSocket},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    time::{Duration, Instant},
};

/// Shared knobs, adjustable while traffic flows.
#[derive(Debug, Default)]
pub struct ChaosControl {
    /// Packet drop probability in per-mille (0..=1000).
    pub drop_per_mille: AtomicU32,
    /// Base one-way latency in milliseconds.
    pub latency_ms: AtomicU64,
    /// Extra random jitter in milliseconds (uniform 0..=jitter).
    pub jitter_ms: AtomicU64,
    /// While true, the link is cut: everything is dropped.
    pub cut: AtomicBool,
}

pub struct ChaosProxy {
    /// Address the receiver should dial instead of the sender.
    pub listen_addr: SocketAddr,
    pub control: Arc<ChaosControl>,
}

impl ChaosProxy {
    /// Start a proxy in front of `upstream` (the sender's real UDP address).
    /// Runs on background threads until the process exits; tests are short.
    pub fn start(upstream: SocketAddr) -> std::io::Result<Self> {
        let listen = UdpSocket::bind("127.0.0.1:0")?;
        let listen_addr = listen.local_addr()?;
        let out = UdpSocket::bind("0.0.0.0:0")?;
        let control = Arc::new(ChaosControl::default());
        let client: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));

        // Client -> upstream.
        {
            let listen = listen.try_clone()?;
            let out = out.try_clone()?;
            let control = control.clone();
            let client = client.clone();
            let tx = delayed_sender(out, Forward::To(upstream), control.clone());
            std::thread::spawn(move || {
                let mut buf = [0u8; 65536];
                while let Ok((n, from)) = listen.recv_from(&mut buf) {
                    if let Ok(mut slot) = client.lock() {
                        *slot = Some(from);
                    }
                    if should_forward(&control) {
                        tx.send((buf[..n].to_vec(), deliver_at(&control))).ok();
                    }
                }
            });
        }

        // Upstream -> client.
        {
            let control = control.clone();
            let client = client.clone();
            let tx = delayed_sender(listen, Forward::ToClient(client), control.clone());
            std::thread::spawn(move || {
                let mut buf = [0u8; 65536];
                while let Ok((n, _from)) = out.recv_from(&mut buf) {
                    if should_forward(&control) {
                        tx.send((buf[..n].to_vec(), deliver_at(&control))).ok();
                    }
                }
            });
        }

        Ok(Self {
            listen_addr,
            control,
        })
    }

    pub fn set_loss_per_mille(&self, per_mille: u32) {
        self.control
            .drop_per_mille
            .store(per_mille, Ordering::Relaxed);
    }

    pub fn set_latency(&self, base: Duration, jitter: Duration) {
        self.control
            .latency_ms
            .store(base.as_millis() as u64, Ordering::Relaxed);
        self.control
            .jitter_ms
            .store(jitter.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn cut(&self, yes: bool) {
        self.control.cut.store(yes, Ordering::Relaxed);
    }
}

enum Forward {
    To(SocketAddr),
    ToClient(Arc<Mutex<Option<SocketAddr>>>),
}

fn should_forward(control: &ChaosControl) -> bool {
    if control.cut.load(Ordering::Relaxed) {
        return false;
    }
    let drop = control.drop_per_mille.load(Ordering::Relaxed);
    drop == 0 || rand::random_range(0..1000) >= drop
}

fn deliver_at(control: &ChaosControl) -> Instant {
    let base = control.latency_ms.load(Ordering::Relaxed);
    let jitter = control.jitter_ms.load(Ordering::Relaxed);
    let extra = if jitter > 0 {
        rand::random_range(0..=jitter)
    } else {
        0
    };
    Instant::now() + Duration::from_millis(base + extra)
}

/// A worker thread that delivers packets at their scheduled time, in order.
fn delayed_sender(
    socket: UdpSocket,
    forward: Forward,
    _control: Arc<ChaosControl>,
) -> mpsc::Sender<(Vec<u8>, Instant)> {
    let (tx, rx) = mpsc::channel::<(Vec<u8>, Instant)>();
    std::thread::spawn(move || {
        while let Ok((packet, at)) = rx.recv() {
            let now = Instant::now();
            if at > now {
                std::thread::sleep(at - now);
            }
            match &forward {
                Forward::To(addr) => {
                    socket.send_to(&packet, addr).ok();
                }
                Forward::ToClient(client) => {
                    let target = client.lock().ok().and_then(|guard| *guard);
                    if let Some(addr) = target {
                        socket.send_to(&packet, addr).ok();
                    }
                }
            }
        }
    });
    tx
}
