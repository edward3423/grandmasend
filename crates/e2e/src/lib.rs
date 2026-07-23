//! Shared helpers for grandmasend end-to-end tests: locating the binary,
//! driving sender/receiver processes, and payload utilities.

use std::{
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

/// Path of the grandmasend binary, building it first so tests never race a
/// stale build.
pub fn grandmasend_bin() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let status = Command::new("cargo")
        .args(["build", "-p", "grandmasend-cli"])
        .current_dir(&root)
        .status()
        .expect("cargo build");
    assert!(status.success(), "building grandmasend-cli failed");
    root.join("target/debug/grandmasend")
}

/// A running `grandmasend send` process with its code and address captured.
pub struct Sender {
    pub child: Child,
    pub code: String,
    pub addr_json: String,
    stderr_lines: mpsc::Receiver<String>,
}

impl Sender {
    /// Spawn a sender for `payload` and wait until it prints the code and
    /// (via the hidden debug flag) its endpoint address. `data_dir` isolates
    /// send state per test.
    pub fn spawn(bin: &Path, payload: &Path, data_dir: &Path) -> Self {
        let mut child = Command::new(bin)
            .arg("send")
            .arg(payload)
            .arg("--print-addr")
            .env("GRANDMASEND_DATA_DIR", data_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn sender");

        let stdout = child.stdout.take().expect("sender stdout");
        let stderr = child.stderr.take().expect("sender stderr");
        let (stderr_tx, stderr_lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                stderr_tx.send(line).ok();
            }
        });

        let (addr_tx, addr_rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(addr) = line.strip_prefix("ADDR ") {
                    addr_tx.send(addr.to_string()).ok();
                    break;
                }
            }
        });

        let deadline = Instant::now() + Duration::from_secs(60);
        let mut code = None;
        while code.is_none() && Instant::now() < deadline {
            match stderr_lines.recv_timeout(Duration::from_secs(1)) {
                Ok(line) => {
                    if let Some(rest) = line.strip_prefix("The code is:") {
                        code = Some(rest.trim().to_string());
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(e) => panic!("sender stderr closed before code appeared: {e}"),
            }
        }
        let code = code.expect("sender never printed a code");
        assert!(!code.is_empty(), "sender printed an empty code");
        let addr_json = addr_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("sender never printed its address");

        Self {
            child,
            code,
            addr_json,
            stderr_lines,
        }
    }

    /// Wait for the sender to exit and return whether it exited cleanly.
    pub fn wait_success(mut self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait sender") {
                return status.success();
            }
            if Instant::now() > deadline {
                self.child.kill().ok();
                panic!("sender did not exit within {timeout:?}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Drain stderr lines seen so far.
    pub fn stderr_so_far(&self) -> Vec<String> {
        self.stderr_lines.try_iter().collect()
    }

    /// Wait for the sender to exit with any status.
    pub fn wait_exit(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait sender") {
                return status;
            }
            if Instant::now() > deadline {
                self.child.kill().ok();
                panic!("sender did not exit within {timeout:?}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.child.kill().ok();
    }
}

/// Outcome of one receiver run.
pub struct ReceiverRun {
    pub stderr: String,
    pub success: bool,
    /// True when the run was killed by the test rather than exiting.
    pub killed: bool,
}

/// How long a receiver run is allowed to live.
pub enum ReceiverMode {
    /// Run until the process exits on its own.
    ToCompletion,
    /// SIGKILL once the partial store under dest grows past this size.
    KillAtBytes(u64),
    /// SIGKILL after this duration regardless of progress.
    KillAfter(Duration),
}

/// Run a receiver process against `code`, using `data_dir` for its
/// persistent identity.
pub fn run_receiver(
    bin: &Path,
    code: &str,
    dest: &Path,
    addr_json: &str,
    data_dir: &Path,
    mode: ReceiverMode,
) -> ReceiverRun {
    let mut cmd = Command::new(bin);
    cmd.arg("receive")
        .args(code.split_whitespace())
        .arg("--dest")
        .arg(dest)
        .arg("--sender-addr")
        .arg(addr_json)
        .env("GRANDMASEND_DATA_DIR", data_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn receiver");
    let stderr = child.stderr.take().expect("receiver stderr");

    let mut killed = false;
    match mode {
        ReceiverMode::KillAtBytes(threshold) => {
            let partial = dest.join(".grandmasend-partial");
            let deadline = Instant::now() + Duration::from_secs(120);
            loop {
                if let Some(_status) = child.try_wait().expect("try_wait receiver") {
                    // Finished before the threshold; nothing left to kill.
                    break;
                }
                if dir_size(&partial) >= threshold {
                    child.kill().expect("kill receiver");
                    child.wait().expect("wait receiver");
                    killed = true;
                    break;
                }
                if Instant::now() > deadline {
                    child.kill().ok();
                    panic!("receiver never reached {threshold} partial bytes");
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        ReceiverMode::KillAfter(duration) => {
            let deadline = Instant::now() + duration;
            loop {
                if child.try_wait().expect("try_wait receiver").is_some() {
                    break;
                }
                if Instant::now() > deadline {
                    child.kill().expect("kill receiver");
                    child.wait().expect("wait receiver");
                    killed = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        ReceiverMode::ToCompletion => {
            let deadline = Instant::now() + Duration::from_secs(300);
            while child.try_wait().expect("try_wait receiver").is_none() {
                if Instant::now() > deadline {
                    child.kill().ok();
                    panic!("receiver did not finish in time");
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }

    let mut stderr_text = String::new();
    let mut reader = stderr;
    reader.read_to_string(&mut stderr_text).ok();
    let success = child
        .try_wait()
        .expect("receiver exit status")
        .map(|s| s.success())
        .unwrap_or(false);
    ReceiverRun {
        stderr: stderr_text,
        success,
        killed,
    }
}

/// Send SIGINT (ctrl-c) to a process; unix only.
#[cfg(unix)]
pub fn interrupt(child: &Child) {
    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("run kill -INT");
    assert!(status.success(), "kill -INT failed");
}

/// Total size in bytes of all files under `dir`; 0 when it does not exist.
pub fn dir_size(dir: &Path) -> u64 {
    fn walk(dir: &Path, acc: &mut u64) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, acc);
            } else if let Ok(meta) = entry.metadata() {
                *acc += meta.len();
            }
        }
    }
    let mut acc = 0;
    walk(dir, &mut acc);
    acc
}

/// Write `size` bytes of pseudo-random data to `path` and return its BLAKE3.
pub fn write_random_payload(path: &Path, size: u64) -> blake3::Hash {
    use rand::Rng;
    use std::io::Write;
    let mut rng = rand::rng();
    let mut hasher = blake3::Hasher::new();
    let mut file = std::io::BufWriter::new(std::fs::File::create(path).expect("create payload"));
    let mut remaining = size;
    let mut buf = vec![0u8; 1024 * 1024];
    while remaining > 0 {
        let n = remaining.min(buf.len() as u64) as usize;
        rng.fill_bytes(&mut buf[..n]);
        hasher.update(&buf[..n]);
        file.write_all(&buf[..n]).expect("write payload");
        remaining -= n as u64;
    }
    file.flush().expect("flush payload");
    hasher.finalize()
}

/// BLAKE3 of a file on disk.
pub fn hash_file(path: &Path) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    let mut file = std::fs::File::open(path).expect("open file for hashing");
    std::io::copy(&mut file, &mut hasher).expect("hash file");
    hasher.finalize()
}
