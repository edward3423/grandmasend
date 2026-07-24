use std::{
    collections::VecDeque,
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use console::style;
use grandmasend_core::{
    code::Code,
    events::{ReceiverEvent, SenderEvent},
    receiver::{self, ReceiveConfig},
    sender::{self, SendConfig},
    state::{self, SendState},
};
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use tokio::sync::mpsc;

mod update;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const UPDATE_CHECK_TIMEOUT: Duration = Duration::from_millis(2500);

/// Send any file, any size, to anyone who can type one command.
#[derive(Parser, Debug)]
#[command(name = "grandmasend", version, about)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Offer a file or folder; prints the four-word code to read to the receiver.
    Send {
        path: PathBuf,
        /// Abandon any previous send of this path: new code, no binding.
        /// Use this to hand the same file to a different person.
        #[clap(long)]
        fresh: bool,
        /// The receiver extracts the archive automatically after
        /// verification (top level only). Requires a single .zip, .rar,
        /// or .7z file.
        #[clap(long)]
        autoextract: bool,
        /// Password for the archive, forwarded to the receiver over the
        /// encrypted channel. Requires --autoextract.
        #[clap(long, requires = "autoextract")]
        password: Option<String>,
        /// Print the bound endpoint address as JSON. Debug/test hook.
        #[clap(long, hide = true)]
        print_addr: bool,
    },
    /// Fetch a transfer by its four-word code.
    #[clap(visible_alias = "recv")]
    Receive {
        /// The four words, in order; spaces or hyphens both work.
        /// Prompted for interactively when omitted.
        code: Vec<String>,
        /// Destination directory; defaults to ~/Downloads.
        #[clap(long)]
        dest: Option<PathBuf>,
        /// Transient run from the bootstrap script: prompt for the code,
        /// skip the update check (the bootstrap always fetches latest).
        #[clap(long, hide = true)]
        transient: bool,
        /// Dial this endpoint address (JSON) instead of discovery. Debug/test hook.
        #[clap(long, hide = true)]
        sender_addr: Option<String>,
    },
    /// List sends that are still waiting for a receiver.
    Status,
    /// Abandon one waiting send by its code: the code stops working and
    /// cannot be revived. The payload file itself is untouched.
    Abandon {
        /// The four words of the send to abandon (see grandmasend status).
        code: Vec<String>,
    },
    /// Remove all waiting sends and interrupted-receive leftovers.
    /// Interrupted receives can no longer resume afterwards.
    Tidy {
        /// Downloads directory whose partial store should be cleaned;
        /// defaults to ~/Downloads.
        #[clap(long)]
        dest: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    match args.command {
        Commands::Send {
            path,
            fresh,
            autoextract,
            password,
            print_addr,
        } => {
            update::check_and_nag(VERSION, UPDATE_CHECK_TIMEOUT).await;
            send(path, fresh, autoextract, password, print_addr).await
        }
        Commands::Receive {
            code,
            dest,
            transient,
            sender_addr,
        } => {
            if transient {
                eprintln!("grandmasend v{VERSION} by edward3423");
            } else {
                update::check_and_nag(VERSION, UPDATE_CHECK_TIMEOUT).await;
            }
            eprintln!(
                "Only receive files from people you trust. Press ctrl-c to stop the transfer."
            );
            receive(code, dest, sender_addr).await
        }
        Commands::Status => {
            update::check_and_nag(VERSION, UPDATE_CHECK_TIMEOUT).await;
            status()
        }
        Commands::Abandon { code } => {
            update::check_and_nag(VERSION, UPDATE_CHECK_TIMEOUT).await;
            abandon(code)
        }
        Commands::Tidy { dest } => {
            update::check_and_nag(VERSION, UPDATE_CHECK_TIMEOUT).await;
            tidy(dest)
        }
    }
}

/// Abandon a single waiting send by code, without re-sending its path
/// (send --fresh covers that case). A running sender process for this code
/// keeps serving until stopped with ctrl-c; abandonment prevents revival.
fn abandon(code: Vec<String>) -> Result<()> {
    let code: Code = code.join(" ").parse()?;
    let data_root = data_root()?;
    let known = state::list(&data_root)?
        .iter()
        .any(|send| send.code == code.canonical());
    if !known {
        eprintln!("No waiting send with that code. See grandmasend status.");
        return Ok(());
    }
    state::remove(&data_root, &code)?;
    eprintln!(
        "Abandoned {}. The code no longer works and cannot be revived.",
        style(code.canonical()).green()
    );
    eprintln!("If a sender window is still serving it, press ctrl-c there too.");
    Ok(())
}

/// Remove every waiting send and all receive partials: the explicit,
/// user-invoked cleanup for abandoned transfers. Nothing expires on its own.
fn tidy(dest: Option<PathBuf>) -> Result<()> {
    let data_root = data_root()?;
    let mut removed_anything = false;

    for send in state::list(&data_root)? {
        if let Ok(code) = send.code.parse::<Code>() {
            state::remove(&data_root, &code)?;
            eprintln!(
                "Removed waiting send: {} ({})",
                style(&send.code).green(),
                send.path.display()
            );
            removed_anything = true;
        }
    }

    let dest = match dest {
        Some(d) => d,
        None => default_downloads()?,
    };
    let partial_root = dest.join(".grandmasend-partial");
    if partial_root.exists() {
        let mut freed = 0u64;
        if let Ok(entries) = std::fs::read_dir(&partial_root) {
            for entry in entries.flatten() {
                freed += dir_size(&entry.path());
            }
        }
        std::fs::remove_dir_all(&partial_root)
            .with_context(|| format!("removing {}", partial_root.display()))?;
        eprintln!(
            "Removed interrupted-receive data: {} freed in {}",
            HumanBytes(freed),
            partial_root.display()
        );
        removed_anything = true;
    }

    if removed_anything {
        eprintln!(
            "Note: interrupted transfers can no longer resume; completed files are untouched."
        );
    } else {
        eprintln!("Nothing to tidy.");
    }
    Ok(())
}

/// Total size in bytes of all files under `dir`.
fn dir_size(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                total += dir_size(&path);
            } else if let Ok(meta) = entry.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

/// The one-command receive line for a code, macOS/Linux, ready to paste.
fn receive_command(code: &Code) -> String {
    let hyphenated = code.canonical().replace(' ', "-");
    format!("curl -fsSL https://edward3423.github.io/grandma.sh | sh -s -- {hyphenated}")
}

/// The one-command receive line for a code, Windows PowerShell.
fn receive_command_windows(code: &Code) -> String {
    format!(
        "$env:GRANDMASEND_CODE='{}'; irm https://edward3423.github.io/grandma.ps1 | iex",
        code.canonical()
    )
}

/// Keyboard listener while serving: 'c' copies the macOS/Linux receive
/// command, 'w' the Windows one (OSC 52); ctrl-c is re-raised so the main
/// loop still sees it. Adapted from sendme's clipboard handling.
fn spawn_clipboard_keys(unix_command: String, windows_command: String) {
    use crossterm::{
        event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
        terminal::{disable_raw_mode, enable_raw_mode},
    };
    use futures::StreamExt;

    if !std::io::IsTerminal::is_terminal(&std::io::stdin()) {
        return;
    }

    tokio::spawn(async move {
        // Raw mode is restored on ctrl-c below; sendme uses the same trick.
        if enable_raw_mode().is_err() {
            return;
        }
        EventStream::new()
            .for_each(move |event| {
                match event {
                    Ok(Event::Key(KeyEvent {
                        code: KeyCode::Char('c'),
                        modifiers: KeyModifiers::NONE,
                        kind: KeyEventKind::Press,
                        ..
                    })) => copy_to_clipboard(&unix_command, "macOS/Linux"),
                    Ok(Event::Key(KeyEvent {
                        code: KeyCode::Char('w'),
                        modifiers: KeyModifiers::NONE,
                        kind: KeyEventKind::Press,
                        ..
                    })) => copy_to_clipboard(&windows_command, "Windows"),
                    Ok(Event::Key(KeyEvent {
                        code: KeyCode::Char('c'),
                        modifiers: KeyModifiers::CONTROL,
                        kind: KeyEventKind::Press,
                        ..
                    })) => {
                        disable_raw_mode().ok();
                        resend_interrupt();
                    }
                    _ => {}
                }
                std::future::ready(())
            })
            .await;
    });
}

fn copy_to_clipboard(command: &str, label: &str) {
    use crossterm::{clipboard::CopyToClipboard, execute};
    match execute!(
        std::io::stdout(),
        CopyToClipboard::to_clipboard_from(command)
    ) {
        Ok(()) => eprint!("Copied the {label} receive command to the clipboard.\r\n"),
        Err(cause) => eprint!("Could not copy to clipboard: {cause}\r\n"),
    }
}

/// Re-deliver ctrl-c to the process so tokio's signal handler runs.
#[cfg(unix)]
fn resend_interrupt() {
    // Safety: raise() just re-sends SIGINT to this process.
    unsafe {
        libc::raise(libc::SIGINT);
    }
}

#[cfg(windows)]
fn resend_interrupt() {
    use windows_sys::Win32::System::Console::{GenerateConsoleCtrlEvent, CTRL_C_EVENT};
    // Safety: re-sends the ctrl-c console event to this process group.
    unsafe {
        GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0);
    }
}

/// Read the code interactively; the transient receiver's whole interface.
fn prompt_for_code() -> Result<Code> {
    use std::io::Write;
    loop {
        eprint!("Type the four-word code, then press Enter: ");
        std::io::stderr().flush().ok();
        let mut line = String::new();
        if std::io::stdin().read_line(&mut line)? == 0 {
            anyhow::bail!("no code entered");
        }
        match line.parse::<Code>() {
            Ok(code) => return Ok(code),
            Err(cause) => eprintln!("{cause}. Try again."),
        }
    }
}

/// App data root; GRANDMASEND_DATA_DIR overrides for tests.
fn data_root() -> Result<PathBuf> {
    if let Some(dir) = std::env::var_os("GRANDMASEND_DATA_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .context("cannot locate home directory")?;
    Ok(PathBuf::from(home).join(".local/share/grandmasend"))
}

/// Trailing-window transfer speed: rate and ETA averaged over the last 10 s.
struct SpeedWindow {
    samples: VecDeque<(Instant, u64)>,
}

impl SpeedWindow {
    const WINDOW: Duration = Duration::from_secs(10);

    fn new() -> Self {
        Self {
            samples: VecDeque::new(),
        }
    }

    /// Record the current position and return (bytes/s, eta) over the window.
    fn update(&mut self, position: u64, total: u64) -> (u64, Option<Duration>) {
        let now = Instant::now();
        self.samples.push_back((now, position));
        while let Some((t, _)) = self.samples.front() {
            if now.duration_since(*t) > Self::WINDOW && self.samples.len() > 2 {
                self.samples.pop_front();
            } else {
                break;
            }
        }
        let Some(&(t0, p0)) = self.samples.front() else {
            return (0, None);
        };
        let dt = now.duration_since(t0).as_secs_f64();
        if dt < 0.2 {
            return (0, None);
        }
        let rate = ((position.saturating_sub(p0)) as f64 / dt) as u64;
        let eta = (rate > 0)
            .then(|| Duration::from_secs_f64(total.saturating_sub(position) as f64 / rate as f64));
        (rate, eta)
    }
}

fn progress_bar(total: u64) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} {msg}",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("#>-"),
    );
    pb
}

fn speed_message(rate: u64, eta: Option<Duration>) -> String {
    let mbps = (rate as f64 * 8.0) / 1_000_000.0;
    match eta {
        Some(eta) => format!("{mbps:.1} Mbps eta {}", indicatif::HumanDuration(eta)),
        None => format!("{mbps:.1} Mbps"),
    }
}

async fn send(
    path: PathBuf,
    fresh: bool,
    autoextract: bool,
    password: Option<String>,
    print_addr: bool,
) -> Result<()> {
    let data_root = data_root()?;
    let canonical = path
        .canonicalize()
        .with_context(|| format!("cannot access {}", path.display()))?;

    if autoextract {
        anyhow::ensure!(
            canonical.is_file()
                && grandmasend_core::extract::ArchiveKind::from_path(&canonical).is_some(),
            "--autoextract needs a single .zip, .rar, or .7z file"
        );
    }

    // --fresh: explicit abandonment. The old code stops existing (its state
    // and store are removed), so a bound-but-unfinished receiver is cut
    // loose and a brand-new code with no binding is generated.
    if fresh {
        if let Some(prior) = state::find_by_path(&data_root, &canonical)? {
            if let Ok(code) = prior.code.parse::<Code>() {
                state::remove(&data_root, &code)?;
                eprintln!("Abandoned the previous send of this path; its code no longer works.");
            }
        }
    }

    // Revival: an interrupted send for the same payload keeps its code and
    // its binding; the receiver can resume as if nothing happened. Fresh
    // command-line flags override revived autoextract settings.
    let revived = state::find_by_path(&data_root, &canonical)?;
    let (code, bound) = match &revived {
        Some(prior) => {
            let code: Code = prior.code.parse().context("saved send has invalid code")?;
            eprintln!(
                "Resuming previous send for {} (started {}).",
                style(canonical.display()).bold(),
                humanize_age(prior.created),
            );
            (code, prior.bound_id())
        }
        None => (Code::generate(), None),
    };
    let autoextract = autoextract || revived.as_ref().map(|p| p.autoextract).unwrap_or(false);
    let password = password.or_else(|| revived.as_ref().and_then(|p| p.archive_password.clone()));

    state::save(
        &data_root,
        &SendState {
            code: code.canonical(),
            path: canonical.clone(),
            bound: bound.map(|id| id.to_string()),
            created: revived
                .as_ref()
                .map(|p| p.created)
                .unwrap_or_else(state::now_unix),
            autoextract,
            archive_password: password.clone(),
        },
    )?;

    let (tx, mut rx) = mpsc::channel(256);
    let config = SendConfig {
        path: canonical.clone(),
        code: Some(code.clone()),
        bound,
        data_dir: state::store_dir(&data_root, &code),
        version: VERSION.to_string(),
        autoextract,
        archive_password: password.clone(),
    };

    let state_root = data_root.clone();
    let state_code = code.canonical();
    let state_path = canonical.clone();
    let state_created = revived.map(|p| p.created).unwrap_or_else(state::now_unix);
    let state_autoextract = autoextract;
    let state_password = password.clone();
    let ui = tokio::spawn(async move {
        let mut bar: Option<ProgressBar> = None;
        let mut speed = SpeedWindow::new();
        let mut total = 0u64;
        while let Some(event) = rx.recv().await {
            match event {
                SenderEvent::Ready {
                    code,
                    payload_size,
                    file_count,
                    name,
                    addr,
                    ..
                } => {
                    total = payload_size;
                    eprintln!(
                        "Offering {} ({}, {} file{})",
                        style(&name).bold(),
                        HumanBytes(payload_size),
                        file_count,
                        if file_count == 1 { "" } else { "s" },
                    );
                    let unix_command = receive_command(&code);
                    let windows_command = receive_command_windows(&code);
                    eprintln!();
                    eprintln!("The code is:  {}", style(&code).bold().green());
                    eprintln!();
                    eprintln!("One-command receive:");
                    eprintln!("  macOS/Linux:  {}", style(&unix_command).bold());
                    eprintln!("  Windows:      {}", style(&windows_command).bold());
                    eprintln!();
                    eprintln!("Alternatively, tell the receiver to run this command:");
                    eprintln!(
                        "  macOS/Linux:  {}",
                        style("curl -fsSL https://edward3423.github.io/grandma.sh | sh").bold()
                    );
                    eprintln!(
                        "  Windows:      {}",
                        style("irm https://edward3423.github.io/grandma.ps1 | iex").bold()
                    );
                    eprintln!("then read the four word code for them to enter.");
                    eprintln!();
                    eprintln!("Keep this window open until the transfer finishes.");
                    eprintln!(
                        "Press c to copy the macOS/Linux command, w to copy the Windows command."
                    );
                    spawn_clipboard_keys(unix_command, windows_command);
                    if print_addr {
                        if let Ok(json) = serde_json::to_string(&addr) {
                            println!("ADDR {json}");
                        }
                    }
                }
                SenderEvent::ReceiverConnected { id, version } => {
                    eprintln!(
                        "{} {}",
                        style("Receiver connected:").bold().cyan(),
                        id.fmt_short()
                    );
                    warn_if_peer_newer("receiver", &version);
                    // Pre-autoextract receivers deliver the archive as-is.
                    if state_autoextract && update::is_older(&version, "0.2.0") {
                        eprintln!(
                            "Note: the receiver runs grandmasend {version}, which does not \
                             auto-extract yet - they will get the archive itself."
                        );
                    }
                }
                SenderEvent::Bound { id } => {
                    // Persist the binding so a revived send only serves the
                    // same receiver.
                    state::save(
                        &state_root,
                        &SendState {
                            code: state_code.clone(),
                            path: state_path.clone(),
                            bound: Some(id.to_string()),
                            created: state_created,
                            autoextract: state_autoextract,
                            archive_password: state_password.clone(),
                        },
                    )
                    .ok();
                }
                SenderEvent::ServeProgress { bytes } => {
                    let pb = bar.get_or_insert_with(|| progress_bar(total));
                    let (rate, eta) = speed.update(bytes, total);
                    pb.set_position(bytes.min(total));
                    pb.set_message(speed_message(rate, eta));
                }
                SenderEvent::Completed { payload_size } => {
                    if let Some(pb) = bar.take() {
                        pb.finish_and_clear();
                    }
                    eprintln!(
                        "{} {} delivered and verified.",
                        style("Done.").bold().green(),
                        HumanBytes(payload_size)
                    );
                }
            }
        }
    });

    let result = tokio::select! {
        r = sender::send(config, tx) => r.map(|_| ()),
        _ = tokio::signal::ctrl_c() => {
            crossterm::terminal::disable_raw_mode().ok();
            eprintln!();
            eprintln!("Stopped. Run the same send again to revive this code.");
            ui.abort();
            return Ok(());
        }
    };
    // The clipboard key listener may have left the terminal in raw mode.
    crossterm::terminal::disable_raw_mode().ok();
    ui.abort();
    // Completion consumed the code; failure keeps state for revival.
    if result.is_ok() {
        state::remove(&data_root, &code)?;
    }
    result
}

async fn receive(
    code: Vec<String>,
    dest: Option<PathBuf>,
    sender_addr: Option<String>,
) -> Result<()> {
    let code: Code = if code.is_empty() {
        prompt_for_code()?
    } else {
        code.join(" ").parse()?
    };
    let dest = match dest {
        Some(d) => d,
        None => default_downloads()?,
    };
    tokio::fs::create_dir_all(&dest).await?;
    let sender_addr = sender_addr
        .map(|s| serde_json::from_str(&s).context("invalid --sender-addr JSON"))
        .transpose()?;

    let (tx, mut rx) = mpsc::channel(256);
    let config = ReceiveConfig {
        code,
        dest,
        data_dir: data_root()?,
        version: VERSION.to_string(),
        sender_addr,
    };

    let ui = tokio::spawn(async move {
        let mut bar: Option<ProgressBar> = None;
        let mut speed = SpeedWindow::new();
        let mut total = 0u64;
        let mut waiting_since: Option<Instant> = None;
        let mut hint = tokio::time::interval(Duration::from_secs(60));
        hint.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        hint.reset();
        loop {
            let event = tokio::select! {
                event = rx.recv() => match event {
                    Some(event) => event,
                    None => break,
                },
                _ = hint.tick(), if waiting_since.is_some() => {
                    // Wrong-but-valid codes look exactly like an offline
                    // sender; nudge the humans to compare codes.
                    eprintln!(
                        "Still waiting - if the sender says they're online, \
                         double-check the code together."
                    );
                    continue;
                }
            };
            match event {
                ReceiverEvent::Connecting => {
                    eprintln!("Looking for the sender...");
                    waiting_since = Some(Instant::now());
                    hint.reset();
                }
                ReceiverEvent::OfferReceived {
                    name,
                    payload_size,
                    file_count,
                    resumed_bytes,
                    sender_version,
                } => {
                    waiting_since = None;
                    warn_if_peer_newer("sender", &sender_version);
                    eprintln!(
                        "Receiving {} ({}, {} file{})",
                        style(&name).bold(),
                        HumanBytes(payload_size),
                        file_count,
                        if file_count == 1 { "" } else { "s" },
                    );
                    if resumed_bytes > 0 {
                        eprintln!("Resuming: {} already here.", HumanBytes(resumed_bytes));
                    }
                    total = payload_size;
                    let pb = progress_bar(payload_size);
                    pb.set_position(resumed_bytes);
                    bar = Some(pb);
                }
                ReceiverEvent::Progress { offset } => {
                    waiting_since = None;
                    if let Some(pb) = &bar {
                        let position = offset.min(total);
                        let (rate, eta) = speed.update(position, total);
                        pb.set_position(position);
                        pb.set_message(speed_message(rate, eta));
                    }
                }
                ReceiverEvent::Interrupted => {
                    if waiting_since.is_none() {
                        eprintln!(
                            "Connection to the sender lost - waiting for them to come back. \
                             Leave this window open, or press ctrl-c and rerun later to resume."
                        );
                        waiting_since = Some(Instant::now());
                        hint.reset();
                    }
                }
                ReceiverEvent::AckUndelivered => {
                    eprintln!(
                        "Note: could not confirm completion with the sender - \
                         their side may still show the send as waiting."
                    );
                }
                ReceiverEvent::Exporting => {
                    if let Some(pb) = bar.take() {
                        pb.finish_and_clear();
                    }
                    eprintln!("All bytes verified. Saving...");
                }
                ReceiverEvent::Extracting { name } => {
                    eprintln!("Extracting {}...", style(&name).bold());
                }
                ReceiverEvent::Extracted { files, dest } => {
                    eprintln!(
                        "Extracted {} file{} to {}",
                        files,
                        if files == 1 { "" } else { "s" },
                        style(dest.display()).bold()
                    );
                }
                ReceiverEvent::ExtractFailed { reason } => {
                    eprintln!(
                        "Could not extract the archive ({reason}). \
                         The archive itself was saved normally."
                    );
                }
                ReceiverEvent::Done { dest } => {
                    eprintln!(
                        "{} Saved to {}",
                        style("Done.").bold().green(),
                        style(dest.display()).bold()
                    );
                }
            }
        }
    });

    let result = tokio::select! {
        r = receiver::receive(config, tx) => r.map(|_| ()),
        _ = tokio::signal::ctrl_c() => {
            eprintln!("\nStopped. Run the same command with the same code to resume.");
            Ok(())
        }
    };
    ui.abort();
    result
}

fn status() -> Result<()> {
    let sends = state::list(&data_root()?)?;
    if sends.is_empty() {
        eprintln!("No sends waiting.");
        return Ok(());
    }
    for send in sends {
        let bound = match send.bound {
            Some(_) => "receiver bound",
            None => "no receiver yet",
        };
        eprintln!(
            "{}  {}  ({}, started {})",
            style(&send.code).bold().green(),
            send.path.display(),
            bound,
            humanize_age(send.created),
        );
    }
    Ok(())
}

/// Offline outdatedness signal: the frozen hello carries versions, so even
/// with no internet we can tell when the peer runs newer. Warn and proceed.
fn warn_if_peer_newer(role: &str, peer_version: &str) {
    if update::is_older(VERSION, peer_version) {
        eprintln!(
            "Note: the {role} runs grandmasend {peer_version}, you run {VERSION}. \
             This copy may be outdated."
        );
    }
}

fn humanize_age(created_unix: u64) -> String {
    let age = state::now_unix().saturating_sub(created_unix);
    match age {
        0..=59 => "moments ago".to_string(),
        60..=3599 => format!("{} min ago", age / 60),
        3600..=86399 => format!("{} h ago", age / 3600),
        _ => format!("{} days ago", age / 86400),
    }
}

fn default_downloads() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .context("cannot locate home directory")?;
    Ok(PathBuf::from(home).join("Downloads"))
}
