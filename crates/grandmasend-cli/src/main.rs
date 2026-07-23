use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use console::style;
use grandmasend_core::{
    code::Code,
    events::{ReceiverEvent, SenderEvent},
    receiver::{self, ReceiveConfig},
    sender::{self, SendConfig},
};
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use tokio::sync::mpsc;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Send any file, any size, to anyone who can type one command.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Offer a file or folder; prints the four-word code to read to the receiver.
    Send {
        path: PathBuf,
        /// Print the bound endpoint address as JSON. Debug/test hook.
        #[clap(long, hide = true)]
        print_addr: bool,
    },
    /// Fetch a transfer by its four-word code.
    #[clap(visible_alias = "recv")]
    Receive {
        /// The four words, in order; spaces or hyphens both work.
        code: Vec<String>,
        /// Destination directory; defaults to ~/Downloads.
        #[clap(long)]
        dest: Option<PathBuf>,
        /// Dial this endpoint address (JSON) instead of discovery. Debug/test hook.
        #[clap(long, hide = true)]
        sender_addr: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    match args.command {
        Commands::Send { path, print_addr } => send(path, print_addr).await,
        Commands::Receive {
            code,
            dest,
            sender_addr,
        } => receive(code, dest, sender_addr).await,
    }
}

fn data_root() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .context("cannot locate home directory")?;
    Ok(PathBuf::from(home).join(".local/share/grandmasend"))
}

async fn send(path: PathBuf, print_addr: bool) -> Result<()> {
    let (tx, mut rx) = mpsc::channel(32);
    // Per-send data dir; suffix keeps concurrent sends isolated.
    let send_id = std::process::id();
    let data_dir = data_root()?.join(format!("send-{send_id}"));
    let config = SendConfig {
        path,
        code: None,
        data_dir: data_dir.clone(),
        version: VERSION.to_string(),
    };

    let ui = tokio::spawn(async move {
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
                    eprintln!(
                        "Offering {} ({}, {} file{})",
                        style(&name).bold(),
                        HumanBytes(payload_size),
                        file_count,
                        if file_count == 1 { "" } else { "s" },
                    );
                    eprintln!();
                    eprintln!("The code is:  {}", style(&code).bold().green());
                    eprintln!();
                    eprintln!("On the other machine, run grandmasend and type this code.");
                    eprintln!("Keep this window open until the transfer finishes.");
                    if print_addr {
                        println!("ADDR {}", serde_json::to_string(&addr).unwrap());
                    }
                }
                SenderEvent::ReceiverConnected { id } => {
                    eprintln!(
                        "{} {}",
                        style("Receiver connected:").bold().cyan(),
                        id.fmt_short()
                    );
                }
                SenderEvent::Completed { payload_size } => {
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
            eprintln!("\nStopped. The code is no longer being served.");
            Ok(())
        }
    };
    // The per-send store holds only references plus small metadata; remove it
    // on any exit. Revival across reruns arrives with persistent send state.
    tokio::fs::remove_dir_all(&data_dir).await.ok();
    ui.abort();
    result
}

async fn receive(
    code: Vec<String>,
    dest: Option<PathBuf>,
    sender_addr: Option<String>,
) -> Result<()> {
    let code: Code = code.join(" ").parse()?;
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
        version: VERSION.to_string(),
        sender_addr,
    };

    let ui = tokio::spawn(async move {
        let mut bar: Option<ProgressBar> = None;
        let mut resumed = 0u64;
        while let Some(event) = rx.recv().await {
            match event {
                ReceiverEvent::Connecting => {
                    eprintln!("Looking for the sender...");
                }
                ReceiverEvent::OfferReceived {
                    name,
                    payload_size,
                    file_count,
                    resumed_bytes,
                } => {
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
                    resumed = resumed_bytes;
                    let pb = ProgressBar::new(payload_size);
                    pb.set_style(
                        ProgressStyle::with_template(
                            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} {binary_bytes_per_sec} eta {eta}",
                        )
                        .unwrap()
                        .progress_chars("#>-"),
                    );
                    pb.set_position(resumed_bytes);
                    bar = Some(pb);
                }
                ReceiverEvent::Progress { offset } => {
                    if let Some(pb) = &bar {
                        pb.set_position(resumed + offset);
                    }
                }
                ReceiverEvent::Exporting => {
                    if let Some(pb) = bar.take() {
                        pb.finish_and_clear();
                    }
                    eprintln!("All bytes verified. Saving...");
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

fn default_downloads() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .context("cannot locate home directory")?;
    Ok(PathBuf::from(home).join("Downloads"))
}
