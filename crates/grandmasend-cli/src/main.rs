use clap::{Parser, Subcommand};

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
    Send { path: std::path::PathBuf },
    /// Fetch a transfer by its four-word code.
    #[clap(visible_alias = "recv")]
    Receive { code: Vec<String> },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    match args.command {
        Commands::Send { path } => {
            anyhow::bail!("send not yet implemented (path: {})", path.display())
        }
        Commands::Receive { code } => {
            let code: grandmasend_core::code::Code = code.join(" ").parse()?;
            anyhow::bail!("receive not yet implemented (code: {code})")
        }
    }
}
