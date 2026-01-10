mod introspect;
mod init;
mod migrate;
mod codegen;
pub mod structs;
mod schema;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "hive")]
#[command(about = "A simple CLI tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands
}
#[derive(Subcommand)]
enum Commands {
    Init {
        #[arg(short, long)]
        path: Option<String>,
    },
    Introspect {
        #[arg(short, long)]
        connect: Option<String>,
        #[arg(short, long, default_value = "./schema/schema.toml")]
        output: String,
    }
}
#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { path } => {
            if let Err(e) = init::run(path.as_deref()) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Commands::Introspect { connect, output } => {
            let result = match connect {
                Some(c) => introspect::run(&c, &output).await,
                None => {
                    eprintln!("Error: --connect argument is required");
                    std::process::exit(1);
                }
            };
            if let Err(e) = result {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}