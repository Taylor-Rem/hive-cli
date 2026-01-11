mod commands;

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
        #[arg(short, long, default_value = ".")]
        path: String
    },
    Introspect {
        #[arg(short, long)]
        db_url: Option<String>,
        #[arg(short, long, default_value = "./schema/schema.toml")]
        output: String
    },
    Migrate {
        #[arg(short, long)]
        db_url: Option<String>,
        #[arg(short, long, default_value = "./schema/schema.toml")]
        schema_path: String
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { path } => { commands::init::run(&path)?; }
        Commands::Introspect { db_url, output } => {
            commands::introspect::run(db_url.as_deref(), output).await?;
        }
        Commands::Migrate { db_url, schema_path } => {
            commands::migrate::run(db_url.as_deref(), &schema_path).await?;
        }
    }
    Ok(())
}
