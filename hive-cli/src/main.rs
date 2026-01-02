mod introspect;

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
    Introspect {
        #[arg(short, long)]
        connect: Option<String>,
        #[arg(short, long, default_value = "./schema/schema.toml")]
        output: String,
    }
}

async fn main() {
    let cli = Cli::parse();

    match cli.command { 
        Commands::Introspect { connect, output } => {
            match connect {
                Some(c) => introspect::run(&c, &output).await,
                None => introspect::run("todo", &output),
            }
        }
    }
}