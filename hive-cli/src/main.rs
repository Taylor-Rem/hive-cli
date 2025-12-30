use clap::{Parser, Subcommand};
use hive_codegen;

#[derive(Parser)]
#[command(name = "hive")]
#[command(about = "A simple CLI tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands
}
#[derive(Subcommand)]
enum Commands {
    Codegen {
        #[arg(short, long)]
        name: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command { 
        Commands::Codegen { name } => {
            match name {
                Some(n) => hive_codegen::greet(&n),
                None => hive_codegen::greet("Alice"),
            }
        }
    }
}