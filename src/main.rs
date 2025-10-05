mod domain;
mod services;
mod config;
mod error;

use std::env;
use std::process;
use services::trx_processor::TrxProcessor;
use error::PaymentError;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <transactions.csv>", args[0]);
        process::exit(1);
    }

    let filepath = &args[1];

    if let Err(e) = run(filepath).await {
        log::error!("Failed to process transactions: {}", e);
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}

async fn run(filepath: &str) -> Result<(), PaymentError> {
    let mut processor = TrxProcessor::new();
    processor.process_file(filepath).await?;
    processor.write_results(std::io::stdout())?;
    Ok(())
}

