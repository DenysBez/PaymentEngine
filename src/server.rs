use payments_engine::config::ProcessorConfig;
use payments_engine::domain::transaction::{RawTrxRecord, Trx};
use payments_engine::error::PaymentError;
use payments_engine::services::payment_engine::PaymentsEngine;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<(), PaymentError> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Vec<String> = std::env::args().collect();
    let bind_addr = if args.len() > 1 {
        args[1].as_str()
    } else {
        "0.0.0.0:8080"
    };

    let config = ProcessorConfig::production();
    let engine = Arc::new(PaymentsEngine::with_max_history(config.max_tx_history));

    let listener = match TcpListener::bind(bind_addr).await {
        Ok(listener) => listener,
        Err(e) => {
            log::error!("Failed to bind to {}: {}", bind_addr, e);
            return Err(e.into());
        }
    };
    log::info!("Payment engine server listening on {}", bind_addr);
    log::info!("Max transaction history: {:?}", config.max_tx_history);
    log::info!("Send CSV transactions via TCP. Server will respond with account states.");
    log::info!("");
    log::info!("CSV Format:");
    log::info!("  type,client,tx,amount");
    log::info!("  deposit,1,1,10.0");
    log::info!("  withdrawal,1,2,5.0");
    log::info!("");

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                let engine = engine.clone();
                let config = config.clone();

                tokio::spawn(async move {
                    log::info!("[{}] Connection accepted", addr);

                    if let Err(e) = handle_connection(socket, engine, config, addr).await {
                        log::error!("[{}] Error: {}", addr, e);
                    }

                    log::info!("[{}] Connection closed", addr);
                });
            }
            Err(e) => {
                log::error!("Failed to accept connection: {}", e);
            }
        }
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    engine: Arc<PaymentsEngine>,
    config: ProcessorConfig,
    addr: std::net::SocketAddr,
) -> Result<(), PaymentError> {
    let mut buffer = Vec::new();
    socket.read_to_end(&mut buffer).await?;

    let cursor = Cursor::new(buffer);
    let mut csv_reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(cursor);

    let mut transaction_count = 0;
    let mut error_count = 0;

    for result in csv_reader.deserialize::<RawTrxRecord>() {
        match result {
            Ok(raw_record) => {
                if let Some(tx) = Trx::from_raw(raw_record) {
                    engine.process(tx).await;
                    transaction_count += 1;
                } else if config.log_warnings {
                    log::warn!("[{}] Skipping transaction with missing amount", addr);
                    error_count += 1;
                }
            }
            Err(e) => {
                if config.skip_malformed {
                    if config.log_warnings {
                        log::warn!("[{}] Skipping malformed row: {}", addr, e);
                    }
                    error_count += 1;
                } else {
                    return Err(e.into());
                }
            }
        }
    }

    log::info!(
        "[{}] Processed {} transactions ({} errors/skipped)",
        addr,
        transaction_count,
        error_count
    );

    log::info!("[{}] Sending account states...", addr);

    let accounts = engine.get_accounts();
    let mut output = Vec::new();
    let mut csv_writer = csv::Writer::from_writer(&mut output);

    for account in accounts {
        csv_writer.serialize(&account)?;
    }

    csv_writer.flush()?;
    drop(csv_writer);

    socket.write_all(&output).await?;
    socket.flush().await?;

    log::info!("[{}] Response sent successfully", addr);

    Ok(())
}
