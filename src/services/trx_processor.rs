use crate::config::ProcessorConfig;
use crate::error::{PaymentError, Result};
use crate::services::payment_engine::PaymentsEngine;
use crate::domain::transaction::{RawTrxRecord, Trx};
use std::fs::File;
use std::io::{BufReader, Write};

pub struct TrxProcessor {
    engine: PaymentsEngine,
    config: ProcessorConfig,
}

impl TrxProcessor {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub fn with_config(config: ProcessorConfig) -> Self {
        TrxProcessor {
            engine: PaymentsEngine::with_max_history(config.max_tx_history),
            config,
        }
    }
}

impl Default for TrxProcessor {
    fn default() -> Self {
        let config = ProcessorConfig::default();
        TrxProcessor {
            engine: PaymentsEngine::with_max_history(config.max_tx_history),
            config,
        }
    }
}

impl TrxProcessor {
    pub async fn process_file(&mut self, filepath: &str) -> Result<()> {
        let file = File::open(filepath)
            .map_err(|_| PaymentError::FileNotFound(filepath.to_string()))?;

        let reader = BufReader::new(file);
        let mut csv_reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(reader);

        for result in csv_reader.deserialize() {
            match result {
                Ok(raw_record) => {
                    let raw: RawTrxRecord = raw_record;
                    if let Some(tx) = Trx::from_raw(raw) {
                        self.engine.process(tx).await;
                    } else if self.config.log_warnings {
                        log::warn!("Skipping transaction with missing amount");
                    }
                }
                Err(e) => {
                    if self.config.skip_malformed {
                        if self.config.log_warnings {
                            log::warn!("Skipping malformed row: {}", e);
                        }
                    } else {
                        return Err(PaymentError::CsvError(e));
                    }
                }
            }
        }

        Ok(())
    }

    pub fn write_results<W: Write>(&self, writer: W) -> Result<()> {
        let mut csv_writer = csv::Writer::from_writer(writer);

        for account in self.engine.get_accounts() {
            csv_writer.serialize(&account)?;
        }

        csv_writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_processor_basic_flow() {
        let mut processor = TrxProcessor::new();

        // Process a simple file
        processor.process_file("tests/fixtures/basic.csv").await.unwrap();

        // Write to a buffer
        let mut buffer = Vec::new();
        processor.write_results(&mut buffer).unwrap();

        // Verify output contains expected data
        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("client,available,held,total,locked"));
        assert!(output.contains("1,0.5000"));
        assert!(output.contains("2,2.0000"));
    }

    #[tokio::test]
    async fn test_processor_with_disputes() {
        let mut processor = TrxProcessor::new();
        processor.process_file("tests/fixtures/chargeback.csv").await.unwrap();

        let mut buffer = Vec::new();
        processor.write_results(&mut buffer).unwrap();

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("true")); // Account should be locked
    }
}
