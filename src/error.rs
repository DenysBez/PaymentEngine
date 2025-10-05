use rust_decimal::Decimal;
use std::fmt;

#[derive(Debug)]
#[allow(dead_code)]
pub enum PaymentError {
    FileNotFound(String),
    CsvError(csv::Error),
    IoError(std::io::Error),
    InvalidTransaction(String),
    InsufficientFunds {
        client: u16,
        available: Decimal,
        requested: Decimal,
    },
    AccountLocked(u16),
    TransactionNotFound(u32),
    InvalidDispute {
        tx_id: u32,
        reason: String,
    },
}

impl fmt::Display for PaymentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PaymentError::FileNotFound(path) => write!(f, "File not found: {}", path),
            PaymentError::CsvError(e) => write!(f, "CSV error: {}", e),
            PaymentError::IoError(e) => write!(f, "I/O error: {}", e),
            PaymentError::InvalidTransaction(msg) => write!(f, "Invalid transaction: {}", msg),
            PaymentError::InsufficientFunds {
                client,
                available,
                requested,
            } => write!(
                f,
                "Insufficient funds for client {}: available {}, requested {}",
                client, available, requested
            ),
            PaymentError::AccountLocked(client) => {
                write!(f, "Account {} is locked due to chargeback", client)
            }
            PaymentError::TransactionNotFound(tx_id) => {
                write!(f, "Transaction {} not found", tx_id)
            }
            PaymentError::InvalidDispute { tx_id, reason } => {
                write!(f, "Invalid dispute for transaction {}: {}", tx_id, reason)
            }
        }
    }
}

impl std::error::Error for PaymentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PaymentError::CsvError(e) => Some(e),
            PaymentError::IoError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<csv::Error> for PaymentError {
    fn from(err: csv::Error) -> Self {
        PaymentError::CsvError(err)
    }
}

impl From<std::io::Error> for PaymentError {
    fn from(err: std::io::Error) -> Self {
        PaymentError::IoError(err)
    }
}

pub type Result<T> = std::result::Result<T, PaymentError>;
