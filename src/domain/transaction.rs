use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrxStatus {
    Normal,
    UnderDispute,
    ChargedBack,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize)]
pub struct RawTrxRecord {
    #[serde(rename = "type")]
    pub tx_type: TrxType,
    pub client: u16,
    pub tx: u32,
    #[serde(default)]
    pub amount: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub enum Trx {
    Deposit { client: u16, tx: u32, amount: Decimal },
    Withdrawal { client: u16, tx: u32, amount: Decimal },
    Dispute { client: u16, tx: u32 },
    Resolve { client: u16, tx: u32 },
    Chargeback { client: u16, tx: u32 },
}

impl Trx {
    pub fn from_raw(raw: RawTrxRecord) -> Option<Self> {
        match raw.tx_type {
            TrxType::Deposit => {
                let amount = raw.amount?;
                Some(Trx::Deposit {
                    client: raw.client,
                    tx: raw.tx,
                    amount,
                })
            }
            TrxType::Withdrawal => {
                let amount = raw.amount?;
                Some(Trx::Withdrawal {
                    client: raw.client,
                    tx: raw.tx,
                    amount,
                })
            }
            TrxType::Dispute => Some(Trx::Dispute {
                client: raw.client,
                tx: raw.tx,
            }),
            TrxType::Resolve => Some(Trx::Resolve {
                client: raw.client,
                tx: raw.tx,
            }),
            TrxType::Chargeback => Some(Trx::Chargeback {
                client: raw.client,
                tx: raw.tx,
            }),
        }
    }

}

#[derive(Debug, Clone)]
pub struct TxRecord {
    pub client: u16,
    pub amount: Decimal,
    pub status: TrxStatus,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_deposit_parsing() {
        let raw = RawTrxRecord {
            tx_type: TrxType::Deposit,
            client: 1,
            tx: 100,
            amount: Some(dec!(10.5)),
        };
        let tx = Trx::from_raw(raw).unwrap();
        match tx {
            Trx::Deposit { client, tx, amount } => {
                assert_eq!(client, 1);
                assert_eq!(tx, 100);
                assert_eq!(amount, dec!(10.5));
            }
            _ => panic!("Wrong transaction type"),
        }
    }

    #[test]
    fn test_dispute_parsing() {
        let raw = RawTrxRecord {
            tx_type: TrxType::Dispute,
            client: 2,
            tx: 200,
            amount: None,
        };
        let tx = Trx::from_raw(raw).unwrap();
        match tx {
            Trx::Dispute { client, tx } => {
                assert_eq!(client, 2);
                assert_eq!(tx, 200);
            }
            _ => panic!("Wrong transaction type"),
        }
    }

    #[test]
    fn test_deposit_missing_amount_returns_none() {
        let raw = RawTrxRecord {
            tx_type: TrxType::Deposit,
            client: 1,
            tx: 100,
            amount: None,
        };
        assert!(Trx::from_raw(raw).is_none());
    }

    #[test]
    fn test_precision_four_decimals() {
        let raw = RawTrxRecord {
            tx_type: TrxType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(dec!(1.1234)),
        };
        let tx = Trx::from_raw(raw).unwrap();
        match tx {
            Trx::Deposit { amount, .. } => {
                assert_eq!(amount, dec!(1.1234));
            }
            _ => panic!("Wrong transaction type"),
        }
    }
}
