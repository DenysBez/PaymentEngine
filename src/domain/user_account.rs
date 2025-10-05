use rust_decimal::Decimal;
use serde::{Serialize, Serializer};

fn serialize_decimal<S>(value: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("{:.4}", value))
}

#[derive(Debug, Clone, Serialize)]
pub struct UserAccount {
    pub client: u16,
    #[serde(serialize_with = "serialize_decimal")]
    pub available: Decimal,
    #[serde(serialize_with = "serialize_decimal")]
    pub held: Decimal,
    #[serde(serialize_with = "serialize_decimal")]
    pub total: Decimal,
    pub locked: bool,
}

impl UserAccount {
    pub fn new(client_id: u16) -> Self {
        UserAccount {
            client: client_id,
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            total: Decimal::ZERO,
            locked: false,
        }
    }

    #[cfg(test)]
    pub fn verify_totals(&self) -> bool {
        self.total == self.available + self.held
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_new_account() {
        let acc = UserAccount::new(1);
        assert_eq!(acc.client, 1);
        assert_eq!(acc.available, Decimal::ZERO);
        assert_eq!(acc.held, Decimal::ZERO);
        assert_eq!(acc.total, Decimal::ZERO);
        assert!(!acc.locked);
    }

    #[test]
    fn test_totals_invariant() {
        let acc = UserAccount {
            client: 1,
            available: dec!(10.5),
            held: dec!(5.25),
            total: dec!(15.75),
            locked: false,
        };
        assert!(acc.verify_totals());
    }

    #[test]
    fn test_totals_invariant_violated() {
        let acc = UserAccount {
            client: 1,
            available: dec!(10.5),
            held: dec!(5.25),
            total: dec!(20.0),
            locked: false,
        };
        assert!(!acc.verify_totals());
    }
}
