use crate::domain::user_account::UserAccount;
use crate::domain::transaction::{Trx, TxRecord, TrxStatus};
use dashmap::DashMap;
use rust_decimal::Decimal;
use indexmap::IndexMap;
use tokio::sync::Mutex;

pub struct PaymentsEngine {
    user_account_map: DashMap<u16, UserAccount>,
    tx_history: Mutex<IndexMap<u32, TxRecord>>,
    max_tx_history: Option<usize>,
}

impl PaymentsEngine {

    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_history(max_tx_history: Option<usize>) -> Self {
        PaymentsEngine {
            user_account_map: DashMap::new(),
            tx_history: Mutex::new(IndexMap::new()),
            max_tx_history,
        }
    }

    pub fn get_or_create_account(&self, client_id: u16) -> dashmap::mapref::one::RefMut<'_, u16, UserAccount> {
        self.user_account_map
            .entry(client_id)
            .or_insert_with(|| UserAccount::new(client_id))
    }

    pub fn get_accounts(&self) -> Vec<UserAccount> {
        let mut accounts: Vec<UserAccount> = self.user_account_map
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        accounts.sort_by_key(|a| a.client);
        accounts
    }

    fn insert_tx_with_eviction(
        &self,
        tx_history: &mut indexmap::IndexMap<u32, TxRecord>,
        tx: u32,
        client: u16,
        amount: Decimal,
    ) {
        if let Some(max) = self.max_tx_history {
            if tx_history.len() >= max {
                tx_history.shift_remove_index(0);
            }
        }

        tx_history.insert(tx, TxRecord {
            client,
            amount,
            status: TrxStatus::Normal,
        });
    }

    fn check_duplicate_tx(
        tx_history: &IndexMap<u32, TxRecord>,
        tx: u32,
        tx_type: &str,
        client: u16,
        amount: Decimal,
    ) -> bool {
        if tx_history.contains_key(&tx) {
            log::error!(
                "{} rejected: client={}, tx={}, amount={} (duplicate transaction ID)",
                tx_type, client, tx, amount
            );
            true
        } else {
            false
        }
    }

    fn checked_add_with_log(
        current: Decimal,
        amount: Decimal,
        field_name: &str,
        tx_type: &str,
        client: u16,
        tx: u32,
    ) -> Option<Decimal> {
        match current.checked_add(amount) {
            Some(val) => Some(val),
            None => {
                log::error!(
                    "{} rejected: client={}, tx={}, amount={}, {}={} (overflow on {})",
                    tx_type, client, tx, amount, field_name, current, field_name
                );
                None
            }
        }
    }

    fn checked_sub_with_log(
        current: Decimal,
        amount: Decimal,
        field_name: &str,
        tx_type: &str,
        client: u16,
        tx: u32,
    ) -> Option<Decimal> {
        match current.checked_sub(amount) {
            Some(val) => Some(val),
            None => {
                log::error!(
                    "{} rejected: client={}, tx={}, amount={}, {}={} (underflow on {})",
                    tx_type, client, tx, amount, field_name, current, field_name
                );
                None
            }
        }
    }

    pub async fn process(&self, tx: Trx) {
        match tx {
            Trx::Deposit { client, tx, amount } => {
                self.process_deposit(client, tx, amount).await;
            }
            Trx::Withdrawal { client, tx, amount } => {
                self.process_withdrawal(client, tx, amount).await;
            }
            Trx::Dispute { client, tx } => {
                self.process_dispute(client, tx).await;
            }
            Trx::Resolve { client, tx } => {
                self.process_resolve(client, tx).await;
            }
            Trx::Chargeback { client, tx } => {
                self.process_chargeback(client, tx).await;
            }
        }
    }

    async fn process_deposit(&self, client: u16, tx: u32, amount: Decimal) {
        let mut tx_history = self.tx_history.lock().await;

        if Self::check_duplicate_tx(&tx_history, tx, "Deposit", client, amount) {
            return;
        }

        let mut account = self.get_or_create_account(client);

        let Some(new_available) = Self::checked_add_with_log(
            account.available, amount, "available", "Deposit", client, tx
        ) else { return };

        let Some(new_total) = Self::checked_add_with_log(
            account.total, amount, "total", "Deposit", client, tx
        ) else { return };

        account.available = new_available;
        account.total = new_total;

        self.insert_tx_with_eviction(&mut tx_history, tx, client, amount);
    }

    async fn process_withdrawal(&self, client: u16, tx: u32, amount: Decimal) {
        let mut tx_history = self.tx_history.lock().await;

        if Self::check_duplicate_tx(&tx_history, tx, "Withdrawal", client, amount) {
            return;
        }

        let mut account = self.get_or_create_account(client);

        if account.available < amount {
            log::warn!(
                "Withdrawal rejected: client={}, tx={}, amount={}, available={} (insufficient funds)",
                client, tx, amount, account.available
            );
            return;
        }

        let Some(new_available) = Self::checked_sub_with_log(
            account.available, amount, "available", "Withdrawal", client, tx
        ) else { return };

        let Some(new_total) = Self::checked_sub_with_log(
            account.total, amount, "total", "Withdrawal", client, tx
        ) else { return };

        account.available = new_available;
        account.total = new_total;

        self.insert_tx_with_eviction(&mut tx_history, tx, client, amount);
    }

    async fn process_dispute(&self, client: u16, tx: u32) {
        let mut tx_history = self.tx_history.lock().await;

        if let Some(tx_record) = tx_history.get_mut(&tx) {
            if tx_record.client != client {
                log::warn!(
                    "Dispute rejected: client={} attempted to dispute tx={} belonging to client={}",
                    client, tx, tx_record.client
                );
                return;
            }

            if tx_record.status == TrxStatus::ChargedBack {
                log::warn!(
                    "Dispute rejected: client={}, tx={} (transaction already charged back)",
                    client, tx
                );
                return;
            }

            if tx_record.status == TrxStatus::UnderDispute {
                log::warn!(
                    "Dispute rejected: client={}, tx={} (already under dispute)",
                    client, tx
                );
                return;
            }

            let amount = tx_record.amount;
            tx_record.status = TrxStatus::UnderDispute;

            if let Some(mut account) = self.user_account_map.get_mut(&client) {
                let Some(new_available) = Self::checked_sub_with_log(
                    account.available, amount, "available", "Dispute", client, tx
                ) else {
                    tx_record.status = TrxStatus::Normal;
                    return;
                };

                let Some(new_held) = Self::checked_add_with_log(
                    account.held, amount, "held", "Dispute", client, tx
                ) else {
                    tx_record.status = TrxStatus::Normal;
                    return;
                };

                if new_available < Decimal::ZERO {
                    log::warn!(
                        "Dispute creates negative balance: client={}, tx={}, amount={}, available={} -> {} (business rule: allowed)",
                        client, tx, amount, account.available, new_available
                    );
                }

                account.available = new_available;
                account.held = new_held;
            }
        } else {
            log::warn!(
                "Dispute rejected: client={}, tx={} (transaction not found - may have been evicted from cache)",
                client, tx
            );
        }
    }

    async fn process_resolve(&self, client: u16, tx: u32) {
        let mut tx_history = self.tx_history.lock().await;

        if let Some(tx_record) = tx_history.get_mut(&tx) {
            if tx_record.client != client {
                log::warn!(
                    "Resolve rejected: client={} attempted to resolve tx={} belonging to client={}",
                    client, tx, tx_record.client
                );
                return;
            }

            if tx_record.status != TrxStatus::UnderDispute {
                log::warn!(
                    "Resolve rejected: client={}, tx={}, status={:?} (not under dispute)",
                    client, tx, tx_record.status
                );
                return;
            }

            let amount = tx_record.amount;
            tx_record.status = TrxStatus::Normal;

            if let Some(mut account) = self.user_account_map.get_mut(&client) {
                let Some(new_held) = Self::checked_sub_with_log(
                    account.held, amount, "held", "Resolve", client, tx
                ) else {
                    tx_record.status = TrxStatus::UnderDispute;
                    return;
                };

                let Some(new_available) = Self::checked_add_with_log(
                    account.available, amount, "available", "Resolve", client, tx
                ) else {
                    tx_record.status = TrxStatus::UnderDispute;
                    return;
                };

                account.held = new_held;
                account.available = new_available;
            }
        } else {
            log::warn!(
                "Resolve rejected: client={}, tx={} (transaction not found - may have been evicted from cache)",
                client, tx
            );
        }
    }

    async fn process_chargeback(&self, client: u16, tx: u32) {
        let mut tx_history = self.tx_history.lock().await;

        if let Some(tx_record) = tx_history.get_mut(&tx) {
            if tx_record.client != client {
                log::warn!(
                    "Chargeback rejected: client={} attempted to chargeback tx={} belonging to client={}",
                    client, tx, tx_record.client
                );
                return;
            }

            if tx_record.status != TrxStatus::UnderDispute {
                log::warn!(
                    "Chargeback rejected: client={}, tx={}, status={:?} (not under dispute)",
                    client, tx, tx_record.status
                );
                return;
            }

            let amount = tx_record.amount;
            tx_record.status = TrxStatus::ChargedBack;

            if let Some(mut account) = self.user_account_map.get_mut(&client) {
                let Some(new_held) = Self::checked_sub_with_log(
                    account.held, amount, "held", "Chargeback", client, tx
                ) else {
                    tx_record.status = TrxStatus::UnderDispute;
                    return;
                };

                let Some(new_total) = Self::checked_sub_with_log(
                    account.total, amount, "total", "Chargeback", client, tx
                ) else {
                    tx_record.status = TrxStatus::UnderDispute;
                    return;
                };

                account.held = new_held;
                account.total = new_total;
                account.locked = true;

                log::info!(
                    "Chargeback processed: client={}, tx={}, amount={}, account locked",
                    client, tx, amount
                );
            }
        } else {
            log::warn!(
                "Chargeback rejected: client={}, tx={} (transaction not found - may have been evicted from cache)",
                client, tx
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use Trx::Deposit;

    #[tokio::test]
    async fn test_create_account_on_first_tx() {
        let engine = PaymentsEngine::new();
        let tx = Deposit {
            client: 1,
            tx: 100,
            amount: dec!(10.0),
        };
        engine.process(tx).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].client, 1);
    }

    #[tokio::test]
    async fn test_totals_invariant_maintained() {
        let engine = PaymentsEngine::new();
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(10.0),
        }).await;

        let accounts = engine.get_accounts();
        assert!(accounts[0].verify_totals());
    }

    #[tokio::test]
    async fn test_deposit_increases_available() {
        let engine = PaymentsEngine::new();
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(10.5),
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(10.5));
        assert_eq!(accounts[0].total, dec!(10.5));
    }

    #[tokio::test]
    async fn test_withdrawal_decreases_when_possible() {
        let engine = PaymentsEngine::new();
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(10.0),
        }).await;
        engine.process(Trx::Withdrawal {
            client: 1,
            tx: 2,
            amount: dec!(5.0),
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(5.0));
        assert_eq!(accounts[0].total, dec!(5.0));
    }

    #[tokio::test]
    async fn test_insufficient_withdrawal_ignored() {
        let engine = PaymentsEngine::new();
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(5.0),
        }).await;
        engine.process(Trx::Withdrawal {
            client: 1,
            tx: 2,
            amount: dec!(10.0),
        }).await;

        let accounts = engine.get_accounts();
        // Should still have original deposit
        assert_eq!(accounts[0].available, dec!(5.0));
        assert_eq!(accounts[0].total, dec!(5.0));
    }

    #[tokio::test]
    async fn test_tx_indexing() {
        let engine = PaymentsEngine::new();
        engine.process(Deposit {
            client: 1,
            tx: 100,
            amount: dec!(10.0),
        }).await;

        // Check transaction was stored
        let tx_history = engine.tx_history.lock().await;
        assert!(tx_history.contains_key(&100));
        let tx_record = &tx_history[&100];
        assert_eq!(tx_record.client, 1);
        assert_eq!(tx_record.amount, dec!(10.0));
        assert_eq!(tx_record.status, TrxStatus::Normal);
    }

    #[tokio::test]
    async fn test_dispute_resolve_roundtrip() {
        let engine = PaymentsEngine::new();

        // Make a deposit
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(10.0),
        }).await;

        // Dispute it
        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(0.0));
        assert_eq!(accounts[0].held, dec!(10.0));
        assert_eq!(accounts[0].total, dec!(10.0));

        // Resolve it
        engine.process(Trx::Resolve {
            client: 1,
            tx: 1,
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(10.0));
        assert_eq!(accounts[0].held, dec!(0.0));
        assert_eq!(accounts[0].total, dec!(10.0));
    }

    #[tokio::test]
    async fn test_dispute_chargeback_locks_account() {
        let engine = PaymentsEngine::new();

        // Make a deposit
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(5.0),
        }).await;

        // Dispute it
        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        // Chargeback
        engine.process(Trx::Chargeback {
            client: 1,
            tx: 1,
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(0.0));
        assert_eq!(accounts[0].held, dec!(0.0));
        assert_eq!(accounts[0].total, dec!(0.0));
        assert!(accounts[0].locked);
    }

    #[tokio::test]
    async fn test_invalid_dispute_wrong_client() {
        let engine = PaymentsEngine::new();

        // Client 1 makes deposit
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(10.0),
        }).await;

        // Client 2 tries to dispute it - should be ignored
        engine.process(Trx::Dispute {
            client: 2,
            tx: 1,
        }).await;

        // Client 1's funds should be unchanged
        let accounts = engine.get_accounts();
        let client1 = accounts.iter().find(|a| a.client == 1).unwrap();
        assert_eq!(client1.available, dec!(10.0));
        assert_eq!(client1.held, dec!(0.0));
    }

    #[tokio::test]
    async fn test_resolve_without_dispute_ignored() {
        let engine = PaymentsEngine::new();

        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(10.0),
        }).await;

        // Try to resolve without disputing first
        engine.process(Trx::Resolve {
            client: 1,
            tx: 1,
        }).await;

        // Should still be in normal state
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(10.0));
        assert_eq!(accounts[0].held, dec!(0.0));
    }

    #[tokio::test]
    async fn test_chargeback_after_chargeback_ignored() {
        let engine = PaymentsEngine::new();

        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(10.0),
        }).await;

        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        engine.process(Trx::Chargeback {
            client: 1,
            tx: 1,
        }).await;

        // Try to dispute again - should be ignored
        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(0.0));
        assert_eq!(accounts[0].held, dec!(0.0));
    }

    #[tokio::test]
    async fn test_lru_pruning_limits_memory() {
        // Create engine with max history of 2 transactions
        let engine = PaymentsEngine::with_max_history(Some(2));

        // Process 3 deposits
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(10.0),
        }).await;
        engine.process(Deposit {
            client: 1,
            tx: 2,
            amount: dec!(20.0),
        }).await;
        engine.process(Deposit {
            client: 1,
            tx: 3,
            amount: dec!(30.0),
        }).await;

        // Verify only 2 transactions are stored (oldest was evicted)
        let tx_history = engine.tx_history.lock().await;
        assert_eq!(tx_history.len(), 2);
        assert!(!tx_history.contains_key(&1)); // tx 1 should be evicted
        assert!(tx_history.contains_key(&2));
        assert!(tx_history.contains_key(&3));

        // Account should still have all deposits
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].total, dec!(60.0));
    }

    #[tokio::test]
    async fn test_dispute_fails_on_pruned_transaction() {
        let engine = PaymentsEngine::with_max_history(Some(1));

        // Process 2 deposits (first will be pruned)
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(10.0),
        }).await;
        engine.process(Deposit {
            client: 1,
            tx: 2,
            amount: dec!(20.0),
        }).await;

        // Try to dispute the pruned transaction - should be ignored
        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        // Account should be unchanged (no funds held)
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(30.0));
        assert_eq!(accounts[0].held, dec!(0.0));
    }

    #[tokio::test]
    async fn test_negative_balance_allowed_on_dispute() {
        // Test that disputes can create negative balances (business rule)
        let engine = PaymentsEngine::new();

        // Deposit 100
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;

        // Withdraw 80
        engine.process(Trx::Withdrawal {
            client: 1,
            tx: 2,
            amount: dec!(80.0),
        }).await;

        // Now available = 20, total = 20

        // Dispute the original deposit of 100
        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        // This creates negative available balance
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(-80.0)); // 20 - 100 = -80
        assert_eq!(accounts[0].held, dec!(100.0));
        assert_eq!(accounts[0].total, dec!(20.0)); // -80 + 100 = 20
        assert!(!accounts[0].locked);
    }

    #[tokio::test]
    async fn test_negative_balance_withdrawal_dispute_scenario() {
        let engine = PaymentsEngine::new();

        // Deposit 50
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(50.0),
        }).await;

        // Withdraw 40
        engine.process(Trx::Withdrawal {
            client: 1,
            tx: 2,
            amount: dec!(40.0),
        }).await;

        // Dispute the withdrawal (holds 40 from available 10)
        engine.process(Trx::Dispute {
            client: 1,
            tx: 2,
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(-30.0)); // 10 - 40 = -30
        assert_eq!(accounts[0].held, dec!(40.0));
        assert_eq!(accounts[0].total, dec!(10.0));
    }

    #[tokio::test]
    async fn test_negative_balance_resolve_returns_to_positive() {
        let engine = PaymentsEngine::new();

        // Create negative balance scenario
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;
        engine.process(Trx::Withdrawal {
            client: 1,
            tx: 2,
            amount: dec!(90.0),
        }).await;
        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        // Available is now -90 (10 - 100)
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(-90.0));

        // Resolve the dispute
        engine.process(Trx::Resolve {
            client: 1,
            tx: 1,
        }).await;

        // Available returns to positive
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(10.0)); // -90 + 100 = 10
        assert_eq!(accounts[0].held, dec!(0.0));
        assert_eq!(accounts[0].total, dec!(10.0));
    }

    #[tokio::test]
    async fn test_negative_balance_chargeback_reduces_total() {
        let engine = PaymentsEngine::new();

        // Create negative balance scenario and chargeback
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;
        engine.process(Trx::Withdrawal {
            client: 1,
            tx: 2,
            amount: dec!(95.0),
        }).await;
        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        // Available is now -95 (5 - 100), held is 100, total is 5
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(-95.0));
        assert_eq!(accounts[0].held, dec!(100.0));
        assert_eq!(accounts[0].total, dec!(5.0));

        // Chargeback
        engine.process(Trx::Chargeback {
            client: 1,
            tx: 1,
        }).await;

        // Available still -95, held removed, total is now -95
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(-95.0));
        assert_eq!(accounts[0].held, dec!(0.0));
        assert_eq!(accounts[0].total, dec!(-95.0)); // -95 + 0 = -95 (lost the 100)
        assert!(accounts[0].locked);
    }

    #[tokio::test]
    async fn test_dispute_already_under_dispute_rejected() {
        let engine = PaymentsEngine::new();

        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;

        // First dispute succeeds
        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].held, dec!(100.0));

        // Second dispute should be rejected (already under dispute)
        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        // Should be unchanged
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(0.0));
        assert_eq!(accounts[0].held, dec!(100.0));
        assert_eq!(accounts[0].total, dec!(100.0));
    }

    #[tokio::test]
    async fn test_deposit_overflow_rejected() {
        let engine = PaymentsEngine::new();

        // Deposit close to max
        let near_max = Decimal::MAX - dec!(10.0);
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: near_max,
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, near_max);
        assert_eq!(accounts[0].total, near_max);

        // Try to deposit more (should overflow and be rejected)
        engine.process(Deposit {
            client: 1,
            tx: 2,
            amount: dec!(20.0),
        }).await;

        // Account should be unchanged (overflow rejected)
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, near_max);
        assert_eq!(accounts[0].total, near_max);

        // Transaction should not be stored
        let tx_history = engine.tx_history.lock().await;
        assert!(!tx_history.contains_key(&2));
    }

    #[tokio::test]
    async fn test_withdrawal_no_underflow() {
        let engine = PaymentsEngine::new();

        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;

        // Valid withdrawal
        engine.process(Trx::Withdrawal {
            client: 1,
            tx: 2,
            amount: dec!(50.0),
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(50.0));
        assert_eq!(accounts[0].total, dec!(50.0));
    }

    #[tokio::test]
    async fn test_dispute_overflow_on_held_rejected() {
        let engine = PaymentsEngine::new();

        // Create account with near-max held
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;
        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        // Manually set held to near max (for testing - wouldn't happen in production)
        {
            let mut account = engine.user_account_map.get_mut(&1).unwrap();
            account.held = Decimal::MAX - dec!(10.0);
        }

        // Deposit and dispute to overflow held
        engine.process(Deposit {
            client: 1,
            tx: 2,
            amount: dec!(100.0),
        }).await;

        // This dispute should be rejected due to held overflow
        engine.process(Trx::Dispute {
            client: 1,
            tx: 2,
        }).await;

        // Check transaction 2 is still Normal (dispute rejected)
        let tx_history = engine.tx_history.lock().await;
        assert_eq!(tx_history[&2].status, TrxStatus::Normal);
    }

    #[tokio::test]
    async fn test_resolve_overflow_on_available_rejected() {
        let engine = PaymentsEngine::new();

        // Create disputed transaction
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;
        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        // Manually set available to near max (for testing)
        {
            let mut account = engine.user_account_map.get_mut(&1).unwrap();
            account.available = Decimal::MAX - dec!(10.0);
        }

        // Resolve should be rejected due to available overflow
        engine.process(Trx::Resolve {
            client: 1,
            tx: 1,
        }).await;

        // Transaction should still be under dispute (resolve rejected)
        let tx_history = engine.tx_history.lock().await;
        assert_eq!(tx_history[&1].status, TrxStatus::UnderDispute);

        // Held should still have the amount
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].held, dec!(100.0));
    }

    #[tokio::test]
    async fn test_duplicate_deposit_rejected() {
        let engine = PaymentsEngine::new();

        // First deposit succeeds
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(100.0));
        assert_eq!(accounts[0].total, dec!(100.0));

        // Second deposit with same tx ID should be rejected
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(50.0),
        }).await;

        // Account should be unchanged (only first deposit applied)
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(100.0));
        assert_eq!(accounts[0].total, dec!(100.0));

        // Transaction history should only have first deposit
        let tx_history = engine.tx_history.lock().await;
        assert_eq!(tx_history[&1].amount, dec!(100.0));
        assert_eq!(tx_history[&1].client, 1);
    }

    #[tokio::test]
    async fn test_duplicate_withdrawal_rejected() {
        let engine = PaymentsEngine::new();

        // Setup with initial deposit
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;

        // First withdrawal succeeds
        engine.process(Trx::Withdrawal {
            client: 1,
            tx: 2,
            amount: dec!(30.0),
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(70.0));

        // Second withdrawal with same tx ID should be rejected
        engine.process(Trx::Withdrawal {
            client: 1,
            tx: 2,
            amount: dec!(20.0),
        }).await;

        // Account should be unchanged
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(70.0));
        assert_eq!(accounts[0].total, dec!(70.0));

        // Transaction history should only have first withdrawal
        let tx_history = engine.tx_history.lock().await;
        assert_eq!(tx_history[&2].amount, dec!(30.0));
    }

    #[tokio::test]
    async fn test_duplicate_different_clients_rejected() {
        let engine = PaymentsEngine::new();

        // Client 1 deposits with tx=1
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;

        // Client 2 tries to deposit with same tx=1 (should be rejected)
        engine.process(Deposit {
            client: 2,
            tx: 1,
            amount: dec!(50.0),
        }).await;

        let accounts = engine.get_accounts();

        // Client 1 should have their deposit
        let client1 = accounts.iter().find(|a| a.client == 1).unwrap();
        assert_eq!(client1.available, dec!(100.0));

        // Client 2 should have nothing (their deposit was rejected)
        assert!(accounts.iter().find(|a| a.client == 2).is_none());

        // Transaction history should only have client 1's transaction
        let tx_history = engine.tx_history.lock().await;
        assert_eq!(tx_history[&1].client, 1);
        assert_eq!(tx_history[&1].amount, dec!(100.0));
    }

    #[tokio::test]
    async fn test_duplicate_mixed_types_rejected() {
        let engine = PaymentsEngine::new();

        // Deposit with tx=1
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;

        // Try withdrawal with same tx=1 (should be rejected)
        engine.process(Trx::Withdrawal {
            client: 1,
            tx: 1,
            amount: dec!(50.0),
        }).await;

        let accounts = engine.get_accounts();
        // Only deposit should have been applied
        assert_eq!(accounts[0].available, dec!(100.0));
        assert_eq!(accounts[0].total, dec!(100.0));

        // Transaction history should only have deposit
        let tx_history = engine.tx_history.lock().await;
        assert_eq!(tx_history.len(), 1);
        assert_eq!(tx_history[&1].amount, dec!(100.0));
    }

    #[tokio::test]
    async fn test_duplicate_after_dispute_rejected() {
        let engine = PaymentsEngine::new();

        // Deposit and dispute it
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;
        engine.process(Trx::Dispute {
            client: 1,
            tx: 1,
        }).await;

        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(0.0));
        assert_eq!(accounts[0].held, dec!(100.0));

        // Try to deposit with same tx=1 (should be rejected even though under dispute)
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(50.0),
        }).await;

        // Account should be unchanged
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(0.0));
        assert_eq!(accounts[0].held, dec!(100.0));
        assert_eq!(accounts[0].total, dec!(100.0));

        // Transaction should still be under dispute with original amount
        let tx_history = engine.tx_history.lock().await;
        assert_eq!(tx_history[&1].status, TrxStatus::UnderDispute);
        assert_eq!(tx_history[&1].amount, dec!(100.0));
    }

    // ============================================
    // CONCURRENCY TESTS
    // ============================================

    #[tokio::test]
    async fn test_concurrent_duplicate_deposits_no_race() {
        // Test that concurrent deposits with same tx ID are properly rejected
        // This tests the fix for the duplicate transaction race condition
        use std::sync::Arc;
        let engine = Arc::new(PaymentsEngine::new());

        // Spawn 100 concurrent tasks trying to deposit with same tx ID
        let mut handles = vec![];
        for _ in 0..100 {
            let engine_clone = engine.clone();
            let handle = tokio::spawn(async move {
                engine_clone.process(Deposit {
                    client: 1,
                    tx: 1,
                    amount: dec!(100.0),
                }).await;
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify only ONE deposit succeeded (no race condition)
        let accounts = engine.get_accounts();
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].available, dec!(100.0), "Account should have exactly 100.0, not more (no duplicate processing)");
        assert_eq!(accounts[0].total, dec!(100.0));

        // Verify only one transaction stored
        let tx_history = engine.tx_history.lock().await;
        assert_eq!(tx_history.len(), 1);
        assert_eq!(tx_history[&1].amount, dec!(100.0));
    }

    #[tokio::test]
    async fn test_concurrent_different_tx_ids_all_succeed() {
        // Test that concurrent deposits with different tx IDs all succeed
        use std::sync::Arc;
        let engine = Arc::new(PaymentsEngine::new());

        // Spawn 100 concurrent deposits with different tx IDs
        let mut handles = vec![];
        for i in 1u32..=100 {
            let engine_clone = engine.clone();
            let handle = tokio::spawn(async move {
                engine_clone.process(Deposit {
                    client: 1,
                    tx: i,
                    amount: dec!(10.0),
                }).await;
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all deposits succeeded
        let accounts = engine.get_accounts();
        assert_eq!(accounts[0].available, dec!(1000.0)); // 100 * 10.0
        assert_eq!(accounts[0].total, dec!(1000.0));

        let tx_history = engine.tx_history.lock().await;
        assert_eq!(tx_history.len(), 100);
    }

    #[tokio::test]
    async fn test_concurrent_deposit_and_dispute_no_deadlock() {
        // Test that concurrent deposits and disputes don't deadlock
        // This tests the fix for inconsistent lock ordering
        use std::sync::Arc;
        let engine = Arc::new(PaymentsEngine::new());

        // First deposit
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(100.0),
        }).await;

        // Spawn concurrent tasks: deposits and disputes
        let mut handles = vec![];

        // Spawn deposits (lock tx_history → account)
        for i in 2u32..=50 {
            let engine_clone = engine.clone();
            let handle = tokio::spawn(async move {
                engine_clone.process(Deposit {
                    client: 1,
                    tx: i,
                    amount: dec!(10.0),
                }).await;
            });
            handles.push(handle);
        }

        // Spawn disputes (lock tx_history → account)
        for i in 1u32..=25 {
            let engine_clone = engine.clone();
            let handle = tokio::spawn(async move {
                engine_clone.process(Trx::Dispute {
                    client: 1,
                    tx: i,
                }).await;
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete (should not deadlock)
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify engine is in a valid state
        let accounts = engine.get_accounts();
        assert!(accounts[0].verify_totals(), "Account totals should be consistent");
    }

    #[tokio::test]
    async fn test_concurrent_withdrawal_dispute_no_race() {
        // Test concurrent withdrawals and disputes maintain consistency
        use std::sync::Arc;
        let engine = Arc::new(PaymentsEngine::new());

        // Setup: deposit funds
        engine.process(Deposit {
            client: 1,
            tx: 1,
            amount: dec!(1000.0),
        }).await;

        // Spawn concurrent withdrawals
        let mut handles = vec![];
        for i in 2u32..=51 {
            let engine_clone = engine.clone();
            let handle = tokio::spawn(async move {
                engine_clone.process(Trx::Withdrawal {
                    client: 1,
                    tx: i,
                    amount: dec!(10.0),
                }).await;
            });
            handles.push(handle);
        }

        // Spawn concurrent disputes of the deposit
        for _ in 0..10 {
            let engine_clone = engine.clone();
            let handle = tokio::spawn(async move {
                engine_clone.process(Trx::Dispute {
                    client: 1,
                    tx: 1,
                }).await;
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify consistency
        let accounts = engine.get_accounts();
        assert!(accounts[0].verify_totals());

        // Check that dispute happened only once
        let tx_history = engine.tx_history.lock().await;
        let disputed = tx_history.values()
            .filter(|t| t.status == TrxStatus::UnderDispute)
            .count();
        assert!(disputed <= 1, "Should have at most 1 disputed transaction");
    }

    #[tokio::test]
    async fn test_concurrent_mixed_operations_consistency() {
        // Stress test: many concurrent operations maintain consistency
        use std::sync::Arc;
        let engine = Arc::new(PaymentsEngine::new());

        let mut handles = vec![];

        // Concurrent deposits
        for i in 1u32..=100 {
            let engine_clone = engine.clone();
            let handle = tokio::spawn(async move {
                engine_clone.process(Deposit {
                    client: ((i - 1) % 5 + 1) as u16, // 5 clients
                    tx: i,
                    amount: dec!(100.0),
                }).await;
            });
            handles.push(handle);
        }

        // Concurrent withdrawals
        for i in 101u32..=150 {
            let engine_clone = engine.clone();
            let client_id = ((i - 101) % 5 + 1) as u16;
            let handle = tokio::spawn(async move {
                engine_clone.process(Trx::Withdrawal {
                    client: client_id,
                    tx: i,
                    amount: dec!(25.0),
                }).await;
            });
            handles.push(handle);
        }

        // Concurrent disputes
        for i in 1u32..=20 {
            let engine_clone = engine.clone();
            let handle = tokio::spawn(async move {
                engine_clone.process(Trx::Dispute {
                    client: ((i - 1) % 5 + 1) as u16,
                    tx: i,
                }).await;
            });
            handles.push(handle);
        }

        // Wait for all
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all accounts maintain invariants
        let accounts = engine.get_accounts();
        for account in &accounts {
            assert!(account.verify_totals(),
                "Account {} should maintain total = available + held",
                account.client);
        }

        // Verify no data corruption
        let tx_history = engine.tx_history.lock().await;
        assert!(tx_history.len() <= 150, "Should have at most 150 unique transactions");
    }

    #[tokio::test]
    async fn test_concurrent_duplicate_different_amounts_rejected() {
        // Test that duplicate tx with different amounts is properly rejected
        use std::sync::Arc;
        let engine = Arc::new(PaymentsEngine::new());

        let mut handles = vec![];

        // Spawn concurrent deposits with same tx but different amounts
        for i in 0..50 {
            let engine_clone = engine.clone();
            let amount = dec!(100.0) + Decimal::from(i);
            let handle = tokio::spawn(async move {
                engine_clone.process(Deposit {
                    client: 1,
                    tx: 1,
                    amount,
                }).await;
            });
            handles.push(handle);
        }

        // Wait for all
        for handle in handles {
            handle.await.unwrap();
        }

        // Should have exactly one deposit (first one wins)
        let accounts = engine.get_accounts();
        let tx_history = engine.tx_history.lock().await;

        assert_eq!(tx_history.len(), 1);
        assert_eq!(accounts[0].total, tx_history[&1].amount,
            "Account total should match the single stored transaction amount");
    }
}

impl Default for PaymentsEngine {
    fn default() -> Self {
        PaymentsEngine {
            user_account_map: DashMap::new(),
            tx_history: Mutex::new(IndexMap::new()),
            max_tx_history: None,
        }
    }
}
