# Payments Engine

Payments processing engine handles deposits, withdrawals, disputes, resolves, and chargebacks.

## Building

### CLI Mode (Default)

```bash
cargo build --release
```

### Server Mode (TCP)

```bash
cargo build --release --bin payments_server
```

## Running

### CLI Mode - Process Local CSV File

```bash
cargo run --bin payments_engine ./csv/transactions.csv > ./csv/accounts.csv
```
### Server Mode - Handle Concurrent TCP Streams

```bash
cargo run --bin payments_server

cargo run --bin payments_server 127.0.0.1:9000
```

**Server Features:**
- Handles 10,000+ concurrent TCP connections
- Bounded memory with FIFO transaction eviction (10M limit)


## Testing

Run all tests:
```bash
cargo test
```

Run specific test suites:
```bash
cargo test --test integration_tests
cargo test --test performance_tests
```

Test server mode:
```bash
./test_server.sh          # Single connection test
./test_concurrent_verbose.sh  # 100 concurrent + detailed analysis
```

## Input Format

The input CSV must have the following columns: `type`, `client`, `tx`, `amount`

Supported transaction types:
- **deposit** - Credits the client account (requires amount)
- **withdrawal** - Debits the client account (requires amount)
- **dispute** - Holds funds from a previous transaction
- **resolve** - Releases held funds back to available
- **chargeback** - Reverses a transaction and locks the account

Example:
```csv
type,client,tx,amount
deposit,1,1,10.0
withdrawal,1,2,5.0
dispute,1,1,
resolve,1,1,
```

## Output Format

The output CSV contains: `client`, `available`, `held`, `total`, `locked`

- **client** - Client ID (u16)
- **available** - Available funds for withdrawal (Decimal, 4 places)
- **held** - Funds held due to disputes (Decimal, 4 places)
- **total** - Total funds (available + held, Decimal, 4 places)
- **locked** - Whether account is locked due to chargeback (boolean)

Example:
```csv
client,available,held,total,locked
1,5.0000,0.0000,5.0000,false
```

## Assumptions

**1. Negative Balance Allowed** - Disputes can create negative `available` balance:

- **Disputing a Deposit (after withdrawal)**:
  - Example: Client deposits $100, withdraws $80, then disputes the deposit
  - Result: `available = -80`, `held = 100`, `total = 20`
  - Reason: Client legitimately owes merchant $80 after disputing the original deposit

- **Disputing a Withdrawal**:
  - Example: Client deposits $50, withdraws $40, then disputes the withdrawal
  - Result: `available = -30`, `held = 40`, `total = 10`
  - Reason: Withdrawal is under investigation (potentially fraudulent), funds must be held

**2. Global Transaction ID Uniqueness**:

- Transaction IDs are **globally unique** across all users (system-wide)
- tx=123 can only exist once, regardless of which user created it
- Required for audit compliance and duplicate detection
- **Implication**: Transaction history uses global lock (concurrency bottleneck)
- **Production improvement**: Sharding tx_history to reduce lock contention

**3. FIFO Eviction Strategy**:

- Oldest inserted transactions are evicted first (insertion order, not access order)
- Simpler than true LRU (no access tracking overhead)
- **Production improvement**: LRU cache with access pattern tracking

## Design Decisions


### Performance & Streaming

The engine successfully processes 100,000+ transactions in under 1 second while maintaining reasonable memory footprint.

#### Sync CSV Parsing Trade-off

- **Current approach**: Uses synchronous CSV parsing (`csv` crate) even in async context
- **Trade-off**: For very large files (multi-GB), synchronous CSV reading can block Tokio worker thread
- **Rationale**: Simplicity over complexity - typical CSV files are small enough that blocking is negligible


#### FIFO Eviction & Cache Strategy

The transaction history acts as an **in-memory cache** with FIFO (First-In-First-Out) eviction:

- When history reaches `max_tx_history` limit, oldest transactions are automatically evicted
- Recent transactions remain available for fast dispute processing
- Disputes on evicted transactions are silently ignored (cache miss, not for production)
- **Production improvement**: Use true LRU cache instead of FIFO


#### Concurrency Bottleneck & Scalability

**Current Architecture:**

**The Problem:**
- Every deposit/withdrawal/dispute acquires the **same global lock** on `tx_history`
- Client 1 depositing blocks Client 2 withdrawing (even for different users)
- Severe bottleneck in server mode with 10,000+ concurrent connections

**Business Logic Constraint:**
- Transaction IDs are **globally unique** across all users (not per-user)
- Required for duplicate detection and audit compliance


**Production Database Integration**:

In a real production system, this cache would be backed by a database:

1. **Cache Layer** - Keep recent N transactions in memory
2. **Database Fallback** - When disputing a transaction not in cache:
   - Query transaction from database
   - Verify transaction exists and belongs to client
   - Process dispute

