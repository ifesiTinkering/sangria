# Cascading Abort Implementation Proposal for Pipelined 2PC

## Overview

This proposal outlines how to implement cascading aborts in Sangria's pipelined 2PC system by leveraging the existing resolver dependency graph and transaction state management infrastructure. When an ancestor transaction in the dependency graph fails, all dependent (child) transactions must be aborted and their database changes rolled back.

## Current State Analysis

### Existing Infrastructure for Cascading Aborts

#### 1. Resolver Dependency Graph (`resolver/src/core/resolver.rs`)

**Current Structure:**
```rust
// resolver/src/core/resolver.rs:16-35
pub struct TransactionInfo {
    pub id: Uuid,
    pub num_dependencies: u32,          // Count of pending dependencies
    pub dependents: HashSet<Uuid>,      // Transactions waiting for this one
    pub participant_ranges_info: Vec<ParticipantRangeInfo>,
    pub fake: bool,
}

// resolver/src/core/resolver.rs:37-41
pub struct State {
    info_per_transaction: HashMap<Uuid, TransactionInfo>,
    resolved_transactions: HashSet<Uuid>,  // Successfully committed transactions
}
```

**Key Insight:** The `dependents` field in `TransactionInfo` already maintains the exact dependency tree needed for cascading aborts!

#### 2. Transaction Pending Operations (`coordinator/src/transaction.rs`)

**Current Structure:**
```rust
// coordinator/src/transaction.rs:33-38
struct ParticipantRange {
    readset: HashSet<Bytes>,           // Keys read by transaction
    writeset: HashMap<Bytes, Bytes>,   // Key-value pairs to write
    deleteset: HashSet<Bytes>,         // Keys to delete
    leader_sequence_number: u64,       // For leader change detection
}

// coordinator/src/transaction.rs:40-54
pub struct Transaction {
    id: Uuid,
    transaction_info: Arc<TransactionInfo>,
    state: State,                      // Running/Preparing/Aborted/Committed
    participant_ranges: HashMap<FullRangeId, ParticipantRange>,  // PENDING OPERATIONS
    dependencies: HashSet<Uuid>,
    // ... other fields
}
```

**Key Insight:** The `participant_ranges` field contains all pending read/write/delete operations that need to be rolled back on abort!

#### 3. Existing Abort Infrastructure (`coordinator/src/transaction.rs`)

**Current Implementation:**
```rust
// coordinator/src/transaction.rs:170-203
async fn record_abort(&mut self) -> Result<(), Error> {
    self.state = State::Aborted;

    // Parallel abort notifications to all participant ranges
    let mut abort_join_set = JoinSet::new();
    for range_id in self.participant_ranges.keys() {
        let range_id = *range_id;
        let range_client = self.range_client.clone();
        let transaction_info = self.transaction_info.clone();
        abort_join_set.spawn_on(async move {
            range_client.abort_transaction(transaction_info, &range_id).await
        }, &self.runtime);
    }

    // Record abort in transaction state store
    let outcome = self.tx_state_store.try_abort_transaction(self.id).await.unwrap();
    match outcome {
        OpResult::TransactionIsAborted => (),
        OpResult::TransactionIsCommitted(_) => {
            panic!("transaction committed without coordinator consent!")
        }
    }
    while abort_join_set.join_next().await.is_some() {}
    Ok(())
}
```

**Key Insight:** Abort infrastructure exists but is only triggered locally per transaction!

## Proposed Implementation

### 1. New Aborted Transaction Tracking in Resolver

**File:** `resolver/src/core/resolver.rs`

**Add to State structure:**
```rust
// resolver/src/core/resolver.rs:37-41 (MODIFY)
#[derive(Default)]
pub struct State {
    info_per_transaction: HashMap<Uuid, TransactionInfo>,
    resolved_transactions: HashSet<Uuid>,
    aborted_transactions: HashSet<Uuid>,  // NEW: Track aborted transactions
}
```

**Add new method - Register Aborted Transactions:**
```rust
// resolver/src/core/resolver.rs (ADD NEW METHOD after line 267)
pub async fn register_aborted_transactions(
    resolver: Arc<Self>,
    aborted_transaction_ids: Vec<Uuid>,
) -> Result<Vec<Uuid>, Error> {
    let mut cascaded_aborts = Vec::new();

    {
        let mut state = resolver.state.write().await;
        let mut aborts_to_propagate = aborted_transaction_ids.clone();

        // Iteratively find all transactions that need to be aborted due to cascading
        while !aborts_to_propagate.is_empty() {
            let abort_tx_id = aborts_to_propagate.pop().unwrap();

            // Mark transaction as aborted
            state.aborted_transactions.insert(abort_tx_id);

            // Find all dependents that need to be cascaded
            if let Some(transaction_info) = state.info_per_transaction.get_mut(&abort_tx_id) {
                let dependents = mem::take(&mut transaction_info.dependents);

                for dependent_id in dependents {
                    // Check if dependent is not already resolved or aborted
                    if !state.resolved_transactions.contains(&dependent_id)
                        && !state.aborted_transactions.contains(&dependent_id) {

                        // Add to cascade abort list
                        aborts_to_propagate.push(dependent_id);
                        cascaded_aborts.push(dependent_id);

                        info!("Cascading abort from {} to dependent {}", abort_tx_id, dependent_id);
                    }
                }
            }

            // Remove from waiting transactions
            if let Some(sender) = resolver.waiting_transactions.write().await.remove(&abort_tx_id) {
                // Send abort signal instead of commit signal
                let _ = sender.send(());  // This will cause the waiting transaction to check state
            }
        }
    }

    info!("Cascading abort affected {} transactions: {:?}", cascaded_aborts.len(), cascaded_aborts);
    Ok(cascaded_aborts)
}
```

### 2. Enhanced Commit Method with Abort Detection

**File:** `resolver/src/core/resolver.rs`

**Modify existing commit method:**
```rust
// resolver/src/core/resolver.rs:65-147 (MODIFY existing method)
pub async fn commit(
    resolver: Arc<Self>,
    transaction_id: Uuid,
    dependencies: HashSet<Uuid>,
    participant_ranges_info: Vec<ParticipantRangeInfo>,
    fake: bool,
) -> Result<(), Error> {
    // Read-only optimization (unchanged)
    if participant_ranges_info.iter().all(|info| !info.has_writes) {
        return Ok(());
    }

    let (s, r) = oneshot::channel();
    let mut num_pending_dependencies = 0;

    // Acquire the write lock and update dependencies (unchanged)
    {
        let mut state = resolver.state.write().await;

        // NEW: Check if any dependencies are already aborted
        for dependency in &dependencies {
            if state.aborted_transactions.contains(dependency) {
                info!("Transaction {} depends on aborted transaction {}, aborting immediately",
                      transaction_id, dependency);
                return Err(Error::TransactionAborted(TransactionAbortReason::DependencyAborted));
            }
        }

        // Rest of dependency handling (unchanged)...
        for dependency in dependencies {
            if !state.resolved_transactions.contains(&dependency) {
                num_pending_dependencies += 1;
                state.info_per_transaction
                    .entry(dependency)
                    .or_insert(TransactionInfo::default(dependency, fake))
                    .dependents
                    .insert(transaction_id);
            }
        }
        // ... rest unchanged
    }

    // Block until transaction is committed or aborted
    r.await.unwrap();

    // NEW: Check if transaction was aborted while waiting
    {
        let state = resolver.state.read().await;
        if state.aborted_transactions.contains(&transaction_id) {
            info!("Transaction {} was aborted due to cascading abort", transaction_id);
            return Err(Error::TransactionAborted(TransactionAbortReason::CascadingAbort));
        }
    }

    info!("Transaction {} finally committed!", transaction_id);
    Ok(())
}
```

### 3. Group Commit Error Handling Enhancement

**File:** `resolver/src/core/group_commit.rs`

**Modify the commit method to handle transaction state store failures:**
```rust
// resolver/src/core/group_commit.rs:279-293 (MODIFY existing error handling)
// Replace the panic with proper error handling
if let Err(e) = tx_state_store_clone
    .try_batch_commit_transactions(&tx_ids_vec, 0)
    .await
{
    // NEW: Handle batch commit failures by registering aborts
    error!("Batch commit failed for range {:?}: {:?}", participant_range_clone, e);

    // Register all transactions in this batch as aborted
    let resolver_clone = resolver.clone();  // Need resolver reference here
    let failed_tx_ids = tx_ids_vec.clone();
    tokio::spawn(async move {
        let _ = Resolver::register_aborted_transactions(resolver_clone, failed_tx_ids).await;
    });

    return Err(Error::GroupCommitFailed);  // NEW error type needed
}
```

### 4. Coordinator Abort Integration

**File:** `coordinator/src/transaction.rs`

**Enhance record_abort to notify resolver:**
```rust
// coordinator/src/transaction.rs:170-203 (MODIFY existing method)
async fn record_abort(&mut self) -> Result<(), Error> {
    self.state = State::Aborted;

    // NEW: Notify resolver of abort before cleaning up ranges
    if let Err(e) = self.resolver.register_aborted_transactions(vec![self.id]).await {
        error!("Failed to register abort with resolver: {:?}", e);
    }

    // Existing abort logic (unchanged)
    let mut abort_join_set = JoinSet::new();
    for range_id in self.participant_ranges.keys() {
        let range_id = *range_id;
        let range_client = self.range_client.clone();
        let transaction_info = self.transaction_info.clone();
        abort_join_set.spawn_on(async move {
            range_client.abort_transaction(transaction_info, &range_id).await
        }, &self.runtime);
    }

    let outcome = self.tx_state_store.try_abort_transaction(self.id).await.unwrap();
    match outcome {
        OpResult::TransactionIsAborted => (),
        OpResult::TransactionIsCommitted(_) => {
            panic!("transaction committed without coordinator consent!")
        }
    }
    while abort_join_set.join_next().await.is_some() {}
    Ok(())
}
```

### 5. New Error Types

**File:** `coordinator_rangeclient/src/error.rs`

**Add new error variants:**
```rust
// Add to existing TransactionAbortReason enum
pub enum TransactionAbortReason {
    // ... existing variants
    DependencyAborted,     // NEW: A dependency was aborted
    CascadingAbort,        // NEW: Aborted due to cascading from ancestor
}

// Add to existing Error enum
pub enum Error {
    // ... existing variants
    GroupCommitFailed,     // NEW: Group commit to state store failed
}
```

### 6. Resolver Client Interface Enhancement

**File:** `resolver/src/resolver_client.rs`

**Add new method to trait:**
```rust
// resolver/src/resolver_client.rs (ADD to ResolverClient trait)
#[async_trait]
pub trait ResolverClient: Send + Sync {
    // ... existing methods

    // NEW: Register aborted transactions
    async fn register_aborted_transactions(
        &self,
        transaction_ids: Vec<Uuid>,
    ) -> Result<Vec<Uuid>, Error>;
}
```

**Implementation in local client:**
```rust
// resolver/src/local/client.rs (ADD implementation)
async fn register_aborted_transactions(
    &self,
    transaction_ids: Vec<Uuid>,
) -> Result<Vec<Uuid>, Error> {
    Resolver::register_aborted_transactions(self.resolver.clone(), transaction_ids).await
}
```

## Implementation Workflow

### Phase 1: Basic Cascading Infrastructure
1. **Add aborted transaction tracking** to resolver state
2. **Implement register_aborted_transactions** method
3. **Add new error types** for abort reasons
4. **Enhance resolver client interface** with abort registration

### Phase 2: Integration with Existing Abort Paths
1. **Modify coordinator record_abort** to notify resolver
2. **Update commit method** to check for aborted dependencies
3. **Add abort detection** in waiting transaction logic

### Phase 3: Group Commit Error Handling
1. **Replace panics with proper error handling** in group commit
2. **Integrate abort registration** on batch commit failures
3. **Add rollback coordination** for partially committed groups

### Phase 4: Testing and Validation
1. **Unit tests** for cascading abort logic
2. **Integration tests** with simulated failures
3. **Performance testing** to ensure abort overhead is minimal

## Key Benefits of This Approach

### 1. Leverages Existing Infrastructure
- **Dependency Graph**: Uses existing `dependents` relationships
- **Abort Mechanism**: Extends current `record_abort` functionality
- **State Management**: Builds on existing resolver state tracking

### 2. Maintains Pipelined 2PC Performance
- **Parallel Operations**: Cascading aborts can be processed in parallel
- **Minimal Overhead**: Only adds abort checking to existing commit paths
- **Group Processing**: Can abort multiple transactions in batches

### 3. Ensures Data Consistency
- **Transaction State Store**: Leverages existing atomic commit/abort decisions
- **Range Coordination**: Uses existing range client abort notifications
- **Dependency Ordering**: Maintains serializability through dependency tracking

### 4. Graceful Error Propagation
- **Structured Error Types**: Clear error reasons for different abort scenarios
- **Async Notification**: Non-blocking abort propagation
- **Resource Cleanup**: Proper cleanup of waiting transactions and state

## Conclusion

This implementation leverages Sangria's existing dependency graph in the resolver and transaction state management in the coordinator to implement cascading aborts efficiently. The proposal maintains the performance benefits of pipelined 2PC while ensuring that failed ancestor transactions properly abort their dependent children and roll back all affected database keys.