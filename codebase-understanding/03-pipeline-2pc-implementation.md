# Pipeline 2PC Implementation

## Overview

Sangria implements an advanced pipelined two-phase commit protocol that optimizes transaction throughput through batching, dependency-based ordering, and adaptive commit strategies. The pipeline implementation centers around the **Group Commit** mechanism and intelligent commit strategy selection.

## Group Commit Architecture (`resolver/src/core/group_commit.rs`)

The Group Commit system is the core of the pipeline 2PC implementation, providing batching optimization for better throughput.

### Key Data Structures

#### Group Commit State
```rust
struct State {
    // Transactions ready to commit grouped by participant range
    group_per_participant: HashMap<FullRangeId, Arc<RwLock<Vec<TransactionInfo>>>>,
}

pub struct GroupCommit {
    state: Arc<RwLock<State>>,
    // Tracks pending commits per transaction across multiple ranges
    num_pending_participant_commits_per_transaction: Arc<RwLock<HashMap<Uuid, u32>>>,
    non_empty_groups: Arc<RwLock<HashSet<FullRangeId>>>,
    range_client: Arc<RangeClient>,
    tx_state_store: Arc<TxStateStoreClient>,
    stats: Arc<RwLock<GroupCommitStats>>,
    returned_transactions: Arc<RwLock<Vec<TransactionInfo>>>,
}
```

### Pipeline Batching Mechanism

#### Transaction Grouping (`add_transactions`, lines 152-225)

**Phase 1: Group by Participant Range**
```rust
// Group transactions by participant range
let mut tmp_group_per_participant = HashMap::new();
let mut tmp_num_pending_commits = HashMap::new();
for transaction in transactions {
    let mut num_pending_commits = 0;
    for participant_range in transaction.participant_ranges_info.iter() {
        // Only include ranges with writes - read-only participants don't need commits
        if participant_range.has_writes {
            num_pending_commits += 1;
            tmp_group_per_participant
                .entry(participant_range.participant_range)
                .or_insert_with(|| Vec::new())
                .push(transaction.clone());
        }
    }
    tmp_num_pending_commits.insert(transaction.id, num_pending_commits);
}
```

**Key Optimizations:**
- **Write-Only Grouping**: Only ranges with writes participate in commit groups
- **Read-Only Optimization**: Read-only ranges are excluded from commit process
- **Parallel Lock Acquisition**: Uses JoinSet to acquire locks for all groups concurrently

**Phase 2: Parallel Group Updates**
```rust
// Spawning async tasks so that we try to acquire the locks for all groups in parallel
for (participant_range, transactions) in tmp_group_per_participant.iter() {
    join_set.spawn(async move {
        let state = state_clone.read().await;
        let mut group = state.group_per_participant.get(&participant_range);
        match group {
            Some(group) => {
                let mut group = group.write().await;
                group.extend(transactions.iter().cloned());
            }
            // ... error handling
        }
    });
}
```

#### Parallel Group Commit (`commit`, lines 227-359)

**Phase 1: Parallel Range Commits**
```rust
// For each non-empty group (participant range)
for participant_range in non_empty_groups.iter() {
    commit_join_set.spawn(async move {
        // 1. Acquire write lock and extract all transactions
        let mut group_clone = group_guard_clone.write().await;
        let transactions = std::mem::take(&mut *group_clone);

        // 2. Clear the group from non-empty tracking
        non_empty_groups_clone.write().await.remove(&participant_range_clone);

        // 3. Batch commit to transaction state store
        if !fake {
            tx_state_store_clone
                .try_batch_commit_transactions(&tx_ids_vec, 0)
                .await?;

            // 4. Notify participant range to apply changes
            range_client
                .commit_transactions(tx_ids_vec, &participant_range_clone, 0)
                .await?;
        }

        // 5. Track committed transactions for completion detection
        returned_transactions_clone.write().await.extend(transactions);
    });
}
```

**Phase 2: Transaction Completion Detection**
```rust
// Determine which transactions have fully committed across all participant ranges
for transaction in returned_transactions {
    *num_pending_participant_commits_per_transaction
        .get_mut(&transaction.id)
        .unwrap() -= 1;

    if *num_pending_participant_commits_per_transaction
        .get_mut(&transaction.id)
        .unwrap() == 0
    {
        // Transaction has committed in all its participant ranges
        num_pending_participant_commits_per_transaction.remove(&transaction.id);
        finished_transactions.push(transaction);
    }
}
```

## Commit Strategy Implementation (`coordinator/src/transaction.rs`)

### Strategy Selection Logic

The pipeline 2PC uses three commit strategies with adaptive selection:

#### 1. Traditional Strategy (lines 291-334)
```rust
CommitStrategy::Traditional => {
    // Standard 2PC: State store first, then participants
    match self.tx_state_store.try_commit_transaction(self.id, epoch).await? {
        OpResult::TransactionIsCommitted(i) => {
            self.state = State::Committed;
            // Notify all participants to apply changes
            for (range_id, info) in self.participant_ranges.iter() {
                if has_writes {
                    range_client.commit_transactions(
                        vec![transaction_info.id],
                        &range_id,
                        epoch,
                    ).await;
                }
            }
        }
        OpResult::TransactionIsAborted => {
            return Err(Error::TransactionAborted(TransactionAbortReason::Other));
        }
    }
}
```

#### 2. Adaptive/Pipelined Strategy (lines 356-433)

**With Dependencies (Pipeline through Resolver):**
```rust
if !self.dependencies.is_empty() {
    // Use resolver for dependency-based ordering and group commit
    info!("Delegating commit to resolver for transaction {}", self.id);
    let participants_info = self.participant_ranges
        .iter()
        .map(|(range_id, info)| {
            ParticipantRangeInfo::new(*range_id, !info.writeset.is_empty())
        })
        .collect();

    // Resolver handles dependency ordering and group commit
    self.resolver
        .commit(self.id, self.dependencies.clone(), participants_info)
        .await?;
}
```

**Without Dependencies (Optimized Direct Path):**
```rust
else {
    // Direct commit optimization - bypass resolver for independent transactions
    info!("Committing transaction {:?} without Resolver", self.id);

    // 1. Direct commit to state store
    let _ = self.tx_state_store.try_commit_transaction(self.id, 0).await.unwrap();
    self.state = State::Committed;

    // 2. Parallel participant notification
    let mut commit_join_set = JoinSet::new();
    for (range_id, info) in self.participant_ranges.iter() {
        if has_writes {
            commit_join_set.spawn_on(async move {
                range_client.commit_transactions(
                    vec![transaction_info.id],
                    &range_id,
                    0,
                ).await
            }, &self.runtime);
        }
    }
    while commit_join_set.join_next().await.is_some() {}

    // 3. Late registration with resolver for early lock release coordination
    if any_early_lock_releases {
        let resolver = self.resolver.clone();
        let tx_id = self.id;
        self.runtime.spawn(async move {
            let _ = resolver.register_committed_transactions(vec![tx_id]).await;
        });
    }
}
```

## Pipeline Optimizations

### 1. Early Lock Release

**Range Server Optimization:**
- Ranges can release locks before receiving final commit confirmation
- Requires coordination with resolver for dependency tracking
- Improves concurrency but requires careful state management

**Implementation:**
```rust
// During prepare phase
if res.released_lock_early {
    any_early_lock_releases = true;
}

// After direct commit
if any_early_lock_releases {
    // Register with resolver asynchronously
    self.runtime.spawn(async move {
        let _ = resolver.register_committed_transactions(vec![tx_id]).await;
    });
}
```

### 2. Dependency-Based Routing

**Smart Commit Path Selection:**
- **No Dependencies**: Direct commit path bypassing resolver
- **With Dependencies**: Route through resolver for ordering
- **Adaptive**: Dynamic selection based on transaction characteristics

### 3. Batch Transaction State Store Operations

**Group Commit to State Store:**
```rust
// Batch commit multiple transactions to state store simultaneously
tx_state_store_clone
    .try_batch_commit_transactions(&tx_ids_vec, 0)
    .await?;
```

**Benefits:**
- Reduces state store round trips
- Improves throughput for transaction-heavy workloads
- Maintains atomicity within each participant range

### 4. Parallel Participant Range Operations

**Concurrent Range Notifications:**
```rust
// Parallel commits across all participant ranges
let mut commit_join_set = JoinSet::<Result<(), Error>>::new();
for participant_range in non_empty_groups.iter() {
    commit_join_set.spawn(async move {
        // Process each range concurrently
        // 1. Batch commit to state store
        // 2. Notify range to apply changes
        // 3. Update statistics
    });
}
```

## Concurrency and Lock Management

### Fine-Grained Locking Strategy

**Three-Level Lock Hierarchy:**
1. **Global State Lock**: Protects the main group_per_participant HashMap
2. **Per-Range Locks**: Each participant range has its own RwLock
3. **Non-Empty Groups Lock**: Tracks which ranges have pending transactions

**Lock Acquisition Patterns:**
```rust
// Pattern 1: Double-checked locking for new participant ranges
{
    let state = self.state.read().await;  // Read lock first
    if !state.group_per_participant.contains_key(&participant_range) {
        // Need to add new range
    }
}
if !participants_to_insert.is_empty() {
    let mut state = self.state.write().await;  // Write lock only if needed
    // Insert new ranges
}

// Pattern 2: Parallel group processing
for (participant_range, transactions) in groups {
    join_set.spawn(async move {
        let state = state_clone.read().await;
        let mut group = state.group_per_participant.get(&participant_range)?.write().await;
        // Process group while holding minimal locks
    });
}
```

### Transaction Completion Tracking

**Multi-Range Transaction Handling:**
```rust
// Track how many ranges each transaction needs to commit in
let mut num_pending_commits = 0;
for participant_range in transaction.participant_ranges_info.iter() {
    if participant_range.has_writes {
        num_pending_commits += 1;
    }
}
tmp_num_pending_commits.insert(transaction.id, num_pending_commits);

// On each range commit completion
*num_pending_participant_commits_per_transaction.get_mut(&transaction.id).unwrap() -= 1;
if *num_pending_participant_commits_per_transaction.get_mut(&transaction.id).unwrap() == 0 {
    // Transaction fully committed across all ranges
    finished_transactions.push(transaction);
}
```

## Performance Benefits

### Throughput Improvements
1. **Batching**: Multiple transactions committed together reduces per-transaction overhead
2. **Parallelism**: Concurrent operations across participant ranges
3. **Direct Path**: Independent transactions bypass dependency resolution
4. **Early Lock Release**: Improved concurrency through optimistic locking

### Latency Considerations
1. **Group Formation Delay**: Transactions may wait for batch formation
2. **Dependency Resolution**: Complex dependency chains increase latency
3. **Lock Contention**: Fine-grained locking reduces contention points

### Adaptive Behavior
- **Load-Aware**: Considers resolver load and system conditions
- **Dependency-Aware**: Routes transactions based on dependency characteristics
- **Write-Pattern Aware**: Optimizes based on read/write patterns