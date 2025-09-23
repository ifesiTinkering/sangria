# Resolver and 2PC Mechanisms

## Resolver Core Implementation (`resolver/src/core/resolver.rs`)

The resolver is responsible for managing transaction dependencies and ensuring serializable execution order through dependency tracking.

### Key Components

#### TransactionInfo Structure
```rust
pub struct TransactionInfo {
    pub id: Uuid,
    pub num_dependencies: u32,          // Number of pending dependencies
    pub dependents: HashSet<Uuid>,      // Transactions waiting for this one
    pub participant_ranges_info: Vec<ParticipantRangeInfo>,
    pub fake: bool,                     // For testing purposes
}
```

#### Resolver State Management
```rust
pub struct State {
    info_per_transaction: HashMap<Uuid, TransactionInfo>,
    resolved_transactions: HashSet<Uuid>,  // Committed transactions
}

pub struct Resolver {
    state: RwLock<State>,
    group_commit: GroupCommit,              // Batching optimization
    waiting_transactions: RwLock<HashMap<Uuid, oneshot::Sender<()>>>,
    bg_runtime: tokio::runtime::Handle,
    stats_tracker: RwLock<StatisticsTracker>,
}
```

### Dependency Resolution Flow

#### 1. Commit Request Processing (`commit` function, lines 65-147)
- **Read-only Optimization**: Skip dependency resolution for read-only transactions
- **Dependency Registration**: Register transaction dependencies in resolver state
- **Immediate Commit**: If no dependencies, commit immediately
- **Blocking**: Otherwise, block until dependencies are resolved

```rust
// Key dependency tracking logic
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
```

#### 2. Commit Triggering (`trigger_commit`, lines 149-179)
- **Group Commit**: Uses group commit mechanism for batching
- **Notification**: Notifies waiting transactions via channels
- **Cascading Resolution**: Triggers registration of newly committed transactions

#### 3. Dependency Chain Resolution (`register_committed_transactions`, lines 195-267)
- **Iterative Resolution**: Resolves dependency chains iteratively
- **Dependency Decrement**: Decrements pending dependency counts
- **Ready Detection**: Identifies newly ready-to-commit transactions
- **Recursive Triggering**: Spawns new commit operations for ready transactions

```rust
// Cascading dependency resolution
while !new_resolved_dependencies.is_empty() {
    let transaction_id = new_resolved_dependencies.pop().unwrap();
    // Process dependents and find newly ready transactions
    for dependent in dependents.iter() {
        dependent_transaction_info.num_dependencies -= 1;
        if dependent_transaction_info.num_dependencies == 0 {
            new_ready_to_commit.push(dependent_transaction_info.clone());
        }
    }
}
```

## 2PC Implementation in Coordinator (`coordinator/src/transaction.rs`)

### Transaction Structure
```rust
pub struct Transaction {
    id: Uuid,
    transaction_info: Arc<TransactionInfo>,
    state: State,                                      // Running/Preparing/Aborted/Committed
    participant_ranges: HashMap<FullRangeId, ParticipantRange>,
    dependencies: HashSet<Uuid>,
    range_client: Arc<RangeClient>,
    commit_strategy: CommitStrategy,                   // Traditional/Pipelined/Adaptive
    resolver: Arc<dyn ResolverClient>,
    // ... other fields
}
```

### Participant Range Management
```rust
struct ParticipantRange {
    readset: HashSet<Bytes>,           // Keys read by transaction
    writeset: HashMap<Bytes, Bytes>,   // Key-value pairs to write
    deleteset: HashSet<Bytes>,         // Keys to delete
    leader_sequence_number: u64,       // For leader change detection
}
```

### Transaction Operations

#### Read Operations (`get`, lines 103-150)
1. **Read-Your-Writes**: Check local writeset/deleteset first
2. **Range Server Query**: If not found locally, query range server
3. **Dependency Collection**: Gather dependencies from range server response
4. **Leadership Validation**: Ensure range leader hasn't changed
5. **Readset Update**: Add key to transaction's readset

#### Write Operations (`put`/`del`, lines 152-168)
- **Local Storage**: Store writes/deletes in local transaction state
- **Conflict Resolution**: Handle overlapping writes/deletes to same key

### 2PC Commit Process (`commit`, lines 220-436)

#### Phase 1: Prepare (lines 227-288)
```rust
// Parallel prepare requests to all participant ranges
for (range_id, info) in &self.participant_ranges {
    prepare_join_set.spawn_on(async move {
        range_client.prepare_transaction(
            transaction_info,
            &range_id,
            has_reads,
            &writes,
            &deletes,
            resolver_average_load,
            num_open_clients,
        ).await
    }, &self.runtime);
}
```

**Prepare Phase Operations:**
- Send parallel prepare requests to all participant ranges
- Collect additional dependencies from each range
- Handle early lock releases for optimization
- Abort on any prepare failure

#### Phase 2: Commit (lines 290-434)
The commit phase varies based on commit strategy:

##### Traditional Strategy (lines 291-334)
1. **Atomic Commit**: Use transaction state store for atomic commit decision
2. **Participant Notification**: Notify all participants to apply changes
3. **Lock Release**: Participants release locks and apply writes

##### Adaptive/Pipelined Strategy (lines 356-433)
**With Dependencies:**
- Delegate to resolver for dependency-based ordering
- Resolver handles commit timing and notification

**Without Dependencies (Optimized Path):**
1. **Direct Commit**: Bypass resolver, commit directly to state store
2. **Participant Notification**: Notify ranges immediately
3. **Late Registration**: Register with resolver for early lock release cases

```rust
if !self.dependencies.is_empty() {
    // Use resolver for dependency ordering
    self.resolver.commit(self.id, self.dependencies.clone(), participants_info).await?;
} else {
    // Direct commit optimization
    let _ = self.tx_state_store.try_commit_transaction(self.id, 0).await.unwrap();
    // ... notify participants
}
```

### Commit Strategy Configuration

```rust
pub enum CommitStrategy {
    Traditional,  // Standard 2PC - always use transaction state store first
    Pipelined,   // Optimized with potential phase overlapping
    Adaptive,    // Dynamic strategy selection based on dependencies
}
```

### Error Handling and Aborts

#### Abort Scenarios
- **Leadership Changes**: Range leader changed during transaction
- **Prepare Failures**: Any participant fails to prepare
- **Timeouts**: Transaction exceeds time limits
- **Explicit Aborts**: User-initiated abort

#### Abort Process (`record_abort`, lines 170-203)
1. **State Transition**: Mark transaction as aborted
2. **Participant Cleanup**: Parallel abort notifications to all participants
3. **State Store Update**: Record abort in transaction state store
4. **Resource Cleanup**: Release locks and clean up transaction state

## Key Optimizations

### Group Commit
- **Batching**: Multiple transactions committed together
- **Throughput**: Reduces per-transaction overhead
- **Latency**: May increase individual transaction latency for better overall throughput

### Early Lock Release
- **Concurrency**: Ranges can release locks before final commit confirmation
- **Risk Management**: Requires careful coordination with resolver
- **Performance**: Improves system concurrency

### Dependency-Based Ordering
- **Serializability**: Ensures correct transaction ordering
- **Conflict Detection**: Identifies read-write dependencies
- **Deadlock Prevention**: Dependency graph prevents cycles

### Adaptive Strategy Selection
- **Dynamic Optimization**: Chooses commit path based on transaction characteristics
- **Load Awareness**: Considers system load in strategy selection
- **Dependency Awareness**: Transactions without dependencies use optimized path