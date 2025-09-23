# Test Architecture and Testing Infrastructure

## Overview

Sangria employs a sophisticated testing infrastructure that simulates a complete distributed database system within single-process test environments. The testing architecture combines comprehensive mocking, real storage integration, and protocol-faithful message passing to validate the 2PC implementation and transaction semantics.

## Test Organization Structure

### Integration Tests
- **Frontend**: `frontend/tests/integration_tests.rs` - End-to-end transaction flow testing
- **RangeClient**: `rangeclient/tests/integration_tests.rs` - 2PC protocol and storage integration
- **Universe**: `universe/tests/integration_tests.rs` - Keyspace management testing

### Unit Tests
- Embedded within component modules using `#[cfg(test)]` blocks
- Found in: `rangeserver/src/storage/cassandra.rs`, `rangeserver/src/range_manager/impl.rs`, etc.
- Use Tokio's async test framework (`#[tokio::test]`)

### Test Frameworks
- **Primary**: Tokio async test framework
- **Utilities**: `test-case = "3"` for parameterized testing
- **Runtime**: Custom `tokio::runtime::Runtime` instances for test isolation

## Mock Infrastructure

### Frontend Mock Components (`frontend/src/for_testing/`)

#### MockEpochPublisher (`mock_epoch_publisher.rs`)
```rust
pub struct MockEpochPublisher {
    cancellation_token: CancellationToken,
    current_epoch: Arc<AtomicU64>,
    network: UdpFastNetwork,
}
```

**Features:**
- Simulates epoch management system for distributed versioning
- Listens for `ReadEpochRequest` messages via UDP Fast Network
- Maintains atomic epoch counter (starts at 1)
- FlatBuffer serialization/deserialization for protocol fidelity
- Graceful shutdown via cancellation tokens

**Key Operations:**
```rust
// Epoch request handling
let epoch_request = ReadEpochRequest::follow(bb, None);
let current_epoch = self.current_epoch.load(Ordering::SeqCst);
let response = ReadEpochResponse::create(&mut builder, &ReadEpochResponseArgs {
    epoch: current_epoch,
});
```

#### MockRangeServer (`mock_rangeserver.rs`)
```rust
pub struct MockRangeServer {
    data: Arc<RwLock<HashMap<Bytes, Bytes>>>,
    pending_prepare_records: Arc<Mutex<HashMap<Bytes, (Bytes, Uuid, u64)>>>,
    epoch_reader: Arc<EpochReader>,
    network: UdpFastNetwork,
}
```

**Capabilities:**
- **2PC Operations**: Complete prepare, commit, abort protocol implementation
- **Data Operations**: CRUD operations with read-your-writes semantics
- **State Management**: In-memory key-value store with pending transaction records
- **Epoch Integration**: Uses EpochReader for epoch lease management
- **Thread Safety**: RwLock for data, Mutex for pending records

**2PC Protocol Implementation:**
```rust
// Prepare phase handling
RangeServerRequest::PrepareTransactionReq(req) => {
    // Validate epoch lease
    // Store prepare records
    // Return dependencies and epoch information
}

// Commit phase handling
RangeServerRequest::CommitTransactionsReq(req) => {
    // Apply prepared writes to storage
    // Clean up pending records
    // Update dependencies
}
```

#### MockUniverse (`mock_universe.rs`)
- **Purpose**: Simulates keyspace management service
- **Features**: gRPC Universe service implementation, keyspace CRUD operations
- **Storage**: In-memory keyspace registry with automatic range ID generation

### RangeServer Mock Components (`rangeserver/src/for_testing/`)

#### MockWarden (`mock_warden.rs`)
- **Purpose**: Simulates cluster coordination and range assignment
- **Features**: Range server registration, assignment management, gRPC streaming

#### EpochSupplier (`epoch_supplier.rs`)
- **Purpose**: Controllable epoch progression for testing
- **Features**: Manual epoch setting, waiter pattern for epoch advancement

## Integration Test Implementation

### Frontend Integration Tests (`frontend/tests/integration_tests.rs`)

#### Full End-to-End Transaction Flow
```rust
#[tokio::test]
async fn test_frontend() {
    // 1. Setup comprehensive test configuration
    let config = Config {
        frontend_proto_address: frontend_address,
        rangeserver_proto_address: mock_rangeserver_address,
        // ... all service addresses
    };

    // 2. Start mock services
    let epoch_publisher = MockEpochPublisher::start(config.clone()).await?;
    let range_server = MockRangeServer::start(config.clone()).await?;
    let universe = MockUniverse::start(config.clone()).await?;
    let frontend = FrontendServer::start(config.clone()).await?;

    // 3. Execute transaction workflow
    let frontend_client = FrontendClient::new(config.clone()).await?;

    // Create keyspace
    let keyspace_id = frontend_client.create_keyspace(...).await?;

    // Start transaction
    let transaction = frontend_client.start_transaction(keyspace_id).await?;

    // Write operation
    transaction.put(key, value).await?;

    // Read-your-writes validation
    let read_value = transaction.get(key).await?;
    assert_eq!(read_value, Some(value));

    // Commit transaction (triggers 2PC)
    transaction.commit().await?;

    // Verify persistence in new transaction
    let new_transaction = frontend_client.start_transaction(keyspace_id).await?;
    let committed_value = new_transaction.get(key).await?;
    assert_eq!(committed_value, Some(value));
}
```

**Test Coverage:**
- Keyspace creation and management
- Transaction lifecycle (start, operations, commit)
- Read-your-writes semantics
- 2PC protocol execution
- Cross-transaction persistence
- Delete operations and abort handling

### RangeClient Integration Tests (`rangeclient/tests/integration_tests.rs`)

#### 2PC Protocol Testing
```rust
#[tokio::test]
async fn read_modify_write() {
    // Setup with real Cassandra storage
    let cassandra_session = create_test_session().await;
    let range_server = RangeServer::start_with_storage(cassandra_session).await;

    // Create transaction info
    let transaction_info = TransactionInfo::new();

    // Read operation (establishes dependencies)
    let get_result = range_client.get(
        transaction_info.clone(),
        &range_id,
        vec![key.clone()]
    ).await?;

    // Write operation (prepare phase)
    let prepare_result = range_client.prepare_transaction(
        transaction_info.clone(),
        &range_id,
        true, // has_reads
        &[Record { key: key.clone(), val: value.clone() }],
        &[],  // no deletes
        0.0,  // resolver_load
        1     // num_clients
    ).await?;

    // Commit phase
    range_client.commit_transactions(
        vec![transaction_info.id],
        &range_id,
        0 // epoch
    ).await?;

    // Verify persistence
    let final_result = range_client.get(
        new_transaction_info,
        &range_id,
        vec![key.clone()]
    ).await?;
    assert_eq!(final_result.vals[0], Some(value));
}
```

**Test Scenarios:**
- **Unknown Range**: Error handling for non-existent ranges
- **Read Initial**: Reading from empty ranges
- **No-Write Commits**: Read-only transaction commits
- **Prefetching**: Read optimization testing

### Universe Integration Tests (`universe/tests/integration_tests.rs`)

#### Keyspace Management Testing
```rust
#[tokio::test]
async fn test_create_and_list_keyspace_handlers() {
    // Real Cassandra integration
    let cassandra_session = create_session().await;
    let universe_service = UniverseService::new(cassandra_session);

    // Test keyspace creation
    let keyspace_req = CreateKeyspaceRequest {
        namespace: "test_namespace".to_string(),
        name: "test_keyspace".to_string(),
        base_key_ranges: create_base_ranges(),
    };

    let keyspace_id = universe_service.create_keyspace(keyspace_req).await?;

    // Test keyspace listing
    let keyspaces = universe_service.list_keyspaces().await?;
    assert!(keyspaces.iter().any(|ks| ks.id == keyspace_id));

    // Test keyspace lookup
    let found_keyspace = universe_service.get_keyspace_by_id(keyspace_id).await?;
    assert_eq!(found_keyspace.namespace, "test_namespace");
}
```

## Test Configuration and Setup

### Configuration Patterns
```rust
let config = Config {
    // Service endpoints
    frontend_proto_address: frontend_address,
    rangeserver_proto_address: mock_rangeserver_address,
    universe_proto_address: mock_universe_address,

    // Network configuration
    frontend_fast_network_address: frontend_fast_address,
    rangeserver_fast_network_address: mock_rangeserver_fast_address,

    // Resource allocation
    frontend_fast_network_dedicated_core: Some(2),
    rangeserver_fast_network_dedicated_core: Some(3),

    // Timing configuration
    transaction_timeout: Duration::from_secs(30),
    operation_timeout: Duration::from_secs(10),

    // Storage configuration
    cassandra_endpoints: vec!["127.0.0.1:9042".to_string()],
    cassandra_keyspace: "test_keyspace".to_string(),
};
```

### Dependencies and Setup
- **Cassandra**: Required for storage-layer integration tests
- **Port Management**: Dynamic port allocation to avoid conflicts
- **Runtime Isolation**: Separate Tokio runtimes for component isolation
- **Network Setup**: UDP/TCP sockets with proper addressing

## Testing Patterns

### Distributed System Simulation
```rust
// Multi-component setup pattern
async fn setup_distributed_system() -> TestEnvironment {
    // Start services in dependency order
    let epoch_publisher = MockEpochPublisher::start(config.clone()).await?;
    let universe = MockUniverse::start(config.clone()).await?;
    let range_server = MockRangeServer::start(config.clone()).await?;
    let frontend = FrontendServer::start(config.clone()).await?;

    // Wait for service readiness
    wait_for_service_ready(&frontend_address).await?;

    TestEnvironment {
        epoch_publisher,
        universe,
        range_server,
        frontend,
        config,
    }
}
```

### Correctness Verification
```rust
// State assertion patterns
async fn verify_transaction_state(range_server: &MockRangeServer, expected_state: &TransactionState) {
    let data = range_server.data.read().await;
    let pending = range_server.pending_prepare_records.lock().await;

    // Verify committed data
    for (key, expected_value) in &expected_state.committed_data {
        assert_eq!(data.get(key), Some(expected_value));
    }

    // Verify no pending records for committed transactions
    for tx_id in &expected_state.committed_transactions {
        assert!(!pending.values().any(|(_, id, _)| id == tx_id));
    }
}
```

### Timing and Concurrency
```rust
// Proper async coordination
async fn coordinate_distributed_operation() {
    let (tx, rx) = oneshot::channel();

    // Background task for service
    tokio::spawn(async move {
        let result = perform_distributed_operation().await;
        tx.send(result).unwrap();
    });

    // Wait with timeout
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        rx
    ).await??;

    verify_operation_result(result);
}
```

## Test Coverage Analysis

### 2PC Protocol Coverage
- **Prepare Phase**: Transaction preparation with epoch validation
- **Commit Phase**: Group commit execution and persistence
- **Abort Phase**: Transaction rollback and cleanup
- **Dependency Handling**: Cross-transaction ordering

### Component Coverage
- **Frontend**: Complete transaction coordinator testing
- **RangeClient**: 2PC participant protocol testing
- **Universe**: Keyspace management testing
- **Storage**: Cassandra integration testing

### Gaps and Limitations
- **Resolver Testing**: Limited dedicated resolver integration tests
- **Failure Injection**: No systematic failure scenario testing
- **Performance Testing**: No load or stress test infrastructure
- **Multi-Node Testing**: Single-process simulation only
- **Network Partition**: Limited network failure simulation

## Key Testing Infrastructure Benefits

### Strengths
1. **Protocol Fidelity**: Tests use actual message formats and network protocols
2. **Real Storage Integration**: Cassandra integration provides realistic storage testing
3. **Comprehensive Mocking**: Well-designed mock infrastructure simulates distributed components
4. **Isolation**: Proper test isolation with separate runtimes and clean state
5. **Async Correctness**: Proper async/await patterns with cancellation and timeouts

### Architecture Quality
The Sangria testing infrastructure demonstrates sophisticated distributed systems testing practices with proper abstractions for network simulation, comprehensive component mocking, and integration with real storage systems. While coverage could be enhanced in areas like failure injection and resolver testing, the existing infrastructure effectively validates core 2PC protocol implementation and transaction semantics.