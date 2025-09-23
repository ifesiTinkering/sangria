# Sangria Codebase Overview

## Project Structure

Sangria is a distributed database system built in Rust that implements adaptive two-phase commit (2PC) protocols with dependency-based transaction ordering.

### Core System Components

```
sangria/
├── frontend/           # Client-facing interface and transaction coordinator entry point
├── coordinator/        # Transaction coordinator implementing 2PC protocols
├── resolver/           # Dependency resolution system for transaction ordering
├── rangeserver/        # Data storage and management for key ranges
├── rangeclient/        # Client library for communicating with range servers
├── universe/           # Cluster membership and range assignment management
├── warden/             # System monitoring and health management
├── tx_state_store/     # Transaction state persistence (uses Cassandra)
├── common/             # Shared data structures, configuration, and utilities
├── proto/              # gRPC protocol definitions and generated code
├── flatbuf/            # FlatBuffers serialization schemas
├── epoch_reader/       # Epoch-based versioning system
├── monitoring/         # Metrics and observability
└── workload-generator/ # Testing and benchmarking tools
```

## Architecture Overview

Sangria implements a sophisticated distributed transaction system with:

1. **Multi-modal 2PC**: Traditional, pipelined, and adaptive commit strategies
2. **Dependency-based Ordering**: Resolver tracks transaction dependencies for serializable execution
3. **Range-based Sharding**: Data partitioned across ranges managed by range servers
4. **Group Commit Optimization**: Batches commits for better throughput
5. **Epoch-based Versioning**: Consistent snapshots across the distributed system
6. **Cassandra Backend**: Persistent storage for transaction state and data

## Key Entry Points

### Server Main Functions
- `frontend/src/main.rs` - Frontend server entry
- `rangeserver/src/main.rs` - Range server entry
- `resolver/src/main.rs` - Resolver server entry
- `universe/src/main.rs` - Universe server entry
- `warden/src/main.rs` - Warden server entry

### Client Libraries
- `coordinator/src/coordinator.rs` - Transaction coordinator
- `workload-generator/src/main.rs` - Workload generation tool

## Testing Infrastructure

### Integration Tests
- `frontend/tests/integration_tests.rs` - Frontend integration tests
- `rangeclient/tests/integration_tests.rs` - Range client tests
- `universe/tests/integration_tests.rs` - Universe cluster tests

### Mock Infrastructure
- `frontend/src/for_testing/` - Mock implementations for testing
  - `mock_rangeserver.rs` - Mock range server
  - `mock_epoch_publisher.rs` - Mock epoch publisher
  - `mock_universe.rs` - Mock universe service