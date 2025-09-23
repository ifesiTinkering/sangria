# Sangria Codebase Understanding

This directory contains comprehensive documentation of the Sangria distributed database system, focusing on the core working parts including resolver code, 2PC mechanisms, pipeline implementations, and test architecture.

## Documentation Structure

### [01 - Codebase Overview](./01-codebase-overview.md)
- **Project Structure**: Main modules and their responsibilities
- **Architecture Overview**: High-level system design and components
- **Key Entry Points**: Server main functions and client libraries
- **Testing Infrastructure**: Overview of test organization

### [02 - Resolver and 2PC Mechanisms](./02-resolver-and-2pc-mechanisms.md)
- **Resolver Core Implementation**: Transaction dependency tracking and resolution
- **2PC Implementation**: Two-phase commit protocol in the coordinator
- **Dependency Resolution Flow**: How transactions wait for dependencies
- **Transaction Operations**: Read, write, delete operations and their semantics
- **Error Handling**: Abort scenarios and recovery mechanisms
- **Key Optimizations**: Group commit, early lock release, adaptive strategies

### [03 - Pipeline 2PC Implementation](./03-pipeline-2pc-implementation.md)
- **Group Commit Architecture**: Batching optimization for transaction throughput
- **Pipeline Batching Mechanism**: How transactions are grouped and committed
- **Commit Strategy Implementation**: Traditional vs Adaptive/Pipelined strategies
- **Pipeline Optimizations**: Early lock release, dependency routing, batch operations
- **Concurrency Management**: Fine-grained locking and parallel processing
- **Performance Benefits**: Throughput improvements and latency considerations

### [04 - Test Architecture](./04-test-architecture.md)
- **Test Organization**: Integration and unit test structure
- **Mock Infrastructure**: Comprehensive mocking of distributed components
- **Integration Test Implementation**: End-to-end transaction flow testing
- **Test Configuration**: Setup patterns and dependency management
- **Testing Patterns**: Distributed system simulation and correctness verification
- **Coverage Analysis**: What's tested and what gaps exist

## Quick Reference

### Core Components
- **Frontend** (`frontend/`): Client-facing transaction coordinator
- **Coordinator** (`coordinator/`): 2PC transaction coordination logic
- **Resolver** (`resolver/`): Dependency resolution and group commit
- **RangeServer** (`rangeserver/`): Data storage and range management
- **Universe** (`universe/`): Cluster membership and keyspace management

### Key Files
- `coordinator/src/transaction.rs` - Main 2PC implementation
- `resolver/src/core/resolver.rs` - Dependency resolution logic
- `resolver/src/core/group_commit.rs` - Pipeline batching mechanism
- `frontend/tests/integration_tests.rs` - End-to-end testing
- `frontend/src/for_testing/` - Mock infrastructure

### Transaction Flow
1. **Start Transaction**: Frontend creates transaction context
2. **Operations**: Read/write operations collect dependencies
3. **Prepare Phase**: Parallel prepare requests to all participant ranges
4. **Dependency Resolution**: Resolver orders transactions by dependencies
5. **Commit Phase**: Group commit executes batched transactions
6. **Completion**: Participants apply changes and release locks

### Commit Strategies
- **Traditional**: Standard 2PC with state store first, then participants
- **Adaptive**: Routes based on dependencies (direct path vs resolver)
- **Pipelined**: Optimized with batching and early lock release

### Testing Approach
- **Comprehensive Mocking**: Full distributed system simulation
- **Real Storage**: Cassandra integration for storage layer testing
- **Protocol Fidelity**: Actual message formats and network protocols
- **Async Correctness**: Proper concurrency and cancellation patterns

## System Architecture Summary

Sangria implements an adaptive distributed transaction system with:

1. **Multi-modal 2PC**: Traditional, pipelined, and adaptive commit strategies
2. **Dependency-based Ordering**: Resolver tracks transaction dependencies for serializable execution
3. **Range-based Sharding**: Data partitioned across ranges managed by range servers
4. **Group Commit Optimization**: Batches commits for better throughput
5. **Epoch-based Versioning**: Consistent snapshots across the distributed system
6. **Cassandra Backend**: Persistent storage for transaction state and data

The system is designed for high-performance distributed transactions with adaptive optimization based on workload characteristics and system load.