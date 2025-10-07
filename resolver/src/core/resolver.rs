use std::{
    collections::{HashMap, HashSet},
    mem,
    sync::Arc,
};
use tokio::sync::{RwLock, oneshot};
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    core::{group_commit::GroupCommit, statistics::StatisticsTracker},
    participant_range_info::ParticipantRangeInfo,
};
use coordinator_rangeclient::error::{Error, TransactionAbortReason};

#[derive(Clone, Debug)]
pub struct TransactionInfo {
    pub id: Uuid,
    pub num_dependencies: u32,
    pub dependents: HashSet<Uuid>,
    pub participant_ranges_info: Vec<ParticipantRangeInfo>,
    pub fake: bool,
}

impl TransactionInfo {
    pub fn default(id: Uuid, fake: bool) -> Self {
        TransactionInfo {
            id,
            num_dependencies: 0,
            dependents: HashSet::new(),
            participant_ranges_info: Vec::new(),
            fake,
        }
    }
}

#[derive(Default)]
pub struct State {
    info_per_transaction: HashMap<Uuid, TransactionInfo>,
    resolved_transactions: HashSet<Uuid>,
    aborted_transactions: HashSet<Uuid>,
}

pub struct Resolver {
    state: RwLock<State>,
    group_commit: GroupCommit,
    waiting_transactions: RwLock<HashMap<Uuid, oneshot::Sender<()>>>,
    bg_runtime: tokio::runtime::Handle,
    stats_tracker: RwLock<StatisticsTracker>,
}

impl Resolver {
    pub fn new(group_commit: GroupCommit, bg_runtime: tokio::runtime::Handle) -> Self {
        Resolver {
            state: RwLock::new(State {
                info_per_transaction: HashMap::new(),
                resolved_transactions: HashSet::new(),
                aborted_transactions: HashSet::new(),
            }),
            group_commit,
            waiting_transactions: RwLock::new(HashMap::new()),
            bg_runtime,
            stats_tracker: RwLock::new(StatisticsTracker::new()),
        }
    }

    pub async fn commit(
        resolver: Arc<Self>,
        transaction_id: Uuid,
        dependencies: HashSet<Uuid>,
        participant_ranges_info: Vec<ParticipantRangeInfo>,
        fake: bool,
    ) -> Result<(), Error> {
        // A transaction that is read-only across all participant ranges will
        // not have a commit phase and so we also ignore any dependencies it may have
        if participant_ranges_info.iter().all(|info| !info.has_writes) {
            return Ok(());
        }

        let (s, r) = oneshot::channel();
        let mut num_pending_dependencies = 0;

        // Acquire the write lock and update the state with new dependencies
        info!("Updating dependencies for transaction {:?}", transaction_id);
        {
            let mut state = resolver.state.write().await;
            for dependency in dependencies {
                if state.aborted_transactions.contains(&dependency) {
                    // Dependency was aborted! This transaction should abort immediately
                    info!(
                        "Transaction {} depends on aborted transaction {}, aborting",
                        transaction_id, dependency
                    );
                    return Err(Error::TransactionAborted(
                        TransactionAbortReason::DependencyAborted,
                    ));
                }

                if !state.resolved_transactions.contains(&dependency) {
                    // Dependency is not yet resolved, so we need to wait for it
                    num_pending_dependencies += 1;
                    // Add the transaction as a dependent of the dependency
                    state
                        .info_per_transaction
                        .entry(dependency)
                        .or_insert(TransactionInfo::default(dependency, fake))
                        .dependents
                        .insert(transaction_id);
                } else {
                    info!(
                        "Dependency {:?} was already resolved in the meantime",
                        dependency
                    );
                }
            }

            let transaction_info = state
                .info_per_transaction
                .entry(transaction_id)
                .or_insert(TransactionInfo::default(transaction_id, fake));

            transaction_info.num_dependencies = num_pending_dependencies;
            transaction_info.participant_ranges_info = participant_ranges_info;
            resolver
                .waiting_transactions
                .write()
                .await
                .insert(transaction_id, s);

            // {
            //     let mut stats_tracker = resolver.stats_tracker.write().await;
            //     stats_tracker.record_request();
            // }

            info!("Updated dependencies for transaction {:?}", transaction_id);
            if num_pending_dependencies == 0 {
                // If there are no pending dependencies, we can commit the transaction
                info!(
                    "No pending dependencies, committing transaction {:?}",
                    transaction_id
                );
                // Add transaction while holding the write lock
                let transaction_info_clone = transaction_info.clone();
                resolver
                    .group_commit
                    .add_transactions(&vec![transaction_info_clone.clone()])
                    .await?;
                let resolver_clone = resolver.clone();
                resolver.bg_runtime.spawn(async move {
                    let _ =
                        Self::trigger_commit(resolver_clone, vec![transaction_info_clone]).await;
                });
            }
        }

        // Block until the transaction is actually committed OR aborted
        r.await.unwrap();

        // Check if transaction was aborted while waiting
        {
            let state = resolver.state.read().await;
            if state.aborted_transactions.contains(&transaction_id) {
                info!(
                    "Transaction {} was aborted due to cascading abort",
                    transaction_id
                );
                return Err(Error::TransactionAborted(
                    TransactionAbortReason::CascadingAbort,
                ));
            }
        }

        info!("Transaction {} finally committed!", transaction_id);
        Ok(())
    }

    async fn trigger_commit(
        resolver: Arc<Self>,
        transactions: Vec<TransactionInfo>,
    ) -> Result<(), Error> {
        info!(
            "Triggering commit for transactions {:?}",
            transactions.iter().map(|tx| tx.id).collect::<Vec<_>>()
        );
        let resolver_clone = resolver.clone();
        let on_abort = move |aborted_tx_ids: Vec<Uuid>| {
            let resolver_inner = resolver_clone.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    Resolver::register_aborted_transactions(resolver_inner, aborted_tx_ids).await
                {
                    error!("Failed to register aborted transactions: {:?}", e);
                }
            });
        };
        let finished_transactions = resolver.group_commit.commit(on_abort).await?;
        let finished_transactions_ids = finished_transactions
            .iter()
            .map(|tx| tx.id)
            .collect::<Vec<_>>();
        // Notify the transactions currently waiting for messages in the channels so that they unblock
        {
            info!("Notifying transactions");
            let mut waiting_transactions = resolver.waiting_transactions.write().await;
            for transaction in finished_transactions {
                let sender = waiting_transactions.remove(&transaction.id).unwrap();
                sender.send(()).unwrap();
            }
            // TODO: Clean up other state too here?
        }
        info!("Registering transactions as committed");
        // Register the transactions as committed so that more dependencies can be resolved
        if !finished_transactions_ids.is_empty() {
            let _ =
                Self::spawn_register_committed_transactions(resolver, finished_transactions_ids);
        }
        Ok(())
    }

    pub fn spawn_register_committed_transactions(
        resolver: Arc<Self>,
        transaction_ids: Vec<Uuid>,
    ) -> Result<(), Error> {
        // Helper function to bypass compiler's "cycle detected" inference error
        // caused by our recursive call: trigger_commit -> register_committed_transactions -> trigger_commit
        let resolver_clone = resolver.clone();
        resolver.bg_runtime.spawn(async move {
            let _ =
                Resolver::register_committed_transactions(resolver_clone, transaction_ids).await;
        });
        Ok(())
    }

    pub async fn register_committed_transactions(
        resolver: Arc<Self>,
        transaction_ids: Vec<Uuid>,
    ) -> Result<(), Error> {
        let mut new_ready_to_commit = Vec::new();
        {
            let mut new_resolved_dependencies = Vec::new();

            let mut state = resolver.state.write().await;
            // TODO: When is it ok to remove transactions from resolved_transactions?
            for transaction_id in transaction_ids {
                state.resolved_transactions.insert(transaction_id);
                new_resolved_dependencies.push(transaction_id);
            }

            // Find iteratively all transactions that are now ready to commit until the new_resolved_dependencies vector is empty
            while !new_resolved_dependencies.is_empty() {
                let transaction_id = new_resolved_dependencies.pop().unwrap();

                if !state.info_per_transaction.contains_key(&transaction_id) {
                    // Transaction has no dependents
                    continue;
                }

                let transaction_info = state.info_per_transaction.get_mut(&transaction_id).unwrap();
                // Move dependents out of the transaction info
                let dependents = mem::take(&mut transaction_info.dependents);
                assert!(transaction_info.dependents.is_empty());

                // Check if any dependencies are now resolved and if any new transactions are ready to commit
                if !dependents.is_empty() {
                    for dependent in dependents.iter() {
                        let dependent_transaction_info =
                            state.info_per_transaction.get_mut(&dependent).unwrap();
                        assert!(
                            dependent_transaction_info.num_dependencies > 0,
                            "Dependent transaction {} has no pending dependencies",
                            dependent
                        );
                        dependent_transaction_info.num_dependencies -= 1;
                        if dependent_transaction_info.num_dependencies == 0 {
                            // Transaction is now unblocked and ready to commit
                            new_ready_to_commit.push(dependent_transaction_info.clone());
                            new_resolved_dependencies.push(*dependent);
                        }
                    }
                }
            }

            // Add transactions to the group commit while holding the write lock so that dependencies order is respected
            if !new_ready_to_commit.is_empty() {
                let _ = resolver
                    .group_commit
                    .add_transactions(&new_ready_to_commit)
                    .await;
            }
        }
        // Trigger a commit so that the new ready transactions are added to the group commit and get committed
        if !new_ready_to_commit.is_empty() {
            info!(
                "New ready to commit transactions: {:?}",
                new_ready_to_commit
                    .iter()
                    .map(|tx| tx.id)
                    .collect::<Vec<_>>()
            );
            let resolver_clone = resolver.clone();
            resolver.bg_runtime.spawn(async move {
                let _ = Resolver::trigger_commit(resolver_clone, new_ready_to_commit).await;
            });
        }
        Ok(())
    }

    pub async fn register_aborted_transactions(
        resolver: Arc<Self>,
        aborted_transaction_ids: Vec<Uuid>,
    ) -> Result<Vec<Uuid>, Error> {
        let mut cascaded_aborts = Vec::new();

        {
            let mut state = resolver.state.write().await;
            let mut aborts_to_propagate = aborted_transaction_ids.clone();

            // Process each failed transaction and find its dependents
            while !aborts_to_propagate.is_empty() {
                let abort_tx_id = aborts_to_propagate.pop().unwrap();

                // Mark this transaction as aborted
                state.aborted_transactions.insert(abort_tx_id);

                if let Some(transaction_info) = state.info_per_transaction.get_mut(&abort_tx_id) {
                    // Get all transactions waiting for this failed one
                    let dependents = mem::take(&mut transaction_info.dependents);

                    for dependent_id in dependents {
                        // Check if dependent hasn't already been resolved or aborted
                        if !state.resolved_transactions.contains(&dependent_id)
                            && !state.aborted_transactions.contains(&dependent_id)
                        {
                            // This dependent must also be aborted!
                            aborts_to_propagate.push(dependent_id);
                            cascaded_aborts.push(dependent_id);

                            info!(
                                "Cascading abort from {} to dependent {}",
                                abort_tx_id, dependent_id
                            );
                        }
                    }
                }

                // Wake up any transactions waiting for the aborted transaction
                if let Some(sender) = resolver.waiting_transactions.write().await.remove(&abort_tx_id)
                {
                    let _ = sender.send(()); // This will cause waiting transaction to check abort status
                }
            }
        }

        info!(
            "Cascaded abort to {} transactions: {:?}",
            cascaded_aborts.len(),
            cascaded_aborts
        );
        Ok(cascaded_aborts)
    }

    // ---------------------- Statistics ----------------------
    pub async fn sample_waiting_transactions(&self) {
        let mut stats_tracker = self.stats_tracker.write().await;
        let waiting_count = self.waiting_transactions.read().await.len();
        stats_tracker.record_waiting_transactions_sample(waiting_count);
    }

    pub async fn get_average_waiting_transactions(&self) -> f64 {
        let stats_tracker = self.stats_tracker.read().await;
        stats_tracker.get_average_waiting_transactions()
    }

    pub async fn get_stats(resolver: Arc<Self>) -> HashMap<String, f64> {
        // Call this function only at the end of the workload since it's reseting the stats!!
        let mut group_commit_stats = resolver.group_commit.get_stats().await;
        let mut stats_tracker = resolver.stats_tracker.write().await;
        let resolver_stats = stats_tracker.get_stats();
        stats_tracker.reset();
        group_commit_stats.extend(resolver_stats);
        group_commit_stats
    }

    pub async fn reset_stats(&self) {
        let mut stats_tracker = self.stats_tracker.write().await;
        stats_tracker.reset();
    }

    pub async fn get_transaction_info_status(&self) -> String {
        let mut status = String::new();
        status.push_str("Info per transaction:\n");

        let state = self.state.read().await;
        for (tx_id, tx_info) in &state.info_per_transaction {
            status.push_str(&format!(
                "  Transaction {}: dependencies={}, dependents={:?}, fake={}\n",
                tx_id, tx_info.num_dependencies, tx_info.dependents, tx_info.fake
            ));
        }
        status
    }

    pub async fn get_resolved_transactions_status(&self) -> String {
        let state = self.state.read().await;
        format!("Resolved transactions: {:?}\n", state.resolved_transactions)
    }

    pub async fn get_waiting_transactions_status(&self) -> String {
        let waiting = self.waiting_transactions.read().await;
        format!(
            "Waiting transactions: {:?}\n",
            waiting.keys().collect::<Vec<_>>()
        )
    }

    pub async fn get_num_waiting_transactions(&self) -> usize {
        let waiting = self.waiting_transactions.read().await;
        waiting.len()
    }

    pub async fn get_group_commit_status(&self) -> String {
        self.group_commit.get_status().await
    }
    // ---------------------- / Statistics ----------------------
}
