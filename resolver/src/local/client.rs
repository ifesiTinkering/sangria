use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use uuid::Uuid;

use crate::{
    core::{group_commit::GroupCommit, resolver::Resolver},
    participant_range_info::ParticipantRangeInfo,
    resolver_client::ResolverClient as ResolverClientTrait,
};
use async_trait::async_trait;
use coordinator_rangeclient::error::Error;
use coordinator_rangeclient::rangeclient::RangeClient;
use tx_state_store::client::Client as TxStateStoreClient;

pub struct ResolverClient {
    resolver: Arc<Resolver>,
}

impl ResolverClient {
    pub async fn new(
        range_client: Arc<RangeClient>,
        tx_state_store: Arc<TxStateStoreClient>,
        bg_runtime: tokio::runtime::Handle,
    ) -> Arc<Self> {
        Arc::new(ResolverClient {
            resolver: Arc::new(Resolver::new(
                GroupCommit::new(range_client, tx_state_store, false, 0.0),
                bg_runtime,
            )),
        })
    }
}

#[async_trait]
impl ResolverClientTrait for ResolverClient {
    async fn get_stats(&self) -> HashMap<String, f64> {
        Resolver::get_stats(self.resolver.clone()).await
    }

    async fn get_transaction_info_status(&self) -> String {
        self.resolver.get_transaction_info_status().await
    }

    async fn get_resolved_transactions_status(&self) -> String {
        self.resolver.get_resolved_transactions_status().await
    }

    async fn get_waiting_transactions_status(&self) -> String {
        self.resolver.get_waiting_transactions_status().await
    }

    async fn get_group_commit_status(&self) -> String {
        self.resolver.get_group_commit_status().await
    }

    async fn get_num_waiting_transactions(&self) -> usize {
        self.resolver.get_num_waiting_transactions().await
    }

    async fn get_average_waiting_transactions(&self) -> f64 {
        self.resolver.get_average_waiting_transactions().await
    }

    async fn commit(
        &self,
        transaction_id: Uuid,
        dependencies: HashSet<Uuid>,
        participant_ranges: Vec<ParticipantRangeInfo>,
    ) -> Result<(), Error> {
        Resolver::commit(
            self.resolver.clone(),
            transaction_id,
            dependencies,
            participant_ranges,
            false,
        )
        .await
    }

    async fn register_committed_transactions(
        &self,
        transaction_ids: Vec<Uuid>,
    ) -> Result<(), Error> {
        Resolver::spawn_register_committed_transactions(self.resolver.clone(), transaction_ids)
    }
}
