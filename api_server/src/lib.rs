mod blob_client;
mod blob_storage;
mod config;
pub mod handler;
mod object_layout;

use blob_client::BlobClient;
pub use config::{BlobStorageBackend, BlobStorageConfig, Config, S3HybridConfig};
pub use data_blob_tracking::{DataBlobTracker, DataBlobTrackingError};
use data_types::{table::KvClientProvider, Versioned};
use metrics::histogram;
use moka::future::Cache;
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;

use slotmap_conn_pool::{ConnPool, Poolable};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{self, Sender};
use tracing::info;
pub type BlobId = uuid::Uuid;

pub struct AppState {
    pub config: Arc<Config>,
    pub cache: Arc<Cache<String, Versioned<String>>>,
    pub az_status_cache: Option<Arc<Cache<String, String>>>,

    rpc_clients_nss: ConnPool<Arc<RpcClientNss>, String>,
    rpc_clients_rss: ConnPool<Arc<RpcClientRss>, String>,

    blob_client: Arc<BlobClient>,
    blob_deletion: Sender<(Option<String>, BlobId, usize)>,
    pub data_blob_tracker: Arc<DataBlobTracker>,
}

impl KvClientProvider for AppState {
    async fn checkout_rpc_client_rss(
        &self,
    ) -> Result<Arc<RpcClientRss>, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        let res = self
            .rpc_clients_rss
            .checkout(self.config.rss_addr.clone())
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        histogram!("checkout_rpc_client_nanos", "type" => "rss")
            .record(start.elapsed().as_nanos() as f64);
        Ok(res)
    }
}

impl AppState {
    pub async fn new(config: Arc<Config>) -> Self {
        let rpc_clients_rss =
            Self::new_rpc_clients_pool_rss(&config.rss_addr, config.rss_conn_num).await;
        let rpc_clients_nss =
            Self::new_rpc_clients_pool_nss(&config.nss_addr, config.nss_conn_num).await;

        let (tx, rx) = mpsc::channel(1024 * 1024);
        let data_blob_tracker = Arc::new(DataBlobTracker::with_pools(
            config.rss_addr.clone(),
            rpc_clients_rss.clone(),
            config.nss_addr.clone(),
            rpc_clients_nss.clone(),
        ));

        let cache = Arc::new(
            Cache::builder()
                .time_to_idle(Duration::from_secs(300))
                .max_capacity(10_000)
                .build(),
        );

        let (blob_client, az_status_cache) = BlobClient::new(
            &config.blob_storage,
            rx,
            config.rpc_timeout(),
            Some(data_blob_tracker.clone()),
        )
        .await
        .expect("Failed to initialize blob client");

        Self {
            config,
            rpc_clients_nss,
            blob_client: Arc::new(blob_client),
            blob_deletion: tx,
            rpc_clients_rss,
            cache,
            az_status_cache,
            data_blob_tracker,
        }
    }

    async fn new_rpc_clients_pool_nss(
        nss_addr: &str,
        nss_conn_num: u16,
    ) -> ConnPool<Arc<RpcClientNss>, String> {
        let rpc_clients_nss = ConnPool::new();

        // Use the Poolable trait's retry logic instead of manual retry
        for i in 0..nss_conn_num as usize {
            info!(
                "Connecting to NSS server at {nss_addr} (connection {}/{})",
                i + 1,
                nss_conn_num
            );
            let client = Arc::new(
                <RpcClientNss as slotmap_conn_pool::Poolable>::new(nss_addr.to_string())
                    .await
                    .unwrap(),
            );
            rpc_clients_nss.pooled(nss_addr.to_string(), client);
        }

        info!("NSS RPC client pool initialized with {nss_conn_num} connections.");
        rpc_clients_nss
    }

    async fn new_rpc_clients_pool_rss(
        rss_addr: &str,
        rss_conn_num: u16,
    ) -> ConnPool<Arc<RpcClientRss>, String> {
        let rpc_clients_rss = ConnPool::new();

        // Use the Poolable trait's retry logic
        for i in 0..rss_conn_num as usize {
            info!(
                "Connecting to RSS server at {rss_addr} (connection {}/{})",
                i + 1,
                rss_conn_num
            );
            let client = Arc::new(
                <RpcClientRss as slotmap_conn_pool::Poolable>::new(rss_addr.to_string())
                    .await
                    .unwrap(),
            );
            rpc_clients_rss.pooled(rss_addr.to_string(), client);
        }

        info!("RSS RPC client pool initialized with {rss_conn_num} connections.");
        rpc_clients_rss
    }

    pub async fn checkout_rpc_client_nss(
        &self,
    ) -> Result<Arc<RpcClientNss>, <RpcClientNss as Poolable>::Error> {
        let start = Instant::now();
        let res = self
            .rpc_clients_nss
            .checkout(self.config.nss_addr.clone())
            .await?;
        histogram!("checkout_rpc_client_nanos", "type" => "nss")
            .record(start.elapsed().as_nanos() as f64);
        Ok(res)
    }

    pub async fn checkout_rpc_client_rss_direct(
        &self,
    ) -> Result<Arc<RpcClientRss>, <RpcClientRss as Poolable>::Error> {
        let start = Instant::now();
        let res = self
            .rpc_clients_rss
            .checkout(self.config.rss_addr.clone())
            .await?;
        histogram!("checkout_rpc_client_nanos", "type" => "rss")
            .record(start.elapsed().as_nanos() as f64);
        Ok(res)
    }

    pub fn get_blob_client(&self) -> Arc<BlobClient> {
        self.blob_client.clone()
    }

    pub fn get_blob_deletion(&self) -> Sender<(Option<String>, BlobId, usize)> {
        self.blob_deletion.clone()
    }
}
