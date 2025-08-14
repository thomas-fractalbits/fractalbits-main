use serde::Deserialize;
use std::{net::SocketAddr, time::Duration};

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum BlobStorageBackend {
    BssOnlySingleAz,
    HybridSingleAz,
    S3ExpressSingleAz,
    S3ExpressMultiAz,
    S3ExpressMultiAzWithTracking,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BlobStorageConfig {
    pub backend: BlobStorageBackend,

    pub bss: Option<BssConfig>,
    pub s3_hybrid: Option<S3HybridConfig>,
    pub s3_express: Option<S3ExpressMultiAzConfig>,
    pub s3_express_single_az: Option<S3ExpressSingleAzConfig>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BssConfig {
    pub addr: SocketAddr,
    pub conn_num: u16,
}

#[derive(Deserialize, Debug, Clone)]
pub struct S3ExpressMultiAzConfig {
    pub local_az_host: String,
    pub local_az_port: u16,
    pub s3_region: String,
    pub local_az_bucket: String,
    pub remote_az_bucket: String,
    pub remote_az_host: Option<String>,
    pub remote_az_port: Option<u16>,
    pub az: String,
    #[serde(default = "default_express_session_auth")]
    pub express_session_auth: bool,
}

fn default_express_session_auth() -> bool {
    false // Default to false for local testing with minio
}

#[derive(Deserialize, Debug, Clone)]
pub struct S3ExpressSingleAzConfig {
    pub s3_host: String,
    pub s3_port: u16,
    pub s3_region: String,
    pub s3_bucket: String,
    pub az: String,
    #[serde(default = "default_force_path_style")]
    pub force_path_style: bool,
}

fn default_force_path_style() -> bool {
    true // Default to true for local testing with minio
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct Config {
    pub nss_addr: SocketAddr,
    pub rss_addr: SocketAddr,
    pub nss_conn_num: u16,
    pub rss_conn_num: u16,

    pub port: u16,
    pub region: String,
    pub root_domain: String,
    pub with_metrics: bool,
    pub http_request_timeout_seconds: u64,
    pub rpc_timeout_seconds: u64,

    pub blob_storage: BlobStorageConfig,
    pub allow_missing_or_bad_signature: bool,
}

impl Config {
    pub fn rpc_timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_timeout_seconds)
    }

    pub fn http_request_timeout(&self) -> Duration {
        Duration::from_secs(self.http_request_timeout_seconds)
    }
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct S3HybridConfig {
    pub s3_host: String,
    pub s3_port: u16,
    pub s3_region: String,
    pub s3_bucket: String,
}
