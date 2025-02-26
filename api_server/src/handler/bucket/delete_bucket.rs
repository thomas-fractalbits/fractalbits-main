use std::sync::Arc;

use axum::{extract::Request, response};
use bucket_tables::{
    api_key_table::{ApiKey, ApiKeyTable},
    bucket_table::{Bucket, BucketTable},
    table::Table,
};
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::ArcRpcClientRss;

pub async fn delete_bucket(
    api_key: Option<ApiKey>,
    bucket: Arc<Bucket>,
    _request: Request,
    _rpc_client_nss: &RpcClientNss,
    rpc_client_rss: ArcRpcClientRss,
) -> response::Result<()> {
    // FIXME: check bucket emptiness
    let mut bucket_table: Table<ArcRpcClientRss, BucketTable> = Table::new(rpc_client_rss.clone());
    bucket_table.delete(&bucket).await;

    let mut api_key = api_key.unwrap();
    let mut api_key_table: Table<ArcRpcClientRss, ApiKeyTable> = Table::new(rpc_client_rss);
    api_key.authorized_buckets.remove(&bucket.bucket_name);
    api_key_table.put(&api_key).await;
    Ok(())
}
