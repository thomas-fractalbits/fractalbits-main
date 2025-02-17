pub mod api_key_table;
pub mod bucket_table;
pub mod table;

use std::sync::Arc;

use bytes::Bytes;
use rpc_client_rss::*;
use table::KvClient;

impl KvClient for Arc<RpcClientRss> {
    async fn put(&mut self, key: String, value: Bytes) -> Bytes {
        RpcClientRss::put(self, key.into(), value).await.unwrap()
    }

    async fn get(&mut self, key: String) -> Bytes {
        RpcClientRss::get(self, key.into()).await.unwrap()
    }

    async fn delete(&mut self, key: String) -> Bytes {
        RpcClientRss::delete(self, key.into()).await.unwrap()
    }
}
