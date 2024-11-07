use bytes::Bytes;
use storage_server_rpc_client::*;
use test_log::test;

#[test(tokio::test)]
async fn test_basic_blob_io() {
    let url = "127.0.0.1:9225";
    tracing::debug!(%url);
    // Skip testing if storage_server is not up
    if let Ok(rpc_client) = rpc_client::RpcClient::new(url).await {
        let key = "hello".into();
        let content = Bytes::from("42");
        let size = nss_put_blob(&rpc_client, key, content).await.unwrap();
        assert_eq!(2, size);
    }
}
