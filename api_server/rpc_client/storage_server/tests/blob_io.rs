use bytes::Bytes;
use storage_server_rpc_client::*;
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
async fn test_basic_blob_io() {
    let url = "127.0.0.1:9225";
    tracing::debug!(%url);
    // Skip testing if storage_server is not up
    if let Ok(rpc_client) = rpc_client::RpcClient::new(url).await {
        let header_len = message::MessageHeader::encode_len();
        let key = String::from("hello");
        let content = Bytes::from("42");
        let mut readback_content = Bytes::new();
        let content_len = content.len();
        let size = nss_put_blob(&rpc_client, key.clone(), content.clone())
            .await
            .unwrap();
        assert_eq!(header_len + content_len, size);

        let size = nss_get_blob(&rpc_client, key, &mut readback_content)
            .await
            .unwrap();
        assert_eq!(header_len + content_len, size);
        assert_eq!(content, readback_content);
    }
}
