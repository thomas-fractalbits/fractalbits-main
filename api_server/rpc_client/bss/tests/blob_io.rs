use bytes::Bytes;
use fake::Fake;
use rpc_client_bss::*;
use tracing_test::traced_test;
use uuid::Uuid;

#[tokio::test]
#[traced_test]
async fn test_basic_blob_io() {
    let url = "127.0.0.1:9225";
    tracing::debug!(%url);
    // Skip testing if blob storage server is not up
    if let Ok(rpc_client) = RpcClientBss::new(url).await {
        for _ in 0..1024 {
            let header_len = message::MessageHeader::SIZE;
            let blob_id = Uuid::now_v7();
            let content = Bytes::from((1..256).fake::<String>());
            let mut readback_content = Bytes::new();
            let content_len = content.len();
            let size = rpc_client
                .put_blob(blob_id.clone(), content.clone())
                .await
                .unwrap();
            assert_eq!(header_len + content_len, size);

            let size = rpc_client
                .get_blob(blob_id, &mut readback_content)
                .await
                .unwrap();
            assert_eq!(header_len + content_len, size);
            assert_eq!(content, readback_content);
        }
    }
}
