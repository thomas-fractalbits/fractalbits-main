use rpc_client_common::{Closeable, RpcClient as GenericRpcClient, RpcError};
use rpc_codec_common::MessageFrame;
use single_conn::Poolable;
use std::time::Duration;

// Create a wrapper struct to avoid orphan rule issues
#[derive(Clone)]
pub struct RpcClient {
    inner: GenericRpcClient<bss_codec::MessageCodec, bss_codec::MessageHeader>,
}

impl RpcClient {
    pub fn gen_request_id(&self) -> u32 {
        self.inner.gen_request_id()
    }

    pub async fn send_request(
        &self,
        request_id: u32,
        frame: MessageFrame<bss_codec::MessageHeader>,
        timeout: Option<Duration>,
    ) -> Result<MessageFrame<bss_codec::MessageHeader>, RpcError> {
        self.inner.send_request(request_id, frame, timeout).await
    }
}

impl RpcClient {
    pub async fn new_from_address(
        address: String,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new_internal(address).await
    }

    async fn new_internal(
        address: String,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let inner = GenericRpcClient::<bss_codec::MessageCodec, bss_codec::MessageHeader>::establish_connection(
            address,
        )
        .await?;
        Ok(RpcClient { inner })
    }

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl Closeable for RpcClient {
    fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl Poolable for RpcClient {
    type AddrKey = String;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn new(address: Self::AddrKey) -> Result<Self, Self::Error> {
        Self::new_internal(address).await
    }

    fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}
