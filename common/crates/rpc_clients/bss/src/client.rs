use rpc_client_common::{RpcClient as GenericRpcClient, RpcError};
use rpc_codec_common::MessageFrame;
use slotmap_conn_pool::Poolable;
use std::time::Duration;
use tokio::net::TcpStream;

// Create a wrapper struct to avoid orphan rule issues
pub struct RpcClient {
    inner: GenericRpcClient<bss_codec::MessageCodec, bss_codec::MessageHeader>,
}

impl RpcClient {
    pub async fn new(stream: TcpStream) -> Result<Self, RpcError> {
        let inner = GenericRpcClient::new(stream).await?;
        Ok(RpcClient { inner })
    }

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

    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl Poolable for RpcClient {
    type AddrKey = String;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn new(address: Self::AddrKey) -> Result<Self, Self::Error> {
        let inner =
            <GenericRpcClient<bss_codec::MessageCodec, bss_codec::MessageHeader> as Poolable>::new(
                address,
            )
            .await?;
        Ok(RpcClient { inner })
    }

    fn is_closed(&self) -> bool {
        self.is_closed()
    }
}
