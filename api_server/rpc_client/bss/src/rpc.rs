use crate::{
    message::{Command, MessageHeader},
    rpc_client::{RpcClient, RpcError},
};
use bytes::{Bytes, BytesMut};
use uuid::Uuid;

impl RpcClient {
    pub async fn put_blob(self: &Self, blob_id: Uuid, content: Bytes) -> Result<usize, RpcError> {
        let mut request_header = MessageHeader::default();
        request_header.id = self.gen_request_id();
        request_header.blob_id = blob_id.into_bytes();
        request_header.command = Command::PutBlob;
        request_header.size = (MessageHeader::encode_len() + content.len()) as u64;

        let mut header_bytes = BytesMut::with_capacity(MessageHeader::encode_len());
        request_header.encode(&mut header_bytes);
        let msgs = vec![header_bytes.freeze(), content];

        let resp = self.send_request(request_header.id, &msgs).await?;
        Ok(resp.header.result as usize)
    }

    pub async fn get_blob(
        self: &Self,
        blob_id: Uuid,
        content: &mut Bytes,
    ) -> Result<usize, RpcError> {
        let mut request_header = MessageHeader::default();
        request_header.id = self.gen_request_id();
        request_header.blob_id = blob_id.into_bytes();
        request_header.command = Command::GetBlob;
        request_header.size = MessageHeader::encode_len() as u64;

        let mut request_bytes = BytesMut::with_capacity(request_header.size as usize);
        request_header.encode(&mut request_bytes);

        let resp = self
            .send_request(request_header.id, &vec![request_bytes.freeze()])
            .await?;
        let size = resp.header.result;
        *content = resp.body;
        Ok(size as usize)
    }
}
