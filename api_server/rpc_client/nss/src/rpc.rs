use crate::{
    message::MessageHeader,
    rpc_client::{RpcClient, RpcError},
};
use bytes::BytesMut;
use prost::Message;

include!(concat!(env!("OUT_DIR"), "/nss_ops.rs"));

impl RpcClient {
    pub async fn put_inode(
        self: &Self,
        key: String,
        value: Vec<u8>,
    ) -> Result<PutInodeResponse, RpcError> {
        let request_body = PutInodeRequest { key, value };

        let mut request_header = MessageHeader::default();
        request_header.id = self.gen_request_id();
        request_header.command = Command::PutInode;
        request_header.size = (MessageHeader::encode_len() + request_body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(request_header.size as usize);
        request_header.encode(&mut request_bytes);
        request_body
            .encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(request_header.id, &vec![request_bytes.freeze()])
            .await?
            .body;
        let resp: PutInodeResponse = Message::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn get_inode(self: &Self, key: String) -> Result<GetInodeResponse, RpcError> {
        let request_body = GetInodeRequest { key };

        let mut request_header = MessageHeader::default();
        request_header.id = self.gen_request_id();
        request_header.command = Command::GetInode;
        request_header.size = (MessageHeader::encode_len() + request_body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(request_header.size as usize);
        request_header.encode(&mut request_bytes);
        request_body
            .encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(request_header.id, &vec![request_bytes.freeze()])
            .await?
            .body;
        let resp: GetInodeResponse = Message::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }
}
