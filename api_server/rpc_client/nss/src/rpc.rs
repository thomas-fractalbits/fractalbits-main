use crate::{
    message::MessageHeader,
    rpc_client::{Message, RpcClient, RpcError},
};
use bytes::{Bytes, BytesMut};
use prost::Message as PbMessage;

include!(concat!(env!("OUT_DIR"), "/nss_ops.rs"));

impl RpcClient {
    pub async fn put_inode(&self, key: String, value: Bytes) -> Result<PutInodeResponse, RpcError> {
        let body = PutInodeRequest { key, value };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::PutInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: PutInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn get_inode(&self, key: String) -> Result<GetInodeResponse, RpcError> {
        let body = GetInodeRequest { key };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::GetInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: GetInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn list_inodes(
        &self,
        max_keys: u32,
        prefix: String,
        start_after: String,
        skip_mpu_parts: bool,
    ) -> Result<ListInodesResponse, RpcError> {
        let body = ListInodesRequest {
            max_keys,
            prefix,
            start_after,
            skip_mpu_parts,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::ListInodes;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: ListInodesResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn delete_inode(&self, key: String) -> Result<DeleteInodeResponse, RpcError> {
        let body = DeleteInodeRequest { key };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::DeleteInode;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: DeleteInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }

    pub async fn create_root_inode(&self) -> Result<CreateRootInodeResponse, RpcError> {
        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::CreateRootInode;
        header.size = MessageHeader::SIZE as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: CreateRootInodeResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        Ok(resp)
    }
}
