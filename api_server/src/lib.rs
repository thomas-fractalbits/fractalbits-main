mod message;
mod ws_client;

use bytes::BytesMut;
use message::MessageHeader;
use nss_ops::*;
use prost::Message;
pub use ws_client::RpcClient;
use ws_client::WebSocketError;

pub mod nss_ops {
    include!(concat!(env!("OUT_DIR"), "/nss_ops.rs"));
}

pub async fn nss_put_inode(
    rpc_client: &RpcClient,
    key: String,
    value: String,
) -> Result<PutInodeResponse, WebSocketError> {
    let request_body = PutInodeRequest { key, value };

    let mut request_header = MessageHeader::default();
    request_header.id = rpc_client.gen_request_id();
    request_header.command = Command::PutInode.into();
    request_header.size = (MessageHeader::encode_len() + request_body.encoded_len()) as u32;

    let mut request_bytes = BytesMut::with_capacity(request_header.size as usize);
    request_header
        .encode(&mut request_bytes)
        .map_err(WebSocketError::EncodeError)?;
    request_body
        .encode(&mut request_bytes)
        .map_err(WebSocketError::EncodeError)?;

    let resp_bytes = rpc_client
        .send_request(request_header.id, request_bytes.freeze())
        .await?;
    let resp: PutInodeResponse =
        Message::decode(resp_bytes).map_err(WebSocketError::DecodeError)?;
    Ok(resp)
}

pub async fn nss_get_inode(
    rpc_client: &RpcClient,
    key: String,
) -> Result<GetInodeResponse, WebSocketError> {
    let request_body = GetInodeRequest { key };

    let mut request_header = MessageHeader::default();
    request_header.id = rpc_client.gen_request_id();
    request_header.command = Command::GetInode.into();
    request_header.size = (MessageHeader::encode_len() + request_body.encoded_len()) as u32;

    let mut request_bytes = BytesMut::with_capacity(request_header.size as usize);
    request_header
        .encode(&mut request_bytes)
        .map_err(WebSocketError::EncodeError)?;
    request_body
        .encode(&mut request_bytes)
        .map_err(WebSocketError::EncodeError)?;

    let resp_bytes = rpc_client
        .send_request(request_header.id, request_bytes.freeze())
        .await?;
    let resp: GetInodeResponse =
        Message::decode(resp_bytes).map_err(WebSocketError::DecodeError)?;
    Ok(resp)
}
