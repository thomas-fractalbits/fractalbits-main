use bytes::BytesMut;
use nss_ops::*;
use prost::Message;
mod ws_client;
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
    let request = PutInodeRequest {
        method: Method::PutInode.into(),
        id: rpc_client.gen_request_id(),
        key,
        value,
    };
    let mut request_bytes = BytesMut::with_capacity(request.encoded_len());
    request
        .encode(&mut request_bytes)
        .map_err(WebSocketError::EncodeError)?;

    let resp_bytes = rpc_client
        .send_request(request.id, request_bytes.freeze())
        .await?;
    let mut resp: PutInodeResponse =
        Message::decode(resp_bytes.as_slice()).map_err(WebSocketError::DecodeError)?;
    resp.id = request.id; // id has already been decoded and verified in ws_client receiving task
    Ok(resp)
}

pub async fn nss_get_inode(
    rpc_client: &RpcClient,
    key: String,
) -> Result<GetInodeResponse, WebSocketError> {
    let request = GetInodeRequest {
        method: Method::GetInode.into(),
        id: rpc_client.gen_request_id(),
        key,
    };
    let mut request_bytes = BytesMut::with_capacity(request.encoded_len());
    request
        .encode(&mut request_bytes)
        .map_err(WebSocketError::EncodeError)?;

    let resp_bytes = rpc_client
        .send_request(request.id, request_bytes.freeze())
        .await?;
    let mut resp: GetInodeResponse =
        Message::decode(&mut resp_bytes.as_slice()).map_err(WebSocketError::DecodeError)?;
    resp.id = request.id; // id has already been decoded and verified in ws_client receiving task
    Ok(resp)
}
