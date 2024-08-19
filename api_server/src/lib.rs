mod utils;
mod ws_client;
use nss_ops::*;
use prost::Message;
use ws_client::RpcClient;

#[macro_export]
macro_rules! io_err {
    [$kind: ident, $msg: expr] => {
        return Err(std::io::Error::new(std::io::ErrorKind::$kind, $msg))
    };
}

pub mod nss_ops {
    include!(concat!(env!("OUT_DIR"), "/nss_ops.rs"));
}

pub async fn nss_put_inode(key: String, value: String) -> PutInodeResponse {
    let mut request = PutInodeRequest::default();
    request.method = Method::PutInode.into();
    request.id = 100; // FIXME: set from RpcClient inside
    request.key = key;
    request.value = value;
    let mut request_bytes = Vec::<u8>::new();
    request.encode(&mut request_bytes).unwrap();

    let mut rpc_client = RpcClient::new("127.0.0.1", 9224).await.unwrap();
    let resp_bytes = rpc_client.send_request(&request_bytes).await.unwrap();
    Message::decode(resp_bytes.as_bytes()).unwrap()
}

pub async fn nss_get_inode(key: String) -> GetInodeResponse {
    let mut request = GetInodeRequest::default();
    request.method = Method::GetInode.into();
    request.id = 1;
    request.key = key;
    let mut request_bytes = Vec::<u8>::new();
    request.encode(&mut request_bytes).unwrap();

    let mut rpc_client = RpcClient::new("127.0.0.1", 9224).await.unwrap();
    let resp_bytes = rpc_client.send_request(&request_bytes).await.unwrap();
    Message::decode(resp_bytes.as_bytes()).unwrap()
}
