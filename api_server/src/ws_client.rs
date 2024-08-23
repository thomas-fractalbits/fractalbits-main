use bytes::{Buf, BytesMut};
use soketto::{
    connection::Sender,
    handshake::{Client, ServerResponse},
};
use std::collections::HashMap;
use std::io::Result;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::{
    net::TcpStream,
    sync::{oneshot, RwLock},
};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub struct RpcClient {
    requests: Arc<RwLock<HashMap<u64, oneshot::Sender<BytesMut>>>>,
    sender: Arc<RwLock<Sender<Compat<TcpStream>>>>,
    next_id: AtomicU64,
}

impl RpcClient {
    pub async fn new(url: &str) -> Result<Self> {
        let socket = TcpStream::connect(url).await?;
        let mut client = Client::new(socket.compat(), url, "/");
        let (sender, mut receiver) = match client.handshake().await {
            Ok(ServerResponse::Accepted { .. }) => client.into_builder().finish(),
            Ok(ServerResponse::Redirect { .. }) => {
                todo!()
                // return Err(WebSocketTestError::Redirect);
            }
            #[allow(unused_variables)]
            Ok(ServerResponse::Rejected { status_code }) => {
                todo!()
                // return Err(WebSocketTestError::RejectedWithStatusCode(status_code))
            }
            Err(_err) => {
                todo!()
                // return Err(WebSocketTestError::Soketto(err));
            }
        };

        let requests = Arc::new(RwLock::new(HashMap::new()));
        {
            let requests_clone = requests.clone();
            tokio::spawn(async move {
                loop {
                    let mut message = Vec::new();
                    receiver.receive_data(&mut message).await.unwrap();
                    let mut buf = BytesMut::from_iter(message.iter());
                    let request_id = RpcClient::extract_request_id(&mut buf);
                    let tx: oneshot::Sender<BytesMut> =
                        requests_clone.write().await.remove(&request_id).unwrap();
                    _ = tx.send(buf);
                }
            });
        }

        Ok(Self {
            requests,
            sender: Arc::new(RwLock::new(sender)),
            next_id: AtomicU64::new(1),
        })
    }

    pub async fn send_request(&self, id: u64, msg: &[u8]) -> impl Buf {
        {
            let mut sender = self.sender.write().await;
            sender.send_binary(msg).await.unwrap();
            sender.flush().await.unwrap();
        }
        let (tx, rx) = oneshot::channel();
        self.requests.write().await.insert(id, tx);
        rx.await.unwrap()
    }

    pub fn gen_request_id(&self) -> u64 {
        let request_id = self.next_id.load(std::sync::atomic::Ordering::SeqCst);
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        request_id
    }

    fn extract_request_id(buf: &mut impl Buf) -> u64 {
        let (tag, wire_type) = prost::encoding::decode_key(buf).unwrap();
        assert_eq!(1, tag);
        assert_eq!(prost::encoding::WireType::Varint, wire_type);
        prost::encoding::decode_varint(buf).unwrap()
    }
}
