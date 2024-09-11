use bytes::Bytes;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::{
    self,
    io::{ReadHalf, WriteHalf},
    net::TcpStream,
    sync::{oneshot, RwLock},
};

use crate::message::MessageHeader;

#[derive(Error, Debug)]
#[error(transparent)]
pub enum RpcError {
    IoError(io::Error),
    OneshotRecvError(oneshot::error::RecvError),
    EncodeError(prost::EncodeError),
    DecodeError(prost::DecodeError),
    #[error("internal request sending error: {0}")]
    InternalRequestError(String),
    #[error("internal response sending error: {0}")]
    InternalResponseError(String),
}

impl From<io::Error> for RpcError {
    fn from(err: io::Error) -> Self {
        RpcError::IoError(err)
    }
}

pub struct RpcClient {
    requests: Arc<RwLock<HashMap<u32, oneshot::Sender<Bytes>>>>,
    sender: tokio::sync::mpsc::Sender<Bytes>,
    next_id: AtomicU32,
}

impl RpcClient {
    pub async fn new(url: &str) -> Result<Self, RpcError> {
        let stream = TcpStream::connect(url).await?;
        stream.set_nodelay(true)?;
        let (receiver, sender) = tokio::io::split(stream);

        // Start message receiver task, for rpc responses
        let requests = Arc::new(RwLock::new(HashMap::new()));
        {
            let requests_clone = requests.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::receive_message_task(receiver, requests_clone).await {
                    tracing::error!("FATAL: receive message task error: {e:?}");
                }
            });
        }

        // Start message sender task, to send rpc requests. We are launching a dedicated task here
        // to reduce lock contention on the sender socket itself.
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        {
            tokio::spawn(async move {
                if let Err(e) = Self::send_message_task(sender, rx).await {
                    tracing::error!("FATAL: receive message task error: {e:?}");
                }
            });
        }

        Ok(Self {
            requests,
            sender: tx,
            next_id: AtomicU32::new(1),
        })
    }

    async fn receive_message_task(
        mut receiver: ReadHalf<TcpStream>,
        requests: Arc<RwLock<HashMap<u32, oneshot::Sender<Bytes>>>>,
    ) -> Result<(), RpcError> {
        loop {
            let mut buffer = vec![0; 1024 * 10];
            let received = match receiver.read(&mut buffer).await {
                Ok(n) => {
                    if n == 0 {
                        tracing::warn!("socket is closed");
                        break Ok(());
                    }
                    n
                }
                Err(e) => {
                    tracing::warn!("receiving error from server: {e}");
                    break Ok(());
                }
            };
            if received <= MessageHeader::encode_len() {
                tracing::warn!("received {received} bytes");
            }
            let mut bytes = Bytes::copy_from_slice(&buffer[0..received]);
            let header = MessageHeader::decode(&mut bytes);
            let tx: oneshot::Sender<Bytes> = match requests.write().await.remove(&header.id) {
                Some(tx) => tx,
                None => continue, // we may have received the response already
            };
            let _ = tx.send(bytes);
        }
    }

    async fn send_message_task(
        mut sender: WriteHalf<TcpStream>,
        mut input: tokio::sync::mpsc::Receiver<Bytes>,
    ) -> Result<(), RpcError> {
        while let Some(message) = input.recv().await {
            sender.write(message.as_ref()).await.unwrap();
        }
        Ok(())
    }

    pub async fn send_request(&self, id: u32, msg: Bytes) -> Result<Bytes, RpcError> {
        self.sender
            .send(msg)
            .await
            .map_err(|e| RpcError::InternalRequestError(e.to_string()))?;
        tracing::info!("request sent from handler: request_id={id}");

        let (tx, rx) = oneshot::channel();
        self.requests.write().await.insert(id, tx);

        rx.await.map_err(RpcError::OneshotRecvError)
    }

    pub fn gen_request_id(&self) -> u32 {
        let request_id = self.next_id.load(std::sync::atomic::Ordering::SeqCst);
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        request_id
    }
}
