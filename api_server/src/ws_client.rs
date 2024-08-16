use crate::utils;
use tokio::io::*;
use web_socket::*;

pub async fn rpc_to_nss(msg: &str) -> Result<()> {
    echo(utils::connect("127.0.0.1:9224", "/").await?, msg).await
}

async fn echo<IO>(mut ws: WebSocket<IO>, msg: &str) -> Result<()>
where
    IO: Unpin + AsyncRead + AsyncWrite,
{
    // let _ = ws.recv().await?; // ignore message: Request served by 4338e324
    for _ in 0..3 {
        ws.send(msg).await?;
        match ws.recv().await? {
            Event::Data { ty, data } => {
                assert!(matches!(ty, DataType::Complete(MessageType::Text)));
                println!("received msg from nss: {}", String::from_utf8_lossy(&*data));
            }
            Event::Ping(data) => ws.send_pong(data).await?,
            Event::Pong(..) => {}
            Event::Error(..) => return ws.close(CloseCode::ProtocolError).await,
            Event::Close { .. } => return ws.close(()).await,
        }
    }
    ws.close("bye!").await
}
