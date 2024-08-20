use api_server::{nss_get_inode, nss_ops, nss_put_inode};
use axum::{routing::post, Router};
use axum_extra::protobuf::Protobuf;

async fn get_obj() -> Protobuf<nss_ops::GetInodeResponse> {
    nss_get_inode("hello".into()).await.into()
}

async fn put_obj() -> Protobuf<nss_ops::PutInodeResponse> {
    let key: String = "hello".into();
    let value: String = key.chars().rev().collect();
    nss_put_inode(key, value).await.into()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let app = Router::new()
        .route("/get_obj", post(get_obj))
        .route("/put_obj", post(put_obj));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
