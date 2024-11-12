use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Result},
};
use http_body_util::BodyExt;
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use uuid::Uuid;

pub async fn put_object(
    request: Request,
    key: String,
    rpc_client_nss: &RpcClientNss,
    _rpc_client_bss: &RpcClientBss,
) -> Result<()> {
    // Write data at first
    // TODO: async stream
    let _content = request.into_body().collect().await.unwrap().to_bytes();
    let blob_id = Uuid::now_v7().as_bytes().into();

    let _resp = rpc_client_nss::rpc::nss_put_inode(rpc_client_nss, key, blob_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    // serde_json::to_string_pretty(&resp.result)
    // .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into());
    Ok(())
}
