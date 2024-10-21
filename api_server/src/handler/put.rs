use axum::http::StatusCode;
use nss_rpc_client::rpc_client::RpcClient;

pub async fn put_object(
    rpc_client: &RpcClient,
    key: String,
    value: String,
) -> Result<String, (StatusCode, String)> {
    let resp = nss_rpc_client::nss_put_inode(rpc_client, key, value)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        .unwrap();
    match serde_json::to_string_pretty(&resp.result) {
        Ok(resp) => Ok(resp),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal server error: {e}"),
        )),
    }
}
