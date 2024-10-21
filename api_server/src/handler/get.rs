use axum::http::StatusCode;
use nss_rpc_client::rpc_client::RpcClient;
use serde::Deserialize;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct GetObjectOptions {
    #[serde(rename(deserialize = "partNumber"))]
    part_number: Option<u64>,
    #[serde(rename(deserialize = "versionId"))]
    version_id: Option<String>,
    #[serde(rename(deserialize = "response-cache-control"))]
    response_cache_control: Option<String>,
    #[serde(rename(deserialize = "response-content-disposition"))]
    response_content_disposition: Option<String>,
    #[serde(rename(deserialize = "response-content-encoding"))]
    response_content_encoding: Option<String>,
    #[serde(rename(deserialize = "response-content-language"))]
    response_content_language: Option<String>,
    #[serde(rename(deserialize = "response-content-type"))]
    response_content_type: Option<String>,
    #[serde(rename(deserialize = "response-expires"))]
    response_expires: Option<String>,
}

pub async fn get_object(
    rpc_client: &RpcClient,
    key: String,
    _opts: GetObjectOptions,
) -> Result<String, (StatusCode, String)> {
    let resp = nss_rpc_client::nss_get_inode(rpc_client, key)
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
