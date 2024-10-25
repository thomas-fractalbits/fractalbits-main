use axum::{
    extract::{Query, Request},
    http::StatusCode,
    response::{IntoResponse, Result},
    RequestExt,
};
use nss_rpc_client::rpc_client::RpcClient;
use serde::Deserialize;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetObjectOptions {
    #[serde(rename(deserialize = "partNumber"))]
    part_number: Option<u64>,
    #[serde(rename(deserialize = "versionId"))]
    version_id: Option<String>,
    response_cache_control: Option<String>,
    response_content_disposition: Option<String>,
    response_content_encoding: Option<String>,
    response_content_language: Option<String>,
    response_content_type: Option<String>,
    response_expires: Option<String>,
}

pub async fn get_object(
    mut request: Request,
    key: String,
    rpc_client: &RpcClient,
) -> Result<String> {
    let Query(_get_obj_opts): Query<GetObjectOptions> = request.extract_parts().await?;
    let resp = nss_rpc_client::nss_get_inode(rpc_client, key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    serde_json::to_string_pretty(&resp.result)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into())
}
