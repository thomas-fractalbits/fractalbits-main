use axum::{
    extract::{Query, Request},
    http::StatusCode,
    response::{self, IntoResponse},
    RequestExt,
};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::{rpc::get_inode_response, RpcClientNss};
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
    rpc_client_nss: &RpcClientNss,
    _rpc_client_bss: &RpcClientBss,
) -> response::Result<Vec<u8>> {
    let Query(_get_obj_opts): Query<GetObjectOptions> = request.extract_parts().await?;
    let resp = rpc_client_nss::rpc::nss_get_inode(rpc_client_nss, key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    match resp.result.unwrap() {
        get_inode_response::Result::Ok(res) => Ok(res),
        get_inode_response::Result::Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)
            .into_response()
            .into()),
    }
}
