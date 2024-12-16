use axum::{extract::Request, response};
use bytes::Bytes;
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use serde::Deserialize;

use super::get_raw_object;

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
    _request: Request,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> response::Result<Bytes> {
    let object = get_raw_object(rpc_client_nss, key).await?;
    let mut content = Bytes::new();
    let _size = rpc_client_bss
        .get_blob(object.blob_id(), 0..object.size(), &mut content)
        .await
        .unwrap();
    Ok(content)
}
