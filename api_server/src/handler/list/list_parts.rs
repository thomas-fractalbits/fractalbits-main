use crate::response_xml::Xml;
use axum::{
    extract::{Query, Request},
    http::StatusCode,
    response::{self, IntoResponse, Response},
    RequestExt,
};
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::list_inodes_response, RpcClientNss};
use serde::{Deserialize, Serialize};

use crate::handler::mpu;
use crate::object_layout::ObjectLayout;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListPartsOptions {
    max_parts: Option<u32>,
    part_number_marker: Option<usize>,
    upload_id: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListPartsResult {
    bucket: String,
    key: String,
    upload_id: String,
    part_number_marker: usize,
    next_part_number_marker: usize,
    max_parts: usize,
    is_truncated: bool,
    part: Part,
    initiator: Initiator,
    owner: Owner,
    storage_class: String,
    checksum_algorithm: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
    etag: String,
    last_modified: String, // timestamp
    part_number: usize,
    size: usize,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Initiator {
    display_name: String,
    id: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Owner {
    display_name: String,
    id: String,
}

pub async fn list_parts(
    mut request: Request,
    key: String,
    rpc_client_nss: &RpcClientNss,
) -> response::Result<Response> {
    let Query(opts): Query<ListPartsOptions> = request.extract_parts().await?;
    let max_parts = opts.max_parts.unwrap_or(1000);
    // TODO: check upload_id
    let _upload_id = match opts.upload_id {
        Some(upload_id) => upload_id,
        None => return Err((StatusCode::BAD_REQUEST, "missing upload id").into()),
    };

    let prefix = mpu::get_upload_part_key(key, 0);
    let resp = rpc_client_nss
        .list_inodes(max_parts, prefix, "".into())
        .await
        .unwrap();

    // Process results
    let inodes = match resp.result.unwrap() {
        list_inodes_response::Result::Ok(res) => res.inodes,
        list_inodes_response::Result::Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
        }
    };

    for inode in inodes {
        let _object = rkyv::from_bytes::<ObjectLayout, Error>(&inode.inode).unwrap();
        // dbg!(&inode.key);
        // dbg!(object.timestamp);
        // dbg!(object.size);
    }
    Ok(Xml(ListPartsResult::default()).into_response())
}
