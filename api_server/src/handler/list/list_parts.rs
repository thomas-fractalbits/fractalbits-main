use crate::{object_layout::ObjectState, response_xml::Xml};
use axum::{
    extract::{Query, Request},
    response::{self, IntoResponse, Response},
    RequestExt,
};
use rpc_client_nss::RpcClientNss;
use serde::{Deserialize, Serialize};

use crate::handler::mpu;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListPartsOptions {
    max_parts: Option<u32>,
    part_number_marker: Option<usize>,
    #[serde(rename = "uploadId")]
    upload_id: String,
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
    part: Vec<Part>,
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
    size: u64,
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
    let upload_id = opts.upload_id;
    let mpu_key = mpu::get_upload_part_key(key, 0);
    let mpus =
        super::list_raw_objects(rpc_client_nss, max_parts, mpu_key, "".into(), false).await?;
    dbg!(mpus.len());
    let mut res = ListPartsResult::default();
    res.upload_id = upload_id;
    for mpu in mpus {
        let mut part = Part::default();
        if let ObjectState::Normal(obj) = mpu.state {
            part.etag = obj.etag;
            part.size = obj.size;
        }
        res.part.push(part);
    }
    Ok(Xml(res).into_response())
}
