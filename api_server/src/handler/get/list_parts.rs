use crate::handler::common::mpu_parse_part_number;
use crate::handler::common::{
    get_raw_object, list_raw_objects, mpu_get_part_prefix, response::xml::Xml, s3_error::S3Error,
    signature::checksum::ChecksumValue, time,
};
use crate::object_layout::{MpuState, ObjectState};
use axum::{extract::Query, response::Response, RequestPartsExt};
use base64::prelude::*;
use bucket_tables::bucket_table::Bucket;
use rpc_client_nss::RpcClientNss;
use serde::{Deserialize, Serialize};

use crate::handler::Request;

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
    #[serde(skip_serializing_if = "Option::is_none")]
    part_number_marker: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_part_number_marker: Option<u32>,
    max_parts: u32,
    is_truncated: bool,
    part: Vec<Part>,
    #[serde(skip_serializing_if = "Option::is_none")]
    initiator: Option<Initiator>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<Owner>,
    storage_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum_algorithm: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum_type: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
    #[serde(rename = "ChecksumCRC32", skip_serializing_if = "Option::is_none")]
    checksum_crc32: Option<String>,
    #[serde(rename = "ChecksumCRC32C", skip_serializing_if = "Option::is_none")]
    checksum_crc32c: Option<String>,
    #[serde(rename = "ChecksumSHA1", skip_serializing_if = "Option::is_none")]
    checksum_sha1: Option<String>,
    #[serde(rename = "ChecksumSHA256", skip_serializing_if = "Option::is_none")]
    checksum_sha256: Option<String>,
    #[serde(rename = "ETag")]
    etag: String,
    last_modified: String, // timestamp
    part_number: u32,
    size: u64,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Initiator {
    display_name: String,
    #[serde(rename = "ID")]
    id: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Owner {
    display_name: String,
    #[serde(rename = "ID")]
    id: String,
}

pub async fn list_parts_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
) -> Result<Response, S3Error> {
    let Query(opts): Query<ListPartsOptions> = request.into_parts().0.extract().await?;
    let max_parts = opts.max_parts.unwrap_or(1000);
    let upload_id = opts.upload_id;
    let object = get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key.clone()).await?;
    if object.version_id.simple().to_string() != upload_id {
        return Err(S3Error::NoSuchUpload);
    }
    if ObjectState::Mpu(MpuState::Uploading) != object.state {
        return Err(S3Error::InvalidObjectState);
    }

    let mpu_prefix = mpu_get_part_prefix(key.clone(), 0);
    let mpus = list_raw_objects(
        bucket.root_blob_name.clone(),
        rpc_client_nss,
        max_parts,
        mpu_prefix,
        "".into(),
        false,
    )
    .await?;
    let mut res = ListPartsResult {
        upload_id,
        ..Default::default()
    };
    for (mpu_key, mpu) in mpus {
        let last_modified = time::format_timestamp(mpu.timestamp);
        let etag = mpu.etag()?;
        let part_number = mpu_parse_part_number(&mpu_key, &key)?;
        let size = mpu.size()?;
        let mut part = Part {
            last_modified,
            etag,
            size,
            part_number,
            ..Default::default()
        };
        if let Some(checksum) = mpu.checksum()? {
            match checksum {
                ChecksumValue::Crc32(x) => part.checksum_crc32 = Some(BASE64_STANDARD.encode(x)),
                ChecksumValue::Crc32c(x) => part.checksum_crc32c = Some(BASE64_STANDARD.encode(x)),
                ChecksumValue::Sha1(x) => part.checksum_sha1 = Some(BASE64_STANDARD.encode(x)),
                ChecksumValue::Sha256(x) => part.checksum_sha256 = Some(BASE64_STANDARD.encode(x)),
            }
        }
        res.part.push(part);
    }
    Xml(res).try_into()
}
