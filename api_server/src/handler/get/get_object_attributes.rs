use crate::handler::{
    common::{
        get_raw_object, response::xml::Xml, s3_error::S3Error, signature::checksum::ChecksumValue,
        time,
    },
    Request,
};
use axum::{
    http::HeaderValue,
    response::{IntoResponse, Response},
};
use base64::{prelude::BASE64_STANDARD, Engine};
use bucket_tables::bucket_table::Bucket;
use rpc_client_nss::RpcClientNss;
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetObjectAttributesOptions {
    #[serde(rename(deserialize = "versionId"))]
    version_id: Option<String>,
    response_cache_control: Option<String>,
    response_content_disposition: Option<String>,
    response_content_encoding: Option<String>,
    response_content_language: Option<String>,
    response_content_type: Option<String>,
    response_expires: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct GetObjectAttributesOutput {
    etag: String,
    checksum: CheckSum,
    object_parts: ObjectParts,
    storage_class: String,
    object_size: usize,
}

impl GetObjectAttributesOutput {
    fn etag(self, etag: String) -> Self {
        Self { etag, ..self }
    }

    fn object_size(self, object_size: usize) -> Self {
        Self {
            object_size,
            ..self
        }
    }

    fn checksum(mut self, checksum: Option<ChecksumValue>) -> Self {
        match checksum {
            Some(ChecksumValue::Crc32(crc32)) => {
                self.checksum.checksum_crc32 = BASE64_STANDARD.encode(crc32);
                self.checksum.checksum_type = "CRC32".to_string();
            }
            Some(ChecksumValue::Crc32c(crc32c)) => {
                self.checksum.checksum_crc32c = BASE64_STANDARD.encode(crc32c);
                self.checksum.checksum_type = "CRC32C".to_string();
            }
            Some(ChecksumValue::Sha1(sha1)) => {
                self.checksum.checksum_sha1 = BASE64_STANDARD.encode(sha1);
                self.checksum.checksum_type = "SHA1".to_string();
            }
            Some(ChecksumValue::Sha256(sha256)) => {
                self.checksum.checksum_sha256 = BASE64_STANDARD.encode(sha256);
                self.checksum.checksum_type = "SHA256".to_string();
            }
            None => (),
        }
        self
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CheckSum {
    #[serde(rename = "ChecksumCRC32")]
    checksum_crc32: String,
    #[serde(rename = "ChecksumCRC32C")]
    checksum_crc32c: String,
    #[serde(rename = "ChecksumCRC64NVME")]
    checksum_crc64nvme: String,
    #[serde(rename = "ChecksumSHA1")]
    checksum_sha1: String,
    #[serde(rename = "ChecksumSHA256")]
    checksum_sha256: String,
    checksum_type: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ObjectParts {
    is_truncated: bool,
    max_parts: usize,
    next_part_number_marker: usize,
    part_number_marker: usize,
    part: Part,
    parts_count: usize,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
    part_number: usize,
    size: usize,
}

#[allow(dead_code)]
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct ResponseHeaders {
    #[serde(rename = "Last-Modified")]
    last_modified: Option<String>, // timestamp
    x_amz_delete_marker: Option<String>,
    x_amz_request_charged: Option<String>,
    x_amz_version_id: Option<String>,
}

pub async fn get_object_attributes(
    _request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
) -> Result<Response, S3Error> {
    let obj = get_raw_object(rpc_client_nss, bucket.root_blob_name.clone(), key.clone()).await?;
    let etag = obj.etag()?;
    let object_size = obj.size()? as usize;
    let checksum = obj.checksum()?;
    let last_modified = time::format_http_date(obj.timestamp);

    let mut resp = Xml(GetObjectAttributesOutput::default()
        .etag(etag)
        .object_size(object_size)
        .checksum(checksum))
    .into_response();
    resp.headers_mut().insert(
        "Last-Modified",
        HeaderValue::from_str(&last_modified).unwrap(),
    );
    Ok(resp)
}
