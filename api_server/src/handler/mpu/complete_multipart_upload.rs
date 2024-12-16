use crate::response_xml::Xml;
use axum::{
    extract::Request,
    http::StatusCode,
    response::{self, IntoResponse, Response},
};
use bytes::Buf;
use http_body_util::BodyExt;
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_nss::{
    rpc::{get_inode_response, put_inode_response},
    RpcClientNss,
};
use serde::{Deserialize, Serialize};

use crate::handler::{list, mpu};
use crate::object_layout::{MpuState, ObjectLayout, ObjectState};

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUpload {
    part: Vec<Part>,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
    #[serde(default)]
    checksum_crc32: String,
    #[serde(default)]
    checksum_crc32c: String,
    #[serde(default)]
    checksum_sha1: String,
    #[serde(default)]
    checksum_sha256: String,
    #[serde(rename = "ETag")]
    etag: String,
    part_number: usize,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUploadResult {
    location: String,
    bucket: String,
    key: String,
    #[serde(rename = "ETag")]
    etag: String,
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
}

pub async fn complete_multipart_upload(
    request: Request,
    bucket: String,
    mut key: String,
    upload_id: String,
    rpc_client_nss: &RpcClientNss,
) -> response::Result<Response> {
    // TODO: verify mpu parts
    let body = request.into_body().collect().await.unwrap().to_bytes();
    let _req_body: CompleteMultipartUpload = quick_xml::de::from_reader(body.reader()).unwrap();

    let resp = rpc_client_nss
        .get_inode(key.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;

    let object_bytes = match resp.result.unwrap() {
        get_inode_response::Result::Ok(res) => res,
        get_inode_response::Result::Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
        }
    };

    // TODO: check upload_id and also do more clean ups and checks
    let mut object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes).unwrap();
    let mpu_key = mpu::get_upload_part_key(key.clone(), 0);
    object.state = ObjectState::Mpu(MpuState::Completed {
        size: get_mpu_inode_size(rpc_client_nss, 10000, mpu_key).await,
        etag: upload_id.clone(),
    });
    let new_object_bytes = to_bytes_in::<_, Error>(&object, Vec::new()).unwrap();
    let resp = rpc_client_nss
        .put_inode(key.clone(), new_object_bytes.into())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    match resp.result.unwrap() {
        put_inode_response::Result::Ok(_) => {}
        put_inode_response::Result::Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
        }
    };

    let mut resp = CompleteMultipartUploadResult::default();
    key.pop();
    resp.bucket = bucket;
    resp.key = key;
    resp.etag = upload_id;
    Ok(Xml(resp).into_response())
}

async fn get_mpu_inode_size(rpc_client_nss: &RpcClientNss, max_parts: u32, mpu_key: String) -> u64 {
    let objs = list::list_raw_objects(rpc_client_nss, max_parts, mpu_key, "".into(), false)
        .await
        .unwrap();
    objs.iter().map(|x| x.size()).sum()
}
