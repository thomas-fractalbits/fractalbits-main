use axum::{
    extract::Request,
    http::StatusCode,
    response::{self, IntoResponse},
};
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::{
    rpc::{get_inode_response, put_inode_response},
    RpcClientNss,
};

use crate::object_layout::{MpuState, ObjectLayout, ObjectState};

pub async fn abort_multipart_upload(
    _request: Request,
    bucket: String,
    key: String,
    _upload_id: String,
    rpc_client_nss: &RpcClientNss,
    _rpc_client_bss: &RpcClientBss,
) -> response::Result<()> {
    let resp = rpc_client_nss
        .get_inode(bucket.clone(), key.clone())
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
    object.state = ObjectState::Mpu(MpuState::Aborted);
    let new_object_bytes = to_bytes_in::<_, Error>(&object, Vec::new()).unwrap();

    let resp = rpc_client_nss
        .put_inode(bucket, key, new_object_bytes.into())
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

    Ok(())
}
