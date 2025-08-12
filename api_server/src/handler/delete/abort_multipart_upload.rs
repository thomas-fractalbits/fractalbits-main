use bytes::Bytes;
use rpc_client_common::{nss_rpc_retry, rpc_retry};

use axum::{body::Body, response::Response};
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_nss::rpc::{get_inode_response, put_inode_response};

use crate::{
    handler::{common::s3_error::S3Error, ObjectRequestContext},
    object_layout::{MpuState, ObjectLayout, ObjectState},
};

pub async fn abort_multipart_upload_handler(
    ctx: ObjectRequestContext,
    _upload_id: String,
) -> Result<Response, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let rpc_timeout = ctx.app.config.rpc_timeout();
    let resp = nss_rpc_retry!(
        ctx.app,
        get_inode(&bucket.root_blob_name, &ctx.key, Some(rpc_timeout))
    )
    .await?;

    let object_bytes = match resp.result.unwrap() {
        get_inode_response::Result::Ok(res) => res,
        get_inode_response::Result::ErrNotFound(()) => {
            return Err(S3Error::NoSuchKey);
        }
        get_inode_response::Result::ErrOthers(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    // TODO: check upload_id and also do more clean ups and checks
    let mut object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes)?;
    object.state = ObjectState::Mpu(MpuState::Aborted);
    let new_object_bytes: Bytes = to_bytes_in::<_, Error>(&object, Vec::new())?.into();

    let resp = nss_rpc_retry!(
        ctx.app,
        put_inode(
            &bucket.root_blob_name,
            &ctx.key,
            new_object_bytes.clone(),
            Some(ctx.app.config.rpc_timeout())
        )
    )
    .await?;
    match resp.result.unwrap() {
        put_inode_response::Result::Ok(_) => {}
        put_inode_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    Ok(Response::new(Body::empty()))
}
