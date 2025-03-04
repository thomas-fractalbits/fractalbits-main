use std::sync::Arc;

use axum::response::{IntoResponse, Response};
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::delete_inode_response, RpcClientNss};
use tokio::sync::mpsc::Sender;

use crate::{handler::common::s3_error::S3Error, object_layout::ObjectLayout, BlobId};
use bucket_tables::bucket_table::Bucket;

pub async fn delete_object(
    bucket: Arc<Bucket>,
    key: String,
    rpc_client_nss: &RpcClientNss,
    blob_deletion: Sender<(BlobId, usize)>,
) -> Result<Response, S3Error> {
    let resp = rpc_client_nss
        .delete_inode(bucket.root_blob_name.clone(), key)
        .await?;

    let object_bytes = match resp.result.unwrap() {
        delete_inode_response::Result::Ok(res) => res,
        delete_inode_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes)?;
    // FIXME: mpu object
    let blob_id = object.blob_id()?;
    let num_blocks = object.num_blocks()?;
    if let Err(e) = blob_deletion.send((blob_id, num_blocks)).await {
        tracing::warn!(
            "Failed to send blob {blob_id} num_blocks={num_blocks} for background deletion: {e}"
        );
    }
    Ok(().into_response())
}
