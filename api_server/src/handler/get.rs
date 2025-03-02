mod get_object;
mod get_object_attributes;

pub use get_object::get_object;
pub use get_object_attributes::get_object_attributes;

use crate::object_layout::ObjectLayout;
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::get_inode_response, RpcClientNss};

use super::common::s3_error::S3Error;

pub async fn get_raw_object(
    rpc_client_nss: &RpcClientNss,
    root_blob_name: String,
    key: String,
) -> Result<ObjectLayout, S3Error> {
    let resp = rpc_client_nss.get_inode(root_blob_name, key).await?;

    let object_bytes = match resp.result.unwrap() {
        get_inode_response::Result::Ok(res) => res,
        get_inode_response::Result::ErrNotFound(_e) => {
            return Err(S3Error::NoSuchKey);
        }
        get_inode_response::Result::ErrOthers(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes)?;
    Ok(object)
}
