pub mod authorization;
pub mod data;
pub mod encoding;
pub mod request;
pub mod response;
pub mod s3_error;
pub mod signature;
pub mod time;
pub mod xheader;

use crate::object_layout::ObjectLayout;
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::get_inode_response, rpc::list_inodes_response, RpcClientNss};
use s3_error::S3Error;

pub async fn get_raw_object(
    rpc_client_nss: &RpcClientNss,
    root_blob_name: String,
    key: String,
) -> Result<ObjectLayout, S3Error> {
    let resp = rpc_client_nss.get_inode(root_blob_name, key).await?;

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

    let object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes)?;
    Ok(object)
}

pub async fn list_raw_objects(
    root_blob_name: String,
    rpc_client_nss: &RpcClientNss,
    max_parts: u32,
    prefix: String,
    start_after: String,
    skip_mpu_parts: bool,
) -> Result<Vec<(String, ObjectLayout)>, S3Error> {
    let resp = rpc_client_nss
        .list_inodes(
            root_blob_name,
            max_parts,
            prefix,
            start_after,
            skip_mpu_parts,
        )
        .await?;

    // Process results
    let inodes = match resp.result.unwrap() {
        list_inodes_response::Result::Ok(res) => res.inodes,
        list_inodes_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    let mut res = Vec::with_capacity(inodes.len());
    for inode in inodes {
        let object = rkyv::from_bytes::<ObjectLayout, Error>(&inode.inode)?;
        res.push((inode.key, object));
    }
    Ok(res)
}

pub fn mpu_get_part_prefix(mut key: String, part_number: u64) -> String {
    assert_eq!(Some('\0'), key.pop());
    key.push('#');
    // if part number is 0, we treat it as object key
    if part_number != 0 {
        // part numbers range is [1, 10000], which can be encoded as 4 digits
        // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
        let part_str = format!("{:04}", part_number - 1);
        key.push_str(&part_str);
    }
    key
}

pub fn mpu_parse_part_number(mpu_key: &str, key: &str) -> u32 {
    let mut part_str = mpu_key.to_owned().split_off(key.len());
    part_str.pop(); // remove trailing '\0'
    part_str.parse::<u32>().unwrap() + 1
}
