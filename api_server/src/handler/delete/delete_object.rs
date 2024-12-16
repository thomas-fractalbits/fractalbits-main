use axum::{
    http::StatusCode,
    response::{self, IntoResponse},
};
use rkyv::{self, rancor::Error};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::{rpc::delete_inode_response, RpcClientNss};

use crate::object_layout::ObjectLayout;

pub async fn delete_object(
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> response::Result<()> {
    let resp = rpc_client_nss
        .delete_inode(key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;

    let object_bytes = match resp.result.unwrap() {
        delete_inode_response::Result::Ok(res) => res,
        delete_inode_response::Result::Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
        }
    };

    let object = rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes).unwrap();
    rpc_client_bss.delete_blob(object.blob_id()).await.unwrap();
    Ok(())
}
