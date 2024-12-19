use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{object_layout::*, BlobId};
use axum::{extract::Request, http::StatusCode, response, response::IntoResponse};
use bytes::BytesMut;
use futures::StreamExt;
use http_body_util::BodyExt;
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_bss::{message::MessageHeader, RpcClientBss};
use rpc_client_nss::{rpc::put_inode_response, RpcClientNss};
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

const BLOCK_CONTENT_SIZE: usize = 2 * 1024 * 1024 - 256;

pub async fn put_object(
    request: Request,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
    blob_deletion: Sender<BlobId>,
) -> response::Result<()> {
    let blob_id = Uuid::now_v7();
    let mut body_stream = request.into_data_stream();
    let mut data_blocks = VecDeque::new();
    data_blocks.push_back(BytesMut::with_capacity(BLOCK_CONTENT_SIZE));
    while let Some(data) = body_stream.next().await {
        let mut data = data.unwrap();
        let len = std::cmp::min(
            BLOCK_CONTENT_SIZE - data_blocks.back().unwrap().len(),
            data.len(),
        );
        data_blocks.back_mut().unwrap().extend(data.split_to(len));

        if data_blocks.back().unwrap().len() == BLOCK_CONTENT_SIZE {
            data_blocks.push_back(BytesMut::with_capacity(BLOCK_CONTENT_SIZE));
            if !data.is_empty() {
                data_blocks.back_mut().unwrap().extend(data);
            }
        }
    }

    let mut size: u64 = 0;
    let mut i: u32 = 0;
    while let Some(data_block) = data_blocks.pop_front() {
        let data_block_len = dbg!(data_block.len());
        let raw_size = rpc_client_bss
            .put_blob(blob_id, i, data_block.freeze())
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
        assert_eq!(data_block_len + MessageHeader::SIZE, raw_size);
        size += data_block_len as u64;
        i += 1;
    }

    dbg!(i);
    dbg!(size);

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let etag = String::new();
    let version_id = gen_version_id();
    let block_size = 1024 * 1024;
    let object_layout = ObjectLayout {
        version_id,
        block_size,
        timestamp,
        state: ObjectState::Normal(ObjectData {
            size,
            blob_id,
            etag,
        }),
    };
    let object_layout_bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new()).unwrap();
    let resp = rpc_client_nss
        .put_inode(key, object_layout_bytes.into())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;

    // Delete old object if it is an overwrite request
    let old_object_bytes = match resp.result.unwrap() {
        put_inode_response::Result::Ok(res) => res,
        put_inode_response::Result::Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e)
                .into_response()
                .into())
        }
    };
    if !old_object_bytes.is_empty() {
        let old_object = rkyv::from_bytes::<ObjectLayout, Error>(&old_object_bytes).unwrap();
        let blob_id = old_object.blob_id();
        if let Err(e) = blob_deletion.send(blob_id).await {
            tracing::warn!("Failed to send blob {blob_id} for background deletion: {e}");
        }
    }

    Ok(())
}
