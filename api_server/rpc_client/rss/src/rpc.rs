use std::time::Instant;

use crate::{
    message::MessageHeader,
    rpc_client::{Message, RpcClient, RpcError},
};
use bytes::BytesMut;
use metrics::histogram;
use prost::Message as PbMessage;

include!(concat!(env!("OUT_DIR"), "/rss_ops.rs"));

impl RpcClient {
    pub async fn put(&self, version: i64, key: String, value: String) -> Result<(), RpcError> {
        let start = Instant::now();
        let body = PutRequest {
            version,
            key,
            value,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::Put;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: PutResponse = PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            put_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "Put_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            put_response::Result::ErrOthers(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Put_ErrOthers")
                    .record(duration.as_nanos() as f64);
                Err(RpcError::InternalResponseError(resp))
            }
            put_response::Result::ErrRetry(()) => {
                histogram!("rss_rpc_nanos", "status" => "Put_ErrRetry")
                    .record(duration.as_nanos() as f64);
                Err(RpcError::Retry)
            }
        }
    }

    pub async fn put_with_extra(
        &self,
        version: i64,
        key: String,
        value: String,
        extra_version: i64,
        extra_key: String,
        extra_value: String,
    ) -> Result<(), RpcError> {
        let start = Instant::now();
        let body = PutWithExtraRequest {
            version,
            key,
            value,
            extra_version,
            extra_key,
            extra_value,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::PutWithExtra;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: PutWithExtraResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            put_with_extra_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "PutWithExtra_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            put_with_extra_response::Result::ErrOthers(resp) => {
                histogram!("rss_rpc_nanos", "status" => "PutWithExtra_ErrOthers")
                    .record(duration.as_nanos() as f64);
                Err(RpcError::InternalResponseError(resp))
            }
            put_with_extra_response::Result::ErrRetry(()) => {
                histogram!("rss_rpc_nanos", "status" => "PutWithExtra_ErrRetry")
                    .record(duration.as_nanos() as f64);
                Err(RpcError::Retry)
            }
        }
    }

    pub async fn get(&self, key: String) -> Result<(i64, String), RpcError> {
        let start = Instant::now();
        let body = GetRequest { key };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::Get;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: GetResponse = PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            get_response::Result::Ok(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Get_Ok")
                    .record(duration.as_nanos() as f64);
                Ok((resp.version, resp.value))
            }
            get_response::Result::ErrNotFound(_resp) => {
                histogram!("rss_rpc_nanos", "status" => "Get_ErrNotFound")
                    .record(duration.as_nanos() as f64);
                Err(RpcError::NotFound)
            }
            get_response::Result::ErrOthers(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Get_ErrOthers")
                    .record(duration.as_nanos() as f64);
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }

    pub async fn delete(&self, key: String) -> Result<(), RpcError> {
        let start = Instant::now();
        let body = DeleteRequest { key };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::Delete;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: DeleteResponse = PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            delete_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "Delete_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            delete_response::Result::Err(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Delete_Err")
                    .record(duration.as_nanos() as f64);
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }

    pub async fn delete_with_extra(
        &self,
        key: String,
        extra_version: i64,
        extra_key: String,
        extra_value: String,
    ) -> Result<(), RpcError> {
        let start = Instant::now();
        let body = DeleteWithExtraRequest {
            key,
            extra_version,
            extra_key,
            extra_value,
        };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::DeleteWithExtra;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: DeleteWithExtraResponse =
            PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            delete_with_extra_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "DeleteWithExtra_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            delete_with_extra_response::Result::ErrOthers(resp) => {
                histogram!("rss_rpc_nanos", "status" => "DeleteWithExtra_ErrOthers")
                    .record(duration.as_nanos() as f64);
                Err(RpcError::InternalResponseError(resp))
            }
            delete_with_extra_response::Result::ErrRetry(()) => {
                histogram!("rss_rpc_nanos", "status" => "DeleteWithExtra_ErrRetry")
                    .record(duration.as_nanos() as f64);
                Err(RpcError::Retry)
            }
        }
    }

    pub async fn list(&self, prefix: String) -> Result<Vec<String>, RpcError> {
        let start = Instant::now();
        let body = ListRequest { prefix };

        let mut header = MessageHeader::default();
        header.id = self.gen_request_id();
        header.command = Command::List;
        header.size = (MessageHeader::SIZE + body.encoded_len()) as u32;

        let mut request_bytes = BytesMut::with_capacity(header.size as usize);
        header.encode(&mut request_bytes);
        body.encode(&mut request_bytes)
            .map_err(RpcError::EncodeError)?;

        let resp_bytes = self
            .send_request(header.id, Message::Bytes(request_bytes.freeze()))
            .await?
            .body;
        let resp: ListResponse = PbMessage::decode(resp_bytes).map_err(RpcError::DecodeError)?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            list_response::Result::Ok(resp) => {
                histogram!("rss_rpc_nanos", "status" => "List_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(resp.kvs)
            }
            list_response::Result::Err(resp) => {
                histogram!("rss_rpc_nanos", "status" => "List_Err")
                    .record(duration.as_nanos() as f64);
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }
}
