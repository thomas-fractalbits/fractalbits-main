pub mod message;
pub mod volume_types;

pub use message::MessageHeader;
pub use volume_types::{BssNode, DataVgInfo, DataVolume, QuorumConfig};

// Type alias for shared codec implementation
pub type MessageCodec = rpc_codec_common::MessageCodec<MessageHeader>;

// Re-export protobuf generated types
include!(concat!(env!("OUT_DIR"), "/rss_ops.rs"));

// Implement RpcCodec trait
use rpc_client_common::RpcCodec;
impl RpcCodec<MessageHeader> for MessageCodec {
    const RPC_TYPE: &'static str = "rss";
}
