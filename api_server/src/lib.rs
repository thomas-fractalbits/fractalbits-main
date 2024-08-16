mod utils;
mod ws_client;

pub use ws_client::rpc_to_nss;

pub mod nss_ops {
    include!(concat!(env!("OUT_DIR"), "/nss_ops.rs"));
}

pub fn nss_put_key(key: String, value: String) -> nss_ops::RawKeyValueOps {
    let mut kv_op = nss_ops::RawKeyValueOps::default();
    kv_op.set_op(nss_ops::raw_key_value_ops::Op::Put);
    kv_op.key = key;
    kv_op.value = value;
    kv_op
}

pub fn nss_get_key(key: String) -> nss_ops::RawKeyValueOps {
    let mut kv_op = nss_ops::RawKeyValueOps::default();
    kv_op.set_op(nss_ops::raw_key_value_ops::Op::Get);
    kv_op.key = key;
    kv_op
}
