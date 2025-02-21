#![allow(dead_code)]
use super::permission::BucketKeyPerm;
use super::table::{Entry, TableSchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiKey {
    pub key_id: String,
    pub secret_key: String,
    pub name: String,
    pub allow_create_bucket: bool,
    pub authorized_buckets: HashMap<String /* bucket name */, BucketKeyPerm>,
    pub is_deleted: bool,
}

impl ApiKey {
    pub fn new(name: &str) -> Self {
        let key_id = format!("{}", hex::encode(&rand::random::<[u8; 12]>()[..]));
        let secret_key = hex::encode(&rand::random::<[u8; 32]>()[..]);
        Self {
            key_id,
            secret_key,
            name: name.to_string(),
            allow_create_bucket: true,
            authorized_buckets: HashMap::new(),
            is_deleted: false,
        }
    }
}

impl Entry for ApiKey {
    fn key(&self) -> String {
        self.key_id.clone()
    }
}

pub struct ApiKeyTable;

impl TableSchema for ApiKeyTable {
    const TABLE_NAME: &'static str = "api_keys";

    type E = ApiKey;
}
