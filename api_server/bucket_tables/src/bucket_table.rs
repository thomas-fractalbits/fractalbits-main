#![allow(dead_code)]
use super::permission::BucketKeyPerm;
use super::table::{Entry, TableSchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct Bucket {
    pub bucket_name: String,
    pub creation_date: u64,
    pub authorized_keys: HashMap<String /* ApiKey id */, BucketKeyPerm>,
    pub root_blob_name: String,
}

impl Bucket {
    pub fn new(bucket_name: String, root_blob_name: String) -> Self {
        Self {
            bucket_name,
            creation_date: 0,
            authorized_keys: HashMap::new(),
            root_blob_name,
        }
    }
}

impl Entry for Bucket {
    fn key(&self) -> String {
        self.bucket_name.clone()
    }
}

pub struct BucketTable;

impl TableSchema for BucketTable {
    const TABLE_NAME: &'static str = "buckets";

    type E = Bucket;
}
