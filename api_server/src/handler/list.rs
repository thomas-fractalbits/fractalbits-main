use serde::Deserialize;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ListObjectsV2Options {
    #[serde(rename(deserialize = "list-type"))]
    list_type: String,
    #[serde(rename(deserialize = "continuation-token"))]
    continuation_token: Option<String>,
    delimiter: Option<String>,
    #[serde(rename(deserialize = "encoding-type"))]
    encoding_type: Option<String>,
    #[serde(rename(deserialize = "fetch-owner"))]
    fetch_owner: Option<bool>,
    #[serde(rename(deserialize = "max-keys"))]
    max_keys: Option<usize>,
    prefix: Option<String>,
    #[serde(rename(deserialize = "start-after"))]
    start_after: Option<String>,
}
