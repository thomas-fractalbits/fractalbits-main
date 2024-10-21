use serde::Deserialize;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct DeleteObjectOptions {
    #[serde(rename(deserialize = "versionId"))]
    version_id: Option<String>,
}
