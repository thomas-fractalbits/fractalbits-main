use axum::{
    async_trait,
    extract::{FromRequestParts, Host},
    http::{request::Parts, uri::Authority, StatusCode},
    RequestPartsExt,
};

pub struct ExtractBucketName(pub String);

#[async_trait]
impl<S> FromRequestParts<S> for ExtractBucketName
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Host(host) = parts.extract::<Host>().await.unwrap();
        // Note current axum (0.7.7)'s host contains port information
        let authority: Authority = host.parse::<Authority>().unwrap();
        let mut found_dot = false;
        let bucket: String = authority
            .host()
            .chars()
            .into_iter()
            .take_while(|x| {
                let is_dot = x == &'.';
                if is_dot {
                    found_dot = true;
                }
                !is_dot
            })
            .collect();
        if !found_dot || bucket.is_empty() {
            Err((StatusCode::BAD_REQUEST, "Invalid bucket name"))
        } else {
            Ok(ExtractBucketName(bucket))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_bucket_name() {}
}
