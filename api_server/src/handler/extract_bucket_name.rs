use axum::{
    async_trait,
    extract::{FromRequestParts, Host},
    http::{request::Parts, uri::Authority, StatusCode},
    RequestPartsExt,
};

pub struct BucketName(pub String);

#[async_trait]
impl<S> FromRequestParts<S> for BucketName
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
            Ok(BucketName(bucket))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request, routing::get, Router};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn app() -> Router {
        Router::new().route("/*key", get(handler))
    }

    async fn handler(BucketName(bucket): BucketName) -> String {
        bucket
    }

    #[tokio::test]
    async fn test_extract_bucket_name_ok() {
        let bucket_name = "my-bucket";
        assert_eq!(send_request_get_body(bucket_name).await, bucket_name);
    }

    async fn send_request_get_body(bucket_name: &str) -> String {
        let body = app()
            .oneshot(
                Request::builder()
                    .uri(format!("http://{bucket_name}.localhost/obj1?query1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body();
        let bytes = body.collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }
}
