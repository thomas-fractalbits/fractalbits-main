use std::convert::Infallible;

use axum::{async_trait, extract::FromRequestParts, http::request::Parts};

pub struct Key(pub String);

#[async_trait]
impl<S> FromRequestParts<S> for Key
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match parts.uri.path() {
            "/" => Ok(Key("/".into())),
            key => {
                // nss requires '\0' for now
                let mut key = key.to_owned();
                key.push('\0');
                Ok(Key(key))
            }
        }
    }
}
