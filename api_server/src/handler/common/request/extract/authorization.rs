#![allow(dead_code)]
use std::collections::BTreeSet;
use std::collections::HashMap;

use crate::handler::common::time::{LONG_DATETIME, SHORT_DATE};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{
    extract::FromRequestParts,
    http::{
        header::{HeaderName, ToStrError, AUTHORIZATION},
        request::Parts,
    },
};
use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};
use thiserror::Error;

const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";
const SCOPE_ENDING: &str = "aws4_request";
const X_AMZ_CONTENT_SHA256: HeaderName = HeaderName::from_static("x-amz-content-sha256");
const X_AMZ_DATE: HeaderName = HeaderName::from_static("x-amz-date");

#[derive(Error, Debug)]
pub enum AuthError {
    #[error(transparent)]
    ToStrError(#[from] ToStrError),
    #[error("invalid format: {0}")]
    Invalid(String),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AuthError::ToStrError(s) => (
                StatusCode::BAD_REQUEST,
                format!("Invalid string in authorization header: {s}"),
            )
                .into_response(),
            AuthError::Invalid(s) => (
                StatusCode::BAD_REQUEST,
                format!("Invalid authorization header: {s}"),
            )
                .into_response(),
        }
    }
}

pub struct AuthorizationFromReq(pub Option<Authorization>);

#[derive(Debug)]
pub struct Authorization {
    pub key_id: String,
    pub scope: Scope,
    pub signed_headers: BTreeSet<String>,
    pub signature: String,
    pub content_sha256: String,
    pub date: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Scope {
    pub date: String,
    pub region: String,
    pub service: String,
}

impl Scope {
    pub fn to_sign_string(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.date, self.region, self.service, SCOPE_ENDING
        )
    }
}

impl<S> FromRequestParts<S> for AuthorizationFromReq
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    // TODO: better error handling
    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let authorization = match parts.headers.get(AUTHORIZATION) {
            Some(auth) => auth.to_str()?,
            None => return Ok(Self(None)),
        };

        let (auth_kind, rest) = authorization
            .split_once(' ')
            .ok_or(AuthError::Invalid("Authorization field too short".into()))?;

        if auth_kind != AWS4_HMAC_SHA256 {
            return Err(AuthError::Invalid(
                "Unsupported authorization method".into(),
            ));
        }

        let mut auth_params = HashMap::new();
        for auth_part in rest.split(',') {
            let auth_part = auth_part.trim();
            let eq = auth_part
                .find('=')
                .ok_or(AuthError::Invalid("missing ,".into()))?;
            let (key, value) = auth_part.split_at(eq);
            auth_params.insert(key.to_string(), value.trim_start_matches('=').to_string());
        }

        let cred = auth_params.get("Credential").ok_or(AuthError::Invalid(
            "Could not find Credential in Authorization field".into(),
        ))?;
        let signed_headers = auth_params
            .get("SignedHeaders")
            .ok_or(AuthError::Invalid(
                "Could not find SignedHeaders in Authorization field".into(),
            ))?
            .split(';')
            .map(|s| s.to_string())
            .collect();
        let signature = auth_params
            .get("Signature")
            .ok_or(AuthError::Invalid(
                "Could not find Signature in Authorization field".into(),
            ))?
            .to_string();

        let content_sha256 = parts
            .headers
            .get(X_AMZ_CONTENT_SHA256)
            .ok_or(AuthError::Invalid(
                "Missing x-amz-content-sha256 field".into(),
            ))?;

        let date = parts
            .headers
            .get(X_AMZ_DATE)
            .ok_or(AuthError::Invalid("Missing x-amz-date field".into()))?
            .to_str()?;
        let date = parse_date(date)?;

        if Utc::now() - date > Duration::hours(24) {
            return Err(AuthError::Invalid("Date is too old".into()));
        }
        let (key_id, scope) = parse_credential(cred)?;
        if scope.date != format!("{}", date.format(SHORT_DATE)) {
            return Err(AuthError::Invalid("Date mismatch".into()));
        }

        let auth = Authorization {
            key_id,
            scope,
            signed_headers,
            signature,
            content_sha256: content_sha256.to_str()?.to_string(),
            date,
        };
        Ok(Self(Some(auth)))
    }
}

fn parse_date(date: &str) -> Result<DateTime<Utc>, AuthError> {
    let date: NaiveDateTime = NaiveDateTime::parse_from_str(date, LONG_DATETIME)
        .map_err(|_| AuthError::Invalid("Invalid date".into()))?;
    Ok(Utc.from_utc_datetime(&date))
}

fn parse_credential(cred: &str) -> Result<(String, Scope), AuthError> {
    let parts: Vec<&str> = cred.split('/').collect();
    if parts.len() != 5 || parts[4] != SCOPE_ENDING {
        return Err(AuthError::Invalid("wrong scope format".into()));
    }

    let scope = Scope {
        date: parts[1].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
    };
    Ok((parts[0].to_string(), scope))
}
