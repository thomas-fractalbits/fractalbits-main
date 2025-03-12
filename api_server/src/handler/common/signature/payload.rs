use std::collections::{BTreeMap, BTreeSet};

use axum::body::Body;
use axum::extract::Query;
use axum::http::header::{HeaderMap, HeaderValue};
use axum::http::{request::Request, Method};
use axum::RequestExt;
use bucket_tables::api_key_table::{ApiKey, ApiKeyTable};
use bucket_tables::table::{Table, Versioned};
use chrono::{DateTime, Utc};
use hmac::Mac;
use itertools::Itertools;
use rpc_client_rss::ArcRpcClientRss;
// use hyper::{body::Incoming as IncomingBody, Method, Request};
use sha2::{Digest, Sha256};

use crate::handler::common::request::extract::authorization::Authorization;

// use garage_table::*;
use super::super::data::Hash;

// use garage_model::garage::Garage;
// use garage_model::key_table::*;

use super::*;

use super::super::encoding::uri_encode;

pub struct CheckedSignature {
    pub key: Option<Versioned<ApiKey>>,
    pub content_sha256_header: ContentSha256Header,
    pub signature_header: Option<String>,
}

pub async fn check_payload_signature(
    auth: &Authorization,
    request: &mut Request<Body>,
    rpc_client_rss: ArcRpcClientRss,
    region: &str,
) -> Result<CheckedSignature, Error> {
    check_standard_signature(auth, request, rpc_client_rss, region).await
    // let query = parse_query_map(request.uri())?;

    // if query.contains_key(&X_AMZ_ALGORITHM) {
    //     // We check for presigned-URL-style authentication first, because
    //     // the browser or something else could inject an Authorization header
    //     // that is totally unrelated to AWS signatures.
    //     check_presigned_signature(garage, service, request, query).await
    // } else if request.headers().contains_key(AUTHORIZATION) {
    //     check_standard_signature(garage, service, request, query).await
    // } else {
    //     // Unsigned (anonymous) request
    //     let content_sha256 = request
    //         .headers()
    //         .get(X_AMZ_CONTENT_SHA256)
    //         .map(|x| x.to_str())
    //         .transpose()?;
    //     Ok(CheckedSignature {
    //         key: None,
    //         content_sha256_header: parse_x_amz_content_sha256(content_sha256)?,
    //         signature_header: None,
    //     })
    // }
}

fn parse_x_amz_content_sha256(header: Option<&str>) -> Result<ContentSha256Header, Error> {
    let header = match header {
        Some(x) => x,
        None => return Ok(ContentSha256Header::UnsignedPayload),
    };
    if header == UNSIGNED_PAYLOAD {
        Ok(ContentSha256Header::UnsignedPayload)
    } else if let Some(rest) = header.strip_prefix("STREAMING-") {
        let (trailer, algo) = if let Some(rest2) = rest.strip_suffix("-TRAILER") {
            (true, rest2)
        } else {
            (false, rest)
        };
        let signed = match algo {
            AWS4_HMAC_SHA256_PAYLOAD => true,
            UNSIGNED_PAYLOAD => false,
            _ => {
                return Err(Error::Other(
                    "invalid or unsupported x-amz-content-sha256".into(),
                ))
            }
        };
        Ok(ContentSha256Header::StreamingPayload { trailer, signed })
    } else {
        let sha256 = hex::decode(header)
            .ok()
            .and_then(|bytes| Hash::try_from(&bytes))
            .unwrap(); // FIXME
                       // .ok_or_else(|| Err::Other("Invalid content sha256 hash".into()))?;
        Ok(ContentSha256Header::Sha256Checksum(sha256))
    }
}

pub async fn check_standard_signature(
    auth: &Authorization,
    request: &mut Request<Body>,
    rpc_client_rss: ArcRpcClientRss,
    region: &str,
) -> Result<CheckedSignature, Error> {
    let query_params: Query<BTreeMap<String, String>> = request.extract_parts().await?;
    let canonical_request = canonical_request(
        request.method(),
        request.uri().path(),
        &query_params,
        request.headers(),
        &auth.signed_headers,
        &auth.content_sha256,
    )?;
    let string_to_sign =
        string_to_sign(&auth.date, &auth.scope.to_sign_string(), &canonical_request);

    tracing::trace!("canonical request:\n{}", canonical_request);
    tracing::trace!("string to sign:\n{}", string_to_sign);

    let key = verify_v4(auth, string_to_sign.as_bytes(), rpc_client_rss, region).await?;

    let content_sha256_header = parse_x_amz_content_sha256(Some(&auth.content_sha256))?;

    Ok(CheckedSignature {
        key,
        content_sha256_header,
        signature_header: Some(auth.signature.clone()),
    })
}

pub fn string_to_sign(datetime: &DateTime<Utc>, scope_string: &str, canonical_req: &str) -> String {
    let mut hasher = Sha256::default();
    hasher.update(canonical_req.as_bytes());
    [
        AWS4_HMAC_SHA256,
        &datetime.format(LONG_DATETIME).to_string(),
        scope_string,
        &hex::encode(hasher.finalize().as_slice()),
    ]
    .join("\n")
}

pub fn canonical_request(
    method: &Method,
    canonical_uri: &str,
    query_params: &BTreeMap<String, String>,
    headers: &HeaderMap<HeaderValue>,
    signed_headers: &BTreeSet<String>,
    content_sha256: &str,
) -> Result<String, Error> {
    // Canonical query string from passed HeaderMap
    let canonical_query_string = {
        let mut items = Vec::with_capacity(query_params.len());
        for (key, value) in query_params.iter() {
            items.push(uri_encode(key, true) + "=" + &uri_encode(value, true));
        }
        items.sort();
        items.join("&")
    };

    // Canonical header string calculated from signed headers
    let canonical_header_string = signed_headers
        .iter()
        .map(|name| {
            let value = headers.get(name).ok_or(Error::Other(format!(
                "signed header `{}` is not present",
                name
            )))?;
            let value = std::str::from_utf8(value.as_bytes())?;
            Ok(format!("{}:{}", name.as_str(), value.trim()))
        })
        .collect::<Result<Vec<String>, Error>>()?
        .join("\n");
    let signed_headers = signed_headers.iter().join(";");

    let list = [
        method.as_str(),
        canonical_uri,
        &canonical_query_string,
        &canonical_header_string,
        "",
        &signed_headers,
        content_sha256,
    ];
    Ok(list.join("\n"))
}

pub async fn verify_v4(
    auth: &Authorization,
    payload: &[u8],
    rpc_client_rss: ArcRpcClientRss,
    region: &str,
) -> Result<Option<Versioned<ApiKey>>, Error> {
    let mut api_key_table: Table<ArcRpcClientRss, ApiKeyTable> = Table::new(rpc_client_rss);
    let key = api_key_table.get(auth.key_id.clone()).await?;

    let mut hmac = signing_hmac(&auth.date, &key.data.secret_key, region)
        .map_err(|_| Error::Other("Unable to build signing HMAC".into()))?;
    hmac.update(payload);
    let signature = hex::decode(&auth.signature)?;
    if hmac.verify_slice(&signature).is_err() {
        return Err(Error::Other("signature mismatch".into()));
    }

    Ok(Some(key))
}
