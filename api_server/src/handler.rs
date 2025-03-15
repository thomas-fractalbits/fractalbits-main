mod bucket;
pub mod common;
mod delete;
mod endpoint;
mod get;
mod head;
mod post;
mod put;

use std::net::SocketAddr;
use std::sync::Arc;

use crate::{AppState, BlobId};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http,
    response::{IntoResponse, Response},
};
use bucket::BucketEndpoint;
use bucket_tables::api_key_table::ApiKey;
use bucket_tables::bucket_table::Bucket;
use bucket_tables::table::Versioned;
use common::request::extract::authorization::Authentication;
use common::request::extract::{
    api_command::ApiCommandFromQuery, api_signature::ApiSignature,
    authorization::AuthenticationFromReq, bucket_name::BucketNameFromHost, key::KeyFromPath,
};
use common::s3_error::S3Error;
use common::signature::{self, body::ReqBody, verify_request, VerifiedRequest};
use delete::DeleteEndpoint;
use endpoint::Endpoint;
use get::GetEndpoint;
use head::HeadEndpoint;
use post::PostEndpoint;
use put::PutEndpoint;
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::{ArcRpcClientRss, RpcErrorRss};
use tokio::sync::mpsc::Sender;

pub type Request<T = ReqBody> = http::Request<T>;

#[allow(clippy::too_many_arguments)]
pub async fn any_handler(
    State(app): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    bucket_name_from_host: Result<BucketNameFromHost, S3Error>,
    ApiCommandFromQuery(api_cmd): ApiCommandFromQuery,
    KeyFromPath(key_from_path): KeyFromPath,
    api_sig: ApiSignature,
    AuthenticationFromReq(auth): AuthenticationFromReq,
    request: http::Request<Body>,
) -> Response {
    let (bucket_name, key) = match bucket_name_from_host {
        Err(e) => return e.into_response(),
        // Virtual-hosted-style request
        Ok(BucketNameFromHost(Some(bucket_name))) => (bucket_name, key_from_path),
        // Path-style request
        Ok(BucketNameFromHost(None)) => get_bucket_and_key_from_path(key_from_path),
    };
    tracing::debug!(%bucket_name, %key);

    let resource = format!("/{bucket_name}{key}");
    let endpoint = match Endpoint::from_extractors(&request, &bucket_name, &key, api_cmd, api_sig) {
        Err(e) => return e.into_response_with_resource(&resource),
        Ok(endpoint) => endpoint,
    };
    match any_handler_inner(app, addr, bucket_name, key, auth, request, endpoint).await {
        Err(e) => e.into_response_with_resource(&resource),
        Ok(response) => response,
    }
}

// Get bucket and key for path-style requests
fn get_bucket_and_key_from_path(path: String) -> (String, String) {
    let mut bucket = String::new();
    let mut key = String::from("/");
    let mut bucket_part = true;
    path.chars().skip_while(|c| c == &'/').for_each(|c| {
        if bucket_part && c == '/' {
            bucket_part = false;
            return;
        }
        if bucket_part && c != '\0' {
            bucket.push(c);
        } else {
            key.push(c);
        }
    });
    // Not a real key, removing hacking for nss
    if key == "/\0" {
        key.pop();
    }
    (bucket, key)
}

async fn any_handler_inner(
    app: Arc<AppState>,
    addr: SocketAddr,
    bucket_name: String,
    key: String,
    auth: Option<Authentication>,
    request: http::Request<Body>,
    endpoint: Endpoint,
) -> Result<Response, S3Error> {
    let rpc_client_rss = app.get_rpc_client_rss();
    let VerifiedRequest {
        request, api_key, ..
    } = match auth {
        None => unreachable!(),
        Some(auth) => {
            match verify_request(
                request,
                &auth,
                rpc_client_rss.clone(),
                &app.config.s3_region,
            )
            .await
            {
                Ok(res) => res,
                Err(signature::error::Error::RpcErrorRss(RpcErrorRss::NotFound)) => {
                    return Err(S3Error::InvalidAccessKeyId)
                }
                Err(_e) => return Err(S3Error::InvalidSignature),
            }
        }
    };

    let rpc_client_nss = app.get_rpc_client_nss(addr);
    let rpc_client_bss = app.get_rpc_client_bss(addr);
    let blob_deletion = app.blob_deletion.clone();
    match endpoint {
        Endpoint::Bucket(bucket_endpoint) => {
            bucket_handler(
                &app,
                request,
                api_key,
                bucket_name,
                rpc_client_nss,
                rpc_client_rss,
                bucket_endpoint,
            )
            .await
        }
        Endpoint::Get(get_endpoint) => {
            let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
            get_handler(
                request,
                &bucket,
                key,
                rpc_client_nss,
                rpc_client_bss,
                get_endpoint,
            )
            .await
        }
        Endpoint::Put(put_endpoint) => {
            let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
            put_handler(
                request,
                &bucket,
                key,
                rpc_client_nss,
                rpc_client_bss,
                blob_deletion,
                put_endpoint,
            )
            .await
        }
        Endpoint::Delete(delete_endpoint) => {
            let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
            delete_handler(
                request,
                &bucket,
                key,
                rpc_client_nss,
                rpc_client_bss,
                blob_deletion,
                delete_endpoint,
            )
            .await
        }
        Endpoint::Post(post_endpoint) => {
            let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
            post_handler(
                request,
                &bucket,
                key,
                rpc_client_nss,
                blob_deletion,
                post_endpoint,
            )
            .await
        }
        Endpoint::Head(head_endpoint) => {
            let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
            head_handler(request, &bucket, key, rpc_client_nss, head_endpoint).await
        }
    }
}

async fn bucket_handler(
    app: &Arc<AppState>,
    request: Request,
    api_key: Option<Versioned<ApiKey>>,
    bucket_name: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_rss: ArcRpcClientRss,
    endpoint: BucketEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        BucketEndpoint::CreateBucket => {
            bucket::create_bucket(
                api_key,
                bucket_name,
                request,
                rpc_client_nss,
                rpc_client_rss,
                &app.config.s3_region,
            )
            .await
        }
        BucketEndpoint::DeleteBucket => {
            let bucket = bucket::resolve_bucket(bucket_name, rpc_client_rss.clone()).await?;
            bucket::delete_bucket(api_key, &bucket, request, rpc_client_nss, rpc_client_rss).await
        }
        BucketEndpoint::HeadBucket => {
            bucket::head_bucket(api_key, bucket_name, rpc_client_rss).await
        }
        BucketEndpoint::ListBuckets => {
            bucket::list_buckets(request, rpc_client_rss, &app.config.s3_region).await
        }
    }
}

async fn head_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    endpoint: HeadEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        HeadEndpoint::HeadObject => head::head_object(request, bucket, key, rpc_client_nss).await,
    }
}

async fn get_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
    endpoint: GetEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        GetEndpoint::GetObject => {
            get::get_object(request, bucket, key, rpc_client_nss, rpc_client_bss).await
        }
        GetEndpoint::GetObjectAttributes => {
            get::get_object_attributes(request, bucket, key, rpc_client_nss).await
        }
        GetEndpoint::ListMultipartUploads => {
            get::list_multipart_uploads(request, rpc_client_nss).await
        }
        GetEndpoint::ListObjects => get::list_objects(request, bucket, rpc_client_nss).await,
        GetEndpoint::ListObjectsV2 => get::list_objects_v2(request, bucket, rpc_client_nss).await,
        GetEndpoint::ListParts => get::list_parts(request, bucket, key, rpc_client_nss).await,
    }
}

async fn put_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
    blob_deletion: Sender<(BlobId, usize)>,
    endpoint: PutEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        PutEndpoint::PutObject => {
            put::put_object(
                request,
                bucket,
                key,
                rpc_client_nss,
                rpc_client_bss,
                blob_deletion,
            )
            .await
        }
        PutEndpoint::UploadPart(part_number, upload_id) => {
            put::upload_part(
                request,
                bucket,
                key,
                part_number,
                upload_id,
                rpc_client_nss,
                rpc_client_bss,
                blob_deletion,
            )
            .await
        }
    }
}

async fn post_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    blob_deletion: Sender<(BlobId, usize)>,
    endpoint: PostEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        PostEndpoint::CompleteMultipartUpload(upload_id) => {
            post::complete_multipart_upload(
                request,
                bucket,
                key,
                upload_id,
                rpc_client_nss,
                blob_deletion,
            )
            .await
        }
        PostEndpoint::CreateMultipartUpload => {
            post::create_multipart_upload(request, bucket, key, rpc_client_nss).await
        }
        PostEndpoint::DeleteObjects => {
            post::delete_objects(request, rpc_client_nss, blob_deletion).await
        }
    }
}

async fn delete_handler(
    request: Request,
    bucket: &Bucket,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
    blob_deletion: Sender<(BlobId, usize)>,
    endpoint: DeleteEndpoint,
) -> Result<Response, S3Error> {
    match endpoint {
        DeleteEndpoint::AbortMultipartUpload(upload_id) => {
            delete::abort_multipart_upload(
                request,
                bucket,
                key,
                upload_id,
                rpc_client_nss,
                rpc_client_bss,
            )
            .await
        }
        DeleteEndpoint::DeleteObject => {
            delete::delete_object(bucket, key, rpc_client_nss, blob_deletion).await
        }
    }
}
