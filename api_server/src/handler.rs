mod delete;
mod get;
mod head;
mod list;
mod put;
mod session;

use std::net::SocketAddr;
use std::sync::Arc;

use super::extract::{api_command::ApiCommandFromQuery, bucket_name::BucketName, key::Key};
use super::AppState;
use crate::extract::api_command::ApiCommand;
use crate::extract::api_signature::ApiSignature;
use axum::http::status::StatusCode;
use axum::http::Method;
use axum::{
    extract::{ConnectInfo, Request, State},
    response::{IntoResponse, Response},
};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;

pub async fn any_handler(
    State(app): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    BucketName(bucket_name): BucketName,
    ApiCommandFromQuery(api_cmd): ApiCommandFromQuery,
    Key(key): Key,
    api_sig: ApiSignature,
    request: Request,
) -> Response {
    tracing::debug!(%bucket_name);
    let rpc_client_nss = app.get_rpc_client_nss(addr);
    let rpc_client_bss = app.get_rpc_client_bss(addr);
    match request.method() {
        &Method::HEAD => head_handler(request, key, rpc_client_nss).await,
        &Method::GET => {
            get_handler(
                request,
                api_cmd,
                api_sig,
                key,
                rpc_client_nss,
                rpc_client_bss,
            )
            .await
        }
        &Method::PUT => {
            put_handler(
                request,
                api_cmd,
                api_sig,
                key,
                rpc_client_nss,
                rpc_client_bss,
            )
            .await
        }
        &Method::DELETE => delete_handler(request, key, rpc_client_nss, rpc_client_bss).await,
        &Method::POST => post_handler(request, api_cmd, key, rpc_client_nss, rpc_client_bss).await,
        method => (StatusCode::BAD_REQUEST, format!("TODO: method {method}")).into_response(),
    }
}

async fn head_handler(request: Request, key: String, rpc_client_nss: &RpcClientNss) -> Response {
    match key.as_str() {
        "/" => StatusCode::BAD_REQUEST.into_response(),
        _key => head::head_object(request, key, rpc_client_nss)
            .await
            .into_response(),
    }
}

async fn get_handler(
    request: Request,
    api_cmd: Option<ApiCommand>,
    api_sig: ApiSignature,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Response {
    match (api_cmd, key.as_str()) {
        (Some(ApiCommand::Attributes), _) => {
            get::get_object_attributes(request, key, rpc_client_nss)
                .await
                .into_response()
        }
        (Some(ApiCommand::Session), _) => session::create_session(request).await,
        (Some(api_cmd), _) => (StatusCode::BAD_REQUEST, format!("TODO: {api_cmd}")).into_response(),
        (None, "/") if api_sig.list_type.is_some() => {
            list::list_objects_v2(request, rpc_client_nss)
                .await
                .into_response()
        }
        (None, "/") => (StatusCode::BAD_REQUEST, "Legacy listObjects api").into_response(),
        (None, _key) if api_sig.upload_id.is_some() => {
            list::list_parts(request, key, rpc_client_nss)
                .await
                .into_response()
        }
        (None, _key) => get::get_object(request, key, rpc_client_nss, rpc_client_bss)
            .await
            .into_response(),
    }
}

async fn put_handler(
    request: Request,
    api_cmd: Option<ApiCommand>,
    _api_sig: ApiSignature,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Response {
    match api_cmd {
        Some(api_cmd) => (StatusCode::BAD_REQUEST, format!("TODO: {api_cmd}")).into_response(),
        None => put::put_object(request, key, rpc_client_nss, rpc_client_bss)
            .await
            .into_response(),
    }
}

async fn delete_handler(
    request: Request,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Response {
    match key.as_str() {
        "/" => StatusCode::BAD_REQUEST.into_response(),
        _key => delete::delete_object(request, key, rpc_client_nss, rpc_client_bss)
            .await
            .into_response(),
    }
}

async fn post_handler(
    request: Request,
    api_cmd: Option<ApiCommand>,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Response {
    match (api_cmd, key.as_str()) {
        (Some(ApiCommand::Delete), "/") => {
            delete::delete_objects(request, rpc_client_nss, rpc_client_bss)
                .await
                .into_response()
        }
        (_, _) => StatusCode::BAD_REQUEST.into_response(),
    }
}
