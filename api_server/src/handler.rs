mod delete;
mod get;
mod list;
mod put;
mod session;

use std::net::SocketAddr;
use std::sync::Arc;

use super::extract::{api_command::Api, bucket_name::BucketName, key::Key};
use super::AppState;
use crate::extract::api_command::ApiCommand;
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
    Api(api_command): Api,
    Key(key): Key,
    request: Request,
) -> Response {
    tracing::debug!(%bucket_name);
    let rpc_client_nss = app.get_rpc_client_nss(addr);
    let rpc_client_bss = app.get_rpc_client_bss(addr);
    match request.method() {
        &Method::GET => {
            get_handler(request, api_command, key, rpc_client_nss, rpc_client_bss).await
        }
        &Method::PUT => {
            put_handler(request, api_command, key, rpc_client_nss, rpc_client_bss).await
        }
        method => panic!("TODO: method {method}"),
    }
}

async fn get_handler(
    request: Request,
    api_command: Option<ApiCommand>,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Response {
    match api_command {
        Some(ApiCommand::Session) => session::create_session(request).await,
        Some(api_command) => panic!("TODO: {api_command}"),
        None => get::get_object(request, key, rpc_client_nss, rpc_client_bss)
            .await
            .into_response(),
    }
}

async fn put_handler(
    request: Request,
    api_command: Option<ApiCommand>,
    key: String,
    rpc_client_nss: &RpcClientNss,
    rpc_client_bss: &RpcClientBss,
) -> Response {
    match api_command {
        Some(api_command) => panic!("TODO: {api_command}"),
        None => put::put_object(request, key, rpc_client_nss, rpc_client_bss)
            .await
            .into_response(),
    }
}
