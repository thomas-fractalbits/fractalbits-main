mod delete;
mod get;
mod list;
mod put;
mod session;

use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::Method;
use axum::{
    extract::{ConnectInfo, Request, State},
    response::{IntoResponse, Response},
};

use crate::extract::api_command::ApiCommand;

use super::extract::{api_command::Api, bucket_name::BucketName, key::Key};
use super::AppState;
use nss_rpc_client::rpc_client::RpcClient;

pub const MAX_NSS_CONNECTION: usize = 8;

pub async fn any_handler(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    BucketName(bucket_name): BucketName,
    Api(api_command): Api,
    Key(key): Key,
    request: Request,
) -> Response {
    tracing::debug!(%bucket_name);
    let rpc_client = get_rpc_client(&state, addr);
    match request.method() {
        &Method::GET => get_handler(request, api_command, key, rpc_client).await,
        &Method::PUT => put_handler(request, api_command, key, rpc_client).await,
        method => panic!("TODO: method {method}"),
    }
}

fn get_rpc_client(app_state: &AppState, addr: SocketAddr) -> &RpcClient {
    fn calculate_hash<T: Hash>(t: &T) -> usize {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish() as usize
    }
    let hash = calculate_hash(&addr) % MAX_NSS_CONNECTION;
    &app_state.rpc_clients[hash]
}

async fn get_handler(
    request: Request,
    api_command: Option<ApiCommand>,
    key: String,
    rpc_client: &RpcClient,
) -> Response {
    match api_command {
        Some(ApiCommand::Session) => session::create_session(request).await,
        Some(api_command) => panic!("TODO: {api_command}"),
        None => get::get_object(request, key, rpc_client)
            .await
            .into_response(),
    }
}

async fn put_handler(
    request: Request,
    api_command: Option<ApiCommand>,
    key: String,
    rpc_client: &RpcClient,
) -> Response {
    match api_command {
        Some(api_command) => panic!("TODO: {api_command}"),
        None => put::put_object(request, key, rpc_client)
            .await
            .into_response(),
    }
}
