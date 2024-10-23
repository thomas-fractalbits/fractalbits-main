mod delete;
mod get;
mod list;
mod put;
mod session;

use std::borrow::Cow;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, Query, Request, State},
    response::{IntoResponse, Response},
};
use strum::EnumString;

use super::extract::bucket_name::BucketName;
use super::AppState;
use nss_rpc_client::rpc_client::RpcClient;

pub const MAX_NSS_CONNECTION: usize = 8;

type QueryPairs<'a> = Query<Vec<(Cow<'a, str>, Cow<'a, str>)>>;

#[derive(Debug, EnumString, Copy, Clone, strum::Display)]
#[strum(serialize_all = "kebab-case")]
enum ApiCommand {
    Accelerate,
    Acl,
    Analytics,
    Cors,
    Delete,
    Encryption,
    IntelligentTiering,
    Inventory,
    LegalHold,
    Lifecycle,
    Location,
    Logging,
    Metrics,
    Notification,
    ObjectLock,
    OwnershipControls,
    Policy,
    PolicyStatus,
    PublicAccessBlock,
    Replication,
    RequestPayment,
    Restore,
    Retention,
    Select,
    Session,
    Tagging,
    Torrent,
    Uploads,
    Versioning,
    Versions,
    Website,
}

pub async fn get_handler(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    BucketName(_bucket): BucketName,
    Query(queries): QueryPairs<'_>,
    request: Request,
) -> Response {
    let key = key_for_nss(request.uri().path());
    let api_command = get_api_command(&queries);
    let rpc_client = get_rpc_client(&state, addr);

    match api_command {
        Some(ApiCommand::Session) => session::create_session(request).await.into_response(),
        Some(api_command) => panic!("TODO: {api_command}"),
        None => get::get_object(rpc_client, key, request)
            .await
            .into_response(),
    }
}

pub async fn put_handler(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    BucketName(_bucket): BucketName,
    Query(queries): QueryPairs<'_>,
    request: Request,
) -> Response {
    let key = key_for_nss(request.uri().path());
    let api_command = get_api_command(&queries);
    let rpc_client = get_rpc_client(&state, addr);

    match api_command {
        Some(api_command) => panic!("TODO: {api_command}"),
        None => put::put_object(rpc_client, key, request)
            .await
            .into_response(),
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

fn get_api_command<T>(query_params: &[(T, T)]) -> Option<ApiCommand>
where
    T: AsRef<str>,
{
    let api_commands: Vec<ApiCommand> = query_params
        .iter()
        .filter_map(|(k, v)| v.as_ref().is_empty().then_some(k))
        .filter_map(|cmd| ApiCommand::from_str(cmd.as_ref()).ok())
        .collect();
    if api_commands.is_empty() {
        None
    } else {
        if api_commands.len() > 1 {
            tracing::debug!("Multiple api command found: {api_commands:?}, pick up the first one");
        }
        Some(api_commands[0])
    }
}

fn key_for_nss(key: &str) -> String {
    if key == "/" {
        return key.into();
    }
    let mut key = key.to_owned();
    key.push('\0');
    key
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

    async fn handler(Query(query_map): Query<Vec<(String, String)>>) -> String {
        get_api_command(&query_map)
            .map(|cmd| cmd.to_string())
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn test_extract_api_command_ok() {
        let api_cmd = "acl";
        assert_eq!(send_request_get_body(api_cmd).await, api_cmd);
    }

    #[tokio::test]
    async fn test_extract_api_command_null() {
        let api_cmd = "";
        assert_eq!(send_request_get_body(api_cmd).await, api_cmd);
    }

    async fn send_request_get_body(api_cmd: &str) -> String {
        let api_cmd = if api_cmd.is_empty() {
            ""
        } else {
            &format!("?{api_cmd}")
        };
        let body = app()
            .oneshot(
                Request::builder()
                    .uri(format!("http://my-bucket.localhost/obj1{api_cmd}"))
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
