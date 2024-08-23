use api_server::RpcClient;
use axum::{extract::Path, extract::State, http::StatusCode, routing::get, Router};
use std::sync::Arc;

struct AppState {
    rpc_client: RpcClient,
}

async fn get_obj(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<String, (StatusCode, String)> {
    let resp = api_server::nss_get_inode(&state.rpc_client, key).await;
    match serde_json::to_string_pretty(&resp.result) {
        Ok(resp) => Ok(resp),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal server error: {e}"),
        )),
    }
}

async fn put_obj(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    value: String,
) -> Result<String, (StatusCode, String)> {
    let resp = api_server::nss_put_inode(&state.rpc_client, key, value).await;
    match serde_json::to_string_pretty(&resp.result) {
        Ok(resp) => Ok(resp),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal server error: {e}"),
        )),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let rpc_client = RpcClient::new("127.0.0.1:9224").await.unwrap();
    let shared_state = Arc::new(AppState { rpc_client });

    let app = Router::new()
        .route("/:key", get(get_obj).post(put_obj))
        .with_state(shared_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
