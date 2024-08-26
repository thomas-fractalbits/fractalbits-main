use api_server::RpcClient;
use axum::{
    extract::{MatchedPath, Path, Request, State},
    http::StatusCode,
    routing::get,
    Router,
};
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

struct AppState {
    rpc_client: RpcClient,
}

async fn get_obj(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<String, (StatusCode, String)> {
    let resp = api_server::nss_get_inode(&state.rpc_client, format!("/{key}"))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
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
    let resp = api_server::nss_put_inode(&state.rpc_client, format!("/{key}"), value)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
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
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let rpc_client = match RpcClient::new("127.0.0.1:9224").await {
        Ok(rpc_client) => rpc_client,
        Err(e) => {
            tracing::error!("failed to start rpc client: {e}");
            return;
        }
    };
    let shared_state = Arc::new(AppState { rpc_client });

    let app = Router::new()
        .route("/*key", get(get_obj).post(put_obj))
        .layer(
            TraceLayer::new_for_http()
                // Create our own span for the request and include the matched path. The matched
                // path is useful for figuring out which handler the request was routed to.
                .make_span_with(|req: &Request| {
                    let method = req.method();
                    let uri = req.uri();

                    // axum automatically adds this extension.
                    let matched_path = req
                        .extensions()
                        .get::<MatchedPath>()
                        .map(|matched_path| matched_path.as_str());

                    tracing::debug_span!("request", %method, %uri, matched_path)
                })
                // By default `TraceLayer` will log 5xx responses but we're doing our specific
                // logging of errors so disable that
                .on_failure(()),
        )
        .with_state(shared_state);

    let addr = "0.0.0.0:3000";
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!("failed to bind addr {addr}: {e}");
            return;
        }
    };

    tracing::info!("server started");
    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!("serer stopped: {e}");
    }
}
