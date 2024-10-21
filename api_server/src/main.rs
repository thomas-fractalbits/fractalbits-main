use std::net::SocketAddr;
use std::sync::Arc;

use api_server::{
    handler::{get_handler, put_handler, MAX_NSS_CONNECTION},
    AppState,
};
use axum::{
    extract::{MatchedPath, Request},
    routing::get,
    Router,
};
use nss_rpc_client::rpc_client::RpcClient;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=warn,tower_http=warn", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let mut rpc_clients = Vec::with_capacity(MAX_NSS_CONNECTION);
    for _i in 0..MAX_NSS_CONNECTION {
        let rpc_client = match RpcClient::new("127.0.0.1:9224").await {
            Ok(rpc_client) => rpc_client,
            Err(e) => {
                tracing::error!("failed to start rpc client: {e}");
                return;
            }
        };
        rpc_clients.push(rpc_client);
    }
    let shared_state = Arc::new(AppState { rpc_clients });

    let app = Router::new()
        .route("/*key", get(get_handler).put(put_handler))
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
        .with_state(shared_state)
        .into_make_service_with_connect_info::<SocketAddr>();

    let addr = "0.0.0.0:3000";
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!("failed to bind addr {addr}: {e}");
            return;
        }
    };

    tracing::info!("server started");
    if let Err(e) = axum::serve(listener, app).tcp_nodelay(true).await {
        tracing::error!("server stopped: {e}");
    }
}
