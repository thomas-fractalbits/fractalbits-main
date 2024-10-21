pub mod handler;

use nss_rpc_client::rpc_client::RpcClient;
pub struct AppState {
    pub rpc_clients: Vec<RpcClient>,
}
