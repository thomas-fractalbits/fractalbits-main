mod extract;
pub mod handler;
pub mod response;

use nss_rpc_client::rpc_client::RpcClient;
pub struct AppState {
    pub rpc_clients: Vec<RpcClient>,
}
