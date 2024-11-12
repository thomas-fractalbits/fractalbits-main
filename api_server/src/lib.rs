mod extract;
pub mod handler;
mod response_xml;

use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;

pub struct AppState {
    pub rpc_clients_nss: Vec<RpcClientNss>,
    pub rpc_clients_bss: Vec<RpcClientBss>,
}

impl AppState {
    const MAX_NSS_CONNECTION: usize = 8;
    const MAX_BSS_CONNECTION: usize = 8;

    pub async fn new(nss_ip: &str, bss_ip: &str) -> Self {
        let mut rpc_clients_nss = Vec::with_capacity(Self::MAX_NSS_CONNECTION);
        for _i in 0..AppState::MAX_NSS_CONNECTION {
            let rpc_client_nss = RpcClientNss::new(nss_ip)
                .await
                .expect("rpc client nss failure");
            rpc_clients_nss.push(rpc_client_nss);
        }

        let mut rpc_clients_bss = Vec::with_capacity(Self::MAX_BSS_CONNECTION);
        for _i in 0..AppState::MAX_NSS_CONNECTION {
            let rpc_client_bss = RpcClientBss::new(bss_ip)
                .await
                .expect("rpc client bss failure");
            rpc_clients_bss.push(rpc_client_bss);
        }

        Self {
            rpc_clients_nss,
            rpc_clients_bss,
        }
    }

    pub fn get_rpc_client_nss(&self, addr: SocketAddr) -> &RpcClientNss {
        fn calculate_hash<T: Hash>(t: &T) -> usize {
            let mut s = DefaultHasher::new();
            t.hash(&mut s);
            s.finish() as usize
        }
        let hash = calculate_hash(&addr) % Self::MAX_NSS_CONNECTION;
        &self.rpc_clients_nss[hash]
    }

    pub fn get_rpc_client_bss(&self, addr: SocketAddr) -> &RpcClientBss {
        fn calculate_hash<T: Hash>(t: &T) -> usize {
            let mut s = DefaultHasher::new();
            t.hash(&mut s);
            s.finish() as usize
        }
        let hash = calculate_hash(&addr) % Self::MAX_BSS_CONNECTION;
        &self.rpc_clients_bss[hash]
    }
}
