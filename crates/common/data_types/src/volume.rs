#[derive(Debug, Clone)]
pub struct DataVgInfo {
    pub volumes: Vec<DataVolume>,
    pub quorum: Option<QuorumConfig>,
}

#[derive(Debug, Clone)]
pub struct DataVolume {
    pub volume_id: u16,
    pub bss_nodes: Vec<BssNode>,
}

#[derive(Debug, Clone)]
pub struct BssNode {
    pub node_id: String,
    pub address: String,
}

#[derive(Debug, Clone)]
pub struct QuorumConfig {
    pub n: u32,
    pub r: u32,
    pub w: u32,
}
