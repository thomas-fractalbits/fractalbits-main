use crate::uring::{config::UringConfig, ring::PerCoreRing};
use core_affinity::{self, CoreId};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{info, warn};

#[derive(Clone)]
pub struct PerCoreBuilder {
    config: Arc<PerCoreConfig>,
    next_worker: Arc<AtomicUsize>,
    core_ids: Arc<Vec<CoreId>>,
}

#[derive(Default, Debug, Clone)]
pub struct PerCoreConfig {
    pub uring: UringConfig,
}

impl PerCoreBuilder {
    pub fn new(config: PerCoreConfig) -> Self {
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        if core_ids.is_empty() {
            warn!("core affinity metadata unavailable; workers will not be pinned");
        }
        Self {
            config: Arc::new(config),
            next_worker: Arc::new(AtomicUsize::new(0)),
            core_ids: Arc::new(core_ids),
        }
    }

    pub fn build_context(&self) -> io::Result<Arc<PerCoreContext>> {
        let worker_index = self.next_worker.fetch_add(1, Ordering::Relaxed);
        let ring = PerCoreRing::new(worker_index, &self.config.uring)?;
        Ok(Arc::new(PerCoreContext::new(worker_index, ring)))
    }

    pub fn pin_current_thread(&self, worker_index: usize) {
        if self.core_ids.is_empty() {
            return;
        }
        let core = self.core_ids[worker_index % self.core_ids.len()];
        if core_affinity::set_for_current(core) {
            info!(worker_index, core_id = core.id, "pinned worker to core");
        } else {
            warn!(
                worker_index,
                core_id = core.id,
                "failed to pin worker to core"
            );
        }
    }
}

pub struct PerCoreContext {
    worker_index: usize,
    ring: PerCoreRing,
}

impl PerCoreContext {
    fn new(worker_index: usize, ring: PerCoreRing) -> Self {
        Self { worker_index, ring }
    }

    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    pub fn ring(&self) -> &PerCoreRing {
        &self.ring
    }
}
