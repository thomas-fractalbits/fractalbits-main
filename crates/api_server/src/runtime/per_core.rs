use crate::uring::{config::UringConfig, ring::PerCoreRing};
use core_affinity::{self, CoreId};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

#[derive(Clone)]
pub struct PerCoreBuilder {
    config: Arc<PerCoreConfig>,
    next_worker: Arc<AtomicUsize>,
    core_ids: Arc<Vec<CoreId>>,
    worker_limit: usize,
    rings: Arc<Vec<Mutex<Option<Arc<PerCoreRing>>>>>,
}

#[derive(Default, Debug, Clone)]
pub struct PerCoreConfig {
    pub uring: UringConfig,
}

impl PerCoreBuilder {
    pub fn new(worker_limit: usize, config: PerCoreConfig) -> Self {
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        if core_ids.is_empty() {
            warn!("core affinity metadata unavailable; workers will not be pinned");
        }
        let rings = (0..worker_limit).map(|_| Mutex::new(None)).collect();

        Self {
            config: Arc::new(config),
            next_worker: Arc::new(AtomicUsize::new(0)),
            core_ids: Arc::new(core_ids),
            worker_limit,
            rings: Arc::new(rings),
        }
    }

    pub fn build_context(&self) -> io::Result<Arc<PerCoreContext>> {
        let raw_index = self.next_worker.fetch_add(1, Ordering::Relaxed);
        let worker_index = raw_index % self.worker_limit;

        let ring = {
            let slot_mutex = self
                .rings
                .get(worker_index)
                .expect("missing ring slot for worker");
            let mut slot = slot_mutex.lock().unwrap();
            slot.get_or_insert_with(|| {
                Arc::new(
                    PerCoreRing::new(worker_index, &self.config.uring)
                        .expect("failed to create per-core ring"),
                )
            })
            .clone()
        };

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
    ring: Arc<PerCoreRing>,
}

impl PerCoreContext {
    fn new(worker_index: usize, ring: Arc<PerCoreRing>) -> Self {
        Self { worker_index, ring }
    }

    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    pub fn ring(&self) -> Arc<PerCoreRing> {
        self.ring.clone()
    }
}
