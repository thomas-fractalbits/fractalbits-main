use bumpalo::Bump;
use std::{
    cell::RefCell,
    ops::Deref,
    rc::Rc,
    sync::atomic::{AtomicBool, Ordering},
};

const BUMP_POOL_OBJ_SIZE: usize = 72 * 1024;
const BUMP_POOL_SIZE: usize = 8192;

thread_local! {
    static BUMP_POOL: RefCell<Vec<Rc<PooledBump>>> = RefCell::new(Vec::with_capacity(BUMP_POOL_SIZE));
    static BUMP_POOL_INITIALIZED: AtomicBool = const { AtomicBool::new(false) };
}

pub fn init_bump_pool() {
    let already_initialized = BUMP_POOL_INITIALIZED.with(|flag| {
        flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    });

    if !already_initialized {
        return;
    }

    BUMP_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        for _ in 0..BUMP_POOL_SIZE {
            let bump = PooledBump::new(BUMP_POOL_OBJ_SIZE);
            bump.inner
                .alloc_slice_fill_default::<u8>(BUMP_POOL_OBJ_SIZE);
            pool.push(Rc::new(bump));
        }
    });
}

pub fn acquire_bump() -> Rc<PooledBump> {
    BUMP_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if let Some(bump) = pool.pop() {
            bump
        } else {
            Rc::new(PooledBump::new(BUMP_POOL_OBJ_SIZE))
        }
    })
}

pub struct PooledBump {
    inner: Bump,
}

impl PooledBump {
    fn new(capacity: usize) -> Self {
        let inner = Bump::with_capacity(capacity);
        Self { inner }
    }
}

impl Deref for PooledBump {
    type Target = Bump;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Drop for PooledBump {
    fn drop(&mut self) {
        BUMP_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            self.inner.reset();
            let bump = std::mem::replace(&mut self.inner, Bump::new());
            pool.push(Rc::new(PooledBump { inner: bump }));
        });
    }
}

pub struct RequestBumpGuard {
    trace_id: u64,
    bump: Rc<PooledBump>,
}

impl RequestBumpGuard {
    pub fn new(trace_id: u64, bump: Rc<PooledBump>) -> Self {
        rpc_client_common::register_request_bump(trace_id, bump.clone());
        Self { trace_id, bump }
    }
}

impl Drop for RequestBumpGuard {
    fn drop(&mut self) {
        tracing::debug!(
            trace_id = self.trace_id,
            allocated_bytes = self.bump.allocated_bytes(),
            "bump allocator stats"
        );
        rpc_client_common::unregister_request_bump(self.trace_id);
    }
}
