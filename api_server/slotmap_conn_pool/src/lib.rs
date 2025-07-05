use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error as StdError;
use std::fmt::{self, Debug};
use std::future::Future;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{self, Poll};
use std::time::{Duration, Instant};

use slotmap::{new_key_type, SlotMap};
use tokio::sync::oneshot;

new_key_type! { struct ConnectionKey; }

pub trait Poolable: Unpin + Send + Sized + 'static {
    fn is_open(&self) -> bool;
}

pub trait Key: Eq + Hash + Clone + Debug + Unpin + Send + 'static {}
impl<T> Key for T where T: Eq + Hash + Clone + Debug + Unpin + Send + 'static {}

struct Idle<T> {
    idle_at: Instant,
    value: T,
}

struct ConnPoolInner<T, K: Key> {
    connections: SlotMap<ConnectionKey, Idle<T>>,
    host_to_conn_keys: HashMap<K, (Vec<ConnectionKey>, usize)>,
    max_connections_per_host: usize,
    connecting: HashSet<K>,
    waiters: HashMap<K, VecDeque<oneshot::Sender<T>>>,
    timeout: Option<Duration>,
}

pub struct ConnPool<T, K: Key> {
    inner: Option<Arc<Mutex<ConnPoolInner<T, K>>>>,
}

impl<T, K: Key> Clone for ConnPool<T, K> {
    fn clone(&self) -> Self {
        ConnPool {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Poolable, K: Key> ConnPool<T, K> {
    pub fn new(max_connections_per_host: usize, idle_timeout: Option<Duration>) -> Self {
        let inner = if max_connections_per_host > 0 {
            Some(Arc::new(Mutex::new(ConnPoolInner {
                connections: SlotMap::with_key(),
                host_to_conn_keys: HashMap::new(),
                max_connections_per_host,
                connecting: HashSet::new(),
                waiters: HashMap::new(),
                timeout: idle_timeout,
            })))
        } else {
            None
        };
        ConnPool { inner }
    }

    pub fn checkout(&self, key: K) -> Checkout<T, K> {
        Checkout {
            key,
            pool: self.clone(),
            waiter: None,
        }
    }

    pub fn connecting(&self, key: &K) -> Option<Connecting<T, K>> {
        if let Some(ref pool_arc) = self.inner {
            let mut inner_guard = pool_arc.lock().unwrap();
            if inner_guard
                .host_to_conn_keys
                .get(key)
                .map_or(0, |(v, _)| v.len())
                < inner_guard.max_connections_per_host
            {
                if inner_guard.connecting.insert(key.clone()) {
                    return Some(Connecting {
                        key: key.clone(),
                        pool: Arc::downgrade(pool_arc),
                    });
                }
            }
        }
        None
    }

    pub fn pooled(&self, connecting: Connecting<T, K>, value: T)
    where
        T: Clone,
    {
        if let Some(pool) = connecting.pool.upgrade() {
            let mut inner = pool.lock().unwrap();
            let key = connecting.key.clone();
            let conn_key = inner.connections.insert(Idle {
                value: value.clone(),
                idle_at: Instant::now(),
            });
            let (keys, _) = inner.host_to_conn_keys.entry(key.clone()).or_default();
            keys.push(conn_key);
            if let Some(waiters) = inner.waiters.remove(&key) {
                for tx in waiters {
                    let _ = tx.send(value.clone());
                }
            }
        }
    }
}

pub struct Connecting<T: Poolable, K: Key> {
    key: K,
    pool: Weak<Mutex<ConnPoolInner<T, K>>>,
}

impl<T: Poolable, K: Key> Drop for Connecting<T, K> {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            if let Ok(mut inner) = pool.lock() {
                inner.connecting.remove(&self.key);
            }
        }
    }
}

pub struct Pooled<T: Poolable, K: Key> {
    value: Option<T>,
    key: K,
    pool: Weak<Mutex<ConnPoolInner<T, K>>>,
}

impl<T: Poolable, K: Key> Deref for Pooled<T, K> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value.as_ref().expect("not dropped")
    }
}

impl<T: Poolable, K: Key> DerefMut for Pooled<T, K> {
    fn deref_mut(&mut self) -> &mut T {
        self.value.as_mut().expect("not dropped")
    }
}

impl<T: Poolable, K: Key> Drop for Pooled<T, K> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            if value.is_open() {
                if let Some(pool) = self.pool.upgrade() {
                    let mut inner = pool.lock().unwrap();
                    let conn_key = inner.connections.insert(Idle {
                        value,
                        idle_at: Instant::now(),
                    });
                    inner
                        .host_to_conn_keys
                        .entry(self.key.clone())
                        .or_default()
                        .0
                        .push(conn_key);
                }
            }
        }
    }
}

pub struct Checkout<T: Poolable, K: Key> {
    key: K,
    pool: ConnPool<T, K>,
    waiter: Option<oneshot::Receiver<T>>,
}

#[derive(Debug)]
pub enum Error {
    PoolDisabled,
    CheckoutCanceled,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::PoolDisabled => write!(f, "pool is disabled"),
            Error::CheckoutCanceled => write!(f, "checkout canceled"),
        }
    }
}

impl StdError for Error {}

impl<T: Poolable + Clone, K: Key> Future for Checkout<T, K> {
    type Output = Result<Pooled<T, K>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        let pool_arc = match self.pool.inner.clone() {
            Some(inner) => inner,
            None => return Poll::Ready(Err(Error::PoolDisabled)),
        };

        loop {
            if let Some(mut rx) = self.waiter.take() {
                match Pin::new(&mut rx).poll(cx) {
                    Poll::Ready(Ok(value)) => {
                        return Poll::Ready(Ok(Pooled {
                            value: Some(value),
                            key: self.key.clone(),
                            pool: Arc::downgrade(&pool_arc),
                        }));
                    }
                    Poll::Pending => {
                        self.waiter = Some(rx);
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(_)) => {
                        return Poll::Ready(Err(Error::CheckoutCanceled));
                    }
                }
            }

            let mut inner = pool_arc.lock().unwrap();

            // Phase 1: Identify closed and idle connections.
            let mut to_remove_keys = Vec::new();
            let now = Instant::now();
            let idle_timeout = inner.timeout;

            if let Some((keys, _)) = inner.host_to_conn_keys.get(&self.key) {
                for &conn_key in keys {
                    if let Some(conn) = inner.connections.get(conn_key) {
                        if !conn.value.is_open()
                            || idle_timeout.map_or(false, |t| now.duration_since(conn.idle_at) > t)
                        {
                            to_remove_keys.push(conn_key);
                        }
                    } else {
                        to_remove_keys.push(conn_key); // Stale key
                    }
                }
            }

            // Phase 2: Remove the closed connections.
            if !to_remove_keys.is_empty() {
                for conn_key in &to_remove_keys {
                    inner.connections.remove(*conn_key);
                }
                if let Some((keys, _)) = inner.host_to_conn_keys.get_mut(&self.key) {
                    keys.retain(|k| !to_remove_keys.contains(k));
                    if keys.is_empty() {
                        inner.host_to_conn_keys.remove(&self.key);
                    }
                }
            }

            // Phase 3: Try to find a healthy connection.
            let mut conn_key = None;
            if let Some((keys, next_idx)) = inner.host_to_conn_keys.get_mut(&self.key) {
                if !keys.is_empty() {
                    let idx = *next_idx;
                    *next_idx = (idx + 1) % keys.len();
                    conn_key = Some(keys[idx]);
                }
            }
            if let Some(conn_key) = conn_key {
                let value = inner.connections[conn_key].value.clone();
                return Poll::Ready(Ok(Pooled {
                    value: Some(value),
                    key: self.key.clone(),
                    pool: Arc::downgrade(&pool_arc),
                }));
            }

            // Phase 4: Wait for a new connection.
            let (tx, rx) = oneshot::channel();
            inner
                .waiters
                .entry(self.key.clone())
                .or_default()
                .push_back(tx);
            self.waiter = Some(rx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct MockConnection {
        id: usize,
        is_open: bool,
    }

    impl Poolable for MockConnection {
        fn is_open(&self) -> bool {
            self.is_open
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct MockKey(String);

    fn mock_conn(id: usize) -> MockConnection {
        MockConnection { id, is_open: true }
    }

    #[tokio::test]
    async fn test_checkout_and_pool() {
        let pool = ConnPool::<MockConnection, MockKey>::new(1, Some(Duration::from_secs(10)));

        let key = MockKey("foo".to_string());
        let p = pool.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                if let Some(connecting) = p.connecting(&key) {
                    p.pooled(connecting, mock_conn(42));
                    break;
                }
            }
        });

        let pooled = pool.checkout(MockKey("foo".to_string())).await.unwrap();
        assert_eq!(pooled.id, 42);
    }

    #[tokio::test]
    async fn test_round_robin() {
        let pool = ConnPool::<MockConnection, MockKey>::new(2, Some(Duration::from_secs(10)));

        let key = MockKey("foo".to_string());
        let key_for_conn1 = key.clone();
        let key_for_conn2 = key.clone();

        let conn1 = pool.connecting(&key_for_conn1).unwrap();
        pool.pooled(conn1, mock_conn(1));

        let conn2 = pool.connecting(&key_for_conn2).unwrap();
        pool.pooled(conn2, mock_conn(2));

        let p1 = pool.checkout(key.clone()).await.unwrap();
        let p2 = pool.checkout(key.clone()).await.unwrap();
        let p3 = pool.checkout(key.clone()).await.unwrap();

        assert_eq!(p1.id, 1);
        assert_eq!(p2.id, 2);
        assert_eq!(p3.id, 1);
    }

    #[tokio::test]
    async fn test_connection_cleanup() {
        let pool = ConnPool::<MockConnection, MockKey>::new(2, Some(Duration::from_secs(10)));

        let key = MockKey("foo".to_string());
        let p = pool.clone();
        let key_for_spawn = key.clone();

        tokio::spawn(async move {
            let conn1 = p.connecting(&key_for_spawn).unwrap();
            p.pooled(conn1, mock_conn(1));

            let conn2 = p.connecting(&key_for_spawn).unwrap();
            let mut closed_conn = mock_conn(2);
            closed_conn.is_open = false;
            p.pooled(conn2, closed_conn);
        });

        let p1 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p1.id, 1);

        let p2 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p2.id, 1);

        tokio::time::sleep(Duration::from_millis(20)).await;

        let inner = pool.inner.as_ref().unwrap().lock().unwrap();
        assert_eq!(
            inner.connections.len(),
            1,
            "Pool should have cleaned up the closed connection"
        );
    }

    #[tokio::test]
    async fn test_max_connections() {
        let pool = ConnPool::<MockConnection, MockKey>::new(1, Some(Duration::from_secs(10)));

        let key = MockKey("foo".to_string());

        let conn1 = pool.connecting(&key).unwrap();
        pool.pooled(conn1, mock_conn(1));

        assert!(
            pool.connecting(&key).is_none(),
            "Should not be able to create a new connection when at max capacity"
        );
    }

    #[tokio::test]
    async fn test_idle_timeout() {
        let pool = ConnPool::<MockConnection, MockKey>::new(1, Some(Duration::from_millis(10)));

        let key = MockKey("foo".to_string());

        // Pool a connection
        let conn1 = pool.connecting(&key).unwrap();
        pool.pooled(conn1, mock_conn(1));

        // Checkout and immediately drop to return to pool
        let _ = pool.checkout(key.clone()).await.unwrap();

        // Wait longer than the idle timeout
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Try to checkout again. The old connection should have timed out.
        // We expect a new connection to be created, so we'll simulate that.
        let p = pool.clone();
        let key_for_spawn = key.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                if let Some(connecting) = p.connecting(&key_for_spawn) {
                    p.pooled(connecting, mock_conn(2));
                    break;
                }
            }
        });

        let p2 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(
            p2.id, 2,
            "Old connection should have timed out, new one should be created"
        );

        let inner = pool.inner.as_ref().unwrap().lock().unwrap();
        assert_eq!(
            inner.connections.len(),
            1,
            "Only one connection should be in the pool after timeout and new connection"
        );
    }
}
