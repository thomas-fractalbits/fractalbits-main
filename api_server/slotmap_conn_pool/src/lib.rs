use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{self, Debug};
use std::future::Future;
use std::hash::Hash;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{self, Poll};

use slotmap::{new_key_type, SlotMap};

new_key_type! { struct ConnectionKey; }

pub trait Poolable: Unpin + Send + Sized + 'static {
    fn is_open(&self) -> bool;
}

impl<T: Poolable + Sync> Poolable for Arc<T> {
    fn is_open(&self) -> bool {
        self.deref().is_open()
    }
}

pub trait Key: Eq + Hash + Clone + Debug + Unpin + Send + 'static {}
impl<T> Key for T where T: Eq + Hash + Clone + Debug + Unpin + Send + 'static {}

struct ConnPoolInner<T, K: Key> {
    connections: SlotMap<ConnectionKey, T>,
    host_to_conn_keys: HashMap<K, (Vec<ConnectionKey>, usize /* current idx */)>,
}

pub struct ConnPool<T, K: Key> {
    inner: Arc<Mutex<ConnPoolInner<T, K>>>,
}

impl<T, K: Key> Clone for ConnPool<T, K> {
    fn clone(&self) -> Self {
        ConnPool {
            inner: self.inner.clone(),
        }
    }
}

#[allow(clippy::new_without_default)]
impl<T: Poolable, K: Key> ConnPool<T, K> {
    pub fn new() -> Self {
        let inner = Arc::new(Mutex::new(ConnPoolInner {
            connections: SlotMap::with_key(),
            host_to_conn_keys: HashMap::new(),
        }));
        ConnPool { inner }
    }

    pub fn checkout(&self, key: K) -> Checkout<T, K> {
        Checkout {
            key,
            pool: self.clone(),
        }
    }

    pub fn pooled(&self, key: K, value: T)
    where
        T: Clone,
    {
        let mut inner = self.inner.lock().unwrap();
        let conn_key = inner.connections.insert(value.clone());
        let (keys, _) = inner.host_to_conn_keys.entry(key.clone()).or_default();
        keys.push(conn_key);
    }
}

pub struct Checkout<T: Poolable, K: Key> {
    key: K,
    pool: ConnPool<T, K>,
}

#[derive(Debug)]
pub enum Error {
    PoolDisabled,
    NoConnectionAvailable,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::PoolDisabled => write!(f, "pool is disabled"),
            Error::NoConnectionAvailable => write!(f, "no connection available"),
        }
    }
}

impl StdError for Error {}

impl<T: Poolable + Clone, K: Key> Future for Checkout<T, K> {
    type Output = Result<T, Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        let pool_arc = self.pool.inner.clone();

        let mut inner = pool_arc.lock().unwrap();

        // Phase 1: Identify closed connections.
        let mut to_remove_keys = Vec::new();

        if let Some((keys, _)) = inner.host_to_conn_keys.get(&self.key) {
            for &conn_key in keys {
                if let Some(conn) = inner.connections.get(conn_key) {
                    if !conn.is_open() {
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
            let value = inner.connections[conn_key].clone();
            return Poll::Ready(Ok(value));
        }

        Poll::Ready(Err(Error::NoConnectionAvailable))
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
        let pool = ConnPool::<MockConnection, MockKey>::new();
        let key = MockKey("foo".to_string());

        // Initially, no connection
        let res = pool.checkout(key.clone()).await;
        assert!(matches!(res, Err(Error::NoConnectionAvailable)));

        // Create and pool a connection
        pool.pooled(key.clone(), mock_conn(42));

        // Now checkout should succeed
        let pooled = pool.checkout(key).await.unwrap();
        assert_eq!(pooled.id, 42);
    }

    #[tokio::test]
    async fn test_round_robin() {
        let pool = ConnPool::<MockConnection, MockKey>::new();

        let key = MockKey("foo".to_string());
        let key_for_conn1 = key.clone();
        let key_for_conn2 = key.clone();

        pool.pooled(key_for_conn1, mock_conn(1));
        pool.pooled(key_for_conn2, mock_conn(2));

        let p1 = pool.checkout(key.clone()).await.unwrap();
        let p2 = pool.checkout(key.clone()).await.unwrap();
        let p3 = pool.checkout(key.clone()).await.unwrap();

        assert_eq!(p1.id, 1);
        assert_eq!(p2.id, 2);
        assert_eq!(p3.id, 1);
    }

    #[tokio::test]
    async fn test_connection_cleanup() {
        let pool = ConnPool::<MockConnection, MockKey>::new();

        let key = MockKey("foo".to_string());

        pool.pooled(key.clone(), mock_conn(1));

        let mut closed_conn = mock_conn(2);
        closed_conn.is_open = false;
        pool.pooled(key.clone(), closed_conn);

        let p1 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p1.id, 1);

        // After checkout, the closed connection should have been cleaned up.
        let inner = pool.inner.lock().unwrap();
        assert_eq!(
            inner.connections.len(),
            1,
            "Pool should have cleaned up the closed connection"
        );
    }
}
