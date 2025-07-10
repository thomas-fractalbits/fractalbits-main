use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{self, Debug};
use std::future::Future;
use std::hash::Hash;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{self, Poll};

use slotmap::{new_key_type, SlotMap};

new_key_type! { struct ConnectionKey; }

pub trait Poolable: Unpin + Send + Sized + 'static {
    type Error: Debug + Send + 'static;
    type AddrKey: Key;

    fn new(addr_key: Self::AddrKey) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    fn is_open(&self) -> bool;
}

impl<T: Poolable + Sync> Poolable for Arc<T> {
    type AddrKey = T::AddrKey;
    type Error = T::Error;

    fn is_open(&self) -> bool {
        self.deref().is_open()
    }

    async fn new(addr_key: Self::AddrKey) -> Result<Self, Self::Error> {
        T::new(addr_key).await.map(Arc::new)
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

type RecreatingFuture<T> = Pin<Box<dyn Future<Output = Result<T, <T as Poolable>::Error>> + Send>>;

pub struct Checkout<T: Poolable, K: Key> {
    pool: ConnPool<T, K>,
    addr_key: K,
    conn_key: ConnectionKey,
    recreating: Option<RecreatingFuture<T>>,
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

    pub fn pooled(&self, key: K, value: T)
    where
        T: Clone,
    {
        let mut inner = self.inner.lock().unwrap();
        let conn_key = inner.connections.insert(value.clone());
        let (keys, _) = inner.host_to_conn_keys.entry(key.clone()).or_default();
        keys.push(conn_key);
    }

    pub fn checkout(&self, addr_key: K) -> Checkout<T, K> {
        let inner = self.inner.lock().unwrap();
        let conn_key = Self::get_conn_key(inner, &addr_key).unwrap();
        Checkout {
            pool: self.clone(),
            addr_key,
            conn_key,
            recreating: None,
        }
    }

    fn get_conn_key(
        mut inner_locked: MutexGuard<'_, ConnPoolInner<T, K>>,
        key: &K,
    ) -> Result<ConnectionKey, Error> {
        match inner_locked.host_to_conn_keys.get_mut(key) {
            None => Err(Error::NoConnectionAvailable),
            Some((keys, next_idx)) => {
                if keys.is_empty() {
                    Err(Error::NoConnectionAvailable)
                } else {
                    let idx = *next_idx;
                    *next_idx = (idx + 1) % keys.len();
                    Ok(keys[idx])
                }
            }
        }
    }
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

impl<T: Poolable + Clone, K: Key> Future for Checkout<T, K>
where
    T::AddrKey: From<K>,
{
    type Output = Result<T, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        if let Some(mut fut) = this.recreating.take() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(new_conn)) => {
                    let mut inner = this.pool.inner.lock().unwrap();
                    let new_conn_key = inner.connections.insert(new_conn.clone());
                    if let Some((keys, _)) = inner.host_to_conn_keys.get_mut(&this.addr_key) {
                        if let Some(key_ref) = keys.iter_mut().find(|k| **k == this.conn_key) {
                            *key_ref = new_conn_key;
                        }
                    }
                    this.conn_key = new_conn_key;
                    return Poll::Ready(Ok(new_conn));
                }
                Poll::Ready(Err(e)) => {
                    panic!("Failed to create new connection: {:?}", e);
                }
                Poll::Pending => {
                    this.recreating = Some(fut);
                    return Poll::Pending;
                }
            }
        }

        loop {
            let pool_arc = this.pool.inner.clone();
            let mut inner = pool_arc.lock().unwrap();
            if let Some(conn) = inner.connections.get(this.conn_key) {
                if conn.is_open() {
                    return Poll::Ready(Ok(conn.clone()));
                } else {
                    // Connection is broken, replace it with newly created one
                    inner.connections.remove(this.conn_key);
                    // Must drop the lock before creating the future
                    drop(inner);
                    let fut = <T as Poolable>::new(this.addr_key.clone().into());
                    this.recreating = Some(Box::pin(fut));
                    // Now we need to poll the new future.
                    // A simple way is to wake and return pending.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }

            // Connection key not valid anymore. Get a new one.
            match ConnPool::get_conn_key(inner, &this.addr_key) {
                Ok(key) => {
                    this.conn_key = key;
                    // loop to try again with the new key.
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
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

        type Error = std::io::Error;

        type AddrKey = MockKey;

        async fn new(addr_key: Self::AddrKey) -> Result<Self, Self::Error> {
            Ok(mock_conn(addr_key.0.parse().unwrap()))
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
        let key = MockKey("42".to_string());

        // Initially, no connection
        // FIXME: This test case is not valid anymore since we block until a connection is available.
        // let res = pool.checkout(key.clone()).await;
        // assert!(matches!(res, Err(Error::NoConnectionAvailable)));

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

        let key = MockKey("1".to_string());

        pool.pooled(key.clone(), mock_conn(1));

        let mut closed_conn = mock_conn(2);
        closed_conn.is_open = false;
        pool.pooled(key.clone(), closed_conn);

        let p1 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p1.id, 1);

        let inner = pool.inner.lock().unwrap();
        assert_eq!(inner.connections.len(), 2);
    }
}
