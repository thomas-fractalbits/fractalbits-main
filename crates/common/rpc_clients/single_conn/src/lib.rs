use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{self, Debug};
use std::future::Future;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;

pub trait Poolable: Unpin + Send + Sized + 'static {
    type Error: Debug + Send + 'static;
    type AddrKey: Key;

    fn new(addr_key: Self::AddrKey) -> impl Future<Output = Result<Self, Self::Error>> + Send;

    fn is_closed(&self) -> bool;
}

impl<T: Poolable + Sync> Poolable for Arc<T> {
    type AddrKey = T::AddrKey;
    type Error = T::Error;

    fn is_closed(&self) -> bool {
        self.deref().is_closed()
    }

    async fn new(addr_key: Self::AddrKey) -> Result<Self, Self::Error> {
        T::new(addr_key).await.map(Arc::new)
    }
}

pub trait Key: Eq + Hash + Clone + Debug + Unpin + Send + 'static {}
impl<T> Key for T where T: Eq + Hash + Clone + Debug + Unpin + Send + 'static {}

struct ConnPoolInner<T, K: Key> {
    connections: HashMap<K, T>,
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
            connections: HashMap::new(),
        }));
        ConnPool { inner }
    }

    pub async fn pooled(&self, key: K, value: T)
    where
        T: Clone,
    {
        let mut inner = self.inner.lock().await;
        inner.connections.insert(key, value);
    }

    pub async fn checkout(&self, addr_key: K) -> Result<T, Error>
    where
        T: Poolable + Clone,
        T::AddrKey: From<K>,
    {
        let (is_closed, conn_option) = {
            let inner = self.inner.lock().await;
            if let Some(conn) = inner.connections.get(&addr_key) {
                (conn.is_closed(), Some(conn.clone()))
            } else {
                (true, None)
            }
        };

        if let Some(conn) = conn_option
            && !is_closed
        {
            return Ok(conn);
        }

        {
            let inner = self.inner.lock().await;

            let existing_conn = inner.connections.get(&addr_key);
            if let Some(conn) = existing_conn
                && !conn.is_closed()
            {
                return Ok(conn.clone());
            }
        }

        debug!("create new connection {addr_key:?}");
        let new_conn = T::new(addr_key.clone().into())
            .await
            .unwrap_or_else(|e| panic!("Failed to create new connection: {e:?}"));

        let mut inner = self.inner.lock().await;
        inner.connections.insert(addr_key.clone(), new_conn.clone());
        Ok(new_conn)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct MockConnection {
        id: usize,
        is_closed: bool,
    }

    impl Poolable for MockConnection {
        fn is_closed(&self) -> bool {
            self.is_closed
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
        MockConnection {
            id,
            is_closed: false,
        }
    }

    #[tokio::test]
    async fn test_checkout_and_pool() {
        let pool = ConnPool::<MockConnection, MockKey>::new();
        let key = MockKey("42".to_string());

        pool.pooled(key.clone(), mock_conn(42)).await;

        let pooled = pool.checkout(key).await.unwrap();
        assert_eq!(pooled.id, 42);
    }

    #[tokio::test]
    async fn test_single_connection_per_key() {
        let pool = ConnPool::<MockConnection, MockKey>::new();

        let key = MockKey("foo".to_string());

        pool.pooled(key.clone(), mock_conn(1)).await;
        pool.pooled(key.clone(), mock_conn(2)).await;

        let p1 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p1.id, 2);
        let p2 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p2.id, 2);
        let p3 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p3.id, 2);
    }

    #[tokio::test]
    async fn test_connection_cleanup() {
        let pool = ConnPool::<MockConnection, MockKey>::new();

        let key = MockKey("1".to_string());

        let mut closed_conn = mock_conn(2);
        closed_conn.is_closed = true;
        pool.pooled(key.clone(), closed_conn).await;

        let p1 = pool.checkout(key.clone()).await.unwrap();
        assert_eq!(p1.id, 1);

        let inner = pool.inner.lock().await;
        assert_eq!(inner.connections.len(), 1);
    }
}
