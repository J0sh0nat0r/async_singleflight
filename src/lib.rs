//! A singleflight implementation for tokio.
//!
//! Inspired by [singleflight](https://crates.io/crates/singleflight).
//!
//! # Examples
//!
//! ```no_run
//! use futures::future::join_all;
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use anyhow::Result;
//! use async_singleflight::Group;
//!
//! const RES: usize = 7;
//!
//! async fn expensive_fn() -> Result<usize> {
//!     tokio::time::sleep(Duration::new(1, 500)).await;
//!     Ok(RES)
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let g = Arc::new(Group::<String, _>::new());
//!     let mut handlers = Vec::new();
//!     for _ in 0..10 {
//!         let g = g.clone();
//!         handlers.push(tokio::spawn(async move {
//!             let res = g.work("key", expensive_fn()).await.0;
//!             let r = res.unwrap();
//!             println!("{}", r);
//!         }));
//!     }
//!
//!     join_all(handlers).await;
//! }
//! ```
//!
use std::{borrow::Borrow, collections::HashMap, future::Future, hash::Hash, sync::Arc};

use once_cell::sync::OnceCell;
use tokio::sync::{Mutex, Notify};

// Call is an in-flight or completed call to work.
struct Call<T>
where
    T: Clone,
{
    notify: Notify,
    res: OnceCell<T>,
}

impl<T> Call<T>
where
    T: Clone,
{
    fn new() -> Call<T> {
        Call {
            notify: Notify::new(),
            res: OnceCell::new(),
        }
    }
}

/// Group represents a class of work and creates a space in which units of work
/// can be executed with duplicate suppression.
#[derive(Default)]
pub struct Group<K, V>
where
    K: Eq + Hash + ToOwned,
    V: Clone,
{
    calls: Mutex<HashMap<K, Arc<Call<V>>>>,
}

impl<K, V> Group<K, V>
where
    K: Eq + Hash + ToOwned,
    V: Clone,
{
    /// Create a new Group to do work with.
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(HashMap::new()),
        }
    }

    /// Execute and return the value for a given function, making sure that only one
    /// operation is in-flight at a given moment. If a duplicate call comes in, that caller will
    /// wait until the original call completes and return the same value.
    /// Only owner call returns error if exists.
    /// The third return value indicates whether the call is the owner.
    pub async fn work<Q, E>(
        &self,
        key: &Q,
        fut: impl Future<Output = Result<V, E>>,
    ) -> (Option<V>, Option<E>, bool)
    where
        K: Borrow<Q>,
        Q: ?Sized + Eq + Hash + ToOwned<Owned = K>,
    {
        // grab lock
        let mut calls = self.calls.lock().await;

        // key already exists
        if let Some(call) = calls.get(key) {
            // need to create Notify first before dropping the lock
            let call = call.clone();
            let notify = call.notify.notified();

            drop(calls);

            // wait for notify
            notify.await;

            return (call.res.get().cloned(), None, false);
        }

        // insert call into map and start call
        let call = Arc::new(Call::new());

        calls.insert(key.to_owned(), call.clone());

        drop(calls);

        let res = fut.await;

        if let Ok(ref value) = res {
            debug_assert!(call.res.set(value.clone()).is_ok())
        }

        // grab lock before set notifying waiters
        let mut calls = self.calls.lock().await;

        call.notify.notify_waiters();

        calls.remove(key).unwrap();

        drop(calls);

        match res {
            Err(err) => (None, Some(err), true),
            Ok(val) => (Some(val), None, true),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::Result;

    use super::Group;

    const RES: usize = 7;

    async fn return_res() -> Result<usize> {
        Ok(RES)
    }

    #[tokio::test]
    async fn test_simple() {
        let g = Group::<String, _>::new();
        let res = g.work("key", return_res()).await.0;
        let r = res.unwrap();
        assert_eq!(r, RES);
    }

    #[tokio::test]
    async fn test_multiple_threads() {
        use futures::future::join_all;
        use std::sync::Arc;
        use std::time::Duration;

        async fn expensive_fn(cnt: Arc<AtomicUsize>) -> Result<usize> {
            cnt.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::new(1, 500)).await;
            Ok(RES)
        }

        let g = Arc::new(Group::<String, _>::new());
        let cnt = Arc::new(AtomicUsize::new(0));

        let mut handlers = Vec::new();
        for _ in 0..10 {
            let g = g.clone();
            let cnt = cnt.clone();

            handlers.push(tokio::spawn(async move {
                let res = g.work("key", expensive_fn(cnt)).await.0;
                let r = res.unwrap();
                assert_eq!(r, RES);
            }));
        }

        join_all(handlers).await;

        assert_eq!(1, cnt.load(Ordering::SeqCst));
    }
}
