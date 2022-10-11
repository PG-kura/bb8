use std::cmp::{max, min};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use futures_channel::oneshot;
use futures_util::stream::{FuturesUnordered, StreamExt};
use futures_util::TryFutureExt;
use tokio::spawn;
use tokio::time::{interval_at, sleep, timeout, Interval};

use crate::api::{Builder, ManageConnection, PooledConnection, RunError};
use crate::internals::{Approval, ApprovalIter, Conn, SharedPool, State};

pub(crate) struct PoolInner<M>
where
    M: ManageConnection + Send,
{
    inner: Arc<SharedPool<M>>,
}

impl<M> PoolInner<M>
where
    M: ManageConnection + Send,
{
    pub(crate) fn new(builder: Builder<M>, manager: M) -> Self {
        let inner = Arc::new(SharedPool::new(builder, manager));

        if inner.statics.max_lifetime.is_some() || inner.statics.idle_timeout.is_some() {
            let s = Arc::downgrade(&inner);
            if let Some(shared) = s.upgrade() {
                let start = Instant::now() + shared.statics.reaper_rate;
                let interval = interval_at(start.into(), shared.statics.reaper_rate);
                schedule_reaping(interval, s);
            }
        }

        Self { inner }
    }

    pub(crate) async fn start_connections(&self) -> Result<(), M::Error> {
        log::info!("PoolInner::start_connections() aquire lock");
        let wanted = {
            let mut locked = self.inner.internals.lock();
            log::info!("PoolInner::start_connections() got lock");
            locked.wanted(&self.inner.statics)
        };
        log::info!("PoolInner::start_connections() release lock");
        let mut stream = self.replenish_idle_connections(wanted);
        while let Some(result) = stream.next().await {
            result?;
        }
        Ok(())
    }

    pub(crate) fn spawn_start_connections(&self) {
        let res = {
            log::info!("PoolInner::spawn_start_connections() aquire lock");
            let mut locked = self.inner.internals.lock();
            log::info!("PoolInner::spawn_start_connections() got lock");
            self.spawn_replenishing_approvals(locked.wanted(&self.inner.statics));
        };
        log::info!("PoolInner::spawn_start_connections() release lock");
        res
    }

    fn spawn_replenishing_approvals(&self, approvals: ApprovalIter) {
        if approvals.len() == 0 {
            return;
        }

        let this = self.clone();
        spawn(async move {
            let mut stream = this.replenish_idle_connections(approvals);
            while let Some(result) = stream.next().await {
                match result {
                    Ok(()) => {}
                    Err(e) => this.inner.statics.error_sink.sink(e),
                }
            }
        });
    }

    fn replenish_idle_connections(
        &self,
        approvals: ApprovalIter,
    ) -> FuturesUnordered<impl Future<Output = Result<(), M::Error>>> {
        let stream = FuturesUnordered::new();
        for approval in approvals {
            let this = self.clone();
            stream.push(async move { this.add_connection(approval).await });
        }
        stream
    }

    pub(crate) async fn get(&self) -> Result<PooledConnection<'_, M>, RunError<M::Error>> {
        self.make_pooled(|this, conn| PooledConnection::new(this, conn))
            .await
    }

    pub(crate) async fn get_owned(
        &self,
    ) -> Result<PooledConnection<'static, M>, RunError<M::Error>> {
        self.make_pooled(|this, conn| {
            let pool = PoolInner {
                inner: Arc::clone(&this.inner),
            };
            PooledConnection::new_owned(pool, conn)
        })
        .await
    }

    pub(crate) async fn make_pooled<'a, 'b, F>(
        &'a self,
        make_pooled_conn: F,
    ) -> Result<PooledConnection<'b, M>, RunError<M::Error>>
    where
        F: Fn(&'a Self, Conn<M::Connection>) -> PooledConnection<'b, M>,
    {
        loop {
            let mut conn = {
                log::info!("PoolInner::make_pooled(1) aquire lock");
                let mut locked = self.inner.internals.lock();
                log::info!("PoolInner::make_pooled(1) got lock");
                match locked.pop(&self.inner.statics) {
                    Some((conn, approvals)) => {
                        self.spawn_replenishing_approvals(approvals);

                        // make_pooled_conn = |this, conn| PooledConnection::new(this, conn)

                        make_pooled_conn(self, conn)
                    }
                    None => {
                        log::info!("PoolInner::make_pooled(1) break loop");
                        break
                    },
                }
            };
            log::info!("PoolInner::make_pooled(1) release lock");

            if !self.inner.statics.test_on_check_out {
                return Ok(conn);
            }

            match self.inner.manager.is_valid(&mut conn).await {
                Ok(()) => return Ok(conn),
                Err(e) => {
                    self.inner.statics.error_sink.sink(e);
                    conn.drop_invalid();
                    continue;
                }
            }
        }

        let (tx, rx) = oneshot::channel();
        {
            let res = {
                log::info!("PoolInner::make_pooled(2) aquire lock");
                let mut locked = self.inner.internals.lock();
                log::info!("PoolInner::make_pooled(2) got lock");
                let approvals = locked.push_waiter(tx, &self.inner.statics);
                log::info!("PoolInner::make_pooled(2) approvals: {:?}", approvals);
                self.spawn_replenishing_approvals(approvals);
            };
            log::info!("PoolInner::make_pooled(2) release lock");
            res
        };

        match timeout(self.inner.statics.connection_timeout, rx).await {
            Ok(Ok(mut guard)) => {
                let res = {
                    log::info!("PoolInner::make_pooled(3) extract");
                    let extracted = guard.extract();
                    log::info!("PoolInner::make_pooled(3) make_pooled_conn() start");
                    // make_pooled_conn = |this, conn| PooledConnection::new(this, conn)
                    let res = make_pooled_conn(self, extracted);
                    log::info!("PoolInner::make_pooled(3) make_pooled_conn() ended");
                    Ok(res)
                };
                log::info!("PoolInner::make_pooled(3) guard dropped");
                res
            },
            _ => Err(RunError::TimedOut),
        }
    }

    pub(crate) async fn connect(&self) -> Result<M::Connection, M::Error> {
        let mut conn = self.inner.manager.connect().await?;
        self.on_acquire_connection(&mut conn).await?;
        Ok(conn)
    }

    /// Return connection back in to the pool
    pub(crate) fn put_back(&self, conn: Option<Conn<M::Connection>>) {
        let res = {
            let conn = conn.and_then(|mut conn| {
                if !self.inner.manager.has_broken(&mut conn.conn) {
                    Some(conn)
                } else {
                    None
                }
            });

            log::info!("PoolInner::put_back() aquire lock");
            let mut locked = self.inner.internals.lock();
            log::info!("PoolInner::put_back() got lock");
            match conn {
                Some(conn) => locked.put(conn, None, self.inner.clone()),
                None => {
                    let approvals = locked.dropped(1, &self.inner.statics);
                    self.spawn_replenishing_approvals(approvals);
                }
            }
        };
        log::info!("PoolInner::put_back() release lock");
        res
    }

    /// Returns information about the current state of the pool.
    pub(crate) fn state(&self) -> State {
        let res = {
            log::info!("PoolInner::state() aquire lock");
            let locked = self.inner.internals.lock();
            log::info!("PoolInner::state() got lock");
            locked.state()
        };
        log::info!("PoolInner::state() release lock");
        res
    }

    fn reap(&self) {
        let res = {
            log::info!("PoolInner::reap() aquire lock");
            let mut internals = self.inner.internals.lock();
            log::info!("PoolInner::reap() got lock");
            let approvals = internals.reap(&self.inner.statics);
            self.spawn_replenishing_approvals(approvals);
        };
        log::info!("PoolInner::reap() release lock");
        res
    }

    // Outside of Pool to avoid borrow splitting issues on self
    async fn add_connection(&self, approval: Approval) -> Result<(), M::Error>
    where
        M: ManageConnection,
    {
        let new_shared = Arc::downgrade(&self.inner);
        let shared = match new_shared.upgrade() {
            None => return Ok(()),
            Some(shared) => shared,
        };

        let start = Instant::now();
        let mut delay = Duration::from_secs(0);
        loop {
            let conn = shared
                .manager
                .connect()
                .and_then(|mut c| async { self.on_acquire_connection(&mut c).await.map(|_| c) })
                .await;

            match conn {
                Ok(conn) => {
                    let conn = Conn::new(conn);

                    {
                        log::info!("PoolInner::add_connection(0) aquire lock");
                        let mut locked = shared
                            .internals
                            .lock();
                        log::info!("PoolInner::add_connection(0) got lock");
                        locked.put(conn, Some(approval), self.inner.clone());
                    }
                    log::info!("PoolInner::add_connection(0) release lock");

                    return Ok(());
                }
                Err(e) => {
                    if Instant::now() - start > self.inner.statics.connection_timeout {
                        let res = {
                            log::info!("PoolInner::add_connection() aquire lock");
                            let mut locked = shared.internals.lock();
                            log::info!("PoolInner::add_connection() got lock");
                            locked.connect_failed(approval);
                            Err(e)
                        };
                        log::info!("PoolInner::add_connection() release lock");
                        return res;
                    } else {
                        delay = max(Duration::from_millis(200), delay);
                        delay = min(self.inner.statics.connection_timeout / 2, delay * 2);
                        sleep(delay).await;
                    }
                }
            }
        }
    }

    async fn on_acquire_connection(&self, conn: &mut M::Connection) -> Result<(), M::Error> {
        match self.inner.statics.connection_customizer.as_ref() {
            Some(customizer) => customizer.on_acquire(conn).await,
            None => Ok(()),
        }
    }
}

impl<M> Clone for PoolInner<M>
where
    M: ManageConnection,
{
    fn clone(&self) -> Self {
        PoolInner {
            inner: self.inner.clone(),
        }
    }
}

impl<M> fmt::Debug for PoolInner<M>
where
    M: ManageConnection,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!("PoolInner({:p})", self.inner))
    }
}

fn schedule_reaping<M>(mut interval: Interval, weak_shared: Weak<SharedPool<M>>)
where
    M: ManageConnection,
{
    spawn(async move {
        loop {
            let _ = interval.tick().await;
            if let Some(inner) = weak_shared.upgrade() {
                PoolInner { inner }.reap();
            } else {
                break;
            }
        }
    });
}
