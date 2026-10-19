use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use hyper::Response;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch},
    task::JoinSet,
    time::{Instant, timeout_at},
};

use crate::{
    Config,
    body::ProxyBody,
    cache::{Cache, CacheKey, Fill, Snapshot},
    error::FetchError,
    upstream::{FetchResult, Upstream},
};

#[derive(Clone)]
pub(crate) enum Outcome {
    Shared(Arc<Snapshot>),
    RetryUnshared,
    Failed(FetchError),
}

struct Flight {
    version: Arc<()>,
    tx: watch::Sender<Option<Outcome>>,
}

impl Flight {
    fn publish(&self, outcome: Outcome) {
        self.tx.send_if_modified(|value| {
            if value.is_some() {
                return false;
            }
            *value = Some(outcome);
            true
        });
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct FlightStats {
    pub(crate) created: u64,
    pub(crate) joined: u64,
    pub(crate) unshared: u64,
    pub(crate) active: usize,
    pub(crate) waiters: usize,
}

#[derive(Default)]
struct State {
    flights: HashMap<CacheKey, Arc<Flight>>,
    waiters: HashMap<CacheKey, usize>,
    tasks: JoinSet<()>,
    closed: bool,
    stats: FlightStats,
}

impl State {
    fn reap(&mut self) {
        while let Some(result) = self.tasks.try_join_next() {
            if let Err(error) = result
                && error.is_panic()
            {
                tracing::error!("single-flight task panicked; waiters received a terminal failure");
            }
        }
    }
}

pub(crate) struct Coordinator {
    state: Mutex<State>,
    cache: Arc<Cache>,
    slots: Arc<Semaphore>,
    waiter_slots: Arc<Semaphore>,
    max_flights: usize,
    max_waiters: usize,
    per_key: usize,
}

pub(crate) enum Admission {
    Hit(Arc<Snapshot>),
    Wait(Ticket),
    Start(Ticket, Work),
    Bypass,
}

impl Coordinator {
    pub(crate) fn new(cache: Arc<Cache>, config: &Config) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            cache,
            slots: Arc::new(Semaphore::new(config.max_flights)),
            waiter_slots: Arc::new(Semaphore::new(config.max_waiters)),
            max_flights: config.max_flights,
            max_waiters: config.max_waiters,
            per_key: config.max_waiters_per_key,
        })
    }

    pub(crate) fn admit(self: &Arc<Self>, key: CacheKey) -> Result<Admission, FetchError> {
        let mut state = self.state.lock().unwrap();
        state.reap();
        if state.closed {
            return Err(FetchError::Shutdown);
        }
        if let Some(snapshot) = self.cache.get(&key, Instant::now()) {
            return Ok(Admission::Hit(snapshot));
        }
        if state
            .waiters
            .get(&key)
            .is_some_and(|count| *count >= self.per_key)
        {
            return Err(FetchError::Overloaded);
        }
        let global = self
            .waiter_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| FetchError::Overloaded)?;
        if let Some(flight) = state.flights.get(&key).cloned() {
            if self.cache.version_matches(&key, &flight.version) {
                state.stats.joined += 1;
                return Ok(Admission::Wait(Ticket::new(
                    self, &mut state, &key, flight, global, None,
                )));
            }
            state.flights.remove(&key);
        }
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| FetchError::Overloaded)?;
        let fill = self.cache.begin_fill(key.clone());
        if !fill.is_current() {
            return Ok(Admission::Bypass);
        }
        let (tx, _) = watch::channel(None);
        let flight = Arc::new(Flight {
            version: fill.version(),
            tx,
        });
        let (private_tx, private_rx) = oneshot::channel();
        let ticket = Ticket::new(
            self,
            &mut state,
            &key,
            flight.clone(),
            global,
            Some(private_rx),
        );
        state.flights.insert(key.clone(), flight.clone());
        state.stats.created += 1;
        Ok(Admission::Start(
            ticket,
            Work {
                coordinator: Arc::downgrade(self),
                key,
                flight,
                fill: Some(fill),
                private: Some(private_tx),
                _permit: permit,
                finished: false,
            },
        ))
    }

    pub(crate) fn spawn(
        &self,
        mut work: Work,
        upstream: Arc<Upstream>,
        request: hyper::Request<ProxyBody>,
        deadline: Instant,
    ) {
        let mut state = self.state.lock().unwrap();
        state.reap();
        if state.closed {
            drop(state);
            work.finish(Outcome::Failed(FetchError::Shutdown));
            return;
        }
        state.tasks.spawn(async move {
            let fill = work.fill.take();
            match upstream.fetch(request, fill, deadline).await {
                FetchResult::Shared(snapshot) => work.finish(Outcome::Shared(snapshot)),
                FetchResult::Private(response) => {
                    if let Some(private) = work.private.take() {
                        let _ = private.send(response);
                    }
                    work.finish(Outcome::RetryUnshared);
                }
                FetchResult::Failed(error) => work.finish(Outcome::Failed(error)),
            }
        });
    }

    pub(crate) fn stats(&self) -> FlightStats {
        let mut state = self.state.lock().unwrap();
        state.reap();
        FlightStats {
            active: self.max_flights - self.slots.available_permits(),
            waiters: self.max_waiters - self.waiter_slots.available_permits(),
            ..state.stats
        }
    }

    pub(crate) async fn shutdown(&self) {
        let mut tasks = {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            for flight in state.flights.values() {
                flight.publish(Outcome::Failed(FetchError::Shutdown));
            }
            state.flights.clear();
            let mut tasks = std::mem::take(&mut state.tasks);
            tasks.abort_all();
            tasks
        };
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result
                && error.is_panic()
            {
                tracing::error!("single-flight task panicked during shutdown");
            }
        }
    }
}

pub(crate) struct Ticket {
    coordinator: Weak<Coordinator>,
    key: CacheKey,
    rx: watch::Receiver<Option<Outcome>>,
    pub(crate) private: Option<oneshot::Receiver<Response<ProxyBody>>>,
    _global: OwnedSemaphorePermit,
}

impl Ticket {
    fn new(
        coordinator: &Arc<Coordinator>,
        state: &mut State,
        key: &CacheKey,
        flight: Arc<Flight>,
        global: OwnedSemaphorePermit,
        private: Option<oneshot::Receiver<Response<ProxyBody>>>,
    ) -> Self {
        *state.waiters.entry(key.clone()).or_default() += 1;
        Self {
            rx: flight.tx.subscribe(),
            coordinator: Arc::downgrade(coordinator),
            key: key.clone(),
            private,
            _global: global,
        }
    }

    pub(crate) async fn wait(&mut self, deadline: Instant) -> Outcome {
        loop {
            let outcome = self.rx.borrow_and_update().clone();
            if let Some(outcome) = outcome {
                return outcome;
            }
            match timeout_at(deadline, self.rx.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Outcome::Failed(FetchError::Internal),
                Err(_) => return Outcome::Failed(FetchError::Timeout),
            }
        }
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        if let Some(coordinator) = self.coordinator.upgrade() {
            let mut state = coordinator.state.lock().unwrap();
            if let Some(count) = state.waiters.get_mut(&self.key) {
                *count -= 1;
                if *count == 0 {
                    state.waiters.remove(&self.key);
                }
            }
        }
    }
}

pub(crate) struct Work {
    coordinator: Weak<Coordinator>,
    key: CacheKey,
    flight: Arc<Flight>,
    fill: Option<Fill>,
    private: Option<oneshot::Sender<Response<ProxyBody>>>,
    _permit: OwnedSemaphorePermit,
    finished: bool,
}

impl Work {
    fn finish(&mut self, outcome: Outcome) {
        let unshared = matches!(outcome, Outcome::RetryUnshared);
        self.flight.publish(outcome);
        if let Some(coordinator) = self.coordinator.upgrade() {
            let mut state = coordinator.state.lock().unwrap();
            if unshared {
                state.stats.unshared += 1;
            }
            if state
                .flights
                .get(&self.key)
                .is_some_and(|current| Arc::ptr_eq(current, &self.flight))
            {
                state.flights.remove(&self.key);
            }
        }
        self.finished = true;
    }
}

impl Drop for Work {
    fn drop(&mut self) {
        if !self.finished {
            let closed = self
                .coordinator
                .upgrade()
                .is_none_or(|coordinator| coordinator.state.lock().unwrap().closed);
            self.finish(Outcome::Failed(if closed {
                FetchError::Shutdown
            } else {
                FetchError::Internal
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::time::Duration;

    fn setup(options: &[&str]) -> Arc<Coordinator> {
        let mut args = vec!["wirecache", "--upstream", "http://127.0.0.1:9"];
        args.extend_from_slice(options);
        let config = Config::try_parse_from(args).unwrap();
        Coordinator::new(Cache::new(4096, 10, 1024), &config)
    }

    fn key(value: &str) -> CacheKey {
        CacheKey(format!("http://127.0.0.1:9/public/{value}"))
    }

    fn start(coordinator: &Arc<Coordinator>, value: &str) -> (Ticket, Work) {
        match coordinator.admit(key(value)).unwrap() {
            Admission::Start(ticket, work) => (ticket, work),
            _ => panic!("expected a new flight"),
        }
    }

    async fn outcome(ticket: &mut Ticket) -> Outcome {
        ticket.wait(Instant::now() + Duration::from_secs(1)).await
    }

    #[tokio::test]
    async fn completion_is_retained_before_waiting_and_for_late_subscribers() {
        let coordinator = setup(&[]);
        let (mut first, mut work) = start(&coordinator, "a");
        work.finish(Outcome::Failed(FetchError::Transport));
        let global = coordinator
            .waiter_slots
            .clone()
            .try_acquire_owned()
            .unwrap();
        let mut late = Ticket::new(
            &coordinator,
            &mut coordinator.state.lock().unwrap(),
            &key("a"),
            work.flight.clone(),
            global,
            None,
        );
        assert!(matches!(
            outcome(&mut first).await,
            Outcome::Failed(FetchError::Transport)
        ));
        assert!(matches!(
            outcome(&mut late).await,
            Outcome::Failed(FetchError::Transport)
        ));
        work.flight.publish(Outcome::RetryUnshared);
        assert!(matches!(
            outcome(&mut late).await,
            Outcome::Failed(FetchError::Transport)
        ));
        drop((first, late, work));
        assert_eq!(coordinator.stats().active, 0);
        assert_eq!(coordinator.stats().waiters, 0);
    }

    #[tokio::test]
    async fn dropped_unstarted_work_publishes_failure_and_allows_retry() {
        let coordinator = setup(&[]);
        let (mut ticket, work) = start(&coordinator, "a");
        drop(work);
        assert!(matches!(
            outcome(&mut ticket).await,
            Outcome::Failed(FetchError::Internal)
        ));
        let (_, replacement) = start(&coordinator, "a");
        drop(replacement);
    }

    #[tokio::test]
    async fn old_cleanup_cannot_remove_a_replacement_after_mutation() {
        let coordinator = setup(&[]);
        let (mut old, mut old_work) = start(&coordinator, "a");
        drop(coordinator.cache.begin_mutation(key("a")));
        let (mut new, mut new_work) = start(&coordinator, "a");
        old_work.finish(Outcome::Failed(FetchError::Transport));
        assert!(
            coordinator
                .state
                .lock()
                .unwrap()
                .flights
                .get(&key("a"))
                .is_some_and(|flight| Arc::ptr_eq(flight, &new_work.flight))
        );
        new_work.finish(Outcome::RetryUnshared);
        assert!(matches!(
            outcome(&mut old).await,
            Outcome::Failed(FetchError::Transport)
        ));
        assert!(matches!(outcome(&mut new).await, Outcome::RetryUnshared));
    }

    #[tokio::test]
    async fn flight_and_per_key_waiter_limits_release_capacity_on_drop() {
        let coordinator = setup(&["--max-flights", "1", "--max-waiters-per-key", "2"]);
        let (first, work) = start(&coordinator, "a");
        let Admission::Wait(second) = coordinator.admit(key("a")).unwrap() else {
            panic!("expected join");
        };
        assert!(matches!(
            coordinator.admit(key("a")),
            Err(FetchError::Overloaded)
        ));
        assert!(matches!(
            coordinator.admit(key("b")),
            Err(FetchError::Overloaded)
        ));
        drop(second);
        assert!(matches!(
            coordinator.admit(key("a")),
            Ok(Admission::Wait(_))
        ));
        drop((first, work));
        let (_, work) = start(&coordinator, "b");
        drop(work);
    }

    #[tokio::test]
    async fn global_waiter_limit_applies_across_keys_and_releases_on_cancel() {
        let coordinator = setup(&["--max-waiters", "1"]);
        let (ticket, work) = start(&coordinator, "a");
        assert!(matches!(
            coordinator.admit(key("b")),
            Err(FetchError::Overloaded)
        ));
        drop(ticket);
        let (_, replacement) = start(&coordinator, "b");
        drop((work, replacement));
        assert_eq!(coordinator.stats().waiters, 0);
    }

    #[tokio::test]
    async fn per_key_limit_includes_callers_on_replaced_flights() {
        let coordinator = setup(&["--max-waiters-per-key", "1"]);
        let (old, work) = start(&coordinator, "a");
        drop(coordinator.cache.begin_mutation(key("a")));
        assert!(matches!(
            coordinator.admit(key("a")),
            Err(FetchError::Overloaded)
        ));
        drop(old);
        let (new, replacement) = start(&coordinator, "a");
        drop((work, replacement, new));
        assert!(coordinator.state.lock().unwrap().waiters.is_empty());
    }

    #[tokio::test]
    async fn caller_timeout_does_not_cancel_owned_work() {
        let coordinator = setup(&["--max-waiters-per-key", "1"]);
        let (mut first, mut work) = start(&coordinator, "a");
        assert!(matches!(
            first.wait(Instant::now()).await,
            Outcome::Failed(FetchError::Timeout)
        ));
        drop(first);
        let Admission::Wait(mut second) = coordinator.admit(key("a")).unwrap() else {
            panic!("expected existing work");
        };
        work.finish(Outcome::RetryUnshared);
        assert!(matches!(outcome(&mut second).await, Outcome::RetryUnshared));
        assert_eq!(coordinator.stats().created, 1);
    }

    #[tokio::test]
    async fn panic_and_abort_publish_terminal_failures_and_clean_registry() {
        for panic in [true, false] {
            let coordinator = setup(&[]);
            let (mut ticket, work) = start(&coordinator, "a");
            let abort = coordinator.state.lock().unwrap().tasks.spawn(async move {
                let _work = work;
                assert!(!panic, "injected flight panic");
                std::future::pending::<()>().await;
            });
            if !panic {
                abort.abort();
            }
            assert!(matches!(
                outcome(&mut ticket).await,
                Outcome::Failed(FetchError::Internal)
            ));
            drop(ticket);
            assert_eq!(coordinator.stats().active, 0);
            let (_, next) = start(&coordinator, "a");
            drop(next);
        }
    }

    #[tokio::test]
    async fn shutdown_aborts_owned_work_and_rejects_new_admission() {
        let coordinator = setup(&[]);
        let (mut obsolete, obsolete_work) = start(&coordinator, "a");
        drop(coordinator.cache.begin_mutation(key("a")));
        let (mut ticket, work) = start(&coordinator, "a");
        coordinator.state.lock().unwrap().tasks.spawn(async move {
            let _work = obsolete_work;
            std::future::pending::<()>().await;
        });
        coordinator.state.lock().unwrap().tasks.spawn(async move {
            let _work = work;
            std::future::pending::<()>().await;
        });
        coordinator.shutdown().await;
        assert!(matches!(
            outcome(&mut ticket).await,
            Outcome::Failed(FetchError::Shutdown)
        ));
        assert!(matches!(
            outcome(&mut obsolete).await,
            Outcome::Failed(FetchError::Shutdown)
        ));
        drop((ticket, obsolete));
        assert_eq!(coordinator.stats().active, 0);
        assert_eq!(coordinator.stats().waiters, 0);
        assert!(matches!(
            coordinator.admit(key("a")),
            Err(FetchError::Shutdown)
        ));
    }

    #[tokio::test]
    async fn active_mutation_bypasses_flights_without_leaking_slots() {
        let coordinator = setup(&[]);
        let mutation = coordinator.cache.begin_mutation(key("a"));
        assert!(matches!(coordinator.admit(key("a")), Ok(Admission::Bypass)));
        assert_eq!(coordinator.stats().active, 0);
        assert_eq!(coordinator.stats().waiters, 0);
        drop(mutation);
        let (_, work) = start(&coordinator, "a");
        drop(work);
    }
}
