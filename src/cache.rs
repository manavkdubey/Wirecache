use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use bytes::Bytes;
use hyper::{
    Response,
    header::{AGE, DATE, HeaderMap},
};
use tokio::time::Instant;

use crate::{
    body::{ProxyBody, full},
    policy::Freshness,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CacheKey(pub(crate) String);

pub(crate) struct Snapshot {
    headers: HeaderMap,
    body: Bytes,
    freshness: Freshness,
}

impl Snapshot {
    pub(crate) fn new(
        mut headers: HeaderMap,
        body: Bytes,
        freshness: Freshness,
        response_time: SystemTime,
    ) -> Self {
        headers
            .entry(DATE)
            .or_insert_with(|| httpdate::fmt_http_date(response_time).parse().unwrap());
        Self {
            headers,
            body,
            freshness,
        }
    }

    pub(crate) fn response(&self, now: Instant) -> Response<ProxyBody> {
        let mut response = Response::new(full(self.body.clone()));
        *response.headers_mut() = self.headers.clone();
        response.headers_mut().insert(
            AGE,
            self.freshness
                .age(now)
                .as_secs()
                .to_string()
                .parse()
                .unwrap(),
        );
        response
    }

    fn charge(&self, key: &CacheKey) -> Option<usize> {
        self.headers.iter().try_fold(
            self.body.len().checked_add(key.0.len().checked_mul(2)?)?,
            |bytes, (name, value)| {
                bytes
                    .checked_add(name.as_str().len())?
                    .checked_add(value.len())
            },
        )
    }
}

struct Entry {
    snapshot: Arc<Snapshot>,
    charge: usize,
    tick: u64,
}

#[derive(Default)]
struct Target {
    version: Arc<()>,
    fills: usize,
    mutations: usize,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct CacheStats {
    pub(crate) bytes: usize,
    pub(crate) entries: usize,
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) evictions: u64,
}

#[derive(Default)]
struct State {
    entries: HashMap<CacheKey, Entry>,
    order: BTreeMap<u64, CacheKey>,
    targets: HashMap<CacheKey, Target>,
    clock: u64,
    stats: CacheStats,
}

impl State {
    fn remove(&mut self, key: &CacheKey) {
        if let Some(entry) = self.entries.remove(key) {
            self.order.remove(&entry.tick);
            self.stats.bytes -= entry.charge;
            self.stats.entries -= 1;
        }
    }

    fn tick(&mut self) -> u64 {
        if self.clock == u64::MAX {
            self.entries.clear();
            self.order.clear();
            self.stats.bytes = 0;
            self.stats.entries = 0;
            self.clock = 0;
        }
        self.clock += 1;
        self.clock
    }
}

pub(crate) struct Cache {
    state: Mutex<State>,
    max_bytes: usize,
    max_entries: usize,
    max_entry_bytes: usize,
}

impl Cache {
    pub(crate) fn new(max_bytes: usize, max_entries: usize, max_entry_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            max_bytes,
            max_entries,
            max_entry_bytes,
        })
    }

    pub(crate) fn get(&self, key: &CacheKey, now: Instant) -> Option<Arc<Snapshot>> {
        let mut state = self.state.lock().unwrap();
        if state
            .targets
            .get(key)
            .is_some_and(|target| target.mutations > 0)
        {
            state.stats.misses += 1;
            return None;
        }
        let Some(entry) = state.entries.get(key) else {
            state.stats.misses += 1;
            return None;
        };
        if entry.snapshot.freshness.expires_at() <= now {
            state.remove(key);
            state.stats.misses += 1;
            return None;
        }
        let snapshot = entry.snapshot.clone();
        let old_tick = entry.tick;
        let tick = state.tick();
        if let Some(entry) = state.entries.get_mut(key) {
            entry.tick = tick;
            state.order.remove(&old_tick);
            state.order.insert(tick, key.clone());
        }
        state.stats.hits += 1;
        Some(snapshot)
    }

    pub(crate) fn begin_fill(self: &Arc<Self>, key: CacheKey) -> Fill {
        let mut state = self.state.lock().unwrap();
        let target = state.targets.entry(key.clone()).or_default();
        target.fills += 1;
        Fill {
            cache: self.clone(),
            key,
            version: target.version.clone(),
        }
    }

    pub(crate) fn begin_mutation(self: &Arc<Self>, key: CacheKey) -> Mutation {
        let mut state = self.state.lock().unwrap();
        state.remove(&key);
        let target = state.targets.entry(key.clone()).or_default();
        target.version = Arc::new(());
        target.mutations += 1;
        Mutation {
            cache: self.clone(),
            key,
        }
    }

    pub(crate) fn stats(&self) -> CacheStats {
        self.state.lock().unwrap().stats
    }
}

pub(crate) struct Fill {
    cache: Arc<Cache>,
    key: CacheKey,
    version: Arc<()>,
}

impl Fill {
    pub(crate) fn insert(&self, snapshot: Arc<Snapshot>, now: Instant) -> bool {
        if snapshot.body.len() > self.cache.max_entry_bytes
            || snapshot.freshness.expires_at() <= now
        {
            return false;
        }
        let Some(charge) = snapshot
            .charge(&self.key)
            .filter(|charge| *charge <= self.cache.max_bytes)
        else {
            return false;
        };
        let mut state = self.cache.state.lock().unwrap();
        let target = state.targets.get(&self.key).unwrap();
        if target.mutations > 0 || !Arc::ptr_eq(&self.version, &target.version) {
            return false;
        }
        state.remove(&self.key);
        let expired: Vec<_> = state
            .entries
            .iter()
            .filter(|(_, entry)| entry.snapshot.freshness.expires_at() <= now)
            .map(|(key, _)| key.clone())
            .collect();
        for key in expired {
            state.remove(&key);
        }
        while state.stats.entries >= self.cache.max_entries
            || state.stats.bytes > self.cache.max_bytes - charge
        {
            let Some((_, key)) = state.order.first_key_value() else {
                break;
            };
            let key = key.clone();
            state.remove(&key);
            state.stats.evictions += 1;
        }
        let tick = state.tick();
        state.order.insert(tick, self.key.clone());
        state.entries.insert(
            self.key.clone(),
            Entry {
                snapshot,
                charge,
                tick,
            },
        );
        state.stats.bytes += charge;
        state.stats.entries += 1;
        true
    }
}

impl Drop for Fill {
    fn drop(&mut self) {
        let mut state = self.cache.state.lock().unwrap();
        let target = state.targets.get_mut(&self.key).unwrap();
        target.fills -= 1;
        if target.fills == 0 && target.mutations == 0 {
            state.targets.remove(&self.key);
        }
    }
}

pub(crate) struct Mutation {
    cache: Arc<Cache>,
    key: CacheKey,
}

impl Drop for Mutation {
    fn drop(&mut self) {
        let mut state = self.cache.state.lock().unwrap();
        state.remove(&self.key);
        let target = state.targets.get_mut(&self.key).unwrap();
        target.version = Arc::new(());
        target.mutations -= 1;
        if target.fills == 0 && target.mutations == 0 {
            state.targets.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::response_freshness;
    use http_body_util::BodyExt;
    use hyper::StatusCode;
    use std::time::Duration;

    fn key(value: &str) -> CacheKey {
        CacheKey(format!("http://origin/public/{value}"))
    }

    fn snapshot(body: &'static str, now: Instant) -> Arc<Snapshot> {
        let mut headers = HeaderMap::new();
        headers.insert("cache-control", "public, max-age=10".parse().unwrap());
        let freshness = response_freshness(
            StatusCode::OK,
            &headers,
            now,
            now,
            SystemTime::UNIX_EPOCH,
            Duration::from_secs(10),
        )
        .unwrap();
        Arc::new(Snapshot::new(
            headers,
            Bytes::from_static(body.as_bytes()),
            freshness,
            SystemTime::UNIX_EPOCH,
        ))
    }

    #[test]
    fn lru_keeps_recently_read_entries_and_bounds_entry_count() {
        let cache = Cache::new(4096, 2, 1024);
        let now = Instant::now();
        assert!(cache.begin_fill(key("a")).insert(snapshot("a", now), now));
        assert!(cache.begin_fill(key("b")).insert(snapshot("b", now), now));
        assert!(cache.get(&key("a"), now).is_some());
        assert!(cache.begin_fill(key("c")).insert(snapshot("c", now), now));
        assert!(cache.get(&key("b"), now).is_none());
        assert!(cache.get(&key("a"), now).is_some());
        assert!(cache.get(&key("c"), now).is_some());
        assert_eq!(cache.stats().entries, 2);
        assert_eq!(cache.stats().evictions, 1);
        let state = cache.state.lock().unwrap();
        assert_eq!(state.order.len(), state.entries.len());
        assert!(state.targets.is_empty());
    }

    #[test]
    fn byte_budget_counts_keys_and_headers_and_replacement_does_not_leak() {
        let now = Instant::now();
        let value = snapshot("data", now);
        let charge = value.charge(&key("a")).unwrap();
        assert!(charge > value.body.len());
        let cache = Cache::new(charge * 2 - 1, 10, 1024);
        for _ in 0..100 {
            assert!(cache.begin_fill(key("a")).insert(value.clone(), now));
            assert_eq!(cache.stats().bytes, charge);
        }
        assert!(cache.begin_fill(key("b")).insert(value, now));
        assert!(cache.get(&key("a"), now).is_none());
        assert_eq!(cache.stats().bytes, charge);
        assert_eq!(cache.stats().entries, 1);
        let oversized = snapshot("too large", now);
        let small = Cache::new(4096, 10, 4);
        assert!(!small.begin_fill(key("large")).insert(oversized, now));
        assert_eq!(small.stats().bytes, 0);
    }

    #[test]
    fn expiry_uses_monotonic_header_time_and_releases_accounting() {
        let now = Instant::now();
        let cache = Cache::new(4096, 2, 1024);
        assert!(
            cache
                .begin_fill(key("a"))
                .insert(snapshot("a", now), now + Duration::from_secs(9))
        );
        assert!(
            cache
                .get(&key("a"), now + Duration::from_secs(10))
                .is_none()
        );
        assert_eq!(cache.stats().entries, 0);
        assert_eq!(cache.stats().bytes, 0);
        assert!(
            !cache
                .begin_fill(key("a"))
                .insert(snapshot("a", now), now + Duration::from_secs(10))
        );
    }

    #[test]
    fn overlapping_mutations_block_hits_and_fence_older_fills() {
        let now = Instant::now();
        let cache = Cache::new(4096, 2, 1024);
        let value = snapshot("old", now);
        assert!(cache.begin_fill(key("a")).insert(value.clone(), now));
        let old_fill = cache.begin_fill(key("a"));
        let first = cache.begin_mutation(key("a"));
        let during = cache.begin_fill(key("a"));
        let second = cache.begin_mutation(key("a"));
        assert!(cache.get(&key("a"), now).is_none());
        assert!(!during.insert(value.clone(), now));
        drop(first);
        assert!(!during.insert(value.clone(), now));
        drop(second);
        assert!(!old_fill.insert(value.clone(), now));
        assert!(!during.insert(value.clone(), now));
        assert!(cache.begin_fill(key("a")).insert(snapshot("new", now), now));
        drop(old_fill);
        drop(during);
        assert!(cache.get(&key("a"), now).is_some());
        assert!(cache.state.lock().unwrap().targets.is_empty());
    }

    #[test]
    fn cancelled_fills_and_mutations_do_not_retain_target_keys() {
        let cache = Cache::new(4096, 2, 1024);
        for number in 0..1000 {
            drop(cache.begin_fill(key(&number.to_string())));
            drop(cache.begin_mutation(key(&number.to_string())));
        }
        assert!(cache.state.lock().unwrap().targets.is_empty());
    }

    #[tokio::test]
    async fn snapshots_produce_independent_headers_and_shared_complete_bytes() {
        let now = Instant::now();
        let value = snapshot("complete", now);
        let mut first = value.response(now);
        first
            .headers_mut()
            .insert("x-private", "first".parse().unwrap());
        let second = value.response(now + Duration::from_secs(2));
        assert!(!second.headers().contains_key("x-private"));
        assert_eq!(second.headers()["age"], "2");
        assert_eq!(
            first.into_body().collect().await.unwrap().to_bytes(),
            "complete"
        );
        assert_eq!(
            second.into_body().collect().await.unwrap().to_bytes(),
            "complete"
        );
    }
}
