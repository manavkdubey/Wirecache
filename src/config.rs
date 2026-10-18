use std::{io, net::SocketAddr};

use clap::Parser;
use hyper::Uri;

#[derive(Clone, Debug, Parser)]
#[command(name = "wirecache", about = "A fixed-origin HTTP/1.1 reverse proxy")]
pub struct Config {
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen: SocketAddr,
    #[arg(long)]
    pub upstream: Uri,
    #[arg(long, default_value_t = 3000)]
    pub upstream_timeout_ms: u64,
    #[arg(long, default_value_t = 128)]
    pub max_connections: usize,
    #[arg(long, default_value_t = 64)]
    pub max_upstream_requests: usize,
    #[arg(long, default_value_t = 8_388_608)]
    pub max_request_body_bytes: usize,
    #[arg(long)]
    pub cache_path_prefix: Option<String>,
    #[arg(long, default_value_t = 64)]
    pub max_cache_mib: usize,
    #[arg(long, default_value_t = 1024)]
    pub max_entry_kib: usize,
    #[arg(long, default_value_t = 4096)]
    pub max_cache_entries: usize,
    #[arg(long, default_value_t = 300)]
    pub max_ttl_secs: u64,
}

impl Config {
    pub(crate) fn validate(&self) -> io::Result<()> {
        let authority = self.upstream.authority();
        if self.upstream.scheme_str() != Some("http")
            || authority.is_none()
            || authority.is_some_and(|value| value.as_str().contains('@'))
            || self.upstream.host().is_none_or(str::is_empty)
            || self
                .upstream
                .path_and_query()
                .is_some_and(|value| value.as_str() != "/")
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "upstream must be an http:// origin without credentials, path, or query",
            ));
        }
        if self.upstream_timeout_ms == 0
            || self.upstream_timeout_ms > 86_400_000
            || self.max_connections == 0
            || self.max_connections > 65_536
            || self.max_upstream_requests == 0
            || self.max_upstream_requests > 65_536
            || self.max_request_body_bytes == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "limits must be positive; timeout must be at most one day and concurrency at most 65536",
            ));
        }
        if self.cache_path_prefix.as_ref().is_some_and(|prefix| {
            !prefix.starts_with('/')
                || !prefix.ends_with('/')
                || prefix.contains(['%', '?', '#', '\\'])
                || prefix
                    .split('/')
                    .any(|segment| matches!(segment, "." | ".."))
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache prefix must be an absolute directory path ending in / without encoded or dot segments",
            ));
        }
        if self.max_cache_mib == 0
            || self.max_entry_kib == 0
            || self.max_cache_mib.checked_mul(1024 * 1024).is_none()
            || self.max_entry_kib.checked_mul(1024).is_none()
            || self.max_entry_kib.saturating_mul(1024)
                > self.max_cache_mib.saturating_mul(1024 * 1024)
            || self.max_cache_entries == 0
            || self.max_cache_entries > 1_000_000
            || self.max_ttl_secs == 0
            || self.max_ttl_secs > 31_536_000
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache limits must be positive, entry size must fit cache, entry count at most 1000000, and TTL at most one year",
            ));
        }
        Ok(())
    }
}
