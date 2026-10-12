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
        Ok(())
    }
}
