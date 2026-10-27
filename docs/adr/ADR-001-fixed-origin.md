# ADR-001: One configured trusted HTTP origin

Status: accepted.

WireCache forwards HTTP/1.1 to one operator-configured `http://` origin. Configuration rejects credentials, upstream path prefixes, and queries. The listener defaults to loopback. Client Host headers and absolute-form targets never select the destination: routing replaces scheme/authority with the configured origin, preserves path/query bytes, and rewrites Host.

This keeps connection pooling, cache identity, invalidation, and admission scoped to one origin and prevents clients from turning the application into an arbitrary-destination proxy. CONNECT and protocol upgrades are rejected. Request/response connection-specific headers are removed before forwarding.

Dynamic upstream selection, TLS termination, tunnels, and multi-origin routing are outside P0. The configured hostname and its DNS resolution remain trusted operator inputs; fixed routing is not a general network sandbox. Tests cover hostile Host/absolute-form targets, unsupported upgrades, and invalid upstream configuration.
