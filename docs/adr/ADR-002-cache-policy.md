# ADR-002: Explicit public routes and one request profile

Status: accepted.

Caching is opt-in through an absolute directory prefix ending in `/`. Only bodyless GETs with Host, Connection close/keep-alive, optional zero Content-Length, Accept `*/*`, and Accept-Encoding `identity` qualify. Candidates are forwarded with fixed Host/Accept/Accept-Encoding and no explicit Content-Length. Extra headers, encoded paths, and dot segments bypass caching. Bypass preserves end-to-end headers and bodies.

The operator must guarantee that the configured routes are public for this profile. A matching prefix alone cannot prove that contract. Unknown headers bypass rather than being silently removed or expanding the cache key.

Responses require 200, explicit positive shared freshness, and a complete bounded body. Set-Cookie, Vary, content ranges, encodings, trailers, malformed or duplicate directives, and unsupported directives prevent storage. `s-maxage` takes precedence; otherwise `public` plus `max-age` is required.

Freshness follows the explicit-age calculation in [RFC 9111 section 4.2.3](https://www.rfc-editor.org/rfc/rfc9111.html#section-4.2.3): initial age is the maximum of apparent Date age and Age plus response delay. Monotonic time accounts for buffering and residence. Local retention caps remaining freshness. Invalid or duplicate Date/Age bypass; missing Date uses response receipt time and is generated on stored responses.

Conditional revalidation and Vary variants remain out of scope. This subset does not claim complete RFC compliance. Future coalescing must apply the same request and response gates before sharing.
