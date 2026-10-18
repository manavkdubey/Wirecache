use std::time::{Duration, SystemTime};

use hyper::{Method, StatusCode, header::HeaderMap};
use tokio::time::Instant;

pub(crate) fn request_candidate(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    empty_body: bool,
    prefix: Option<&str>,
) -> bool {
    if method != Method::GET
        || !empty_body
        || !prefix.is_some_and(|prefix| path.starts_with(prefix))
        || path.contains(['%', '\\'])
        || path.split('/').any(|part| matches!(part, "." | ".."))
    {
        return false;
    }
    headers.keys().all(|name| {
        let mut values = headers.get_all(name).iter();
        let Some(value) = values.next().and_then(|value| value.to_str().ok()) else {
            return false;
        };
        if values.next().is_some() {
            return false;
        }
        match name.as_str() {
            "host" => true,
            "content-length" => value == "0",
            "accept" => value.trim() == "*/*",
            "accept-encoding" => value.trim().eq_ignore_ascii_case("identity"),
            "connection" => value.split(',').all(|token| {
                matches!(
                    token.trim().to_ascii_lowercase().as_str(),
                    "close" | "keep-alive"
                )
            }),
            _ => false,
        }
    })
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Freshness {
    response_at: Instant,
    age_at_response: Duration,
    remaining: Duration,
}

impl Freshness {
    pub(crate) fn age(&self, now: Instant) -> Duration {
        self.age_at_response
            .saturating_add(now.saturating_duration_since(self.response_at))
    }

    pub(crate) fn expires_at(&self) -> Instant {
        self.response_at + self.remaining
    }
}

pub(crate) fn response_freshness(
    status: StatusCode,
    headers: &HeaderMap,
    requested_at: Instant,
    response_at: Instant,
    response_time: SystemTime,
    max_ttl: Duration,
) -> Option<Freshness> {
    if status != StatusCode::OK
        || [
            "set-cookie",
            "vary",
            "content-range",
            "content-encoding",
            "trailer",
        ]
        .iter()
        .any(|name| headers.contains_key(*name))
    {
        return None;
    }
    let lifetime = cache_lifetime(headers)?;
    let date = single_header(headers, "date")?
        .map(httpdate::parse_http_date)
        .transpose()
        .ok()?
        .unwrap_or(response_time);
    let age = match single_header(headers, "age")? {
        Some(value) => delta_seconds(value)?,
        None => Duration::ZERO,
    };
    let apparent_age = response_time.duration_since(date).unwrap_or_default();
    let corrected_age = age.checked_add(response_at.checked_duration_since(requested_at)?)?;
    let age_at_response = apparent_age.max(corrected_age);
    let remaining = lifetime.checked_sub(age_at_response)?.min(max_ttl);
    if remaining.is_zero() {
        return None;
    }
    Some(Freshness {
        response_at,
        age_at_response,
        remaining,
    })
}

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<Option<&'a str>> {
    let mut values = headers.get_all(name).iter();
    let value = values.next().map(|value| value.to_str()).transpose().ok()?;
    if values.next().is_some() {
        return None;
    }
    Some(value)
}

fn delta_seconds(value: &str) -> Option<Duration> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(Duration::from_secs(value.parse().ok()?))
}

fn cache_lifetime(headers: &HeaderMap) -> Option<Duration> {
    let mut public = false;
    let mut max_age = None;
    let mut shared_age = None;
    let mut flags = 0u8;
    for value in headers.get_all("cache-control") {
        for directive in value.to_str().ok()?.split(',') {
            let (name, value) = match directive.trim().split_once('=') {
                Some((name, value)) => (name.trim(), Some(value.trim())),
                None => (directive.trim(), None),
            };
            match name.to_ascii_lowercase().as_str() {
                "public" if value.is_none() && !public => public = true,
                "max-age" | "s-maxage" => {
                    let raw = value?;
                    let raw = if raw.starts_with('"') {
                        raw.strip_prefix('"')?.strip_suffix('"')?
                    } else {
                        raw
                    };
                    let parsed = delta_seconds(raw)?;
                    let slot = if name.eq_ignore_ascii_case("s-maxage") {
                        &mut shared_age
                    } else {
                        &mut max_age
                    };
                    if slot.replace(parsed).is_some() {
                        return None;
                    }
                }
                "must-revalidate" | "proxy-revalidate" | "no-transform" | "immutable"
                    if value.is_none() =>
                {
                    let bit = match name.to_ascii_lowercase().as_str() {
                        "must-revalidate" => 1,
                        "proxy-revalidate" => 2,
                        "no-transform" => 4,
                        _ => 8,
                    };
                    if flags & bit != 0 {
                        return None;
                    }
                    flags |= bit;
                }
                _ => return None,
            }
        }
    }
    let lifetime = shared_age.or(if public { max_age } else { None })?;
    if lifetime.is_zero() {
        return None;
    }
    Some(lifetime)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("cache-control", value.parse().unwrap());
        headers
    }

    #[test]
    fn admits_only_explicit_unambiguous_shared_freshness() {
        for (value, seconds) in [
            ("public, max-age=60", Some(60)),
            ("PUBLIC, MAX-AGE=\"60\"", Some(60)),
            ("s-maxage=20", Some(20)),
            ("public, max-age=60, s-maxage=20", Some(20)),
            ("public, max-age=0, s-maxage=20", Some(20)),
            (
                "public, max-age=60, must-revalidate, no-transform",
                Some(60),
            ),
            ("max-age=60", None),
            ("public", None),
            ("public, max-age=0", None),
            ("public, max-age=-1", None),
            ("public, max-age=+1", None),
            ("public, max-age=1.5", None),
            ("public, max-age=18446744073709551616", None),
            ("public, max-age=1, max-age=1", None),
            ("public, max-age=1, MAX-AGE=2", None),
            ("public, s-maxage=1, s-maxage=2", None),
            ("public, public, max-age=60", None),
            ("public, max-age=\"60", None),
            ("public, max-age=60,", None),
            ("public=1, max-age=60", None),
            ("public, max-age=60, private", None),
            ("public, max-age=60, private=\"cookie\"", None),
            ("public, max-age=60, no-store", None),
            ("public, max-age=60, no-cache", None),
            ("public, max-age=60, must-understand", None),
            ("public, max-age=60, unknown-extension", None),
        ] {
            assert_eq!(
                cache_lifetime(&headers(value)).map(|ttl| ttl.as_secs()),
                seconds,
                "{value}"
            );
        }
        let mut split = headers("public");
        split.append("cache-control", "max-age=60".parse().unwrap());
        assert_eq!(cache_lifetime(&split), Some(Duration::from_secs(60)));
        split.append("cache-control", "private".parse().unwrap());
        assert_eq!(cache_lifetime(&split), None);
    }

    #[test]
    fn request_profile_bypasses_private_variant_and_ambiguous_requests() {
        let base = HeaderMap::new();
        let candidate = |method: &Method, path: &str, headers: &HeaderMap, empty| {
            request_candidate(method, path, headers, empty, Some("/public/"))
        };
        assert!(candidate(&Method::GET, "/public/data", &base, true));
        for name in [
            "authorization",
            "cookie",
            "range",
            "if-none-match",
            "if-modified-since",
            "if-anything",
            "cache-control",
            "pragma",
            "accept-language",
            "user-agent",
            "x-user",
            "transfer-encoding",
            "upgrade",
        ] {
            let mut values = base.clone();
            values.insert(
                hyper::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                "value".parse().unwrap(),
            );
            assert!(
                !candidate(&Method::GET, "/public/data", &values, true),
                "{name}"
            );
        }
        for (name, value) in [
            ("accept", "text/html"),
            ("accept-encoding", "gzip"),
            ("content-length", "1"),
            ("connection", "close, cookie"),
        ] {
            let mut values = base.clone();
            values.insert(
                hyper::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
            assert!(!candidate(&Method::GET, "/public/data", &values, true));
        }
        for path in [
            "/private/data",
            "/publicity/data",
            "/public/../private",
            "/public/%2e%2e/private",
            "/public/a\\b",
        ] {
            assert!(!candidate(&Method::GET, path, &base, true), "{path}");
        }
        assert!(!candidate(&Method::POST, "/public/data", &base, true));
        assert!(!candidate(&Method::GET, "/public/data", &base, false));
        assert!(!request_candidate(
            &Method::GET,
            "/public/data",
            &base,
            true,
            None
        ));
        let mut allowed = HeaderMap::new();
        for (name, value) in [
            ("host", "untrusted.example"),
            ("accept", "*/*"),
            ("accept-encoding", "identity"),
            ("content-length", "0"),
            ("connection", "keep-alive, close"),
        ] {
            allowed.insert(
                hyper::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        assert!(candidate(&Method::GET, "/public/data", &allowed, true));
        allowed.append("accept", "*/*".parse().unwrap());
        assert!(!candidate(&Method::GET, "/public/data", &allowed, true));
    }

    #[test]
    fn age_includes_origin_age_response_delay_and_resident_time() {
        let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let requested = Instant::now();
        let received = requested + Duration::from_secs(2);
        let mut values = headers("public, max-age=60");
        values.insert(
            "date",
            httpdate::fmt_http_date(wall - Duration::from_secs(10))
                .parse()
                .unwrap(),
        );
        values.insert("age", "50".parse().unwrap());
        let freshness = response_freshness(
            StatusCode::OK,
            &values,
            requested,
            received,
            wall,
            Duration::from_secs(300),
        )
        .unwrap();
        assert_eq!(freshness.age(received), Duration::from_secs(52));
        assert_eq!(
            freshness.age(received + Duration::from_secs(3)),
            Duration::from_secs(55)
        );
        assert_eq!(freshness.expires_at(), received + Duration::from_secs(8));
        let capped = response_freshness(
            StatusCode::OK,
            &values,
            requested,
            received,
            wall,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(capped.expires_at(), received + Duration::from_secs(5));
        values.insert("age", "60".parse().unwrap());
        assert!(
            response_freshness(
                StatusCode::OK,
                &values,
                requested,
                received,
                wall,
                Duration::from_secs(300)
            )
            .is_none()
        );
    }

    #[test]
    fn rejects_unsafe_metadata_duplicate_dates_and_malformed_age() {
        let now = Instant::now();
        let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let check = |status, values: &HeaderMap| {
            response_freshness(status, values, now, now, wall, Duration::from_secs(300))
        };
        assert!(check(StatusCode::OK, &headers("public, max-age=60")).is_some());
        for name in [
            "set-cookie",
            "vary",
            "content-range",
            "content-encoding",
            "trailer",
        ] {
            let mut values = headers("public, max-age=60");
            values.insert(
                hyper::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                "value".parse().unwrap(),
            );
            assert!(check(StatusCode::OK, &values).is_none(), "{name}");
        }
        for status in [
            StatusCode::PARTIAL_CONTENT,
            StatusCode::CREATED,
            StatusCode::NOT_MODIFIED,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(check(status, &headers("public, max-age=60")).is_none());
        }
        for age in ["-1", "bad", "1, 2", "18446744073709551616"] {
            let mut values = headers("public, max-age=60");
            values.insert("age", age.parse().unwrap());
            assert!(check(StatusCode::OK, &values).is_none());
        }
        for name in ["date", "age"] {
            let mut values = headers("public, max-age=60");
            let value = if name == "date" {
                httpdate::fmt_http_date(wall)
            } else {
                "0".to_owned()
            };
            values.append(name, value.parse().unwrap());
            values.append(name, value.parse().unwrap());
            assert!(check(StatusCode::OK, &values).is_none());
        }
        let mut values = headers("public, max-age=60");
        values.insert("date", "invalid".parse().unwrap());
        assert!(check(StatusCode::OK, &values).is_none());
    }
}
