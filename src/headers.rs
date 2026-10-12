use hyper::header::{CONNECTION, HeaderMap, HeaderName};

pub(crate) fn has_connection_token(headers: &HeaderMap, token: &str) -> bool {
    headers.get_all(CONNECTION).iter().any(|value| {
        value.to_str().is_ok_and(|value| {
            value
                .split(',')
                .any(|item| item.trim().eq_ignore_ascii_case(token))
        })
    })
}

pub(crate) fn sanitize(headers: &mut HeaderMap) -> Result<(), ()> {
    let mut nominated = Vec::new();
    for value in headers.get_all(CONNECTION) {
        for name in value.to_str().map_err(|_| ())?.split(',') {
            if !name.trim().is_empty() {
                nominated.push(HeaderName::from_bytes(name.trim().as_bytes()).map_err(|_| ())?);
            }
        }
    }
    for name in nominated {
        if name == hyper::header::CONTENT_LENGTH || name == hyper::header::HOST {
            return Err(());
        }
        headers.remove(name);
    }
    for name in [
        "connection",
        "proxy-connection",
        "keep-alive",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "proxy-authorization",
        "proxy-authenticate",
    ] {
        headers.remove(name);
    }
    Ok(())
}
