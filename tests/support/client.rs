use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use std::{net::SocketAddr, time::Duration};
use tokio::{net::TcpStream, task::AbortHandle};

struct AbortConnection(AbortHandle);
impl Drop for AbortConnection {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) async fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    extra: &[(&str, &str)],
) -> Response<Bytes> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let stream = TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        let connection = tokio::spawn(connection);
        let _guard = AbortConnection(connection.abort_handle());
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "local")
            .header("connection", "close");
        for (name, value) in extra {
            builder = builder.header(*name, *value);
        }
        let response = sender
            .send_request(builder.body(Full::new(Bytes::new())).unwrap())
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        drop(sender);
        connection.await.unwrap().unwrap();
        Response::from_parts(parts, body)
    })
    .await
    .unwrap()
}
