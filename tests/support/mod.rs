use clap::Parser;
use std::{net::SocketAddr, time::Duration};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use wirecache::{Config, Proxy, serve};

pub(crate) struct RunningProxy {
    pub(crate) address: SocketAddr,
    pub(crate) stop: Option<oneshot::Sender<()>>,
    pub(crate) task: JoinHandle<std::io::Result<()>>,
}

impl RunningProxy {
    pub(crate) async fn start(origin: SocketAddr, options: &[&str]) -> Self {
        let mut args = vec![
            "wirecache".to_owned(),
            "--upstream".to_owned(),
            format!("http://{origin}"),
        ];
        args.extend(options.iter().map(|value| (*value).to_owned()));
        let config = Config::try_parse_from(args).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(serve(listener, Proxy::new(config).unwrap(), async {
            let _ = stopped.await;
        }));
        Self {
            address,
            stop: Some(stop),
            task,
        }
    }

    pub(crate) async fn shutdown(&mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(7), &mut self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

impl Drop for RunningProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
