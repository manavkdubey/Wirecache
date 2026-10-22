use std::{
    io::{self, Write},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Request, StatusCode, Uri};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioTimer},
};
use tokio::{sync::Barrier, task::JoinSet, time::Instant};

#[derive(Parser)]
#[command(
    about = "HTTP/1.1 fixture load runner; validates a fixed ASCII-x payload and emits raw JSON samples"
)]
struct Args {
    #[arg(long)]
    url: Uri,
    #[arg(long)]
    warmup_url: Option<Uri>,
    #[arg(long, default_value_t = 600)]
    requests: usize,
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    #[arg(long, default_value_t = 32)]
    warmup_requests: usize,
    #[arg(long, default_value_t = 4096)]
    expect_bytes: usize,
    #[arg(long, default_value_t = 5000)]
    timeout_ms: u64,
    #[arg(long)]
    controlled: bool,
}

#[derive(Clone, Copy)]
enum Outcome {
    Success,
    Timeout,
    Transport,
    Status,
    Body,
}

impl Outcome {
    fn name(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::Status => "status",
            Self::Body => "body",
        }
    }
}

struct Sample {
    nanos: u128,
    outcome: Outcome,
}
type HttpClient = Client<HttpConnector, Full<Bytes>>;

async fn fetch(
    client: HttpClient,
    url: Uri,
    bytes: usize,
    timeout: Duration,
    gate: Option<Arc<Barrier>>,
) -> Sample {
    if let Some(gate) = gate {
        gate.wait().await;
    }
    let start = Instant::now();
    let operation = async {
        let request = Request::builder()
            .uri(url)
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = match client.request(request).await {
            Ok(value) => value,
            Err(_) => return Outcome::Transport,
        };
        if response.status() != StatusCode::OK {
            return Outcome::Status;
        }
        match Limited::new(response.into_body(), bytes).collect().await {
            Ok(body) => {
                let body = body.to_bytes();
                if body.len() == bytes && body.iter().all(|byte| *byte == b'x') {
                    Outcome::Success
                } else {
                    Outcome::Body
                }
            }
            Err(_) => Outcome::Body,
        }
    };
    let outcome = tokio::time::timeout(timeout, operation)
        .await
        .unwrap_or(Outcome::Timeout);
    Sample {
        nanos: start.elapsed().as_nanos(),
        outcome,
    }
}

async fn run(client: &HttpClient, url: &Uri, count: usize, args: &Args) -> (Duration, Vec<Sample>) {
    let initial = count.min(args.concurrency);
    let barrier = Arc::new(Barrier::new(initial + 1));
    let mut tasks = JoinSet::new();
    let timeout = Duration::from_millis(args.timeout_ms);
    for _ in 0..initial {
        tasks.spawn(fetch(
            client.clone(),
            url.clone(),
            args.expect_bytes,
            timeout,
            Some(barrier.clone()),
        ));
    }
    let start = Instant::now();
    barrier.wait().await;
    let mut launched = initial;
    let mut samples = Vec::with_capacity(count);
    while let Some(sample) = tasks.join_next().await {
        samples.push(sample.expect("load worker panicked"));
        if launched < count {
            tasks.spawn(fetch(
                client.clone(),
                url.clone(),
                args.expect_bytes,
                timeout,
                None,
            ));
            launched += 1;
        }
    }
    (start.elapsed(), samples)
}

fn valid_url(uri: &Uri) -> bool {
    uri.scheme_str() == Some("http")
        && uri
            .authority()
            .is_some_and(|authority| !authority.as_str().contains('@'))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    if !valid_url(&args.url)
        || args.warmup_url.as_ref().is_some_and(|url| !valid_url(url))
        || args.requests == 0
        || args.requests > 1_000_000
        || args.concurrency == 0
        || args.concurrency > 256
        || args.warmup_requests > 10_000
        || args.expect_bytes == 0
        || args.expect_bytes > 8 * 1024 * 1024
        || args.timeout_ms == 0
        || args.timeout_ms > 60_000
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "use HTTP URLs without credentials, 1..1000000 requests, 1..256 concurrency, <=10000 warmups, 1..8388608 payload bytes, and 1..60000 ms timeout").into());
    }
    let client = Client::builder(TokioExecutor::new())
        .pool_timer(TokioTimer::new())
        .pool_max_idle_per_host(args.concurrency)
        .retry_canceled_requests(false)
        .build_http();
    if args.warmup_requests > 0 {
        let url = args.warmup_url.as_ref().unwrap_or(&args.url);
        let (_, samples) = run(&client, url, args.warmup_requests, &args).await;
        if samples
            .iter()
            .any(|sample| !matches!(sample.outcome, Outcome::Success))
        {
            return Err(io::Error::other("warmup failed; no benchmark result emitted").into());
        }
    }
    if args.controlled {
        println!("READY");
        io::stdout().flush()?;
        let mut command = String::new();
        io::stdin().read_line(&mut command)?;
        if command.trim() != "start" {
            return Err(io::Error::other("expected start command").into());
        }
    }
    let (elapsed, samples) = run(&client, &args.url, args.requests, &args).await;
    let mut output = io::BufWriter::new(io::stdout().lock());
    write!(
        output,
        "{{\"elapsed_seconds\":{},\"samples\":[",
        elapsed.as_secs_f64()
    )?;
    for (index, sample) in samples.iter().enumerate() {
        if index != 0 {
            write!(output, ",")?;
        }
        write!(
            output,
            "{{\"latency_ns\":{},\"outcome\":\"{}\"}}",
            sample.nanos,
            sample.outcome.name()
        )?;
    }
    writeln!(output, "]}}")?;
    Ok(())
}
