use clap::Parser;
use tokio::net::TcpListener;
use wirecache::{Config, Proxy, serve};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let config = Config::parse();
    let proxy = Proxy::new(config.clone())?;
    let listener = TcpListener::bind(config.listen).await?;
    tracing::info!(address = %listener.local_addr()?, "WireCache listening");
    serve(listener, proxy, async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "shutdown signal failed");
        }
    })
    .await?;
    Ok(())
}
