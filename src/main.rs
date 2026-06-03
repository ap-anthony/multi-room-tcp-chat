use anyhow::Result;
use clap::Parser;
use tcp_chat::registry::registry_task;
use tcp_chat::{protocol::Command, server::start_server};
use tokio::net::TcpListener;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    addr: String,

    #[arg(long, default_value = "7878")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let listener = match TcpListener::bind(format!("{}:{}", args.addr, args.port)).await {
        Ok(l) => {
            eprintln!("listening on {}", l.local_addr()?);
            l
        }
        Err(e) => {
            eprintln!("failed to bind on {}:{} {e}", args.addr, args.port);
            std::process::exit(2)
        }
    };
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let signal_tx = shutdown_tx.clone();
    let signal_task = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::SignalKind;
            let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
                .expect("insall SIGTERM handler");
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
        }

        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        let _ = signal_tx.send(());
    });

    let (reg_tx, reg_rx) = tokio::sync::mpsc::channel::<Command>(5);
    let reg_task = tokio::spawn(registry_task(reg_rx));

    start_server(listener, shutdown_tx, reg_tx).await?;

    // join signal_task
    signal_task.abort();
    let _ = signal_task.await;

    Ok(())
}
