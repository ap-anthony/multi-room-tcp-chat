use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use anyhow::{Result, bail};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, tcp::OwnedWriteHalf},
    sync::{broadcast, mpsc, oneshot},
    task::JoinSet,
};

use crate::client::Client;
use crate::protocol::Command;

// lifecyle
// server loop
//      connection happens
//      connected user added to registry but with no nickname, no room
//      await user typing in nickname and room
pub async fn start_server(
    listener: TcpListener,
    shutdown_tx: broadcast::Sender<()>,
    reg_tx: mpsc::Sender<Command>,
) -> Result<()> {
    let mut total_conns: u64 = 0;
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();
    let active_conns = Arc::new(AtomicU32::new(0));
    let mut shutdown = shutdown_tx.subscribe();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => {
                eprintln!("shutdown received");
                break;
            },
            socket = listener.accept() => {
                match socket {
                    Ok((stream, addr)) => {
                        total_conns += 1;
                        eprintln!("accepted conn #{} from {addr}", total_conns);
                        active_conns.fetch_add(1, Ordering::SeqCst);

                        let reg_clone = reg_tx.clone();
                        let shutdown_sub = shutdown_tx.subscribe();
                        let client = Client::new(total_conns, stream, shutdown_sub, reg_clone);
                        let active_clone = active_conns.clone();
                        tasks.spawn(async move {
                            let result = client.start().await;
                            active_clone.fetch_sub(1, Ordering::Relaxed);
                            result
                        });
                    },
                    Err(e) => {
                        eprintln!("{e}");
                    }
                }
            },
            Some(result) = tasks.join_next() => {
                match result {
                    Ok(Ok(())) => {},
                    Ok(Err(e)) => eprintln!("{e}"),
                    Err(e) => eprintln!("task panicked: {e}"),
                }
            }
        }
    }
    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res? {
            eprintln!("conn error: {e}");
        }
    }
    Ok(())
}
