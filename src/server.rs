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

use crate::protocol::Command;

enum ClientAction {
    Continue,
    Quit,
}

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
                        let active_clone = active_conns.clone();
                        let reg_clone = reg_tx.clone();
                        let shutdown_sub = shutdown_tx.subscribe();
                        tasks.spawn(async move {
                            let result = handle_conn(total_conns, stream, shutdown_sub, reg_clone).await;
                            active_clone.fetch_sub(1, Ordering::SeqCst);
                            result
                        });
                    },
                    Err(e) => {
                        eprintln!("{e}");
                    }
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

async fn handle_conn(
    id: u64,
    stream: TcpStream,
    mut shutdown: broadcast::Receiver<()>,
    mut reg_tx: mpsc::Sender<Command>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => {
                break;
            },
            line = lines.next_line() => {
                match line? {
                    None => {
                        eprintln!("conn #{id} closed by peer");
                        break;
                    },
                    Some(line) => {
                        // parse what the user typed, keep looping
                        let reg_clone = reg_tx.clone();
                        let result = handle_command(id, &mut writer, &line, reg_clone).await?;
                        if matches!(result, ClientAction::Quit) {
                            break;
                        }
                    }
                }
            }
        }
    }
    eprintln!("conn #{id} disconnected");
    Ok(())
}

async fn handle_command(
    id: u64,
    stream: &mut OwnedWriteHalf,
    command: &str,
    reg_tx: mpsc::Sender<Command>,
) -> Result<ClientAction> {
    let mut cmd = "";
    let mut params = "";
    match command.split_once(" ") {
        Some((v1, v2)) => {
            cmd = v1;
            params = v2;
        }
        None => {
            cmd = command;
        }
    }

    if cmd.is_empty() {
        bail!("empty command")
    }

    match cmd {
        "/nick" => {
            let (reply_tx, reply_rx) = oneshot::channel();
            reg_tx
                .send(Command::SetNick {
                    conn_id: id,
                    nick: params.to_string(),
                    reply: reply_tx,
                })
                .await?;
            match reply_rx.await? {
                Ok(()) => {
                    eprintln!("conn #{id} set nick to {params}");
                    stream
                        .write_all(&format!("* you are now {params}\n").into_bytes())
                        .await?;
                }
                Err(e) => {
                    eprintln!("conn #{id} error: {e}");
                }
            }
        }
        "/join" => {
            let (reply_tx, reply_rx) = oneshot::channel();
            reg_tx
                .send(Command::Join {
                    conn_id: id,
                    room: params.to_string(),
                    reply: reply_tx,
                })
                .await?;
            match reply_rx.await? {
                Ok(nick) => {
                    eprintln!("conn #{id} joined room {params}");
                    stream
                        .write_all(&format!("* {nick} joined {params}\n").into_bytes())
                        .await?;
                }
                Err(e) => {
                    eprintln!("conn #{id} error: {e}");
                }
            }
        }
        "/leave" => {
            let (reply_tx, reply_rx) = oneshot::channel();
            reg_tx
                .send(Command::Leave {
                    conn_id: id,
                    reply: reply_tx,
                })
                .await?;
            match reply_rx.await? {
                Ok((nick, room)) => {
                    eprintln!("conn #{id} left room {room}");
                    stream.write_all(&format!("* {nick} left {room}\n").into_bytes()).await?;
                }
                Err(e) => {}
            }
        }
        "/rooms" => {
            eprintln!("list rooms");
        }
        "/who" => {
            eprintln!("list users");
        }
        "/quit" => {
            stream.write_all(b"exiting...").await?;
            return Ok(ClientAction::Quit);
        }
        _ => {
            eprintln!("unknown command detected");
        }
    }

    Ok(ClientAction::Continue)
}
