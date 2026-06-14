use crate::protocol::Command;
use anyhow::{Result, bail};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{broadcast::Receiver, mpsc::Sender, oneshot},
};

enum ClientState {
    NoNick,
    HasNick,
    InRoom,
}

enum ClientAction {
    Continue,
    Quit,
}

pub struct Client {
    id: u64,
    state: ClientState,
    writer: OwnedWriteHalf,
    lines: Lines<BufReader<OwnedReadHalf>>,
    shutdown_rx: Receiver<()>,
    registry_tx: Sender<Command>,
    nick: String,
}

impl Client {
    pub fn new(
        id: u64,
        socket: TcpStream,
        shutdown_rx: Receiver<()>,
        registry_tx: Sender<Command>,
    ) -> Self {
        let (reader, writer) = socket.into_split();
        Self {
            id,
            state: ClientState::NoNick,
            writer,
            lines: BufReader::new(reader).lines(),
            shutdown_rx,
            registry_tx,
            nick: String::new(),
        }
    }

    pub async fn start(mut self) -> Result<()> {
        self.send("welcome to chat. set a nick with /nick <name>, join a room with /join <name>").await?;
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown_rx.recv() => {
                    break;
                },

                // TODO 4096 line length cap
                // TODO CRLF + LF handling (telnet + nc)
                // TODO backpressure
                line = self.lines.next_line() => {
                    match line? {
                        None => {
                            eprintln!("conn #{} closed by peer", self.id);
                            break;
                        },
                        Some(line) => {
                            match self.handle_command(&line).await {
                                Ok(result) => {
                                    if matches!(result, ClientAction::Quit) {
                                        break;
                                    }
                                },
                                Err(e) => {
                                    eprintln!("conn #{} error handle command: {e}", self.id);
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("conn #{} disconnected", self.id);
        Ok(())
    }

    async fn handle_command(&mut self, command: &str) -> Result<ClientAction> {
        let cmd: &str;
        let mut params = "";
        let id = self.id;
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
                self.set_nick(params).await?;
            }
            "/join" => {
                self.join_room(params).await?;
            }
            "/leave" => {
                self.leave_room().await?;
            }
            "/rooms" => {
                self.list_rooms().await?;
            }
            "/who" => {
                self.who().await?;
            }
            "/quit" => {
                match self.registry_tx.send(Command::Quit { conn_id: id }).await {
                    Ok(_) => match self.state {
                        ClientState::HasNick | ClientState::InRoom => {
                            let nick = self.nick.to_string();
                            self.send(&format!("goodbye, {nick}")).await?;
                        }
                        ClientState::NoNick => {
                            self.send("goodbye").await?;
                        }
                    },
                    Err(e) => {
                        eprintln!("conn #{} error: {e}", self.id);
                        self.send_error("failed to /quit").await?;
                    }
                }
                return Ok(ClientAction::Quit);
            }
            _ => {
                eprintln!("conn #{} error: unknown command {command}", self.id);
                self.send_error("unknown command {command}").await?;
            }
        }

        Ok(ClientAction::Continue)
    }

    async fn send_error(&mut self, msg: &str) -> Result<()> {
        Ok(self
            .writer
            .write_all(format!("* error: {msg}\n").as_bytes())
            .await?)
    }

    async fn send(&mut self, msg: &str) -> Result<()> {
        Ok(self
            .writer
            .write_all(format!("* {msg}\n").as_bytes())
            .await?)
    }

    async fn request<T>(
        &mut self,
        make_cmd: impl FnOnce(oneshot::Sender<T>) -> Command,
    ) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        self.registry_tx.send(make_cmd(tx)).await?;
        Ok(rx.await?)
    }

    async fn set_nick(&mut self, nick: &str) -> Result<()> {
        let conn_id = self.id;
        let result = self.request(|reply| Command::SetNick {
            conn_id,
            nick: nick.to_string(),
            reply,
        });
        match result.await? {
            Ok(()) => {
                eprintln!("conn #{} set nick to {nick}", self.id);
                self.send(&format!("you are now {nick}")).await?;
                self.state = ClientState::HasNick;
            }
            Err(e) => {
                eprintln!("conn #{} error: {e}", self.id);
                self.send_error(&format!("failed to set nickname to {nick}"))
                    .await?;
            }
        }
        Ok(())
    }

    async fn join_room(&mut self, room_name: &str) -> Result<()> {
        match self.state {
            ClientState::NoNick => {
                // error -- must have nick set first
                eprintln!(
                    "conn #{} error: nick must be set before joining a room",
                    self.id
                );
                self.send_error("you must set a nick before joining a room")
                    .await?;
                Ok(())
            }
            ClientState::HasNick | ClientState::InRoom => {
                let conn_id = self.id; // pull id out so we don't have to use self in the closure
                let result = self
                    .request(|reply| Command::Join {
                        conn_id,
                        room: room_name.to_string(),
                        reply,
                    })
                    .await?;
                match result {
                    Ok(nick) => {
                        eprintln!("conn #{} joined room {room_name}", self.id);
                        self.send(&format!("{nick} joined {room_name}")).await?;
                        self.state = ClientState::InRoom;
                    }
                    Err(e) => {
                        eprintln!("conn #{} error: {e}", self.id);
                    }
                }
                Ok(())
            }
        }
    }

    async fn leave_room(&mut self) -> Result<()> {
        let conn_id = self.id;
        match self.state {
            ClientState::NoNick | ClientState::HasNick => {
                // error state
                eprintln!("conn #{conn_id} error: not in a room");
                self.send_error("you are not in a room").await?;
            }
            ClientState::InRoom => {
                let result = self
                    .request(|reply| Command::Leave { conn_id, reply })
                    .await?;
                match result {
                    Ok((nick, room)) => {
                        eprintln!("conn #{} left room {room}", self.id);
                        self.send(&format!("{nick} left {room}")).await?;
                        self.state = ClientState::HasNick;

                        // TODO broadcast leaving
                    }
                    Err(e) => {
                        eprintln!("conn #{} error: {e}", conn_id);
                        self.send_error("failed to leave room").await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn list_rooms(&mut self) -> Result<()> {
        let rooms = self.request(|reply| Command::ListRooms { reply }).await?;
        let val = rooms.iter().fold(String::new(), |val, x| {
            val.to_string() + &x.0 + " (" + &x.1.to_string() + "), "
        });
        let temp = val.rsplit_once(", ").unwrap().0;
        self.send(temp).await?;
        Ok(())
    }

    async fn who(&mut self) -> Result<()> {
        match self.state {
            ClientState::InRoom => {
                let conn_id = self.id;
                let who = self.request(|reply| Command::Who { conn_id, reply });
                match who.await? {
                    Ok(who_vec) => {
                        let val = who_vec
                            .iter()
                            .fold(String::new(), |val, x| val.to_string() + x + ", ");
                        let temp = val.rsplit_once(", ").unwrap_or_default().0;
                        self.send(temp).await?;
                    }
                    Err(e) => {
                        eprintln!("conn #{} error {e}", self.id);
                        self.send_error("failed to run /who").await?;
                    }
                }
            }
            ClientState::NoNick | ClientState::HasNick => {
                self.send_error("you are not in a room").await?;
            }
        }
        Ok(())
    }
}
