use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use tokio::sync::mpsc;

use crate::{protocol::Command, room::Room};

#[derive(Default)]
struct Registry {
    rooms: HashMap<String, Room>,
    nicknames: HashMap<u64, String>,

    // reverse lookup to find what room a user is in based on their conn id
    connected_rooms: HashMap<u64, String>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_nick(&mut self, conn_id: u64, nick: &str) {
        self.nicknames.entry(conn_id).or_insert(nick.to_string());
    }

    pub fn join(&mut self, conn_id: u64, room_name: &str) -> Result<()> {
        let nick = self
            .nicknames
            .get(&conn_id)
            .context(format!("no user found with id {conn_id}"))?
            .clone();
        let room = self.rooms.entry(room_name.to_string()).or_default();
        room.join(conn_id, &nick)?;
        self.connected_rooms.insert(conn_id, room_name.to_string());
        Ok(())
    }

    // pub fn leave(&mut self, conn_id: u64) {
    //     // find the room the user is in and remove them.
    //     if let Some(room_name) = self.connected_rooms.remove(&conn_id) &&
    //         let Some(room) = self.rooms.get_mut(&room_name) {
    //             room.leave(conn_id);
    //     }
    // }

    pub fn get_nickname(&mut self, conn_id: u64) -> Result<String> {
        match self.nicknames.get(&conn_id) {
            Some(nick) => Ok(nick.to_string()),
            None => bail!(format!("no nickname set for conn #{conn_id}")),
        }
    }

    // pub fn get_connected_room_name(&mut self, conn_id: u64) -> Result<String> {
    //     match self.connected_rooms.get(&conn_id) {
    //         Some(room) => Ok(room.to_string()),
    //         None => bail!(format!("no room found for conn #{conn_id}")),
    //     }
    // }

    pub fn leave_room(&mut self, conn_id: u64) -> Result<(String, String)> {
        // TODO leave room is failing
        let room_name = self
            .connected_rooms
            .remove(&conn_id)
            .ok_or_else(|| anyhow::anyhow!("conn #{conn_id} not in a room"))?;
        let nick = self.get_nickname(conn_id)?;
        self.rooms
            .get_mut(&room_name)
            .expect("connected_rooms referenced a room that does not exist")
            .leave(conn_id);
        eprintln!("removed conn #{conn_id} from room {room_name}");
        Ok((nick, room_name))
    }

    pub fn remove_user(&mut self, conn_id: u64) -> Result<()> {
        let _ = self
            .nicknames
            .remove(&conn_id)
            .ok_or_else(|| anyhow::anyhow!("conn #{conn_id} not a registered user"))?;
        Ok(())
    }
}

pub async fn registry_task(mut rx: mpsc::Receiver<Command>) {
    let mut state = Registry::new();

    while let Some(cmd) = rx.recv().await {
        match cmd {
            Command::SetNick {
                conn_id,
                nick,
                reply,
            } => {
                state.set_nick(conn_id, &nick);
                let _ = reply.send(Ok(()));
            } // ... other variants
            Command::Join {
                conn_id,
                room,
                reply,
            } => {
                let _ = state.join(conn_id, &room);
                let _ = reply.send(Ok(state.nicknames.entry(conn_id).or_default().clone()));
            }
            Command::Leave { conn_id, reply } => {
                let result = state.leave_room(conn_id);
                match result {
                    Ok((nick, room)) => {
                        let _ = reply.send(Ok((nick, room)));
                    }
                    Err(e) => {
                        let _ = reply.send(Err("you are not in a room"));
                        eprintln!("registry error: {e}");
                    }
                }
            }
            Command::Quit { conn_id } => {
                // remove from room and connected_rooms
                eprintln!("removing from room...");
                let result = state.leave_room(conn_id);
                if let Err(e) = result {
                    eprintln!("conn #{conn_id} error: {e}");
                }

                // remove from nicknames
                eprintln!("removing from nicknames...");
                let result = state.remove_user(conn_id);
                if let Err(e) = result {
                    eprintln!("conn #{conn_id} error: {e}");
                }
            }
            Command::ListRooms { reply } => {
                // TODO insertion order
                let _ = reply.send(
                    state
                        .rooms
                        .iter()
                        .map(|(room_name, room)| (room_name.to_string(), room.users.len()))
                        .collect(),
                );
            }
            Command::Who {
                conn_id: _,
                reply: _,
            } => {
                // TODO insertion order
                todo!();
            }
            Command::Chat {
                conn_id: _,
                text: _,
            } => todo!(),
        }
    }
}
