use anyhow::{Context, Result, bail};
use std::collections::{HashMap, hash_map::Entry::Occupied};
use tokio::sync::mpsc;

use crate::{protocol::Command, room::Room};

#[derive(Default)]
struct Registry {
    rooms: HashMap<String, Room>,
    nicknames: HashMap<u64, String>,

    // reverse lookup to find what room a user is in based on their conn id
    connected_rooms: HashMap<u64, String>,

    ordered_rooms: Vec<String>,
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
        let room_name_clone = room_name.to_string();
        if !self.ordered_rooms.contains(&room_name_clone) {
            self.ordered_rooms.push(room_name_clone);
        }
        room.join(conn_id, &nick)?;
        self.connected_rooms.insert(conn_id, room_name.to_string());
        Ok(())
    }

    pub fn get_nickname(&mut self, conn_id: u64) -> Result<String> {
        match self.nicknames.get(&conn_id) {
            Some(nick) => Ok(nick.to_string()),
            None => bail!(format!("no nickname set for conn #{conn_id}")),
        }
    }

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
            .remove(&conn_id);
        Ok(())
    }

    pub fn is_in_room(&self, conn_id: u64) -> bool {
        self.connected_rooms.contains_key(&conn_id)
    }

    pub fn has_nickname(&self, conn_id: u64) -> bool {
        self.nicknames.contains_key(&conn_id)
    }
}

pub async fn registry_task(mut rx: mpsc::Receiver<Command>) -> Result<()> {
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
                if let Occupied(_) = state.connected_rooms.entry(conn_id) {
                    let _ = state.leave_room(conn_id);
                }
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
                if state.is_in_room(conn_id) {
                    let result = state.leave_room(conn_id);
                    if let Err(e) = result {
                        eprintln!("conn #{conn_id} error: {e}");
                    }
                }

                // remove from nicknames
                if state.has_nickname(conn_id) {
                    let result = state.remove_user(conn_id);
                    if let Err(e) = result {
                        eprintln!("conn #{conn_id} error: {e}");
                    }
                }
            }
            Command::ListRooms { reply } => {
                // TODO clean up
                let _ = reply.send(
                    state
                        .ordered_rooms
                        .iter()
                        .map(|r| (r.to_string(), state.rooms.get(r).unwrap().users.len()))
                        .collect()
                );
            }
            Command::Who {
                conn_id,
                reply,
            } => {
                let room_name = state.connected_rooms.get(&conn_id).ok_or_else(|| anyhow::anyhow!("registry error: no room"))?;
                let room = state.rooms.get(room_name);
                if let Some(r) = room {
                    let _ = reply.send(Ok(r.ordered_users.clone()));
                }
            }
            Command::Chat {
                conn_id: _,
                text: _,
            } => todo!(),
        }
    }
    Ok(())
}
