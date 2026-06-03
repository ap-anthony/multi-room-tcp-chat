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
        let room = self
            .rooms
            .entry(room_name.to_string())
            .or_insert(Room::new());
        room.join(conn_id, &nick)?;
        self.connected_rooms.insert(conn_id, room_name.to_string());
        Ok(())
    }

    pub fn leave(&mut self, conn_id: u64) {
        // find the room the user is in and remove them.
        if let Some(room_name) = self.connected_rooms.remove(&conn_id) {
            if let Some(room) = self.rooms.get_mut(&room_name) {
                room.leave(conn_id);
            }
        }
    }

    pub fn get_nickname(&mut self, conn_id: u64) -> Result<String> {
        match self.nicknames.get(&conn_id) {
            Some(nick) => Ok(nick.to_string()),
            None => bail!(format!("no nickname set for conn #{conn_id}"))
        }
    }

    pub fn get_connected_room_name(&mut self, conn_id: u64) -> Result<String> {
        match self.connected_rooms.get(&conn_id) {
            Some(room) => Ok(room.to_string()),
            None => bail!(format!("no room found for conn #{conn_id}"))
        }
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
                let result = state.set_nick(conn_id, &nick);
                let _ = reply.send(Ok(result));
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
                let result = state.get_nickname(conn_id)
                    .and_then(|nick| {
                        state.get_connected_room_name(conn_id)
                            .map(|room| (nick, room))
                    });
                match result {
                    Ok((nick, room)) => {
                        let _ = reply.send(Ok((nick, room)));
                    },
                    Err(e) => {
                        let _ = reply.send(Err("you are not in a room"));
                    }
                }
            }
            Command::Quit { conn_id } => todo!(),
            Command::ListRooms { reply } => {
                
            },
            Command::Who { conn_id, reply } => todo!(),
            Command::Chat { conn_id, text } => todo!(),
            _ => {}
        }
    }
}
