use std::collections::{
    HashMap,
    hash_map::Entry::{Occupied, Vacant},
};

use anyhow::{Result, bail};

#[derive(Default)]
pub struct Room {
    pub users: HashMap<u64, String>,
}

impl Room {
    pub fn new() -> Self {
        Room::default()
    }

    pub fn join(&mut self, id: u64, nick: &str) -> Result<()> {
        match self.users.entry(id) {
            Occupied(_) => bail!("user already in room"),
            Vacant(e) => e.insert(nick.to_string()),
        };
        Ok(())
    }

    pub fn leave(&mut self, id: u64) {
        if let Occupied(entry) = self.users.entry(id) {
            entry.remove();
        }
    }
}
