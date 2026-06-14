use tokio::sync::oneshot;

pub enum Command {
    SetNick {
        conn_id: u64,
        nick: String,
        reply: oneshot::Sender<Result<(), &'static str>>,
    },
    Join {
        conn_id: u64,
        room: String,
        reply: oneshot::Sender<Result<String, &'static str>>,
    },
    Leave {
        conn_id: u64,
        reply: oneshot::Sender<Result<(String, String), &'static str>>,
    },
    Quit {
        conn_id: u64,
    },
    ListRooms {
        reply: oneshot::Sender<Vec<(String, usize)>>,
    },
    Who {
        conn_id: u64,
        reply: oneshot::Sender<Result<Vec<String>, &'static str>>,
    },
    Chat {
        conn_id: u64,
        text: String,
    },
}