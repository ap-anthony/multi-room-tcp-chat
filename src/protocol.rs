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

// Strings section -- we suffix each string with _S or _C for server versus client 
// facing strings

// SPEC 3.3 defined strings
// ---------------------------------------------------------------------------
// CLIENT FACING STRINGS
// ---------------------------------------------------------------------------

pub const WELCOME_C: &str = "welcome to chat. set a nick with /nick <name>, join a room with /join <name>";

/// {0} - new nickname
pub const NICK_CHANGED_C: &str = "you are now {}";

/// {0} - old nickname, {1} - new nickname
pub const NICK_CHANGED_BROADCAST_C: &str = "{} is now {}";

/// {0} - user that joined, {1} - room name
pub const JOINED_ROOM_C: &str = "{} joined {}";

/// {0} - user that left, {1} - room name
pub const LEFT_ROOM_C: &str = "{} left {}";

/// {0} - room name, {1} - user that sent message, {2} - message
pub const SENT_MESSAGE_C: &str = "({}) {}: {}";

/// `{0}` - joined list of rooms
pub const LIST_ROOMS_C: &str = "rooms: {}";

/// `{0}` - room name, `{1}` - joined list of users
pub const WHO_C: &str = "in {}: {}";

/// {0} - nick name that is already in use
pub const ERR_NICK_IN_USE_C: &str = "nick {} already in use";

pub const ERR_SET_NICK_BEFORE_JOIN_C: &str = "you must set a nick before joining a room";

pub const ERR_NOT_IN_ROOM_C: &str = "you are not in a room";

/// {0} - user inputted command that failed
pub const ERR_UNKNOWN_CMD_C: &str = "unknown command {}";

pub const ERR_INVALID_NICK_C: &str = "invalid nick (alphanumeric, _, -, 1-20 chars)";

/// {0} - nickname of user leaving
pub const GOODBYE_C: &str = "goodbye, {}";

pub const GOODBYE_NO_NICK_C: &str = "goodbye";

pub const SERVER_SHUTDOWN_C: &str = "* server shutting down";


// ---------------------------------------------------------------------------
// SERVER FACING STRINGS
// ---------------------------------------------------------------------------

/// {0} - server local addr binded on
pub const LISTENING_S: &str = "listening on {}";

/// {0} - conn_id, {1} - conn local_addr
pub const ACCEPTED_CONN_S: &str = "accepted conn #{} from {}";

/// {0} - conn_id, {1} - new nickame
pub const NICK_CHANGED_S: &str = "conn #{} nick set to {}";

/// {0} - conn_id, {1} - room name
pub const JOINED_ROOM_S: &str = "conn #{} joined room {}";

/// {0} - conn_id, {1} - room name
pub const LEFT_ROOM_S: &str = "conn #{} left room {}";

/// {0} - conn_id, {1} - closed by (on shutdown | by peer)
pub const CONN_CLOSED_S: &str = "conn #{} closed {}";

/// {0} - conn_id, {1} - error message
pub const ERR_S: &str = "conn #{} error: {}";

/// {0} - # of connections, {1} - connection or connections, depending on # of connections
pub const SHUTDOWN_REQ_S: &str = "shutdown requested, draining {} active {}";

pub const SHUTDOWN_COMPLETE_S: &str = "shutdown complete";