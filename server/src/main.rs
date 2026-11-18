use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
    routing::get,
    Router,
};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_rusqlite::{params, Connection};

// === Tipos ===
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ServerInfo { id: i64, name: String, icon: String }

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChannelInfo { id: i64, server_id: i64, name: String }

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChatMessage {
    id: Option<i64>, user: String, channel_id: i64, text: String, timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize)]
enum ClientMsg {
    Join { username: String, channel_id: i64 },
    Send(ChatMessage),
    Leave { channel_id: i64 },
    GetServers,
    GetChannels { server_id: i64 },
    GetMessages { channel_id: i64, limit: i64 },
}

#[derive(Debug, Serialize, Deserialize)]
enum ServerMsg {
    Welcome { user: String }, Message(ChatMessage),
    UserJoined { user: String, channel_id: i64 }, UserLeft { user: String, channel_id: i64 },
    Servers(Vec<ServerInfo>), Channels(Vec<ChannelInfo>), Messages(Vec<ChatMessage>),
}

struct AppState {
    db: Connection,
    rooms: DashMap<i64, Vec<(usize, mpsc::UnboundedSender<Message>)>>,
}

static NEXT_CONNECTION_ID: AtomicUsize = AtomicUsize::new(1);

async fn init_db() -> Connection {
    let db = Connection::open("figdih.db").await.unwrap();

    db.call(|conn| {
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS servers (id INTEGER PRIMARY KEY, name TEXT, icon TEXT);
             CREATE TABLE IF NOT EXISTS channels (id INTEGER PRIMARY KEY, server_id INTEGER, name TEXT);
             CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, user TEXT, channel_id INTEGER, text TEXT, timestamp INTEGER);"
        )?;

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM servers", [], |row| row.get(0))?;
        if count == 0 {
            conn.execute("INSERT INTO servers (name, icon) VALUES (?, ?)", params!["General", "🌍"])?;
            conn.execute("INSERT INTO servers (name, icon) VALUES (?, ?)", params!["Gaming", "🎮"])?;
            conn.execute("INSERT INTO channels (server_id, name) VALUES (?, ?)", params![1, "general"])?;
            conn.execute("INSERT INTO channels (server_id, name) VALUES (?, ?)", params![1, "random"])?;
            conn.execute("INSERT INTO channels (server_id, name) VALUES (?, ?)", params![2, "valorant"])?;
        }
        Ok(())
    }).await.unwrap();

    db
}

#[tokio::main]
async fn main() {
    let db = init_db().await;
    let state = Arc::new(AppState { db, rooms: DashMap::new() });

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/health", get(|| async { "FigDih v2.0 OK 🚀" }))
        .with_state(state);

    println!("🚀 FigDih Server v2.0 en http://0.0.0.0:3000");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    let conn_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() { break; }
        }
    });

    let state_clone = state.clone();
    let recv_task = tokio::spawn(async move {
        let mut my_channel: Option<i64> = None;
        let mut my_username: Option<String> = None;

        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(text) = msg {
                if let Ok(client_msg) = serde_json::from_str::<ClientMsg>(&text) {
                    match client_msg {
                        ClientMsg::Join { username, channel_id } => {
                            state_clone.rooms.entry(channel_id).or_default().push((conn_id, tx.clone()));
                            my_channel = Some(channel_id);
                            my_username = Some(username.clone());
                            broadcast(&state_clone, channel_id, &ServerMsg::UserJoined { user: username.clone(), channel_id }, conn_id);
                            let _ = tx.send(Message::Text(serde_json::to_string(&ServerMsg::Welcome { user: username }).unwrap()));
                        }
                        ClientMsg::Send(mut chat_msg) => {
                            let channel_id = chat_msg.channel_id;
                            let db = state_clone.db.clone();
                            let msg_clone = chat_msg.clone();

                            // ✅ ? convierte el error; Ok devuelve el id con el tipo correcto
                            let insert_ok = db.call(move |conn| {
                                conn.execute(
                                    "INSERT INTO messages (user, channel_id, text, timestamp) VALUES (?, ?, ?, ?)",
                                    params![msg_clone.user, msg_clone.channel_id, msg_clone.text, msg_clone.timestamp],
                                )?;
                                Ok(conn.last_insert_rowid())
                            }).await;

                            if let Ok(id) = insert_ok {
                                chat_msg.id = Some(id);
                                broadcast(&state_clone, channel_id, &ServerMsg::Message(chat_msg), conn_id);
                            }
                        }
                        ClientMsg::Leave { channel_id } => {
                            let user = my_username.clone().unwrap_or_default();
                            remove_from_room(&state_clone, channel_id, conn_id);
                            broadcast(&state_clone, channel_id, &ServerMsg::UserLeft { user, channel_id }, conn_id);
                            my_channel = None;
                        }
                        ClientMsg::GetServers => {
                            let db = state_clone.db.clone();
                            let servers: Vec<ServerInfo> = db.call(|conn| {
                                let mut stmt = conn.prepare("SELECT id, name, icon FROM servers ORDER BY id")?;
                                let rows = stmt.query_map([], |row| {
                                    Ok(ServerInfo { id: row.get(0)?, name: row.get(1)?, icon: row.get(2)? })
                                })?
                                .filter_map(|r| r.ok())
                                .collect();
                                Ok(rows)
                            }).await.unwrap();
                            let _ = tx.send(Message::Text(serde_json::to_string(&ServerMsg::Servers(servers)).unwrap()));
                        }
                        ClientMsg::GetChannels { server_id } => {
                            let db = state_clone.db.clone();
                            let channels: Vec<ChannelInfo> = db.call(move |conn| {
                                let mut stmt = conn.prepare("SELECT id, server_id, name FROM channels WHERE server_id = ? ORDER BY id")?;
                                let rows = stmt.query_map(params![server_id], |row| {
                                    Ok(ChannelInfo { id: row.get(0)?, server_id: row.get(1)?, name: row.get(2)? })
                                })?
                                .filter_map(|r| r.ok())
                                .collect();
                                Ok(rows)
                            }).await.unwrap();
                            let _ = tx.send(Message::Text(serde_json::to_string(&ServerMsg::Channels(channels)).unwrap()));
                        }
                        ClientMsg::GetMessages { channel_id, limit } => {
                            let db = state_clone.db.clone();
                            let mut messages: Vec<ChatMessage> = db.call(move |conn| {
                                let mut stmt = conn.prepare("SELECT id, user, channel_id, text, timestamp FROM messages WHERE channel_id = ? ORDER BY timestamp DESC LIMIT ?")?;
                                let rows = stmt.query_map(params![channel_id, limit], |row| {
                                    Ok(ChatMessage {
                                        id: Some(row.get(0)?), user: row.get(1)?, channel_id: row.get(2)?,
                                        text: row.get(3)?, timestamp: row.get(4)?
                                    })
                                })?
                                .filter_map(|r| r.ok())
                                .collect();
                                Ok(rows)
                            }).await.unwrap();

                            messages.reverse(); // Orden cronológico
                            let _ = tx.send(Message::Text(serde_json::to_string(&ServerMsg::Messages(messages)).unwrap()));
                        }
                    }
                }
            }
        }
        if let Some(ch) = my_channel { remove_from_room(&state_clone, ch, conn_id); }
    });

    let _ = tokio::join!(send_task, recv_task);
}

fn broadcast(state: &Arc<AppState>, channel_id: i64, msg: &ServerMsg, exclude_conn_id: usize) {
    if let Some(senders) = state.rooms.get(&channel_id) {
        let text = serde_json::to_string(msg).unwrap();
        for (id, tx) in senders.iter() {
            if *id != exclude_conn_id { let _ = tx.send(Message::Text(text.clone())); }
        }
    }
}

fn remove_from_room(state: &Arc<AppState>, channel_id: i64, conn_id: usize) {
    if let Some(mut senders) = state.rooms.get_mut(&channel_id) {
        senders.retain(|(id, _)| *id != conn_id);
    }
}