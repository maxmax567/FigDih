use argon2::password_hash::{rand_core::OsRng, SaltString};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, State},
    response::Response,
    routing::get,
    Router,
};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_rusqlite::{params, Connection};

// === Tipos compartidos ===
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ServerInfo { id: i64, name: String, icon: String, my_role: i64 }

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChannelInfo { id: i64, server_id: i64, name: String }

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChatMessage { id: i64, user_id: i64, username: String, channel_id: i64, text: String, timestamp: i64 }

#[derive(Debug, Serialize, Deserialize, Clone)]
struct MemberInfo { user_id: i64, username: String, role: i64, online: bool, muted_until: i64 }

#[derive(Debug, Serialize, Deserialize)]
enum ClientMsg {
    Register { username: String, password: String },
    Login { username: String, password: String },
    GetServers,
    CreateServer { name: String, icon: String },
    GetChannels { server_id: i64 },
    CreateChannel { server_id: i64, name: String },
    Join { channel_id: i64 },
    Leave,
    Send { text: String },
    GetMessages { channel_id: i64, limit: i64 },
    GetMembers { server_id: i64 },
    DeleteMessage { message_id: i64 },
    SetRole { server_id: i64, username: String, role: i64 },
    Timeout { server_id: i64, username: String, minutes: i64 },
    Kick { server_id: i64, username: String },
    Ban { server_id: i64, username: String },
    Unban { server_id: i64, username: String },
}

#[derive(Debug, Serialize, Deserialize)]
enum ServerMsg {
    AuthOk { user_id: i64, username: String },
    AuthErr { reason: String },
    Error { text: String },
    Notice { text: String },
    Servers(Vec<ServerInfo>),
    Channels { server_id: i64, channels: Vec<ChannelInfo> },
    Messages { channel_id: i64, messages: Vec<ChatMessage> },
    Message(ChatMessage),
    MessageDeleted { channel_id: i64, message_id: i64 },
    Members { server_id: i64, members: Vec<MemberInfo> },
    System { channel_id: i64, text: String },
    Kicked { server_id: i64 },
    Banned { server_id: i64 },
    TimedOut { until: i64 },
}

// === Estado ===
struct Conn {
    tx: mpsc::UnboundedSender<Message>,
    user_id: i64, // 0 = sin autenticar
    username: String,
    channel: Option<i64>,
}

struct AppState {
    db: Connection,
    conns: DashMap<usize, Conn>,
}

static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64
}

fn send_to(tx: &mpsc::UnboundedSender<Message>, msg: &ServerMsg) {
    let _ = tx.send(Message::Text(serde_json::to_string(msg).unwrap()));
}

fn broadcast_channel(state: &AppState, channel_id: i64, msg: &ServerMsg) {
    let text = serde_json::to_string(msg).unwrap();
    for c in state.conns.iter() {
        if c.channel == Some(channel_id) {
            let _ = c.tx.send(Message::Text(text.clone()));
        }
    }
}

async fn server_channels(state: &AppState, server_id: i64) -> Vec<i64> {
    state.db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT id FROM channels WHERE server_id = ?")?;
        let rows = stmt.query_map(params![server_id], |r| r.get::<_, i64>(0))?
            .filter_map(|r| r.ok()).collect();
        Ok(rows)
    }).await.unwrap_or_default()
}

async fn broadcast_server(state: &AppState, server_id: i64, msg: &ServerMsg) {
    let chs = server_channels(state, server_id).await;
    let text = serde_json::to_string(msg).unwrap();
    for c in state.conns.iter() {
        if let Some(ch) = c.channel {
            if chs.contains(&ch) {
                let _ = c.tx.send(Message::Text(text.clone()));
            }
        }
    }
}

async fn role_of(state: &AppState, server_id: i64, user_id: i64) -> i64 {
    state.db.call(move |conn| {
        let owner: i64 = conn.query_row("SELECT owner_id FROM servers WHERE id = ?", params![server_id], |r| r.get(0))?;
        if owner == user_id { return Ok(3); }
        let role: Option<i64> = conn.query_row(
            "SELECT role FROM members WHERE server_id = ? AND user_id = ?",
            params![server_id, user_id], |r| r.get(0)).optional()?;
        Ok(role.unwrap_or(0))
    }).await.unwrap_or(0)
}

async fn user_id_by_name(state: &AppState, username: &str) -> Option<i64> {
    let name = username.to_string();
    state.db.call(move |conn| {
        let id: Option<i64> = conn.query_row(
            "SELECT id FROM users WHERE username = ?", params![name],
            |r| r.get::<_, i64>(0)).optional()?;
        Ok(id)
    }).await.ok().flatten()
}

async fn channel_server(state: &AppState, channel_id: i64) -> Option<i64> {
    state.db.call(move |conn| {
        let sid: Option<i64> = conn.query_row(
            "SELECT server_id FROM channels WHERE id = ?", params![channel_id],
            |r| r.get::<_, i64>(0)).optional()?;
        Ok(sid)
    }).await.ok().flatten()
}

async fn mod_check(state: &AppState, server_id: i64, actor: i64, target: &str, min_role: i64) -> Result<i64, String> {
    let actor_role = role_of(state, server_id, actor).await;
    if actor_role < min_role { return Err("No tienes permisos para eso".into()); }
    let tid = user_id_by_name(state, target).await.ok_or("Ese usuario no existe")?;
    if tid == actor { return Err("No puedes moderarte a ti mismo".into()); }
    if role_of(state, server_id, tid).await >= actor_role {
        return Err("No puedes moderar a alguien de igual o mayor rango".into());
    }
    Ok(tid)
}

async fn send_servers(state: &AppState, tx: &mpsc::UnboundedSender<Message>, uid: i64) {
    let servers: Vec<ServerInfo> = state.db.call(move |conn| {
        let mut stmt = conn.prepare(
            "SELECT s.id, s.name, s.icon, s.owner_id, COALESCE(m.role, 0)
             FROM servers s LEFT JOIN members m ON m.server_id = s.id AND m.user_id = ?
             ORDER BY s.id")?;
        let rows = stmt.query_map(params![uid], |r| {
            let owner: i64 = r.get(3)?;
            let mut role: i64 = r.get(4)?;
            if owner == uid { role = 3; }
            Ok(ServerInfo { id: r.get(0)?, name: r.get(1)?, icon: r.get(2)?, my_role: role })
        })?.filter_map(|r| r.ok()).collect();
        Ok(rows)
    }).await.unwrap_or_default();
    send_to(tx, &ServerMsg::Servers(servers));
}

async fn push_channels(state: &AppState, server_id: i64) {
    let channels: Vec<ChannelInfo> = state.db.call(move |conn| {
        let mut stmt = conn.prepare("SELECT id, server_id, name FROM channels WHERE server_id = ? ORDER BY id")?;
        let rows = stmt.query_map(params![server_id], |r| {
            Ok(ChannelInfo { id: r.get(0)?, server_id: r.get(1)?, name: r.get(2)? })
        })?.filter_map(|r| r.ok()).collect();
        Ok(rows)
    }).await.unwrap_or_default();
    broadcast_server(state, server_id, &ServerMsg::Channels { server_id, channels }).await;
}

async fn push_members(state: &AppState, server_id: i64) {
    let mut members: Vec<MemberInfo> = state.db.call(move |conn| {
        let owner: i64 = conn.query_row("SELECT owner_id FROM servers WHERE id = ?", params![server_id], |r| r.get(0))?;
        let mut stmt = conn.prepare(
            "SELECT u.id, u.username, m.role, m.muted_until
             FROM members m JOIN users u ON u.id = m.user_id WHERE m.server_id = ? ORDER BY m.role DESC, u.username")?;
        let rows = stmt.query_map(params![server_id], |r| {
            let uid: i64 = r.get(0)?;
            let mut role: i64 = r.get(2)?;
            if uid == owner { role = 3; }
            Ok(MemberInfo { user_id: uid, username: r.get(1)?, role, online: false, muted_until: r.get(3)? })
        })?.filter_map(|r| r.ok()).collect();
        Ok(rows)
    }).await.unwrap_or_default();
    for m in &mut members {
        m.online = state.conns.iter().any(|c| c.user_id == m.user_id);
    }
    broadcast_server(state, server_id, &ServerMsg::Members { server_id, members }).await;
}

async fn kick_or_ban(st: &AppState, tx: &mpsc::UnboundedSender<Message>, server_id: i64, uid: i64, username: &str, is_ban: bool) {
    let min_role = if is_ban { 2 } else { 1 };
    match mod_check(st, server_id, uid, username, min_role).await {
        Ok(tid) => {
            let _: Result<(), _> = st.db.call(move |conn| {
                if is_ban {
                    conn.execute("INSERT OR IGNORE INTO bans (server_id, user_id) VALUES (?, ?)", params![server_id, tid])?;
                }
                conn.execute("DELETE FROM members WHERE server_id = ? AND user_id = ?", params![server_id, tid])?;
                Ok(())
            }).await;
            let chs = server_channels(st, server_id).await;
            for mut c in st.conns.iter_mut() {
                if c.user_id == tid {
                    if let Some(canal) = c.channel {
                        if chs.contains(&canal) {
                            c.channel = None;
                            let m = if is_ban { ServerMsg::Banned { server_id } } else { ServerMsg::Kicked { server_id } };
                            send_to(&c.tx, &m);
                        }
                    }
                }
            }
            push_members(st, server_id).await;
            send_to(tx, &ServerMsg::Notice { text: format!("{} {}", if is_ban { "🚫 Baneado" } else { "👢 Expulsado" }, username) });
        }
        Err(e) => send_to(tx, &ServerMsg::Error { text: e }),
    }
}

async fn init_db() -> Connection {
    let db = Connection::open("figdih-v3.db").await.unwrap();
    db.call(|conn| {
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS servers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, icon TEXT, owner_id INTEGER DEFAULT 0);
             CREATE TABLE IF NOT EXISTS channels (id INTEGER PRIMARY KEY, server_id INTEGER NOT NULL, name TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, user_id INTEGER, username TEXT, channel_id INTEGER, text TEXT, timestamp INTEGER, deleted INTEGER DEFAULT 0);
             CREATE TABLE IF NOT EXISTS members (server_id INTEGER, user_id INTEGER, role INTEGER DEFAULT 0, muted_until INTEGER DEFAULT 0, PRIMARY KEY (server_id, user_id));
             CREATE TABLE IF NOT EXISTS bans (server_id INTEGER, user_id INTEGER, PRIMARY KEY (server_id, user_id));
             CREATE INDEX IF NOT EXISTS idx_msg_channel ON messages(channel_id, deleted);"
        )?;
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM servers", [], |r| r.get(0))?;
        if count == 0 {
            conn.execute("INSERT INTO servers (name, icon, owner_id) VALUES ('General', '🌍', 0)", [])?;
            conn.execute("INSERT INTO channels (server_id, name) VALUES (1, 'general')", [])?;
            conn.execute("INSERT INTO channels (server_id, name) VALUES (1, 'random')", [])?;
        }
        Ok(())
    }).await.unwrap();
    db
}

#[tokio::main]
async fn main() {
    let state = Arc::new(AppState { db: init_db().await, conns: DashMap::new() });
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/health", get(|| async { "FigDih v3.0 OK 🚀" }))
        .with_state(state);
    println!("🚀 FigDih v3.0 en http://0.0.0.0:3000 — auth + roles + moderación");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    let conn_id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    state.conns.insert(conn_id, Conn { tx: tx.clone(), user_id: 0, username: String::new(), channel: None });

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() { break; }
        }
    });

    let st = state.clone();
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            let Message::Text(text) = msg else { continue };
            let Ok(cmd) = serde_json::from_str::<ClientMsg>(&text) else { continue };

            // Snapshot de la conexión (nunca mantener refs de DashMap across .await)
            let (uid, uname, ch) = {
                let c = st.conns.get(&conn_id).unwrap();
                (c.user_id, c.username.clone(), c.channel)
            };

            match cmd {
                ClientMsg::Register { username, password } => {
                    if username.len() < 3 || username.len() > 20 || username.contains(' ') {
                        send_to(&tx, &ServerMsg::AuthErr { reason: "Usuario: 3-20 caracteres, sin espacios".into() });
                        continue;
                    }
                    if password.len() < 4 {
                        send_to(&tx, &ServerMsg::AuthErr { reason: "Contraseña: mínimo 4 caracteres".into() });
                        continue;
                    }
                    let salt = SaltString::generate(&mut OsRng);
                    let hash = match Argon2::default().hash_password(password.as_bytes(), &salt) {
                        Ok(h) => h.to_string(),
                        Err(_) => { send_to(&tx, &ServerMsg::AuthErr { reason: "Error interno".into() }); continue; }
                    };
                    let uname2 = username.clone();
                    let res: Result<i64, _> = st.db.call(move |conn| {
                        conn.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", params![uname2, hash])?;
                        let id = conn.last_insert_rowid();
                        let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
                        if count == 1 {
                            conn.execute("UPDATE servers SET owner_id = ? WHERE name = 'General'", params![id])?;
                            conn.execute("INSERT OR IGNORE INTO members (server_id, user_id, role) VALUES (1, ?, 3)", params![id])?;
                        }
                        Ok(id)
                    }).await;
                    match res {
                        Ok(id) => {
                            if let Some(mut c) = st.conns.get_mut(&conn_id) { c.user_id = id; c.username = username.clone(); }
                            send_to(&tx, &ServerMsg::AuthOk { user_id: id, username });
                            send_servers(&st, &tx, id).await;
                        }
                        Err(_) => send_to(&tx, &ServerMsg::AuthErr { reason: "Ese nombre ya está registrado".into() }),
                    }
                }
                ClientMsg::Login { username, password } => {
                    let uname2 = username.clone();
                    let row: Option<(i64, String)> = st.db.call(move |conn| {
                        let r: Option<(i64, String)> = conn.query_row(
                            "SELECT id, password_hash FROM users WHERE username = ?", params![uname2],
                            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))).optional()?;
                        Ok(r)
                    }).await.ok().flatten();
                    let ok = row.as_ref().map(|(_, h)| {
                        PasswordHash::new(h).map(|p| Argon2::default().verify_password(password.as_bytes(), &p).is_ok()).unwrap_or(false)
                    }).unwrap_or(false);
                    if let (true, Some((id, _))) = (ok, row) {
                        if let Some(mut c) = st.conns.get_mut(&conn_id) { c.user_id = id; c.username = username.clone(); }
                        send_to(&tx, &ServerMsg::AuthOk { user_id: id, username });
                        send_servers(&st, &tx, id).await;
                    } else {
                        send_to(&tx, &ServerMsg::AuthErr { reason: "Usuario o contraseña incorrectos".into() });
                    }
                }
                _ if uid == 0 => send_to(&tx, &ServerMsg::Error { text: "Inicia sesión primero".into() }),
                ClientMsg::GetServers => send_servers(&st, &tx, uid).await,
                ClientMsg::CreateServer { name, icon } => {
                    if name.trim().is_empty() { send_to(&tx, &ServerMsg::Error { text: "Nombre vacío".into() }); continue; }
                    let (n, i) = (name.trim().to_string(), if icon.trim().is_empty() { "💬".to_string() } else { icon.trim().to_string() });
                    let res: Result<i64, _> = st.db.call(move |conn| {
                        conn.execute("INSERT INTO servers (name, icon, owner_id) VALUES (?, ?, ?)", params![n, i, uid])?;
                        let sid = conn.last_insert_rowid();
                        conn.execute("INSERT INTO members (server_id, user_id, role) VALUES (?, ?, 3)", params![sid, uid])?;
                        conn.execute("INSERT INTO channels (server_id, name) VALUES (?, 'general')", params![sid])?;
                        Ok(sid)
                    }).await;
                    if res.is_ok() {
                        send_servers(&st, &tx, uid).await;
                        send_to(&tx, &ServerMsg::Notice { text: "🎉 Servidor creado. Eres el 👑 owner".into() });
                    }
                }
                ClientMsg::GetChannels { server_id } => {
                    let channels: Vec<ChannelInfo> = st.db.call(move |conn| {
                        let mut stmt = conn.prepare("SELECT id, server_id, name FROM channels WHERE server_id = ? ORDER BY id")?;
                        let rows = stmt.query_map(params![server_id], |r| {
                            Ok(ChannelInfo { id: r.get(0)?, server_id: r.get(1)?, name: r.get(2)? })
                        })?.filter_map(|r| r.ok()).collect();
                        Ok(rows)
                    }).await.unwrap_or_default();
                    send_to(&tx, &ServerMsg::Channels { server_id, channels });
                }
                ClientMsg::CreateChannel { server_id, name } => {
                    if role_of(&st, server_id, uid).await < 2 {
                        send_to(&tx, &ServerMsg::Error { text: "Necesitas 🛡️ admin para crear canales".into() });
                        continue;
                    }
                    let n = name.trim().trim_start_matches('#').to_string();
                    if n.is_empty() { continue; }
                    let res: Result<(), _> = st.db.call(move |conn| {
                        conn.execute("INSERT INTO channels (server_id, name) VALUES (?, ?)", params![server_id, n])?;
                        Ok(())
                    }).await;
                    if res.is_ok() { push_channels(&st, server_id).await; }
                }
                ClientMsg::Join { channel_id } => {
                    let sid = match channel_server(&st, channel_id).await {
                        Some(s) => s,
                        None => continue,
                    };
                    let banned: i64 = st.db.call(move |conn| {
                        let b: i64 = conn.query_row("SELECT COUNT(*) FROM bans WHERE server_id = ? AND user_id = ?", params![sid, uid], |r| r.get(0))?;
                        if b == 0 {
                            conn.execute("INSERT OR IGNORE INTO members (server_id, user_id, role, muted_until) VALUES (?, ?, 0, 0)", params![sid, uid])?;
                        }
                        Ok(b)
                    }).await.unwrap_or(1);
                    if banned > 0 {
                        send_to(&tx, &ServerMsg::Banned { server_id: sid });
                        continue;
                    }
                    if let Some(mut c) = st.conns.get_mut(&conn_id) { c.channel = Some(channel_id); }
                    broadcast_channel(&st, channel_id, &ServerMsg::System { channel_id, text: format!("→ {} se unió", uname) });
                    push_members(&st, sid).await;
                }
                ClientMsg::Leave => {
                    if let Some(old) = ch {
                        if let Some(mut c) = st.conns.get_mut(&conn_id) { c.channel = None; }
                        broadcast_channel(&st, old, &ServerMsg::System { channel_id: old, text: format!("← {} salió", uname) });
                        if let Some(sid) = channel_server(&st, old).await { push_members(&st, sid).await; }
                    }
                }
                ClientMsg::Send { text } => {
                    let Some(canal) = ch else { send_to(&tx, &ServerMsg::Error { text: "Únete a un canal".into() }); continue };
                    if text.trim().is_empty() { continue; }
                    let (t, ts, u2) = (text.trim().to_string(), now(), uname.clone());
                    let res: Result<Result<i64, String>, _> = st.db.call(move |conn| {
                        let sid: i64 = conn.query_row("SELECT server_id FROM channels WHERE id = ?", params![canal], |r| r.get(0))?;
                        let banned: i64 = conn.query_row("SELECT COUNT(*) FROM bans WHERE server_id = ? AND user_id = ?", params![sid, uid], |r| r.get(0))?;
                        if banned > 0 { return Ok(Err("Estás baneado de este servidor".to_string())); }
                        let muted: i64 = conn.query_row("SELECT muted_until FROM members WHERE server_id = ? AND user_id = ?", params![sid, uid], |r| r.get(0)).optional()?.unwrap_or(0);
                        if muted > ts { return Ok(Err(format!("🔇 Silenciado {}s más", muted - ts))); }
                        conn.execute("INSERT INTO messages (user_id, username, channel_id, text, timestamp) VALUES (?, ?, ?, ?, ?)",
                            params![uid, u2, canal, t, ts])?;
                        Ok(Ok(conn.last_insert_rowid()))
                    }).await;
                    match res {
                        Ok(Ok(id)) => broadcast_channel(&st, canal, &ServerMsg::Message(ChatMessage {
                            id, user_id: uid, username: uname, channel_id: canal, text: text.trim().to_string(), timestamp: now(),
                        })),
                        Ok(Err(e)) => send_to(&tx, &ServerMsg::Error { text: e }),
                        Err(_) => send_to(&tx, &ServerMsg::Error { text: "Error guardando el mensaje".into() }),
                    }
                }
                ClientMsg::GetMessages { channel_id, limit } => {
                    let mut messages: Vec<ChatMessage> = st.db.call(move |conn| {
                        let mut stmt = conn.prepare(
                            "SELECT id, user_id, username, channel_id, text, timestamp FROM messages
                             WHERE channel_id = ? AND deleted = 0 ORDER BY id DESC LIMIT ?")?;
                        let rows = stmt.query_map(params![channel_id, limit], |r| {
                            Ok(ChatMessage { id: r.get(0)?, user_id: r.get(1)?, username: r.get(2)?, channel_id: r.get(3)?, text: r.get(4)?, timestamp: r.get(5)? })
                        })?.filter_map(|r| r.ok()).collect();
                        Ok(rows)
                    }).await.unwrap_or_default();
                    messages.reverse();
                    send_to(&tx, &ServerMsg::Messages { channel_id, messages });
                }
                ClientMsg::GetMembers { server_id } => push_members(&st, server_id).await,
                ClientMsg::DeleteMessage { message_id } => {
                    let info: Option<(i64, i64, i64)> = st.db.call(move |conn| {
                        let r: Option<(i64, i64, i64)> = conn.query_row(
                            "SELECT m.channel_id, m.user_id, c.server_id FROM messages m JOIN channels c ON c.id = m.channel_id WHERE m.id = ?",
                            params![message_id],
                            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?))).optional()?;
                        Ok(r)
                    }).await.ok().flatten();
                    if let Some((canal, author, sid)) = info {
                        if author != uid && role_of(&st, sid, uid).await < 1 {
                            send_to(&tx, &ServerMsg::Error { text: "Solo 🔨 mod+ puede borrar mensajes ajenos".into() });
                            continue;
                        }
                        let _: Result<(), _> = st.db.call(move |conn| {
                            conn.execute("UPDATE messages SET deleted = 1 WHERE id = ?", params![message_id])?;
                            Ok(())
                        }).await;
                        broadcast_channel(&st, canal, &ServerMsg::MessageDeleted { channel_id: canal, message_id });
                    }
                }
                ClientMsg::SetRole { server_id, username, role } => {
                    match mod_check(&st, server_id, uid, &username, 2).await {
                        Ok(tid) => {
                            let actor_role = role_of(&st, server_id, uid).await;
                            if !(0..=2).contains(&role) || role >= actor_role {
                                send_to(&tx, &ServerMsg::Error { text: "Solo puedes asignar roles menores al tuyo".into() });
                                continue;
                            }
                            let _: Result<(), _> = st.db.call(move |conn| {
                                conn.execute("INSERT OR IGNORE INTO members (server_id, user_id, role, muted_until) VALUES (?, ?, 0, 0)", params![server_id, tid])?;
                                conn.execute("UPDATE members SET role = ? WHERE server_id = ? AND user_id = ?", params![role, server_id, tid])?;
                                Ok(())
                            }).await;
                            push_members(&st, server_id).await;
                            send_to(&tx, &ServerMsg::Notice { text: format!("Rol de {} actualizado", username) });
                        }
                        Err(e) => send_to(&tx, &ServerMsg::Error { text: e }),
                    }
                }
                ClientMsg::Timeout { server_id, username, minutes } => {
                    match mod_check(&st, server_id, uid, &username, 1).await {
                        Ok(tid) => {
                            let until = now() + minutes.max(1) * 60;
                            let _: Result<(), _> = st.db.call(move |conn| {
                                conn.execute("INSERT OR IGNORE INTO members (server_id, user_id, role, muted_until) VALUES (?, ?, 0, 0)", params![server_id, tid])?;
                                conn.execute("UPDATE members SET muted_until = ? WHERE server_id = ? AND user_id = ?", params![until, server_id, tid])?;
                                Ok(())
                            }).await;
                            for c in st.conns.iter() {
                                if c.user_id == tid { send_to(&c.tx, &ServerMsg::TimedOut { until }); }
                            }
                            push_members(&st, server_id).await;
                            send_to(&tx, &ServerMsg::Notice { text: format!("🔇 {} silenciado {} min", username, minutes) });
                        }
                        Err(e) => send_to(&tx, &ServerMsg::Error { text: e }),
                    }
                }
                ClientMsg::Kick { server_id, username } => {
                    kick_or_ban(&st, &tx, server_id, uid, &username, false).await;
                }
                ClientMsg::Ban { server_id, username } => {
                    kick_or_ban(&st, &tx, server_id, uid, &username, true).await;
                }
                ClientMsg::Unban { server_id, username } => {
                    if role_of(&st, server_id, uid).await < 2 {
                        send_to(&tx, &ServerMsg::Error { text: "Necesitas 🛡️ admin".into() });
                        continue;
                    }
                    if let Some(tid) = user_id_by_name(&st, &username).await {
                        let _: Result<(), _> = st.db.call(move |conn| {
                            conn.execute("DELETE FROM bans WHERE server_id = ? AND user_id = ?", params![server_id, tid])?;
                            Ok(())
                        }).await;
                        send_to(&tx, &ServerMsg::Notice { text: format!("✅ {} desbaneado", username) });
                    }
                }
            }
        }

        // Limpieza al desconectar
        let (uid2, uname2, ch2) = {
            match st.conns.get(&conn_id) {
                Some(c) => (c.user_id, c.username.clone(), c.channel),
                None => (0, String::new(), None),
            }
        };
        st.conns.remove(&conn_id);
        if let Some(canal) = ch2 {
            broadcast_channel(&st, canal, &ServerMsg::System { channel_id: canal, text: format!("← {} se desconectó", uname2) });
            if uid2 != 0 {
                if let Some(sid) = channel_server(&st, canal).await { push_members(&st, sid).await; }
            }
        }
    });

    let _ = tokio::join!(send_task, recv_task);
}