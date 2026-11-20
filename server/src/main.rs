use argon2::password_hash::{rand_core::OsRng, SaltString};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use axum::extract::ws::{Message as WsMsg, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_rusqlite::Connection;

// ===================== Protocolo =====================
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ServerInfo {
    id: i64, name: String, icon: String,
    description: String, banner: String, color: String,
    owner_id: i64, my_role: i64, members: i64,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChannelInfo { id: i64, server_id: i64, name: String }
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChatMessage {
    id: i64, user_id: i64, username: String, display_name: String,
    channel_id: i64, text: String, timestamp: i64,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
struct MemberInfo {
    user_id: i64, username: String, display_name: String,
    avatar_emoji: String, avatar_color: String, status: i64,
    role: i64, online: bool, muted_until: i64,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ProfileInfo {
    user_id: i64, username: String, display_name: String,
    avatar_emoji: String, avatar_color: String, status: i64, bio: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum ClientMsg {
    Register { username: String, password: String },
    Login { username: String, password: String },
    GetServers,
    CreateServer { name: String, icon: String },
    UpdateServer { server_id: i64, name: String, icon: String, description: String, banner: String, color: String },
    GetInvite { server_id: i64 },
    RegenInvite { server_id: i64 },
    JoinByInvite { code: String },
    LeaveServer { server_id: i64 },
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
    UpdateProfile { display_name: String, avatar_emoji: String, avatar_color: String, status: i64, bio: String },
    GetProfile { user_id: i64 },
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
    Invite { server_id: i64, code: String },
    Profile(ProfileInfo),
    ServerLeft { server_id: i64 },
}

// ===================== Estado =====================
type Tx = mpsc::UnboundedSender<String>;

struct Conn { tx: Tx, uid: i64, username: String, channel: Option<i64>, viewing: Option<i64> }

struct AppState {
    db: Connection,
    conns: Mutex<HashMap<u64, Conn>>,
    next: AtomicU64,
}

fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64
}
fn gen_code() -> String {
    rand::thread_rng().sample_iter(rand::distributions::Alphanumeric)
        .take(8).map(char::from).collect::<String>().to_lowercase()
}
fn clean(s: &str, max: usize) -> String {
    s.trim().chars().take(max).collect::<String>()
}

fn push(tx: &Tx, m: &ServerMsg) {
    if let Ok(j) = serde_json::to_string(m) { let _ = tx.send(j); }
}
async fn reply(st: &AppState, cid: u64, m: &ServerMsg) {
    let c = st.conns.lock().await;
    if let Some(c) = c.get(&cid) { push(&c.tx, m); }
}
async fn err(st: &AppState, cid: u64, t: &str) {
    reply(st, cid, &ServerMsg::Error { text: t.to_string() }).await;
}
async fn note(st: &AppState, cid: u64, t: &str) {
    reply(st, cid, &ServerMsg::Notice { text: t.to_string() }).await;
}
async fn to_channel(st: &AppState, channel: i64, m: &ServerMsg) {
    let c = st.conns.lock().await;
    for x in c.values() { if x.channel == Some(channel) { push(&x.tx, m); } }
}
async fn to_server(st: &AppState, sid: i64, m: &ServerMsg) {
    let c = st.conns.lock().await;
    for x in c.values() { if x.viewing == Some(sid) { push(&x.tx, m); } }
}
async fn to_user(st: &AppState, uid: i64, m: &ServerMsg) {
    let c = st.conns.lock().await;
    for x in c.values() { if x.uid == uid { push(&x.tx, m); } }
}
async fn online_ids(st: &AppState) -> Vec<i64> {
    let c = st.conns.lock().await;
    let mut v: Vec<i64> = c.values().filter(|x| x.uid != 0).map(|x| x.uid).collect();
    v.sort_unstable(); v.dedup(); v
}

// ===================== Consultas =====================
async fn role_of(st: &AppState, sid: i64, uid: i64) -> i64 {
    st.db.call(move |c| {
        let owner: i64 = c.query_row("SELECT owner_id FROM servers WHERE id=?", params![sid],
            |r| r.get::<_, i64>(0)).unwrap_or(0);
        if owner != 0 && owner == uid { return Ok(3i64); }
        let role: Option<i64> = c.query_row(
            "SELECT role FROM members WHERE server_id=? AND user_id=?", params![sid, uid],
            |r| r.get::<_, i64>(0)).optional()?;
        Ok(role.unwrap_or(-1i64))
    }).await.unwrap_or(-1)
}

async fn uid_by_name(st: &AppState, name: &str) -> Option<i64> {
    let n = name.to_string();
    st.db.call(move |c| {
        let id: Option<i64> = c.query_row("SELECT id FROM users WHERE username=?", params![n],
            |r| r.get::<_, i64>(0)).optional()?;
        Ok(id)
    }).await.ok().flatten()
}

async fn channel_server(st: &AppState, ch: i64) -> Option<i64> {
    st.db.call(move |c| {
        let sid: Option<i64> = c.query_row("SELECT server_id FROM channels WHERE id=?", params![ch],
            |r| r.get::<_, i64>(0)).optional()?;
        Ok(sid)
    }).await.ok().flatten()
}

async fn is_banned(st: &AppState, sid: i64, uid: i64) -> bool {
    st.db.call(move |c| {
        let n: i64 = c.query_row("SELECT COUNT(*) FROM bans WHERE server_id=? AND user_id=?",
            params![sid, uid], |r| r.get::<_, i64>(0))?;
        Ok(n > 0)
    }).await.unwrap_or(false)
}

async fn send_servers(st: &AppState, cid: u64, uid: i64) {
    let rows: Vec<ServerInfo> = st.db.call(move |c| {
        let mut s = c.prepare(
            "SELECT s.id, s.name, s.icon, s.description, s.banner, s.color, s.owner_id, m.role, \
             (SELECT COUNT(*) FROM members mm WHERE mm.server_id = s.id) \
             FROM servers s JOIN members m ON m.server_id = s.id \
             WHERE m.user_id = ? ORDER BY s.id")?;
        let it = s.query_map(params![uid], |r| Ok(ServerInfo {
            id: r.get::<_, i64>(0)?,
            name: r.get::<_, String>(1)?,
            icon: r.get::<_, String>(2)?,
            description: r.get::<_, String>(3)?,
            banner: r.get::<_, String>(4)?,
            color: r.get::<_, String>(5)?,
            owner_id: r.get::<_, i64>(6)?,
            my_role: r.get::<_, i64>(7)?,
            members: r.get::<_, i64>(8)?,
        }))?;
        let v = it.collect::<Result<Vec<_>, _>>()?;
        Ok(v)
    }).await.unwrap_or_default();
    reply(st, cid, &ServerMsg::Servers(rows)).await;
}

async fn send_channels(st: &AppState, sid: i64) {
    let rows: Vec<ChannelInfo> = st.db.call(move |c| {
        let mut s = c.prepare("SELECT id, server_id, name FROM channels WHERE server_id=? ORDER BY id")?;
        let it = s.query_map(params![sid], |r| Ok(ChannelInfo {
            id: r.get::<_, i64>(0)?, server_id: r.get::<_, i64>(1)?, name: r.get::<_, String>(2)?,
        }))?;
        let v = it.collect::<Result<Vec<_>, _>>()?;
        Ok(v)
    }).await.unwrap_or_default();
    to_server(st, sid, &ServerMsg::Channels { server_id: sid, channels: rows }).await;
}

async fn send_members(st: &AppState, sid: i64) {
    let on = online_ids(st).await;
    let rows: Vec<MemberInfo> = st.db.call(move |c| {
        let mut s = c.prepare(
            "SELECT u.id, u.username, u.display_name, u.avatar_emoji, u.avatar_color, u.status, m.role, m.muted_until \
             FROM members m JOIN users u ON u.id = m.user_id WHERE m.server_id=? \
             ORDER BY m.role DESC, u.username COLLATE NOCASE")?;
        let it = s.query_map(params![sid], |r| Ok(MemberInfo {
            user_id: r.get::<_, i64>(0)?,
            username: r.get::<_, String>(1)?,
            display_name: r.get::<_, String>(2)?,
            avatar_emoji: r.get::<_, String>(3)?,
            avatar_color: r.get::<_, String>(4)?,
            status: r.get::<_, i64>(5)?,
            role: r.get::<_, i64>(6)?,
            online: false,
            muted_until: r.get::<_, i64>(7)?,
        }))?;
        let v = it.collect::<Result<Vec<_>, _>>()?;
        Ok(v)
    }).await.unwrap_or_default();
    let rows: Vec<MemberInfo> = rows.into_iter().map(|mut m| {
        m.online = on.contains(&m.user_id); m
    }).collect();
    to_server(st, sid, &ServerMsg::Members { server_id: sid, members: rows }).await;
}

async fn profile_of(st: &AppState, uid: i64) -> Option<ProfileInfo> {
    st.db.call(move |c| {
        let p = c.query_row(
            "SELECT id, username, display_name, avatar_emoji, avatar_color, status, bio FROM users WHERE id=?",
            params![uid], |r| Ok(ProfileInfo {
                user_id: r.get::<_, i64>(0)?,
                username: r.get::<_, String>(1)?,
                display_name: r.get::<_, String>(2)?,
                avatar_emoji: r.get::<_, String>(3)?,
                avatar_color: r.get::<_, String>(4)?,
                status: r.get::<_, i64>(5)?,
                bio: r.get::<_, String>(6)?,
            })).optional()?;
        Ok(p)
    }).await.ok().flatten()
}

// ===================== Moderación =====================
async fn kick_or_ban(st: &AppState, cid: u64, sid: i64, actor: i64, target: &str, ban: bool) {
    let need = if ban { 2 } else { 1 };
    let my = role_of(st, sid, actor).await;
    if my < need { err(st, cid, "No tienes permisos para eso").await; return; }
    let tuid = match uid_by_name(st, target).await {
        Some(u) => u,
        None => { err(st, cid, "Ese usuario no existe").await; return; }
    };
    if tuid == actor { err(st, cid, "No puedes hacer eso contigo mismo").await; return; }
    let tr = role_of(st, sid, tuid).await;
    if tr < 0 { err(st, cid, "Ese usuario no está en el servidor").await; return; }
    if tr >= my { err(st, cid, "No puedes actuar sobre alguien de igual o mayor rango").await; return; }

    let t = target.to_string();
    let _ = st.db.call(move |c| {
        if ban { c.execute("INSERT OR IGNORE INTO bans(server_id,user_id) VALUES(?,?)", params![sid, tuid])?; }
        c.execute("DELETE FROM members WHERE server_id=? AND user_id=?", params![sid, tuid])?;
        Ok(())
    }).await;

    // saca al afectado de cualquier canal de ese servidor
    let chs: Vec<i64> = st.db.call(move |c| {
        let mut s = c.prepare("SELECT id FROM channels WHERE server_id=?")?;
        let it = s.query_map(params![sid], |r| r.get::<_, i64>(0))?;
        let v = it.collect::<Result<Vec<_>, _>>()?;
        Ok(v)
    }).await.unwrap_or_default();
    {
        let mut cs = st.conns.lock().await;
        for x in cs.values_mut() {
            if x.uid == tuid {
                if let Some(ch) = x.channel { if chs.contains(&ch) { x.channel = None; } }
                if x.viewing == Some(sid) { x.viewing = None; }
            }
        }
    }
    let msg = if ban { ServerMsg::Banned { server_id: sid } } else { ServerMsg::Kicked { server_id: sid } };
    to_user(st, tuid, &msg).await;
    send_servers(st, cid, actor).await;
    for ch in chs {
        let t2 = if ban { format!("🚫 {} fue baneado", t) } else { format!("👢 {} fue expulsado", t) };
        to_channel(st, ch, &ServerMsg::System { channel_id: ch, text: t2 }).await;
    }
    send_members(st, sid).await;
    note(st, cid, if ban { "🚫 Usuario baneado" } else { "👢 Usuario expulsado" }).await;
}

// ===================== Init / migraciones =====================
async fn init_db(db: &Connection) {
    db.call(|c| {
        c.execute_batch("
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS users(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS servers(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                icon TEXT NOT NULL DEFAULT '🌍');
            CREATE TABLE IF NOT EXISTS channels(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                server_id INTEGER NOT NULL,
                name TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS messages(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                timestamp INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS members(
                server_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                role INTEGER NOT NULL DEFAULT 0,
                muted_until INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(server_id,user_id));
            CREATE TABLE IF NOT EXISTS bans(
                server_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                PRIMARY KEY(server_id,user_id));
            CREATE INDEX IF NOT EXISTS idx_msg_ch ON messages(channel_id, id);
        ")?;
        // Migraciones tolerantes (ignoran error si la columna ya existe)
        for sql in [
            "ALTER TABLE users ADD COLUMN display_name TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE users ADD COLUMN avatar_emoji TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE users ADD COLUMN avatar_color TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE users ADD COLUMN status INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE users ADD COLUMN bio TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE servers ADD COLUMN description TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE servers ADD COLUMN banner TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE servers ADD COLUMN color TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE servers ADD COLUMN invite_code TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE servers ADD COLUMN owner_id INTEGER NOT NULL DEFAULT 0",
        ] { let _ = c.execute(sql, []); }
        Ok(())
    }).await.expect("init db");

    // Servidor General por defecto
    db.call(|c| {
        let n: i64 = c.query_row("SELECT COUNT(*) FROM servers", [], |r| r.get::<_, i64>(0))?;
        if n == 0 {
            c.execute("INSERT INTO servers(name,icon,description) VALUES('General','🌍','Servidor público de bienvenida')", [])?;
            let sid = c.last_insert_rowid();
            c.execute("INSERT INTO channels(server_id,name) VALUES(?,'general')", params![sid])?;
            c.execute("INSERT INTO channels(server_id,name) VALUES(?,'random')", params![sid])?;
        }
        Ok(())
    }).await.expect("seed");

    // Códigos de invitación que falten
    let missing: Vec<i64> = db.call(|c| {
        let mut s = c.prepare("SELECT id FROM servers WHERE invite_code=''")?;
        let it = s.query_map([], |r| r.get::<_, i64>(0))?;
        let v = it.collect::<Result<Vec<_>, _>>()?;
        Ok(v)
    }).await.unwrap_or_default();
    for sid in missing {
        let code = gen_code();
        let _ = db.call(move |c| {
            c.execute("UPDATE servers SET invite_code=? WHERE id=?", params![code, sid])?;
            Ok(())
        }).await;
    }
}

// ===================== Comandos =====================
async fn handle_cmd(st: &Arc<AppState>, cid: u64, cmd: ClientMsg) {
    let (uid, uname, channel, _viewing) = {
        let cs = st.conns.lock().await;
        match cs.get(&cid) {
            Some(x) => (x.uid, x.username.clone(), x.channel, x.viewing),
            None => return,
        }
    };

    match cmd {
        ClientMsg::Register { username, password } => {
            let u = clean(&username, 24);
            if u.len() < 3 { err(st, cid, "Usuario mínimo 3 caracteres").await; return; }
            if password.len() < 4 { err(st, cid, "Contraseña mínimo 4 caracteres").await; return; }
            if !u.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
                err(st, cid, "Usuario: solo letras, números, _ y -").await; return;
            }
            let salt = SaltString::generate(&mut OsRng);
            let hash = match Argon2::default().hash_password(password.as_bytes(), &salt) {
                Ok(h) => h.to_string(),
                Err(_) => { err(st, cid, "Error interno de hash").await; return; }
            };
            let u2 = u.clone();
            let res: Result<i64, String> = st.db.call(move |c| {
                let n: i64 = c.query_row("SELECT COUNT(*) FROM users WHERE username=?", params![u2],
                    |r| r.get::<_, i64>(0))?;
                if n > 0 { return Ok(Err("Ese usuario ya existe".to_string())); }
                c.execute("INSERT INTO users(username,password_hash) VALUES(?,?)", params![u2, hash])?;
                let id = c.last_insert_rowid();
                let total: i64 = c.query_row("SELECT COUNT(*) FROM users", [], |r| r.get::<_, i64>(0))?;
                let first: i64 = c.query_row("SELECT id FROM servers ORDER BY id LIMIT 1", [],
                    |r| r.get::<_, i64>(0)).unwrap_or(0);
                if first != 0 {
                    let role = if total == 1 { 3 } else { 0 };
                    c.execute("INSERT OR IGNORE INTO members(server_id,user_id,role) VALUES(?,?,?)",
                        params![first, id, role])?;
                    if total == 1 { c.execute("UPDATE servers SET owner_id=? WHERE id=?", params![id, first])?; }
                }
                Ok(Ok(id))
            }).await.map_err(|e| e.to_string()).and_then(|x| x);

            match res {
                Ok(id) => {
                    { let mut cs = st.conns.lock().await;
                      if let Some(x) = cs.get_mut(&cid) { x.uid = id; x.username = u.clone(); } }
                    reply(st, cid, &ServerMsg::AuthOk { user_id: id, username: u.clone() }).await;
                    note(st, cid, "✨ Cuenta creada. ¡Bienvenido!").await;
                    if let Some(p) = profile_of(st, id).await { reply(st, cid, &ServerMsg::Profile(p)).await; }
                }
                Err(e) => reply(st, cid, &ServerMsg::AuthErr { reason: e }).await,
            }
        }

        ClientMsg::Login { username, password } => {
            let u = clean(&username, 24);
            let u2 = u.clone();
            let row: Option<(i64, String)> = st.db.call(move |c| {
                let r: Option<(i64, String)> = c.query_row(
                    "SELECT id, password_hash FROM users WHERE username=?", params![u2],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))).optional()?;
                Ok(r)
            }).await.ok().flatten();

            let Some((id, hash)) = row else {
                reply(st, cid, &ServerMsg::AuthErr { reason: "Usuario o contraseña incorrectos".into() }).await;
                return;
            };
            let ok = PasswordHash::new(&hash).ok()
                .map(|h| Argon2::default().verify_password(password.as_bytes(), &h).is_ok())
                .unwrap_or(false);
            if !ok {
                reply(st, cid, &ServerMsg::AuthErr { reason: "Usuario o contraseña incorrectos".into() }).await;
                return;
            }
            // garantiza acceso al servidor de bienvenida
            let _ = st.db.call(move |c| {
                let first: i64 = c.query_row("SELECT id FROM servers ORDER BY id LIMIT 1", [],
                    |r| r.get::<_, i64>(0)).unwrap_or(0);
                if first != 0 {
                    c.execute("INSERT OR IGNORE INTO members(server_id,user_id,role) VALUES(?,?,0)",
                        params![first, id])?;
                }
                Ok(())
            }).await;
            { let mut cs = st.conns.lock().await;
              if let Some(x) = cs.get_mut(&cid) { x.uid = id; x.username = u.clone(); } }
            reply(st, cid, &ServerMsg::AuthOk { user_id: id, username: u.clone() }).await;
            if let Some(p) = profile_of(st, id).await { reply(st, cid, &ServerMsg::Profile(p)).await; }
        }

        ClientMsg::GetServers => {
            if uid == 0 { err(st, cid, "No has iniciado sesión").await; return; }
            send_servers(st, cid, uid).await;
        }

        ClientMsg::CreateServer { name, icon } => {
            if uid == 0 { err(st, cid, "No has iniciado sesión").await; return; }
            let n = clean(&name, 40);
            if n.is_empty() { err(st, cid, "Ponle un nombre al servidor").await; return; }
            let ic = if clean(&icon, 8).is_empty() { "🌍".to_string() } else { clean(&icon, 8) };
            let code = gen_code();
            let sid: i64 = st.db.call(move |c| {
                c.execute("INSERT INTO servers(name,icon,invite_code,owner_id) VALUES(?,?,?,?)",
                    params![n, ic, code, uid])?;
                let sid = c.last_insert_rowid();
                c.execute("INSERT INTO channels(server_id,name) VALUES(?,'general')", params![sid])?;
                c.execute("INSERT INTO members(server_id,user_id,role) VALUES(?,?,3)", params![sid, uid])?;
                Ok(sid)
            }).await.unwrap_or(0);
            if sid == 0 { err(st, cid, "No se pudo crear").await; return; }
            { let mut cs = st.conns.lock().await;
              if let Some(x) = cs.get_mut(&cid) { x.viewing = Some(sid); } }
            send_servers(st, cid, uid).await;
            send_channels(st, sid).await;
            send_members(st, sid).await;
            note(st, cid, "🎉 Servidor creado. Eres el 👑 owner").await;
        }

        ClientMsg::UpdateServer { server_id, name, icon, description, banner, color } => {
            if role_of(st, server_id, uid).await < 2 { err(st, cid, "Solo admin u owner pueden editar el servidor").await; return; }
            let n = clean(&name, 40);
            if n.is_empty() { err(st, cid, "El nombre no puede estar vacío").await; return; }
            let ic = clean(&icon, 8);
            let de = clean(&description, 200);
            let ba = clean(&banner, 200);
            let co = clean(&color, 9);
            let _ = st.db.call(move |c| {
                c.execute("UPDATE servers SET name=?, icon=?, description=?, banner=?, color=? WHERE id=?",
                    params![n, ic, de, ba, co, server_id])?;
                Ok(())
            }).await;
            let members: Vec<i64> = st.db.call(move |c| {
                let mut s = c.prepare("SELECT user_id FROM members WHERE server_id=?")?;
                let it = s.query_map(params![server_id], |r| r.get::<_, i64>(0))?;
                let v = it.collect::<Result<Vec<_>, _>>()?;
                Ok(v)
            }).await.unwrap_or_default();
            for m in members { refresh_user_servers(st, m).await; }
            send_channels(st, server_id).await;
            note(st, cid, "✅ Servidor actualizado").await;
        }

        ClientMsg::GetInvite { server_id } => {
            if role_of(st, server_id, uid).await < 2 { err(st, cid, "Solo admin u owner ven el código").await; return; }
            let code: String = st.db.call(move |c| {
                let s: String = c.query_row("SELECT invite_code FROM servers WHERE id=?", params![server_id],
                    |r| r.get::<_, String>(0)).unwrap_or_default();
                Ok(s)
            }).await.unwrap_or_default();
            reply(st, cid, &ServerMsg::Invite { server_id, code }).await;
        }

        ClientMsg::RegenInvite { server_id } => {
            if role_of(st, server_id, uid).await < 3 { err(st, cid, "Solo el owner puede regenerar el código").await; return; }
            let code = gen_code();
            let c2 = code.clone();
            let _ = st.db.call(move |c| {
                c.execute("UPDATE servers SET invite_code=? WHERE id=?", params![c2, server_id])?;
                Ok(())
            }).await;
            reply(st, cid, &ServerMsg::Invite { server_id, code }).await;
            note(st, cid, "🔄 Código regenerado (el anterior ya no sirve)").await;
        }

        ClientMsg::JoinByInvite { code } => {
            if uid == 0 { err(st, cid, "No has iniciado sesión").await; return; }
            let code = clean(&code, 32).to_lowercase();
            let sid: Option<i64> = st.db.call(move |c| {
                let s: Option<i64> = c.query_row("SELECT id FROM servers WHERE invite_code=?", params![code],
                    |r| r.get::<_, i64>(0)).optional()?;
                Ok(s)
            }).await.ok().flatten();
            let Some(sid) = sid else { err(st, cid, "Código de invitación no válido").await; return; };
            if is_banned(st, sid, uid).await { err(st, cid, "Estás baneado de ese servidor").await; return; }
            if role_of(st, sid, uid).await >= 0 { note(st, cid, "Ya estás en ese servidor").await; send_servers(st, cid, uid).await; return; }
            let _ = st.db.call(move |c| {
                c.execute("INSERT OR IGNORE INTO members(server_id,user_id,role) VALUES(?,?,0)", params![sid, uid])?;
                Ok(())
            }).await;
            { let mut cs = st.conns.lock().await;
              if let Some(x) = cs.get_mut(&cid) { x.viewing = Some(sid); } }
            send_servers(st, cid, uid).await;
            send_channels(st, sid).await;
            send_members(st, sid).await;
            note(st, cid, "🎉 Te has unido al servidor").await;
        }

        ClientMsg::LeaveServer { server_id } => {
            let r = role_of(st, server_id, uid).await;
            if r < 0 { err(st, cid, "No estás en ese servidor").await; return; }
            if r == 3 { err(st, cid, "El owner no puede salir: transfiere o borra el servidor").await; return; }
            let _ = st.db.call(move |c| {
                c.execute("DELETE FROM members WHERE server_id=? AND user_id=?", params![server_id, uid])?;
                Ok(())
            }).await;
            { let mut cs = st.conns.lock().await;
              if let Some(x) = cs.get_mut(&cid) { if x.viewing == Some(server_id) { x.viewing = None; } } }
            reply(st, cid, &ServerMsg::ServerLeft { server_id }).await;
            send_servers(st, cid, uid).await;
            send_members(st, server_id).await;
        }

        ClientMsg::GetChannels { server_id } => {
            if role_of(st, server_id, uid).await < 0 { err(st, cid, "No estás en ese servidor").await; return; }
            { let mut cs = st.conns.lock().await;
              if let Some(x) = cs.get_mut(&cid) { x.viewing = Some(server_id); } }
            send_channels(st, server_id).await;
        }

        ClientMsg::CreateChannel { server_id, name } => {
            if role_of(st, server_id, uid).await < 2 { err(st, cid, "Solo admin u owner crean canales").await; return; }
            let n = clean(&name, 32).to_lowercase().replace(' ', "-");
            if n.is_empty() { err(st, cid, "Nombre de canal vacío").await; return; }
            let _ = st.db.call(move |c| {
                c.execute("INSERT INTO channels(server_id,name) VALUES(?,?)", params![server_id, n])?;
                Ok(())
            }).await;
            send_channels(st, server_id).await;
            note(st, cid, "✅ Canal creado").await;
        }

        ClientMsg::Join { channel_id } => {
            let Some(sid) = channel_server(st, channel_id).await else { err(st, cid, "Canal inexistente").await; return; };
            if role_of(st, sid, uid).await < 0 { err(st, cid, "No estás en ese servidor").await; return; }
            if is_banned(st, sid, uid).await { err(st, cid, "Estás baneado").await; return; }
            { let mut cs = st.conns.lock().await;
              if let Some(x) = cs.get_mut(&cid) { x.channel = Some(channel_id); x.viewing = Some(sid); } }
            note(st, cid, "💬 Canal abierto").await;
        }

        ClientMsg::Leave => {
            let mut cs = st.conns.lock().await;
            if let Some(x) = cs.get_mut(&cid) { x.channel = None; }
        }

        ClientMsg::Send { text } => {
            let Some(ch) = channel else { err(st, cid, "Abre un canal primero").await; return; };
            let Some(sid) = channel_server(st, ch).await else { return; };
            if role_of(st, sid, uid).await < 0 { err(st, cid, "No estás en ese servidor").await; return; }
            if is_banned(st, sid, uid).await { err(st, cid, "Estás baneado").await; return; }
            let t = clean(&text, 2000);
            if t.is_empty() { return; }
            let muted: i64 = st.db.call(move |c| {
                let m: Option<i64> = c.query_row("SELECT muted_until FROM members WHERE server_id=? AND user_id=?",
                    params![sid, uid], |r| r.get::<_, i64>(0)).optional()?;
                Ok(m.unwrap_or(0))
            }).await.unwrap_or(0);
            if muted > now() {
                reply(st, cid, &ServerMsg::TimedOut { until: muted }).await;
                err(st, cid, "Estás silenciado").await; return;
            }
            let ts = now();
            let id: i64 = st.db.call(move |c| {
                c.execute("INSERT INTO messages(channel_id,user_id,text,timestamp) VALUES(?,?,?,?)",
                    params![ch, uid, t, ts])?;
                Ok(c.last_insert_rowid())
            }).await.unwrap_or(0);
            if id == 0 { err(st, cid, "No se pudo guardar").await; return; }
            let dn = profile_of(st, uid).await.map(|p| if p.display_name.is_empty() { p.username.clone() } else { p.display_name }).unwrap_or_else(|| uname.clone());
            let m = ChatMessage { id, user_id: uid, username: uname.clone(), display_name: dn, channel_id: ch, text: clean(&text, 2000), timestamp: ts };
            to_channel(st, ch, &ServerMsg::Message(m)).await;
        }

        ClientMsg::GetMessages { channel_id, limit } => {
            let Some(sid) = channel_server(st, channel_id).await else { return; };
            if role_of(st, sid, uid).await < 0 { err(st, cid, "No estás en ese servidor").await; return; }
            let lim = limit.clamp(1, 1000);
            let rows: Vec<ChatMessage> = st.db.call(move |c| {
                let mut s = c.prepare(
                    "SELECT m.id, m.user_id, u.username, u.display_name, m.channel_id, m.text, m.timestamp \
                     FROM messages m JOIN users u ON u.id=m.user_id \
                     WHERE m.channel_id=? ORDER BY m.id DESC LIMIT ?")?;
                let it = s.query_map(params![channel_id, lim], |r| Ok(ChatMessage {
                    id: r.get::<_, i64>(0)?,
                    user_id: r.get::<_, i64>(1)?,
                    username: r.get::<_, String>(2)?,
                    display_name: r.get::<_, String>(3)?,
                    channel_id: r.get::<_, i64>(4)?,
                    text: r.get::<_, String>(5)?,
                    timestamp: r.get::<_, i64>(6)?,
                }))?;
                let v = it.collect::<Result<Vec<_>, _>>()?;
                Ok(v)
            }).await.unwrap_or_default();
            let rows: Vec<ChatMessage> = rows.into_iter().rev().collect();
            reply(st, cid, &ServerMsg::Messages { channel_id, messages: rows }).await;
        }

        ClientMsg::GetMembers { server_id } => {
            if role_of(st, server_id, uid).await < 0 { err(st, cid, "No estás en ese servidor").await; return; }
            { let mut cs = st.conns.lock().await;
              if let Some(x) = cs.get_mut(&cid) { x.viewing = Some(server_id); } }
            send_members(st, server_id).await;
        }

        ClientMsg::DeleteMessage { message_id } => {
            let info: Option<(i64, i64, i64)> = st.db.call(move |c| {
                let r: Option<(i64, i64, i64)> = c.query_row(
                    "SELECT m.channel_id, m.user_id, c.server_id FROM messages m \
                     JOIN channels c ON c.id=m.channel_id WHERE m.id=?", params![message_id],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?))).optional()?;
                Ok(r)
            }).await.ok().flatten();
            let Some((ch, author, sid)) = info else { return; };
            let my = role_of(st, sid, uid).await;
            if author != uid && my < 1 { err(st, cid, "No puedes borrar ese mensaje").await; return; }
            let _ = st.db.call(move |c| {
                c.execute("DELETE FROM messages WHERE id=?", params![message_id])?;
                Ok(())
            }).await;
            to_channel(st, ch, &ServerMsg::MessageDeleted { channel_id: ch, message_id }).await;
        }

        ClientMsg::SetRole { server_id, username, role } => {
            let my = role_of(st, server_id, uid).await;
            if my < 2 { err(st, cid, "Solo admin u owner cambian roles").await; return; }
            let r = role.clamp(0, 2);
            if r == 2 && my < 3 { err(st, cid, "Solo el owner nombra admins").await; return; }
            let Some(t) = uid_by_name(st, &username).await else { err(st, cid, "Usuario inexistente").await; return; };
            if t == uid { err(st, cid, "No puedes cambiarte el rol a ti mismo").await; return; }
            if role_of(st, server_id, t).await >= my { err(st, cid, "Rango insuficiente").await; return; }
            let _ = st.db.call(move |c| {
                c.execute("UPDATE members SET role=? WHERE server_id=? AND user_id=?", params![r, server_id, t])?;
                Ok(())
            }).await;
            send_members(st, server_id).await;
            refresh_user_servers(st, t).await;
            note(st, cid, "✅ Rol actualizado").await;
        }

        ClientMsg::Timeout { server_id, username, minutes } => {
            let my = role_of(st, server_id, uid).await;
            if my < 1 { err(st, cid, "Solo mod o superior silencian").await; return; }
            let Some(t) = uid_by_name(st, &username).await else { err(st, cid, "Usuario inexistente").await; return; };
            if t == uid || role_of(st, server_id, t).await >= my { err(st, cid, "Rango insuficiente").await; return; }
            let mins = minutes.clamp(1, 1440);
            let until = now() + mins * 60;
            let _ = st.db.call(move |c| {
                c.execute("UPDATE members SET muted_until=? WHERE server_id=? AND user_id=?", params![until, server_id, t])?;
                Ok(())
            }).await;
            to_user(st, t, &ServerMsg::TimedOut { until }).await;
            send_members(st, server_id).await;
            note(st, cid, "🔇 Usuario silenciado").await;
        }

        ClientMsg::Kick { server_id, username } => kick_or_ban(st, cid, server_id, uid, &username, false).await,
        ClientMsg::Ban { server_id, username } => kick_or_ban(st, cid, server_id, uid, &username, true).await,

        ClientMsg::Unban { server_id, username } => {
            if role_of(st, server_id, uid).await < 2 { err(st, cid, "Solo admin u owner desbanean").await; return; }
            let Some(t) = uid_by_name(st, &username).await else { err(st, cid, "Usuario inexistente").await; return; };
            let _ = st.db.call(move |c| {
                c.execute("DELETE FROM bans WHERE server_id=? AND user_id=?", params![server_id, t])?;
                Ok(())
            }).await;
            note(st, cid, "✅ Usuario desbaneado (debe volver con el código)").await;
        }

        ClientMsg::UpdateProfile { display_name, avatar_emoji, avatar_color, status, bio } => {
            if uid == 0 { err(st, cid, "No has iniciado sesión").await; return; }
            let dn = clean(&display_name, 32);
            let ae = clean(&avatar_emoji, 8);
            let ac = clean(&avatar_color, 9);
            let stt = status.clamp(0, 3);
            let b = clean(&bio, 200);
            let _ = st.db.call(move |c| {
                c.execute("UPDATE users SET display_name=?, avatar_emoji=?, avatar_color=?, status=?, bio=? WHERE id=?",
                    params![dn, ae, ac, stt, b, uid])?;
                Ok(())
            }).await;
            if let Some(p) = profile_of(st, uid).await { reply(st, cid, &ServerMsg::Profile(p)).await; }
            let sids: Vec<i64> = st.db.call(move |c| {
                let mut s = c.prepare("SELECT server_id FROM members WHERE user_id=?")?;
                let it = s.query_map(params![uid], |r| r.get::<_, i64>(0))?;
                let v = it.collect::<Result<Vec<_>, _>>()?;
                Ok(v)
            }).await.unwrap_or_default();
            for s in sids { send_members(st, s).await; }
            note(st, cid, "✅ Perfil actualizado").await;
        }

        ClientMsg::GetProfile { user_id } => {
            if uid == 0 { return; }
            match profile_of(st, user_id).await {
                Some(p) => reply(st, cid, &ServerMsg::Profile(p)).await,
                None => err(st, cid, "Perfil no encontrado").await,
            }
        }
    }
}

async fn refresh_user_servers(st: &Arc<AppState>, target: i64) {
    let txs: Vec<Tx> = {
        let cs = st.conns.lock().await;
        cs.values().filter(|x| x.uid == target).map(|x| x.tx.clone()).collect()
    };
    if txs.is_empty() { return; }
    let rows: Vec<ServerInfo> = st.db.call(move |c| {
        let mut s = c.prepare(
            "SELECT s.id, s.name, s.icon, s.description, s.banner, s.color, s.owner_id, m.role, \
             (SELECT COUNT(*) FROM members mm WHERE mm.server_id = s.id) \
             FROM servers s JOIN members m ON m.server_id=s.id WHERE m.user_id=? ORDER BY s.id")?;
        let it = s.query_map(params![target], |r| Ok(ServerInfo {
            id: r.get::<_, i64>(0)?, name: r.get::<_, String>(1)?, icon: r.get::<_, String>(2)?,
            description: r.get::<_, String>(3)?, banner: r.get::<_, String>(4)?, color: r.get::<_, String>(5)?,
            owner_id: r.get::<_, i64>(6)?, my_role: r.get::<_, i64>(7)?, members: r.get::<_, i64>(8)?,
        }))?;
        let v = it.collect::<Result<Vec<_>, _>>()?;
        Ok(v)
    }).await.unwrap_or_default();
    let m = ServerMsg::Servers(rows);
    for tx in txs { push(&tx, &m); }
}

// ===================== WebSocket =====================
async fn ws_handler(ws: WebSocketUpgrade, State(st): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |sock| handle_socket(sock, st))
}

async fn handle_socket(sock: WebSocket, st: Arc<AppState>) {
    let (mut sink, mut stream) = sock.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let cid = st.next.fetch_add(1, Ordering::Relaxed);
    st.conns.lock().await.insert(cid, Conn {
        tx: tx.clone(), uid: 0, username: String::new(), channel: None, viewing: None,
    });

    let writer = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            if sink.send(WsMsg::Text(m)).await.is_err() { break; }
        }
    });

    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            WsMsg::Text(t) => {
                if let Ok(cmd) = serde_json::from_str::<ClientMsg>(&t) {
                    handle_cmd(&st, cid, cmd).await;
                }
            }
            WsMsg::Close(_) => break,
            _ => {}
        }
    }

    let (uid, viewing) = {
        let mut cs = st.conns.lock().await;
        let v = cs.get(&cid).map(|x| (x.uid, x.viewing)).unwrap_or((0, None));
        cs.remove(&cid);
        v
    };
    writer.abort();
    if uid != 0 {
        if let Some(s) = viewing { send_members(&st, s).await; }
    }
}

#[tokio::main]
async fn main() {
    let db = Connection::open("figdih-v3.db").await.expect("no se pudo abrir la BD");
    init_db(&db).await;
    let st = Arc::new(AppState { db, conns: Mutex::new(HashMap::new()), next: AtomicU64::new(1) });

    let app = Router::new()
        .route("/health", get(|| async { "FigDih v0.6 OK 🚀" }))
        .route("/ws", get(ws_handler))
        .with_state(st);

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    println!("🚀 FigDih v0.6 en http://{} — identidad + invites + perfiles", addr);
    axum::serve(listener, app).await.expect("serve");
}