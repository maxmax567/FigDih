use eframe::egui;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tungstenite::Message as WsMessage;

// === Tipos (idénticos al servidor) ===
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

enum ModAction { Role(i64), Timeout, Kick, Ban }

fn role_badge(r: i64) -> &'static str {
    match r { 3 => "👑", 2 => "🛡️", 1 => "🔨", _ => "" }
}

fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64
}

fn time_ago(ts: i64) -> String {
    let diff = (now() - ts).max(0);
    if diff < 60 { "ahora".into() }
    else if diff < 3600 { format!("hace {}m", diff / 60) }
    else if diff < 86400 { format!("hace {}h", diff / 3600) }
    else { format!("hace {}d", diff / 86400) }
}

struct FigDihApp {
    authed: bool,
    my_id: i64,
    username: String,
    password: String,
    auth_error: String,
    servers: Vec<ServerInfo>,
    channels: Vec<ChannelInfo>,
    messages: Vec<ChatMessage>,
    members: Vec<MemberInfo>,
    selected_server: Option<i64>,
    selected_channel: Option<i64>,
    my_role: i64,
    input_text: String,
    new_server_name: String,
    new_server_icon: String,
    new_channel_name: String,
    unban_name: String,
    show_create_server: bool,
    status: String,
    ws_tx: Option<mpsc::UnboundedSender<String>>,
    ws_rx: Option<mpsc::UnboundedReceiver<String>>,
    _rt: Arc<Runtime>,
}

impl FigDihApp {
    fn new() -> Self {
        let (gui_tx, mut gui_rx) = mpsc::unbounded_channel::<String>();
        let (ws_tx, ws_rx) = mpsc::unbounded_channel::<String>();
        let rt = Arc::new(Runtime::new().unwrap());
        let rt2 = rt.clone();
        rt2.spawn(async move {
            if let Ok((stream, _)) = connect_async("ws://localhost:3000/ws").await {
                let (mut write, mut read) = stream.split();
                tokio::spawn(async move {
                    while let Some(m) = gui_rx.recv().await {
                        if write.send(WsMessage::Text(m)).await.is_err() { break; }
                    }
                });
                while let Some(Ok(m)) = read.next().await {
                    if let WsMessage::Text(t) = m { let _ = ws_tx.send(t); }
                }
            } else {
                eprintln!("⚠️ No se pudo conectar al servidor");
            }
        });
        Self {
            authed: false, my_id: 0, username: String::new(), password: String::new(),
            auth_error: String::new(), servers: vec![], channels: vec![], messages: vec![],
            members: vec![], selected_server: None, selected_channel: None, my_role: 0,
            input_text: String::new(), new_server_name: String::new(), new_server_icon: String::new(),
            new_channel_name: String::new(), unban_name: String::new(), show_create_server: false,
            status: String::new(), ws_tx: Some(gui_tx), ws_rx: Some(ws_rx), _rt: rt,
        }
    }

    fn send_command(&self, cmd: ClientMsg) {
        if let Some(tx) = &self.ws_tx {
            if let Ok(json) = serde_json::to_string(&cmd) { let _ = tx.send(json); }
        }
    }

    fn refresh_my_role(&mut self) {
        self.my_role = self.servers.iter()
            .find(|s| Some(s.id) == self.selected_server)
            .map(|s| s.my_role).unwrap_or(0);
    }

    fn select_server(&mut self, id: i64) {
        self.selected_server = Some(id);
        self.selected_channel = None;
        self.channels.clear();
        self.messages.clear();
        self.members.clear();
        self.refresh_my_role();
        self.send_command(ClientMsg::GetChannels { server_id: id });
        self.send_command(ClientMsg::GetMembers { server_id: id });
    }

    fn select_channel(&mut self, id: i64) {
        if self.selected_channel == Some(id) { return; }
        self.send_command(ClientMsg::Leave);
        self.selected_channel = Some(id);
        self.messages.clear();
        self.send_command(ClientMsg::Join { channel_id: id });
        self.send_command(ClientMsg::GetMessages { channel_id: id, limit: 50 });
    }
}

impl eframe::App for FigDihApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // === Procesar mensajes del servidor ===
        // 1) Drenamos a un Vec: el préstamo de ws_rx termina aquí
        let mut incoming: Vec<String> = Vec::new();
        if let Some(rx) = &mut self.ws_rx {
            while let Ok(text) = rx.try_recv() {
                incoming.push(text);
            }
        }
        // 2) Procesamos con self completamente libre
        for text in incoming {
            if let Ok(m) = serde_json::from_str::<ServerMsg>(&text) {
                match m {
                    ServerMsg::AuthOk { user_id, username } => {
                        self.authed = true; self.my_id = user_id; self.username = username;
                        self.auth_error.clear();
                        self.send_command(ClientMsg::GetServers);
                    }
                    ServerMsg::AuthErr { reason } => self.auth_error = reason,
                    ServerMsg::Error { text } => self.status = format!("⚠️ {}", text),
                    ServerMsg::Notice { text } => self.status = text,
                    ServerMsg::Servers(v) => { self.servers = v; self.refresh_my_role(); }
                    ServerMsg::Channels { server_id, channels } => {
                        if self.selected_server == Some(server_id) { self.channels = channels; }
                    }
                    ServerMsg::Messages { channel_id, messages } => {
                        if self.selected_channel == Some(channel_id) { self.messages = messages; }
                    }
                    ServerMsg::Message(m) => {
                        if self.selected_channel == Some(m.channel_id) { self.messages.push(m); }
                    }
                    ServerMsg::MessageDeleted { channel_id, message_id } => {
                        if self.selected_channel == Some(channel_id) {
                            self.messages.retain(|m| m.id != message_id);
                        }
                    }
                    ServerMsg::Members { server_id, members } => {
                        if self.selected_server == Some(server_id) { self.members = members; }
                    }
                    ServerMsg::System { channel_id, text } => {
                        if self.selected_channel == Some(channel_id) {
                            self.messages.push(ChatMessage { id: 0, user_id: 0, username: "📢".into(), channel_id, text, timestamp: 0 });
                        }
                    }
                    ServerMsg::Kicked { server_id } => {
                        self.status = "👢 Te expulsaron de ese servidor".into();
                        if self.selected_server == Some(server_id) { self.selected_channel = None; self.messages.clear(); }
                    }
                    ServerMsg::Banned { server_id } => {
                        self.status = "🚫 Estás baneado de ese servidor".into();
                        if self.selected_server == Some(server_id) { self.selected_channel = None; self.messages.clear(); }
                    }
                    ServerMsg::TimedOut { until } => {
                        self.status = format!("🔇 Te silenciaron {} min", ((until - now()) / 60).max(1));
                    }
                }
            }
        }

        // === Pantalla de login/registro ===
        if !self.authed {
            egui::CentralPanel::default().show(ctx, |ui| {
                ui.vertical_centered(|ui| {
                    ui.add_space(80.0);
                    ui.heading("💬 FigDih");
                    ui.label("Chat ultraligero en Rust");
                    ui.add_space(30.0);
                    ui.add(egui::TextEdit::singleline(&mut self.username).hint_text("Usuario").desired_width(220.0));
                    ui.add_space(8.0);
                    let pw = ui.add(egui::TextEdit::singleline(&mut self.password).password(true).hint_text("Contraseña").desired_width(220.0));
                    ui.add_space(15.0);
                    let enter = pw.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                    ui.horizontal(|ui| {
                        ui.add_space(ui.available_width() / 2.0 - 110.0);
                        if ui.button("Entrar 🔑").clicked() || enter {
                            self.send_command(ClientMsg::Login { username: self.username.clone(), password: self.password.clone() });
                        }
                        if ui.button("Registrarse ✨").clicked() {
                            self.send_command(ClientMsg::Register { username: self.username.clone(), password: self.password.clone() });
                        }
                    });
                    if !self.auth_error.is_empty() {
                        ui.add_space(10.0);
                        ui.colored_label(egui::Color32::from_rgb(237, 66, 69), &self.auth_error);
                    }
                });
            });
            ctx.request_repaint_after(std::time::Duration::from_millis(100));
            return;
        }

        // === Rail de servidores ===
        let mut server_clicked: Option<i64> = None;
        let mut create_clicked = false;
        let mut refresh_clicked = false;
        egui::SidePanel::left("servers").default_width(72.0).show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(10.0);
                if ui.add(egui::Button::new("＋").min_size(egui::vec2(48.0, 48.0)).rounding(egui::Rounding::same(16.0))).on_hover_text("Crear servidor").clicked() {
                    create_clicked = true;
                }
                if ui.add(egui::Button::new("🔄").min_size(egui::vec2(48.0, 30.0))).on_hover_text("Refrescar").clicked() {
                    refresh_clicked = true;
                }
                ui.separator();
                ui.add_space(8.0);
                for s in &self.servers {
                    let sel = self.selected_server == Some(s.id);
                    let mut btn = egui::Button::new(&s.icon).min_size(egui::vec2(48.0, 48.0)).rounding(egui::Rounding::same(if sel { 12.0 } else { 24.0 }));
                    if sel { btn = btn.fill(egui::Color32::from_rgb(88, 101, 242)); }
                    if ui.add(btn).on_hover_text(format!("{} {}", s.name, role_badge(s.my_role))).clicked() {
                        server_clicked = Some(s.id);
                    }
                    ui.add_space(8.0);
                }
            });
        });
        if create_clicked { self.show_create_server = true; }
        if refresh_clicked { self.send_command(ClientMsg::GetServers); }
        if let Some(id) = server_clicked { self.select_server(id); }

        // === Panel de canales ===
        let mut channel_clicked: Option<i64> = None;
        egui::SidePanel::left("channels").default_width(230.0).show(ctx, |ui| {
            if let Some(sid) = self.selected_server {
                let name = self.servers.iter().find(|s| s.id == sid).map(|s| s.name.clone()).unwrap_or_default();
                ui.add_space(8.0);
                ui.heading(format!("{} {}", name, role_badge(self.my_role)));
                ui.separator();
                ui.add_space(8.0);
                ui.label(egui::RichText::new("CANALES DE TEXTO").small().color(egui::Color32::GRAY));
                ui.add_space(4.0);
                for c in &self.channels {
                    let sel = self.selected_channel == Some(c.id);
                    let mut btn = egui::Button::new(format!("# {}", c.name)).min_size(egui::vec2(ui.available_width(), 30.0));
                    if sel { btn = btn.fill(egui::Color32::from_rgb(79, 84, 92)); }
                    if ui.add(btn).clicked() { channel_clicked = Some(c.id); }
                }
                if self.my_role >= 2 {
                    ui.add_space(10.0);
                    ui.separator();
                    ui.label(egui::RichText::new("🛡️ Crear canal").small());
                    ui.add(egui::TextEdit::singleline(&mut self.new_channel_name).hint_text("nombre").desired_width(150.0));
                    if ui.button("Crear").clicked() && !self.new_channel_name.trim().is_empty() {
                        self.send_command(ClientMsg::CreateChannel { server_id: sid, name: self.new_channel_name.clone() });
                        self.new_channel_name.clear();
                    }
                }
            } else {
                ui.add_space(20.0);
                ui.label("← Elige o crea un servidor");
            }
        });
        if let Some(id) = channel_clicked { self.select_channel(id); }

        // === Panel de miembros ===
        let mut mod_action: Option<(String, ModAction)> = None;
        egui::SidePanel::right("members").default_width(200.0).show(ctx, |ui| {
            if self.selected_server.is_some() {
                ui.add_space(8.0);
                ui.label(egui::RichText::new("MIEMBROS").small().color(egui::Color32::GRAY));
                ui.add_space(4.0);
                let t = now();
                for m in &self.members {
                    let dot = if m.online { "🟢" } else { "⚫" };
                    let mute = if m.muted_until > t { " 🔇" } else { "" };
                    let label = format!("{} {} {}{}", dot, role_badge(m.role), m.username, mute);
                    let resp = ui.add(egui::Label::new(label).sense(egui::Sense::click()));
                    let can_mod = m.user_id != self.my_id && m.role < self.my_role;
                    if can_mod {
                        resp.context_menu(|ui| {
                            if self.my_role >= 1 {
                                if ui.button("🔇 Timeout 10 min").clicked() { mod_action = Some((m.username.clone(), ModAction::Timeout)); ui.close_menu(); }
                                if ui.button("👢 Expulsar").clicked() { mod_action = Some((m.username.clone(), ModAction::Kick)); ui.close_menu(); }
                            }
                            if self.my_role >= 2 {
                                if ui.button("🚫 Banear").clicked() { mod_action = Some((m.username.clone(), ModAction::Ban)); ui.close_menu(); }
                                if m.role < 1 && ui.button("🔨 Hacer mod").clicked() { mod_action = Some((m.username.clone(), ModAction::Role(1))); ui.close_menu(); }
                                if m.role > 0 && ui.button("👤 Quitar rol").clicked() { mod_action = Some((m.username.clone(), ModAction::Role(0))); ui.close_menu(); }
                            }
                            if self.my_role == 3 && m.role < 2 {
                                if ui.button("🛡️ Hacer admin").clicked() { mod_action = Some((m.username.clone(), ModAction::Role(2))); ui.close_menu(); }
                            }
                        });
                    }
                }
                if self.my_role >= 2 {
                    ui.add_space(10.0);
                    ui.separator();
                    ui.label(egui::RichText::new("🚫 Desbanear").small());
                    ui.add(egui::TextEdit::singleline(&mut self.unban_name).hint_text("usuario").desired_width(120.0));
                    if ui.button("Desbanear").clicked() && !self.unban_name.trim().is_empty() {
                        if let Some(sid) = self.selected_server {
                            self.send_command(ClientMsg::Unban { server_id: sid, username: self.unban_name.clone() });
                            self.unban_name.clear();
                        }
                    }
                }
            }
        });
        if let Some((uname, act)) = mod_action {
            if let Some(sid) = self.selected_server {
                let cmd = match act {
                    ModAction::Role(r) => ClientMsg::SetRole { server_id: sid, username: uname, role: r },
                    ModAction::Timeout => ClientMsg::Timeout { server_id: sid, username: uname, minutes: 10 },
                    ModAction::Kick => ClientMsg::Kick { server_id: sid, username: uname },
                    ModAction::Ban => ClientMsg::Ban { server_id: sid, username: uname },
                };
                self.send_command(cmd);
            }
        }

        // === Barra superior ===
        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if let Some(cid) = self.selected_channel {
                    let n = self.channels.iter().find(|c| c.id == cid).map(|c| c.name.clone()).unwrap_or_default();
                    ui.label(egui::RichText::new(format!("# {}", n)).strong());
                } else {
                    ui.label("FigDih v3.0");
                }
                ui.separator();
                ui.label(egui::RichText::new(&self.status).small().color(egui::Color32::from_rgb(250, 166, 26)));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(format!("{} {}", role_badge(self.my_role), self.username));
                });
            });
        });

        // === Input ===
        egui::TopBottomPanel::bottom("input").show(ctx, |ui| {
            ui.add_space(4.0);
            if self.selected_channel.is_some() {
                ui.horizontal(|ui| {
                    let te = ui.add(egui::TextEdit::singleline(&mut self.input_text)
                        .hint_text("Escribe un mensaje...").desired_width(ui.available_width() - 90.0));
                    let enter = te.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                    if ui.add(egui::Button::new("Enviar 🚀").min_size(egui::vec2(80.0, 28.0))).clicked() || enter {
                        if !self.input_text.trim().is_empty() {
                            self.send_command(ClientMsg::Send { text: self.input_text.clone() });
                            self.input_text.clear();
                        }
                    }
                });
            } else {
                ui.label("Selecciona un canal para chatear");
            }
            ui.add_space(4.0);
        });

        // === Mensajes ===
        let mut delete_clicked: Option<i64> = None;
        egui::CentralPanel::default().show(ctx, |ui| {
            if self.selected_channel.is_some() {
                egui::ScrollArea::vertical().stick_to_bottom(true).auto_shrink([false; 2]).show(ui, |ui| {
                    ui.add_space(8.0);
                    for m in &self.messages {
                        if m.user_id == 0 {
                            ui.label(egui::RichText::new(&m.text).italics().small().color(egui::Color32::GRAY));
                        } else {
                            ui.horizontal(|ui| {
                                ui.label(egui::RichText::new(&m.username).strong().color(egui::Color32::from_rgb(88, 101, 242)));
                                ui.label(egui::RichText::new(time_ago(m.timestamp)).small().color(egui::Color32::GRAY));
                                let resp = ui.add(egui::Label::new(&m.text).sense(egui::Sense::click()));
                                if m.user_id == self.my_id || self.my_role >= 1 {
                                    resp.context_menu(|ui| {
                                        if ui.button("🗑️ Eliminar mensaje").clicked() {
                                            delete_clicked = Some(m.id);
                                            ui.close_menu();
                                        }
                                    });
                                }
                            });
                        }
                        ui.add_space(4.0);
                    }
                });
            } else {
                ui.centered_and_justified(|ui| {
                    ui.heading("👋 Elige un servidor y un canal");
                });
            }
        });
        if let Some(id) = delete_clicked { self.send_command(ClientMsg::DeleteMessage { message_id: id }); }

        // === Ventana crear servidor ===
        if self.show_create_server {
            egui::Window::new("Crear servidor").collapsible(false).resizable(false).show(ctx, |ui| {
                ui.label("Nombre:");
                ui.add(egui::TextEdit::singleline(&mut self.new_server_name).desired_width(200.0));
                ui.label("Emoji (icono):");
                ui.add(egui::TextEdit::singleline(&mut self.new_server_icon).hint_text("🎮").desired_width(60.0));
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Crear 🎉").clicked() && !self.new_server_name.trim().is_empty() {
                        self.send_command(ClientMsg::CreateServer { name: self.new_server_name.clone(), icon: self.new_server_icon.clone() });
                        self.new_server_name.clear();
                        self.new_server_icon.clear();
                        self.show_create_server = false;
                    }
                    if ui.button("Cerrar").clicked() { self.show_create_server = false; }
                });
            });
        }

        ctx.request_repaint_after(std::time::Duration::from_millis(100));
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 750.0])
            .with_min_inner_size([900.0, 550.0])
            .with_title("FigDih v3.0"),
        ..Default::default()
    };
    eframe::run_native("FigDih", options, Box::new(|_cc| Box::new(FigDihApp::new())))
}