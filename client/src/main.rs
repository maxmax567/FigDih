use eframe::egui;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tungstenite::Message as WsMessage;

// === Tipos del servidor ===
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

// === Temas ===
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
enum Theme {
    Dark, Light, Blurple, Midnight, Forest, Sunset, Cyberpunk, Mono,
}
impl Default for Theme { fn default() -> Self { Theme::Blurple } }
impl Theme {
    fn all() -> [(Self, &'static str); 8] {
        use Theme::*;
        [(Dark, "🌑 Dark"), (Light, "☀️ Light"), (Blurple, "💎 Blurple"),
         (Midnight, "🌌 Midnight"), (Forest, "🌲 Forest"), (Sunset, "🌅 Sunset"),
         (Cyberpunk, "🤖 Cyberpunk"), (Mono, "🖤 Mono")]
    }
    fn visuals(self) -> egui::Visuals {
        let mut v = match self {
            Theme::Light => egui::Visuals::light(),
            _ => egui::Visuals::dark(),
        };
        let (panel, window, extreme, widget, hover, active, selection) = match self {
            Theme::Dark     => ((30,31,34),  (43,45,49),  (27,28,31),  (54,57,63),  (79,84,92),  (88,101,242), (88,101,242)),
            Theme::Light    => ((255,255,255), (245,245,245), (230,230,230), (200,200,200), (180,180,180), (88,101,242), (88,101,242)),
            Theme::Blurple  => ((30,31,34),  (43,45,49),  (27,28,31),  (54,57,63),  (79,84,92),  (88,101,242), (88,101,242)),
            Theme::Midnight => ((15,23,42),  (30,41,59),  (2,6,23),    (51,65,85),  (71,85,105), (99,102,241), (99,102,241)),
            Theme::Forest   => ((20,35,25),  (28,48,35),  (12,24,17),  (45,80,55),  (65,110,80), (76,175,80), (76,175,80)),
            Theme::Sunset   => ((45,25,35),  (65,35,50),  (30,15,25),  (100,55,70), (130,70,85), (255,107,74), (255,107,74)),
            Theme::Cyberpunk=> ((15,10,30),  (25,18,45),  (8,5,20),    (45,30,75),  (70,45,110), (255,0,170), (0,255,255)),
            Theme::Mono     => ((10,10,10),  (20,20,20),  (0,0,0),     (40,40,40),  (60,60,60),  (200,200,200), (200,200,200)),
        };
        v.panel_fill = egui::Color32::from_rgb(panel.0, panel.1, panel.2);
        v.window_fill = egui::Color32::from_rgb(window.0, window.1, window.2);
        v.extreme_bg_color = egui::Color32::from_rgb(extreme.0, extreme.1, extreme.2);
        v.widgets.inactive.bg_fill = egui::Color32::from_rgb(widget.0, widget.1, widget.2);
        v.widgets.hovered.bg_fill = egui::Color32::from_rgb(hover.0, hover.1, hover.2);
        v.widgets.active.bg_fill = egui::Color32::from_rgb(active.0, active.1, active.2);
        v.selection.bg_fill = egui::Color32::from_rgb(selection.0, selection.1, selection.2);
        v.window_rounding = egui::Rounding::same(10.0);
        v
    }
    fn accent(self) -> egui::Color32 {
        match self {
            Theme::Dark | Theme::Blurple => egui::Color32::from_rgb(88,101,242),
            Theme::Light => egui::Color32::from_rgb(88,101,242),
            Theme::Midnight => egui::Color32::from_rgb(99,102,241),
            Theme::Forest => egui::Color32::from_rgb(76,175,80),
            Theme::Sunset => egui::Color32::from_rgb(255,107,74),
            Theme::Cyberpunk => egui::Color32::from_rgb(0,255,255),
            Theme::Mono => egui::Color32::from_rgb(200,200,200),
        }
    }
}

// === Notificaciones ===
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
enum NotifyLevel { None, Mentions, All }
impl Default for NotifyLevel { fn default() -> Self { NotifyLevel::Mentions } }

// === Ajustes persistentes ===
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Settings {
    url: String,
    theme: Theme,
    zoom: f32,
    timestamps: bool,
    compact: bool,
    notify: NotifyLevel,
    font_size: f32,
    show_member_count: bool,
    show_emoji_panel: bool,
    primary_color_hex: String,
    bubble_radius: f32,
    message_spacing: f32,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            url: "ws://localhost:3000/ws".into(),
            theme: Theme::Blurple,
            zoom: 1.0,
            timestamps: true,
            compact: false,
            notify: NotifyLevel::Mentions,
            font_size: 14.0,
            show_member_count: true,
            show_emoji_panel: false,
            primary_color_hex: "#5865F2".into(),
            bubble_radius: 8.0,
            message_spacing: 10.0,
        }
    }
}
impl Settings {
    fn load() -> Self {
        std::fs::read_to_string("figdih-settings.json").ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }
    fn save(&self) {
        if let Ok(s) = serde_json::to_string_pretty(self) {
            let _ = std::fs::write("figdih-settings.json", s);
        }
    }
}

/// === Helpers ===
fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64
}
fn time_ago(ts: i64) -> String {
    let d = (now() - ts).max(0);
    if d < 60 { "ahora".into() }
    else if d < 3600 { format!("hace {}m", d / 60) }
    else if d < 86400 { format!("hace {}h", d / 3600) }
    else { format!("hace {}d", d / 86400) }
}
fn role_badge(r: i64) -> &'static str {
    match r { 3 => "👑", 2 => "🛡️", 1 => "🔨", _ => "" }
}
fn role_color(r: i64) -> egui::Color32 {
    match r {
        3 => egui::Color32::from_rgb(250,166,26),
        2 => egui::Color32::from_rgb(88,101,242),
        1 => egui::Color32::from_rgb(87,242,135),
        _ => egui::Color32::from_rgb(219,222,225),
    }
}
fn avatar_color(id: i64) -> egui::Color32 {
    const P: [(u8,u8,u8); 6] = [(88,101,242),(87,242,135),(250,166,26),(237,66,69),(155,89,182),(26,188,156)];
    let c = P[(id.unsigned_abs() as usize) % P.len()];
    egui::Color32::from_rgb(c.0, c.1, c.2)
}

// Emojis comunes para picker rápido
const EMOJI_PICK: &[&str] = &[
    "😀","😂","😍","🥰","😎","🤔","😢","😡","👍","👎","❤️","🔥","🎉","✨","💯","🙏",
    "👀","💪","🤝","🚀","⭐","🌟","💡","🎵","🎮","⚡","☕","🍕","🌈","🦄","🐱","🐶",
];

struct FigDihApp {
    authed: bool, my_id: i64,
    username: String, password: String, auth_error: String,
    servers: Vec<ServerInfo>, channels: Vec<ChannelInfo>,
    messages: Vec<ChatMessage>, members: Vec<MemberInfo>,
    selected_server: Option<i64>, selected_channel: Option<i64>, my_role: i64,
    input_text: String,
    new_server_name: String, new_server_icon: String,
    new_channel_name: String, unban_name: String,
    show_create_server: bool, show_settings: bool, show_emoji: bool, status: String,
    settings: Settings,
    stop: Arc<AtomicBool>,
    ws_tx: Option<mpsc::UnboundedSender<String>>,
    ws_rx: Option<mpsc::UnboundedReceiver<String>>,
    _rt: Arc<Runtime>,
    notify_tx: std::sync::mpsc::Sender<String>,
}

impl FigDihApp {
    fn new() -> Self {
        let settings = Settings::load();
        let (notify_tx, notify_rx) = std::sync::mpsc::channel::<String>();

        // Hilo dedicado a notificaciones (para no bloquear la GUI)
        std::thread::spawn(move || {
            while let Ok(msg) = notify_rx.recv() {
                let _ = notify_rust::Notification::new()
                    .summary("FigDih")
                    .body(&msg)
                    .timeout(notify_rust::Timeout::Milliseconds(4000))
                    .show();
            }
        });

        let mut app = Self {
            authed: false, my_id: 0, username: String::new(), password: String::new(),
            auth_error: String::new(), servers: vec![], channels: vec![], messages: vec![],
            members: vec![], selected_server: None, selected_channel: None, my_role: 0,
            input_text: String::new(), new_server_name: String::new(), new_server_icon: String::new(),
            new_channel_name: String::new(), unban_name: String::new(),
            show_create_server: false, show_settings: false, show_emoji: false,
            status: String::new(), settings,
            stop: Arc::new(AtomicBool::new(false)),
            ws_tx: None, ws_rx: None, _rt: Arc::new(Runtime::new().unwrap()),
            notify_tx,
        };
        let url = app.settings.url.clone();
        app.connect(url);
        app
    }

    fn connect(&mut self, url: String) {
        self.stop.store(true, Ordering::Relaxed);
        let stop = Arc::new(AtomicBool::new(false));
        self.stop = stop.clone();
        let (gui_tx, mut gui_rx) = mpsc::unbounded_channel::<String>();
        let (ws_tx, ws_rx) = mpsc::unbounded_channel::<String>();
        let rt = self._rt.clone();
        self.status = format!("🔌 Conectando a {} ...", url);
        rt.spawn(async move {
            match connect_async(url.as_str()).await {
                Ok((stream, _)) => {
                    let (mut write, mut read) = stream.split();
                    tokio::spawn(async move {
                        while let Some(m) = gui_rx.recv().await {
                            if write.send(WsMessage::Text(m)).await.is_err() { break; }
                        }
                    });
                    loop {
                        tokio::select! {
                            m = read.next() => {
                                match m {
                                    Some(Ok(WsMessage::Text(t))) => { if ws_tx.send(t).is_err() { break; } }
                                    Some(Ok(_)) => {}
                                    _ => break,
                                }
                            }
                            _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {
                                if stop.load(Ordering::Relaxed) { break; }
                            }
                        }
                    }
                }
                Err(_) => {
                    let _ = ws_tx.send(serde_json::to_string(&ServerMsg::AuthErr {
                        reason: format!("No se pudo conectar a {}", url),
                    }).unwrap());
                }
            }
        });
        self.ws_tx = Some(gui_tx);
        self.ws_rx = Some(ws_rx);
        self.authed = false; self.my_id = 0;
        self.servers.clear(); self.channels.clear(); self.messages.clear(); self.members.clear();
        self.selected_server = None; self.selected_channel = None; self.my_role = 0;
    }

    fn send_command(&self, cmd: ClientMsg) {
        if let Some(tx) = &self.ws_tx {
            if let Ok(j) = serde_json::to_string(&cmd) { let _ = tx.send(j); }
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
        self.channels.clear(); self.messages.clear(); self.members.clear();
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

        fn settings_window(&mut self, ctx: &egui::Context) {
        if !self.show_settings { return; }
        let mut do_connect: Option<String> = None;
        let mut do_logout = false;
        let mut close = false;
        egui::Window::new("⚙️ Configuración")
            .collapsible(false).resizable(true)
            .default_size([420.0, 600.0]).min_width(360.0)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    ui.set_width(ui.available_width());

                    // === CONEXIÓN ===
                    ui.heading("🔌 Conexión");
                    ui.label("URL del servidor:");
                    ui.add(egui::TextEdit::singleline(&mut self.settings.url).desired_width(380.0));
                    ui.add_space(4.0);
                    ui.horizontal(|ui| {
                        if ui.button("Reconectar").clicked() { do_connect = Some(self.settings.url.clone()); }
                        if ui.button("Usar localhost").clicked() {
                            self.settings.url = "ws://localhost:3000/ws".into();
                            self.settings.save();
                        }
                    });

                    ui.add_space(10.0); ui.separator();

                    // === TEMA ===
                    ui.heading("🎨 Tema");
                    egui::Grid::new("theme_grid").num_columns(4).show(ui, |ui| {
                        for (i, (t, name)) in Theme::all().iter().enumerate() {
                            let sel = self.settings.theme == *t;
                            let mut btn = egui::Button::new(*name).min_size(egui::vec2(90.0, 28.0));
                            if sel { btn = btn.fill(t.accent()).stroke(egui::Stroke::new(2.0_f32, egui::Color32::WHITE)); }
                            if ui.add(btn).clicked() {
                                self.settings.theme = *t;
                                self.settings.save();
                            }
                            if (i + 1) % 4 == 0 { ui.end_row(); }
                        }
                    });

                    // === TIPOGRAFÍA ===
                    ui.add_space(10.0); ui.separator();
                    ui.heading("📝 Tipografía");
                    ui.horizontal(|ui| {
                        ui.label("Tamaño:");
                        let mut fs = self.settings.font_size;
                        if ui.add(egui::Slider::new(&mut fs, 10.0..=24.0).text("px")).changed() {
                            self.settings.font_size = fs; self.settings.save();
                        }
                    });
                    ui.horizontal(|ui| {
                        ui.label("Zoom UI:");
                        let mut z = self.settings.zoom;
                        if ui.add(egui::Slider::new(&mut z, 0.7..=1.8).text("x")).changed() {
                            self.settings.zoom = z; self.settings.save();
                        }
                    });

                    // === DISEÑO ===
                    ui.add_space(10.0); ui.separator();
                    ui.heading("🖼️ Diseño");
                    let mut ts = self.settings.timestamps;
                    if ui.checkbox(&mut ts, "Mostrar horas en mensajes").changed() {
                        self.settings.timestamps = ts; self.settings.save();
                    }
                    let mut cp = self.settings.compact;
                    if ui.checkbox(&mut cp, "Modo compacto").changed() {
                        self.settings.compact = cp; self.settings.save();
                    }
                    let mut mc = self.settings.show_member_count;
                    if ui.checkbox(&mut mc, "Mostrar contador de miembros").changed() {
                        self.settings.show_member_count = mc; self.settings.save();
                    }
                    let mut ep = self.settings.show_emoji_panel;
                    if ui.checkbox(&mut ep, "Mostrar botón de emojis").changed() {
                        self.settings.show_emoji_panel = ep; self.settings.save();
                    }
                    ui.horizontal(|ui| {
                        ui.label("Radio de burbujas:");
                        let mut r = self.settings.bubble_radius;
                        if ui.add(egui::Slider::new(&mut r, 0.0..=20.0).text("px")).changed() {
                            self.settings.bubble_radius = r; self.settings.save();
                        }
                    });
                    ui.horizontal(|ui| {
                        ui.label("Espaciado mensajes:");
                        let mut s = self.settings.message_spacing;
                        if ui.add(egui::Slider::new(&mut s, 2.0..=30.0).text("px")).changed() {
                            self.settings.message_spacing = s; self.settings.save();
                        }
                    });

                    // === NOTIFICACIONES ===
                    ui.add_space(10.0); ui.separator();
                    ui.heading("🔔 Notificaciones");
                    ui.horizontal(|ui| {
                        ui.label("Nivel:");
                        let current = self.settings.notify;
                        if ui.selectable_label(current == NotifyLevel::None, "Ninguna").clicked() {
                            self.settings.notify = NotifyLevel::None; self.settings.save();
                        }
                        if ui.selectable_label(current == NotifyLevel::Mentions, "Solo menciones").clicked() {
                            self.settings.notify = NotifyLevel::Mentions; self.settings.save();
                        }
                        if ui.selectable_label(current == NotifyLevel::All, "Todos los mensajes").clicked() {
                            self.settings.notify = NotifyLevel::All; self.settings.save();
                        }
                    });
                    if ui.button("Probar notificación").clicked() {
                        let _ = notify_rust::Notification::new()
                            .summary("FigDih")
                            .body("¡Las notificaciones funcionan! 🎉")
                            .show();
                    }

                    // === ACCIONES ===
                    ui.add_space(10.0); ui.separator();
                    ui.horizontal(|ui| {
                        if self.authed && ui.button("🚪 Cerrar sesión").clicked() { do_logout = true; }
                        if ui.button("Restablecer ajustes").clicked() {
                            self.settings = Settings::default();
                            self.settings.save();
                        }
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.button("Cerrar").clicked() { close = true; }
                        });
                    });
                });
            });
        if let Some(u) = do_connect { self.settings.save(); self.connect(u); self.show_settings = false; }
        if do_logout { self.password.clear(); let u = self.settings.url.clone(); self.connect(u); self.show_settings = false; }
        if close { self.show_settings = false; }
    }
}

impl eframe::App for FigDihApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        ctx.set_visuals(self.settings.theme.visuals());
        ctx.set_zoom_factor(self.settings.zoom);

        // === Drenar y procesar mensajes ===
        let mut incoming: Vec<String> = Vec::new();
        if let Some(rx) = &mut self.ws_rx {
            while let Ok(t) = rx.try_recv() { incoming.push(t); }
        }
        for text in incoming {
            if let Ok(m) = serde_json::from_str::<ServerMsg>(&text) {
                match m {
                    ServerMsg::AuthOk { user_id, username } => {
                        self.authed = true; self.my_id = user_id; self.username = username;
                        self.auth_error.clear(); self.status = "🟢 Conectado".into();
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
                        if self.selected_channel == Some(m.channel_id) { self.messages.push(m.clone()); }
                        // Notificación si aplica
                        let should_notify = match self.settings.notify {
                            NotifyLevel::None => false,
                            NotifyLevel::All => true,
                            NotifyLevel::Mentions => m.text.contains(&self.username),
                        };
                        if should_notify && m.user_id != self.my_id && self.selected_channel != Some(m.channel_id) {
                            let ch_name = self.channels.iter().find(|c| c.id == m.channel_id).map(|c| c.name.as_str()).unwrap_or("?");
                            let _ = self.notify_tx.send(format!("💬 #{} — {}: {}", ch_name, m.username, m.text.chars().take(80).collect::<String>()));
                        }
                    }
                    ServerMsg::MessageDeleted { channel_id, message_id } => {
                        if self.selected_channel == Some(channel_id) { self.messages.retain(|m| m.id != message_id); }
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

        // === Login / Registro ===
        if !self.authed {
            egui::CentralPanel::default().show(ctx, |ui| {
                ui.vertical_centered(|ui| {
                    ui.add_space(60.0);
                    ui.heading("💬 FigDih");
                    ui.label(egui::RichText::new("Chat ultraligero en Rust · v3.1").color(egui::Color32::GRAY));
                    ui.add_space(24.0);
                    egui::Frame::none()
                        .fill(ctx.style().visuals.window_fill)
                        .rounding(egui::Rounding::same(12.0))
                        .inner_margin(egui::Margin::same(24.0))
                        .show(ui, |ui| {
                            ui.set_width(260.0);
                            ui.label("Usuario");
                            ui.add(egui::TextEdit::singleline(&mut self.username).desired_width(260.0));
                            ui.add_space(8.0);
                            ui.label("Contraseña");
                            let pw = ui.add(egui::TextEdit::singleline(&mut self.password).password(true).desired_width(260.0));
                            ui.add_space(16.0);
                            let enter = pw.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                            ui.horizontal(|ui| {
                                if ui.add_sized([125.0, 32.0], egui::Button::new("Entrar 🔑")).clicked() || enter {
                                    self.send_command(ClientMsg::Login { username: self.username.clone(), password: self.password.clone() });
                                }
                                if ui.add_sized([125.0, 32.0], egui::Button::new("Registrarse ✨")).clicked() {
                                    self.send_command(ClientMsg::Register { username: self.username.clone(), password: self.password.clone() });
                                }
                            });
                            if !self.auth_error.is_empty() {
                                ui.add_space(10.0);
                                ui.colored_label(egui::Color32::from_rgb(237, 66, 69), &self.auth_error);
                            }
                        });
                    ui.add_space(16.0);
                    if ui.button("⚙️ Configuración de conexión").clicked() { self.show_settings = true; }
                    ui.add_space(8.0);
                    ui.label(egui::RichText::new(&self.status).small().color(egui::Color32::GRAY));
                });
            });
            self.settings_window(ctx);
            ctx.request_repaint_after(std::time::Duration::from_millis(100));
            return;
        }

        // === Rail de servidores ===
        let mut server_clicked: Option<i64> = None;
        let mut create_clicked = false;
        let mut refresh_clicked = false;
        let mut settings_clicked = false;
        egui::SidePanel::left("servers").default_width(72.0).show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(10.0);
                if ui.add(egui::Button::new("+").min_size(egui::vec2(48.0, 48.0)).rounding(egui::Rounding::same(16.0)))
                    .on_hover_text("Crear servidor").clicked() { create_clicked = true; }
                ui.add_space(6.0);
                if ui.add(egui::Button::new("🔄").min_size(egui::vec2(48.0, 30.0)))
                    .on_hover_text("Refrescar").clicked() { refresh_clicked = true; }
                if ui.add(egui::Button::new("⚙️").min_size(egui::vec2(48.0, 30.0)))
                    .on_hover_text("Configuración").clicked() { settings_clicked = true; }
                ui.separator();
                ui.add_space(8.0);
                egui::ScrollArea::vertical().show(ui, |ui| {
                    for s in &self.servers {
                        let sel = self.selected_server == Some(s.id);
                        let mut btn = egui::Button::new(&s.icon)
                            .min_size(egui::vec2(48.0, 48.0))
                            .rounding(egui::Rounding::same(if sel { 12.0 } else { 24.0 }));
                        if sel { btn = btn.fill(egui::Color32::from_rgb(88, 101, 242)); }
                        if ui.add(btn).on_hover_text(format!("{} {}", s.name, role_badge(s.my_role))).clicked() {
                            server_clicked = Some(s.id);
                        }
                        ui.add_space(8.0);
                    }
                });
            });
        });
        if create_clicked { self.show_create_server = true; }
        if refresh_clicked { self.send_command(ClientMsg::GetServers); }
        if settings_clicked { self.show_settings = true; }
        if let Some(id) = server_clicked { self.select_server(id); }

        // === Canales ===
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
                    ui.add_space(10.0); ui.separator();
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

        // === Miembros ===
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
                    let txt = format!("{} {} {}{}", dot, role_badge(m.role), m.username, mute);
                    let resp = ui.add(egui::Label::new(egui::RichText::new(txt).color(role_color(m.role))).sense(egui::Sense::click()));
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
                    ui.add_space(10.0); ui.separator();
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
                    ui.label("FigDih v3.1");
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
                    if self.settings.show_emoji_panel && ui.button("😀").clicked() {
                        self.show_emoji = !self.show_emoji;
                    }
                    let te = ui.add(egui::TextEdit::singleline(&mut self.input_text)
                        .hint_text("Escribe un mensaje...").desired_width(ui.available_width() - 90.0 - if self.settings.show_emoji_panel { 40.0 } else { 0.0 }));
                    let enter = te.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                    if ui.add_sized([80.0, 28.0], egui::Button::new("Enviar 🚀")).clicked() || enter {
                        if !self.input_text.trim().is_empty() {
                            self.send_command(ClientMsg::Send { text: self.input_text.clone() });
                            self.input_text.clear();
                        }
                    }
                });
                // Panel de emojis rápido
                if self.show_emoji {
                    ui.add_space(4.0);
                    egui::Frame::none()
                        .fill(ctx.style().visuals.window_fill)
                        .rounding(egui::Rounding::same(6.0))
                        .inner_margin(egui::Margin::same(4.0))
                        .show(ui, |ui| {
                            ui.horizontal_wrapped(|ui| {
                                for &em in EMOJI_PICK {
                                    if ui.add(egui::Button::new(em).frame(false)).clicked() {
                                        self.input_text.push_str(em);
                                    }
                                }
                            });
                        });
                }
            } else {
                ui.label("Selecciona un canal para chatear");
            }
            ui.add_space(4.0);
        });

        // === Mensajes con avatares ===
        let mut delete_clicked: Option<i64> = None;
        egui::CentralPanel::default().show(ctx, |ui| {
            if self.selected_channel.is_some() {
                egui::ScrollArea::vertical().stick_to_bottom(true).auto_shrink([false; 2]).show(ui, |ui| {
                    ui.add_space(8.0);
                    let gap = if self.settings.compact { 2.0 } else { 10.0 };
                    for m in &self.messages {
                        if m.user_id == 0 {
                            ui.label(egui::RichText::new(&m.text).italics().small().color(egui::Color32::GRAY));
                            ui.add_space(gap);
                            continue;
                        }
                        let role = self.members.iter().find(|mm| mm.user_id == m.user_id).map(|mm| mm.role).unwrap_or(0);
                        ui.horizontal_top(|ui| {
                            let (rect, _) = ui.allocate_exact_size(egui::vec2(34.0, 34.0), egui::Sense::hover());
                            ui.painter().circle_filled(rect.center(), 17.0, avatar_color(m.user_id));
                            let ini: String = m.username.chars().next().unwrap_or('?').to_uppercase().collect();
                            ui.painter().text(rect.center(), egui::Align2::CENTER_CENTER, ini,
                                egui::FontId::proportional(16.0), egui::Color32::WHITE);
                            ui.add_space(6.0);
                            ui.vertical(|ui| {
                                ui.horizontal(|ui| {
                                    ui.label(egui::RichText::new(&m.username).strong().color(role_color(role)));
                                    if self.settings.timestamps {
                                        ui.label(egui::RichText::new(time_ago(m.timestamp)).small().color(egui::Color32::GRAY));
                                    }
                                });
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
                        });
                        ui.add_space(gap);
                    }
                });
            } else {
                ui.centered_and_justified(|ui| {
                    ui.vertical_centered(|ui| {
                        ui.heading("👋 Bienvenido a FigDih");
                        ui.add_space(8.0);
                        ui.label("Elige un servidor y un canal para empezar");
                    });
                });
            }
        });
        if let Some(id) = delete_clicked { self.send_command(ClientMsg::DeleteMessage { message_id: id }); }

        // === Crear servidor ===
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
                        self.new_server_name.clear(); self.new_server_icon.clear();
                        self.show_create_server = false;
                    }
                    if ui.button("Cerrar").clicked() { self.show_create_server = false; }
                });
            });
        }

        self.settings_window(ctx);
        ctx.request_repaint_after(std::time::Duration::from_millis(100));
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 750.0])
            .with_min_inner_size([900.0, 550.0])
            .with_title("FigDih v3.1"),
        ..Default::default()
    };
    eframe::run_native("FigDih", options, Box::new(|cc| {
        // Fuente de emojis como fallback (adiós a los cuadraditos □)
        let mut fonts = egui::FontDefinitions::default();
        fonts.font_data.insert("emoji".to_owned(),
            egui::FontData::from_static(include_bytes!("../assets/NotoEmoji.ttf")));
        if let Some(f) = fonts.families.get_mut(&egui::FontFamily::Proportional) {
            f.push("emoji".to_owned());
        }
        cc.egui_ctx.set_fonts(fonts);
        Box::new(FigDihApp::new())
    }))
}