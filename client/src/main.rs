use eframe::egui;
use futures_util::{SinkExt, StreamExt};
use rodio::Source;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tungstenite::Message as WsMessage;

// ===== Protocolo (idéntico al servidor v3) =====
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

// ===== Temas =====
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
enum Theme { Dark, Light, Blurple, Midnight, Forest, Sunset, Cyberpunk, Mono }
impl Default for Theme { fn default() -> Self { Theme::Blurple } }
impl Theme {
    fn all() -> [(Self, &'static str); 8] {
        use Theme::*;
        [(Dark,"🌑 Dark"),(Light,"☀️ Light"),(Blurple,"💎 Blurple"),(Midnight,"🌌 Midnight"),
         (Forest,"🌲 Forest"),(Sunset,"🌅 Sunset"),(Cyberpunk,"🤖 Cyberpunk"),(Mono,"🖤 Mono")]
    }
    fn pal(self) -> ((u8,u8,u8),(u8,u8,u8),(u8,u8,u8),(u8,u8,u8),(u8,u8,u8),(u8,u8,u8)) {
        match self {
            Theme::Dark|Theme::Blurple => ((30,31,34),(43,45,49),(27,28,31),(54,57,63),(79,84,92),(88,101,242)),
            Theme::Light    => ((242,243,245),(255,255,255),(227,229,232),(210,213,218),(190,194,200),(88,101,242)),
            Theme::Midnight => ((15,23,42),(30,41,59),(2,6,23),(51,65,85),(71,85,105),(99,102,241)),
            Theme::Forest   => ((20,35,25),(28,48,35),(12,24,17),(45,80,55),(65,110,80),(76,175,80)),
            Theme::Sunset   => ((45,25,35),(65,35,50),(30,15,25),(100,55,70),(130,70,85),(255,107,74)),
            Theme::Cyberpunk=> ((15,10,30),(25,18,45),(8,5,20),(45,30,75),(70,45,110),(255,0,170)),
            Theme::Mono     => ((10,10,10),(20,20,20),(0,0,0),(40,40,40),(60,60,60),(200,200,200)),
        }
    }
    fn visuals(self) -> egui::Visuals {
        let mut v = if self == Theme::Light { egui::Visuals::light() } else { egui::Visuals::dark() };
        let (p,w,e,i,h,a) = self.pal();
        v.panel_fill = egui::Color32::from_rgb(p.0,p.1,p.2);
        v.window_fill = egui::Color32::from_rgb(w.0,w.1,w.2);
        v.extreme_bg_color = egui::Color32::from_rgb(e.0,e.1,e.2);
        v.widgets.noninteractive.bg_fill = egui::Color32::from_rgb(w.0,w.1,w.2);
        v.widgets.inactive.bg_fill = egui::Color32::from_rgb(i.0,i.1,i.2);
        v.widgets.hovered.bg_fill = egui::Color32::from_rgb(h.0,h.1,h.2);
        v.widgets.active.bg_fill = egui::Color32::from_rgb(a.0,a.1,a.2);
        v.selection.bg_fill = egui::Color32::from_rgb(a.0,a.1,a.2);
        v.window_rounding = egui::Rounding::same(10.0);
        v.menu_rounding = egui::Rounding::same(8.0);
        v
    }
    fn accent(self) -> egui::Color32 { let a = self.pal().5; egui::Color32::from_rgb(a.0,a.1,a.2) }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
enum NotifyLevel { None, Mentions, All }
impl Default for NotifyLevel { fn default() -> Self { NotifyLevel::Mentions } }

// ===== Ajustes =====
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
struct Settings {
    url: String,
    theme: Theme,
    zoom: f32,
    font_size: f32,
    timestamps: bool,
    compact: bool,
    group: bool,
    avatars: bool,
    members_panel: bool,
    emoji_button: bool,
    notify: NotifyLevel,
    sound: bool,
    volume: f32,
    server_colors: HashMap<i64, [u8; 3]>,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            url: default_url(),
            theme: Theme::Blurple, zoom: 1.0, font_size: 15.0,
            timestamps: true, compact: false, group: true, avatars: true,
            members_panel: true, emoji_button: true,
            notify: NotifyLevel::Mentions, sound: true, volume: 0.25,
            server_colors: HashMap::new(),
        }
    }
}
impl Settings {
    fn load() -> Self {
        std::fs::read_to_string("figdih-settings.json").ok()
            .and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default()
    }
    fn save(&self) {
        if let Ok(s) = serde_json::to_string_pretty(self) { let _ = std::fs::write("figdih-settings.json", s); }
    }
}

// ===== Helpers =====
fn now() -> i64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64 }
fn default_url() -> String {
    option_env!("FIGDIH_DEFAULT_URL").unwrap_or("ws://localhost:3000/ws").to_string()
}
fn time_ago(ts: i64) -> String {
    let d = (now() - ts).max(0);
    if d < 60 { "ahora".into() } else if d < 3600 { format!("{}m", d/60) }
    else if d < 86400 { format!("{}h", d/3600) } else { format!("{}d", d/86400) }
}
fn clock(ts: i64) -> String {
    let secs = ts.rem_euclid(86400);
    format!("{:02}:{:02}", secs/3600, (secs%3600)/60)
}
fn role_name(r: i64) -> &'static str { match r { 3=>"OWNER", 2=>"ADMIN", 1=>"MOD", _=>"MIEMBROS" } }
fn role_badge(r: i64) -> &'static str { match r { 3=>"👑", 2=>"🛡", 1=>"🔨", _=>"" } }
fn role_color(r: i64) -> egui::Color32 {
    match r { 3=>egui::Color32::from_rgb(250,166,26), 2=>egui::Color32::from_rgb(88,101,242),
              1=>egui::Color32::from_rgb(87,242,135), _=>egui::Color32::from_rgb(180,185,190) }
}
fn avatar_color(id: i64) -> egui::Color32 {
    const P: [(u8,u8,u8);6] = [(88,101,242),(87,242,135),(250,166,26),(237,66,69),(155,89,182),(26,188,156)];
    let c = P[(id.unsigned_abs() as usize) % P.len()];
    egui::Color32::from_rgb(c.0,c.1,c.2)
}
const EMOJIS: &[&str] = &[
    "😀","😂","😍","🥰","😎","🤔","😢","😡","👍","👎","❤️","🔥","🎉","✨","💯","🙏",
    "👀","💪","🤝","🚀","⭐","💡","🎵","🎮","⚡","☕","🍕","🌈","🦄","🐱","🐶","😴",
];

struct FigDihApp {
    authed: bool, my_id: i64,
    username: String, password: String, auth_error: String,
    servers: Vec<ServerInfo>, channels: Vec<ChannelInfo>,
    messages: Vec<ChatMessage>, members: Vec<MemberInfo>,
    selected_server: Option<i64>, selected_channel: Option<i64>, my_role: i64,
    unread: HashMap<i64, u32>,
    muted_until: i64,
    history_limit: i64,
    search: String,
    input_text: String,
    new_server_name: String, new_server_icon: String,
    new_channel_name: String, unban_name: String,
    show_create_server: bool, show_settings: bool, show_emoji: bool,
    settings_tab: usize,
    status: String,
    settings: Settings,
    stop: Arc<AtomicBool>,
    ws_tx: Option<mpsc::UnboundedSender<String>>,
    ws_rx: Option<mpsc::UnboundedReceiver<String>>,
    _rt: Arc<Runtime>,
    notify_tx: std::sync::mpsc::Sender<String>,
    audio: Option<(rodio::OutputStream, rodio::OutputStreamHandle)>,
}

impl FigDihApp {
    fn new() -> Self {
        let settings = Settings::load();
        let (notify_tx, notify_rx) = std::sync::mpsc::channel::<String>();
        std::thread::spawn(move || {
            while let Ok(msg) = notify_rx.recv() {
                let _ = notify_rust::Notification::new().summary("FigDih").body(&msg)
                    .timeout(notify_rust::Timeout::Milliseconds(4000)).show();
            }
        });
        let audio = rodio::OutputStream::try_default().ok();
        let mut app = Self {
            authed: false, my_id: 0, username: String::new(), password: String::new(),
            auth_error: String::new(), servers: vec![], channels: vec![], messages: vec![],
            members: vec![], selected_server: None, selected_channel: None, my_role: 0,
            unread: HashMap::new(), muted_until: 0, history_limit: 50, search: String::new(),
            input_text: String::new(), new_server_name: String::new(), new_server_icon: String::new(),
            new_channel_name: String::new(), unban_name: String::new(),
            show_create_server: false, show_settings: false, show_emoji: false, settings_tab: 0,
            status: String::new(), settings, stop: Arc::new(AtomicBool::new(false)),
            ws_tx: None, ws_rx: None, _rt: Arc::new(Runtime::new().unwrap()),
            notify_tx, audio,
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
                Err(e) => {
                    let _ = ws_tx.send(serde_json::to_string(&ServerMsg::AuthErr {
                        reason: format!("No se pudo conectar: {}", e),
                    }).unwrap());
                }
            }
        });
        self.ws_tx = Some(gui_tx);
        self.ws_rx = Some(ws_rx);
        self.authed = false; self.my_id = 0;
        self.servers.clear(); self.channels.clear(); self.messages.clear(); self.members.clear();
        self.unread.clear();
        self.selected_server = None; self.selected_channel = None; self.my_role = 0;
    }

    fn send_command(&self, cmd: ClientMsg) {
        if let Some(tx) = &self.ws_tx {
            if let Ok(j) = serde_json::to_string(&cmd) { let _ = tx.send(j); }
        }
    }

    fn play_sound(&self) {
        if !self.settings.sound { return; }
        if let Some((_, handle)) = &self.audio {
            if let Ok(sink) = rodio::Sink::try_new(handle) {
                let v = self.settings.volume.clamp(0.0, 1.0) * 0.4;
                sink.append(rodio::source::SineWave::new(880.0)
                    .take_duration(std::time::Duration::from_millis(80)).amplify(v)
                    .fade_in(std::time::Duration::from_millis(8)));
                sink.append(rodio::source::SineWave::new(1318.0)
                    .take_duration(std::time::Duration::from_millis(110)).amplify(v)
                    .fade_in(std::time::Duration::from_millis(8)));
                sink.detach();
            }
        }
    }

    fn accent(&self) -> egui::Color32 {
        if let Some(sid) = self.selected_server {
            if let Some(c) = self.settings.server_colors.get(&sid) {
                return egui::Color32::from_rgb(c[0], c[1], c[2]);
            }
        }
        self.settings.theme.accent()
    }

    fn refresh_my_role(&mut self) {
        self.my_role = self.servers.iter().find(|s| Some(s.id) == self.selected_server)
            .map(|s| s.my_role).unwrap_or(0);
    }

    fn select_server(&mut self, id: i64) {
        self.selected_server = Some(id);
        self.selected_channel = None;
        self.channels.clear(); self.messages.clear(); self.members.clear();
        self.search.clear();
        self.refresh_my_role();
        self.send_command(ClientMsg::GetChannels { server_id: id });
        self.send_command(ClientMsg::GetMembers { server_id: id });
    }

    fn select_channel(&mut self, id: i64) {
        if self.selected_channel == Some(id) { return; }
        self.send_command(ClientMsg::Leave);
        self.selected_channel = Some(id);
        self.messages.clear();
        self.search.clear();
        self.history_limit = 50;
        self.unread.remove(&id);
        self.send_command(ClientMsg::Join { channel_id: id });
        self.send_command(ClientMsg::GetMessages { channel_id: id, limit: 50 });
    }

    fn settings_window(&mut self, ctx: &egui::Context) {
        if !self.show_settings { return; }
        let mut do_connect: Option<String> = None;
        let mut do_logout = false;
        let mut close = false;
        let mut test_sound = false;
        egui::Window::new("⚙ Configuración").collapsible(false).resizable(true)
            .default_size([460.0, 560.0]).show(ctx, |ui| {
                ui.horizontal(|ui| {
                    for (i, t) in ["🔌 Conexión", "🎨 Apariencia", "🔔 Avisos"].iter().enumerate() {
                        if ui.selectable_label(self.settings_tab == i, *t).clicked() { self.settings_tab = i; }
                    }
                });
                ui.separator();
                egui::ScrollArea::vertical().max_height(400.0).show(ui, |ui| {
                    match self.settings_tab {
                        0 => {
                            ui.label("URL del servidor WebSocket:");
                            ui.add(egui::TextEdit::singleline(&mut self.settings.url).desired_width(400.0));
                            ui.add_space(6.0);
                            ui.horizontal(|ui| {
                                if ui.button("🔌 Reconectar").clicked() { do_connect = Some(self.settings.url.clone()); }
                                if ui.button("↩ localhost").clicked() {
                                    self.settings.url = "ws://localhost:3000/ws".into(); self.settings.save();
                                }
                                if ui.button("↺ URL de esta build").clicked() {
                                    self.settings.url = default_url(); self.settings.save();
                                }
                            });
                            ui.add_space(10.0);
                            ui.label(egui::RichText::new("Usa wss:// para servidores con HTTPS (túneles).").small().color(egui::Color32::GRAY));
                        }
                        1 => {
                            ui.label("Tema:");
                            egui::Grid::new("themes").num_columns(4).spacing([6.0, 6.0]).show(ui, |ui| {
                                for (i, (t, name)) in Theme::all().iter().enumerate() {
                                    let sel = self.settings.theme == *t;
                                    let mut b = egui::Button::new(*name).min_size(egui::vec2(96.0, 28.0));
                                    if sel { b = b.fill(t.accent()); }
                                    if ui.add(b).clicked() { self.settings.theme = *t; self.settings.save(); }
                                    if (i + 1) % 4 == 0 { ui.end_row(); }
                                }
                            });
                            ui.add_space(8.0);
                            if let Some(sid) = self.selected_server {
                                ui.separator();
                                ui.label("🎨 Color propio de este servidor:");
                                let mut c = self.settings.server_colors.get(&sid).copied()
                                    .unwrap_or([88, 101, 242]);
                                ui.horizontal(|ui| {
                                    egui::color_picker::color_edit_button_srgb(ui, &mut c);
                                    if ui.button("Guardar color").clicked() {
                                        self.settings.server_colors.insert(sid, c); self.settings.save();
                                    }
                                    if ui.button("Quitar").clicked() {
                                        self.settings.server_colors.remove(&sid); self.settings.save();
                                    }
                                });
                            }
                            ui.add_space(8.0); ui.separator();
                            let mut fs = self.settings.font_size;
                            if ui.add(egui::Slider::new(&mut fs, 11.0..=22.0).text("Tamaño de texto")).changed() {
                                self.settings.font_size = fs; self.settings.save();
                            }
                            let mut z = self.settings.zoom;
                            if ui.add(egui::Slider::new(&mut z, 0.7..=1.8).text("Zoom")).changed() {
                                self.settings.zoom = z; self.settings.save();
                            }
                                                        let labels = ["Mostrar horas", "Agrupar mensajes seguidos", "Mostrar avatares",
                                          "Modo compacto", "Panel de miembros", "Botón de emojis"];
                            let mut vals = [
                                self.settings.timestamps, self.settings.group, self.settings.avatars,
                                self.settings.compact, self.settings.members_panel, self.settings.emoji_button,
                            ];
                            let mut changed = false;
                            for (i, label) in labels.iter().enumerate() {
                                if ui.checkbox(&mut vals[i], *label).changed() { changed = true; }
                            }
                            if changed {
                                self.settings.timestamps = vals[0];
                                self.settings.group = vals[1];
                                self.settings.avatars = vals[2];
                                self.settings.compact = vals[3];
                                self.settings.members_panel = vals[4];
                                self.settings.emoji_button = vals[5];
                                self.settings.save();
                            }
                        }
                        _ => {
                            ui.label("Notificaciones de escritorio:");
                            ui.horizontal(|ui| {
                                for (lvl, name) in [(NotifyLevel::None,"Ninguna"),(NotifyLevel::Mentions,"Menciones"),(NotifyLevel::All,"Todas")] {
                                    if ui.selectable_label(self.settings.notify == lvl, name).clicked() {
                                        self.settings.notify = lvl; self.settings.save();
                                    }
                                }
                            });
                            ui.add_space(10.0); ui.separator();
                            let mut s = self.settings.sound;
                            if ui.checkbox(&mut s, "🔊 Sonido al recibir mensaje").changed() {
                                self.settings.sound = s; self.settings.save();
                            }
                            let mut vol = self.settings.volume;
                            if ui.add(egui::Slider::new(&mut vol, 0.0..=1.0).text("Volumen")).changed() {
                                self.settings.volume = vol; self.settings.save();
                            }
                            if ui.button("▶ Probar sonido").clicked() { test_sound = true; }
                            if self.audio.is_none() {
                                ui.colored_label(egui::Color32::from_rgb(237,66,69), "⚠ No hay dispositivo de audio disponible");
                            }
                            if ui.button("🔔 Probar notificación").clicked() {
                                let _ = self.notify_tx.send("¡Las notificaciones funcionan! 🎉".into());
                            }
                        }
                    }
                });
                ui.separator();
                ui.horizontal(|ui| {
                    if self.authed && ui.button("🚪 Cerrar sesión").clicked() { do_logout = true; }
                    if ui.button("↺ Restablecer").clicked() { self.settings = Settings::default(); self.settings.save(); }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.button("Cerrar").clicked() { close = true; }
                    });
                });
            });
        if test_sound { self.play_sound(); }
        if let Some(u) = do_connect { self.settings.save(); self.connect(u); self.show_settings = false; }
        if do_logout { self.password.clear(); let u = self.settings.url.clone(); self.connect(u); self.show_settings = false; }
        if close { self.show_settings = false; }
    }
}

impl eframe::App for FigDihApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        ctx.set_visuals(self.settings.theme.visuals());
        ctx.set_zoom_factor(self.settings.zoom);
        let fs = self.settings.font_size;
        ctx.style_mut(|s| {
            s.text_styles.insert(egui::TextStyle::Body, egui::FontId::proportional(fs));
            s.text_styles.insert(egui::TextStyle::Button, egui::FontId::proportional(fs));
            s.text_styles.insert(egui::TextStyle::Small, egui::FontId::proportional((fs - 3.0).max(8.0)));
            s.spacing.item_spacing = egui::vec2(8.0, 6.0);
        });
        if ctx.input(|i| i.key_pressed(egui::Key::Escape)) {
            self.show_settings = false; self.show_create_server = false; self.show_emoji = false;
        }

        // ===== Mensajes entrantes =====
        let mut incoming: Vec<String> = Vec::new();
        if let Some(rx) = &mut self.ws_rx { while let Ok(t) = rx.try_recv() { incoming.push(t); } }
        for text in incoming {
            if let Ok(m) = serde_json::from_str::<ServerMsg>(&text) {
                match m {
                    ServerMsg::AuthOk { user_id, username } => {
                        self.authed = true; self.my_id = user_id; self.username = username;
                        self.auth_error.clear(); self.status = "🟢 Conectado".into();
                        self.send_command(ClientMsg::GetServers);
                    }
                    ServerMsg::AuthErr { reason } => self.auth_error = reason,
                    ServerMsg::Error { text } => self.status = format!("⚠ {}", text),
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
                        else if m.user_id != 0 { *self.unread.entry(m.channel_id).or_insert(0) += 1; }
                        if m.user_id != self.my_id && m.user_id != 0 {
                            let mentioned = m.text.to_lowercase().contains(&self.username.to_lowercase());
                            let want = match self.settings.notify {
                                NotifyLevel::None => false, NotifyLevel::All => true, NotifyLevel::Mentions => mentioned,
                            };
                            if want {
                                self.play_sound();
                                if self.selected_channel != Some(m.channel_id) {
                                    let _ = self.notify_tx.send(format!("{}: {}", m.username, m.text.chars().take(90).collect::<String>()));
                                }
                            }
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
                            self.messages.push(ChatMessage { id: 0, user_id: 0, username: String::new(), channel_id, text, timestamp: now() });
                        }
                    }
                    ServerMsg::Kicked { server_id } => {
                        self.status = "👢 Te expulsaron del servidor".into();
                        if self.selected_server == Some(server_id) { self.selected_channel = None; self.messages.clear(); }
                    }
                    ServerMsg::Banned { server_id } => {
                        self.status = "🚫 Estás baneado de ese servidor".into();
                        if self.selected_server == Some(server_id) { self.selected_channel = None; self.messages.clear(); }
                    }
                    ServerMsg::TimedOut { until } => {
                        self.muted_until = until;
                        self.status = format!("🔇 Silenciado {} min", ((until - now())/60).max(1));
                    }
                }
            }
        }

        // ===== Login =====
        if !self.authed {
            egui::CentralPanel::default().show(ctx, |ui| {
                ui.vertical_centered(|ui| {
                    ui.add_space(70.0);
                    ui.label(egui::RichText::new("💬").size(56.0));
                    ui.heading("FigDih");
                    ui.label(egui::RichText::new("Chat ultraligero en Rust · v0.5").color(egui::Color32::GRAY));
                    ui.add_space(26.0);
                    egui::Frame::none().fill(ctx.style().visuals.window_fill)
                        .rounding(egui::Rounding::same(14.0))
                        .inner_margin(egui::Margin::same(26.0))
                        .stroke(egui::Stroke::new(1.0_f32, egui::Color32::from_black_alpha(40)))
                        .show(ui, |ui| {
                            ui.set_width(280.0);
                            ui.label(egui::RichText::new("Usuario").small().color(egui::Color32::GRAY));
                            ui.add(egui::TextEdit::singleline(&mut self.username).desired_width(280.0));
                            ui.add_space(10.0);
                            ui.label(egui::RichText::new("Contraseña").small().color(egui::Color32::GRAY));
                            let pw = ui.add(egui::TextEdit::singleline(&mut self.password).password(true).desired_width(280.0));
                            ui.add_space(18.0);
                            let enter = pw.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                            ui.horizontal(|ui| {
                                if ui.add_sized([134.0, 34.0], egui::Button::new("Entrar")).clicked() || enter {
                                    self.send_command(ClientMsg::Login { username: self.username.clone(), password: self.password.clone() });
                                }
                                if ui.add_sized([134.0, 34.0], egui::Button::new("Registrarse")).clicked() {
                                    self.send_command(ClientMsg::Register { username: self.username.clone(), password: self.password.clone() });
                                }
                            });
                            if !self.auth_error.is_empty() {
                                ui.add_space(10.0);
                                ui.colored_label(egui::Color32::from_rgb(237,66,69), &self.auth_error);
                            }
                        });
                    ui.add_space(16.0);
                    if ui.button("⚙ Configuración de conexión").clicked() { self.show_settings = true; }
                    ui.add_space(6.0);
                    ui.label(egui::RichText::new(&self.status).small().color(egui::Color32::GRAY));
                });
            });
            self.settings_window(ctx);
            ctx.request_repaint_after(std::time::Duration::from_millis(100));
            return;
        }

        let accent = self.accent();

        // ===== Rail de servidores =====
        let mut server_clicked: Option<i64> = None;
        let mut create_clicked = false;
        let mut refresh_clicked = false;
        let mut settings_clicked = false;
        egui::SidePanel::left("rail").exact_width(72.0).resizable(false).show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(10.0);
                egui::ScrollArea::vertical().max_height(ui.available_height() - 110.0).show(ui, |ui| {
                    ui.vertical_centered(|ui| {
                        for s in &self.servers {
                            let sel = self.selected_server == Some(s.id);
                            let col = self.settings.server_colors.get(&s.id)
                                .map(|c| egui::Color32::from_rgb(c[0],c[1],c[2]))
                                .unwrap_or(accent);
                            let mut b = egui::Button::new(egui::RichText::new(&s.icon).size(22.0))
                                .min_size(egui::vec2(48.0, 48.0))
                                .rounding(egui::Rounding::same(if sel { 14.0 } else { 24.0 }));
                            if sel { b = b.fill(col); }
                            if ui.add(b).on_hover_text(format!("{}  ·  {}", s.name, role_name(s.my_role))).clicked() {
                                server_clicked = Some(s.id);
                            }
                            ui.add_space(8.0);
                        }
                    });
                });
                ui.add_space(6.0);
                if ui.add(egui::Button::new(egui::RichText::new("+").size(24.0)).min_size(egui::vec2(48.0,40.0))
                    .rounding(egui::Rounding::same(20.0))).on_hover_text("Crear servidor").clicked() { create_clicked = true; }
                ui.add_space(4.0);
                if ui.add(egui::Button::new("🔄").min_size(egui::vec2(48.0,28.0))).on_hover_text("Refrescar").clicked() { refresh_clicked = true; }
                ui.add_space(4.0);
                if ui.add(egui::Button::new("⚙").min_size(egui::vec2(48.0,28.0))).on_hover_text("Configuración").clicked() { settings_clicked = true; }
            });
        });
        if create_clicked { self.show_create_server = true; }
        if refresh_clicked { self.send_command(ClientMsg::GetServers); }
        if settings_clicked { self.show_settings = true; }
        if let Some(id) = server_clicked { self.select_server(id); }

        // ===== Canales =====
        let mut channel_clicked: Option<i64> = None;
        egui::SidePanel::left("channels").exact_width(232.0).resizable(false).show(ctx, |ui| {
            if let Some(sid) = self.selected_server {
                let name = self.servers.iter().find(|s| s.id == sid).map(|s| s.name.clone()).unwrap_or_default();
                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    ui.add_space(6.0);
                    ui.label(egui::RichText::new(&name).strong().size(17.0));
                    ui.label(egui::RichText::new(role_badge(self.my_role)).color(role_color(self.my_role)));
                });
                ui.add_space(6.0); ui.separator(); ui.add_space(6.0);
                ui.label(egui::RichText::new("  CANALES").small().color(egui::Color32::GRAY));
                ui.add_space(4.0);
                for c in &self.channels {
                    let sel = self.selected_channel == Some(c.id);
                    let n = self.unread.get(&c.id).copied().unwrap_or(0);
                    let label = if n > 0 { format!("#  {}   ({})", c.name, n) } else { format!("#  {}", c.name) };
                    let mut txt = egui::RichText::new(label);
                    if sel || n > 0 { txt = txt.strong(); }
                    let mut b = egui::Button::new(txt).min_size(egui::vec2(ui.available_width() - 8.0, 30.0))
                        .rounding(egui::Rounding::same(6.0)).frame(true);
                    if sel { b = b.fill(accent.gamma_multiply(0.35)); }
                    ui.horizontal(|ui| {
                        ui.add_space(4.0);
                        if ui.add(b).clicked() { channel_clicked = Some(c.id); }
                    });
                    ui.add_space(2.0);
                }
                if self.my_role >= 2 {
                    ui.add_space(10.0); ui.separator();
                    ui.label(egui::RichText::new("  Nuevo canal").small().color(egui::Color32::GRAY));
                    ui.horizontal(|ui| {
                        ui.add_space(4.0);
                        ui.add(egui::TextEdit::singleline(&mut self.new_channel_name).hint_text("nombre").desired_width(140.0));
                        if ui.button("+").clicked() && !self.new_channel_name.trim().is_empty() {
                            self.send_command(ClientMsg::CreateChannel { server_id: sid, name: self.new_channel_name.clone() });
                            self.new_channel_name.clear();
                        }
                    });
                }
            } else {
                ui.add_space(24.0);
                ui.vertical_centered(|ui| {
                    ui.label(egui::RichText::new("👈").size(28.0));
                    ui.add_space(6.0);
                    ui.label("Elige o crea un servidor");
                });
            }
        });
        if let Some(id) = channel_clicked { self.select_channel(id); }

        // ===== Miembros =====
        let mut mod_action: Option<(String, ModAction)> = None;
        if self.settings.members_panel && self.selected_server.is_some() {
            egui::SidePanel::right("members").exact_width(200.0).resizable(false).show(ctx, |ui| {
                ui.add_space(10.0);
                let online = self.members.iter().filter(|m| m.online).count();
                ui.label(egui::RichText::new(format!("  EN LÍNEA — {}", online)).small().color(egui::Color32::GRAY));
                ui.add_space(6.0);
                let t = now();
                let mut prev_role = -1;
                egui::ScrollArea::vertical().max_height(ui.available_height() - 110.0).show(ui, |ui| {
                    for m in &self.members {
                        if m.role != prev_role {
                            prev_role = m.role;
                            ui.add_space(6.0);
                            ui.label(egui::RichText::new(format!("  {}", role_name(m.role))).small().color(role_color(m.role)));
                        }
                        let mute = if m.muted_until > t { "  🔇" } else { "" };
                        let txt = format!("{}{}", m.username, mute);
                        let col = if m.online { role_color(m.role) } else { egui::Color32::from_rgb(120,124,128) };
                        let row = ui.horizontal(|ui| {
                            ui.add_space(6.0);
                            let (rect, _) = ui.allocate_exact_size(egui::vec2(10.0, 14.0), egui::Sense::hover());
                            let c = if m.online { egui::Color32::from_rgb(87,242,135) } else { egui::Color32::from_rgb(90,94,100) };
                            ui.painter().circle_filled(rect.center(), 4.0, c);
                            ui.add(egui::Label::new(egui::RichText::new(txt).color(col)).sense(egui::Sense::click()))
                        });
                        let resp = row.inner;
                        if m.user_id != self.my_id && m.role < self.my_role {
                            let uname = m.username.clone();
                            let mrole = m.role;
                            resp.context_menu(|ui| {
                                if self.my_role >= 1 {
                                    if ui.button("🔇 Timeout 10 min").clicked() { mod_action = Some((uname.clone(), ModAction::Timeout)); ui.close_menu(); }
                                    if ui.button("👢 Expulsar").clicked() { mod_action = Some((uname.clone(), ModAction::Kick)); ui.close_menu(); }
                                }
                                if self.my_role >= 2 {
                                    if ui.button("🚫 Banear").clicked() { mod_action = Some((uname.clone(), ModAction::Ban)); ui.close_menu(); }
                                    if mrole < 1 && ui.button("🔨 Hacer mod").clicked() { mod_action = Some((uname.clone(), ModAction::Role(1))); ui.close_menu(); }
                                    if mrole > 0 && ui.button("👤 Quitar rol").clicked() { mod_action = Some((uname.clone(), ModAction::Role(0))); ui.close_menu(); }
                                }
                                if self.my_role == 3 && mrole < 2 && ui.button("🛡 Hacer admin").clicked() {
                                    mod_action = Some((uname.clone(), ModAction::Role(2))); ui.close_menu();
                                }
                            });
                        }
                        ui.add_space(2.0);
                    }
                });
                if self.my_role >= 2 {
                    ui.separator();
                    ui.label(egui::RichText::new("  Desbanear").small().color(egui::Color32::GRAY));
                    ui.horizontal(|ui| {
                        ui.add_space(4.0);
                        ui.add(egui::TextEdit::singleline(&mut self.unban_name).hint_text("usuario").desired_width(110.0));
                        if ui.button("🔓").clicked() && !self.unban_name.trim().is_empty() {
                            if let Some(sid) = self.selected_server {
                                self.send_command(ClientMsg::Unban { server_id: sid, username: self.unban_name.clone() });
                                self.unban_name.clear();
                            }
                        }
                    });
                }
            });
        }
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

        // ===== Barra superior =====
        let mut load_more = false;
        let mut toggle_members = false;
        egui::TopBottomPanel::top("top").exact_height(44.0).show(ctx, |ui| {
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.add_space(6.0);
                if let Some(cid) = self.selected_channel {
                    let n = self.channels.iter().find(|c| c.id == cid).map(|c| c.name.clone()).unwrap_or_default();
                    ui.label(egui::RichText::new(format!("# {}", n)).strong().size(16.0));
                } else { ui.label(egui::RichText::new("FigDih").strong()); }
                ui.separator();
                ui.add(egui::TextEdit::singleline(&mut self.search).hint_text("🔍 Buscar...").desired_width(170.0));
                if !self.search.is_empty() {
                    let q = self.search.to_lowercase();
                    let n = self.messages.iter().filter(|m| m.text.to_lowercase().contains(&q)).count();
                    ui.label(egui::RichText::new(format!("{} resultados", n)).small().color(accent));
                    if ui.button("x").on_hover_text("Limpiar").clicked() { self.search.clear(); }
                }
                if ui.button("📜 Más").on_hover_text("Cargar historial más antiguo").clicked() { load_more = true; }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.add_space(6.0);
                    ui.label(egui::RichText::new(format!("{} {}", role_badge(self.my_role), self.username)).strong());
                    ui.separator();
                    if ui.selectable_label(self.settings.members_panel, "👥").on_hover_text("Miembros").clicked() { toggle_members = true; }
                    ui.label(egui::RichText::new(&self.status).small().color(egui::Color32::from_rgb(250,166,26)));
                });
            });
        });
        if toggle_members { self.settings.members_panel = !self.settings.members_panel; self.settings.save(); }
        if load_more {
            if let Some(cid) = self.selected_channel {
                self.history_limit = (self.history_limit + 100).min(1000);
                self.send_command(ClientMsg::GetMessages { channel_id: cid, limit: self.history_limit });
            }
        }

        // ===== Composer =====
        let muted_left = self.muted_until - now();
        egui::TopBottomPanel::bottom("composer").show(ctx, |ui| {
            ui.add_space(6.0);
            if self.selected_channel.is_some() {
                if muted_left > 0 {
                    ui.horizontal(|ui| {
                        ui.add_space(8.0);
                        ui.colored_label(egui::Color32::from_rgb(237,66,69),
                            format!("🔇 Estás silenciado — quedan {}:{} min", muted_left/60, muted_left%60));
                    });
                } else {
                    ui.horizontal(|ui| {
                        ui.add_space(6.0);
                        if self.settings.emoji_button {
                            if ui.add_sized([34.0, 30.0], egui::Button::new("😀")).clicked() { self.show_emoji = !self.show_emoji; }
                        }
                        let te = ui.add(egui::TextEdit::singleline(&mut self.input_text)
                            .hint_text("Escribe un mensaje...").desired_width(ui.available_width() - 96.0));
                        let enter = te.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                        let send = ui.add_sized([84.0, 30.0], egui::Button::new("Enviar")).clicked();
                        if (send || enter) && !self.input_text.trim().is_empty() {
                            self.send_command(ClientMsg::Send { text: self.input_text.clone() });
                            self.input_text.clear();
                            self.show_emoji = false;
                        }
                    });
                    if self.show_emoji {
                        ui.add_space(6.0);
                        egui::Frame::none().fill(ctx.style().visuals.window_fill)
                            .rounding(egui::Rounding::same(8.0)).inner_margin(egui::Margin::same(6.0))
                            .show(ui, |ui| {
                                ui.horizontal_wrapped(|ui| {
                                    for e in EMOJIS { if ui.button(*e).clicked() { self.input_text.push_str(e); } }
                                });
                            });
                    }
                }
            } else {
                ui.horizontal(|ui| { ui.add_space(8.0); ui.label("Selecciona un canal para chatear"); });
            }
            ui.add_space(6.0);
        });

        // ===== Mensajes =====
        let mut delete_clicked: Option<i64> = None;
        let q = self.search.to_lowercase();
        let gap = if self.settings.compact { 2.0 } else { 8.0 };
        let me = self.username.to_lowercase();
        egui::CentralPanel::default().show(ctx, |ui| {
            if self.selected_channel.is_none() {
                ui.centered_and_justified(|ui| {
                    ui.vertical_centered(|ui| {
                        ui.label(egui::RichText::new("💬").size(48.0));
                        ui.add_space(8.0);
                        ui.heading("Elige un canal");
                        ui.label(egui::RichText::new("Tu historial se guarda en el servidor").color(egui::Color32::GRAY));
                    });
                });
                return;
            }
            egui::ScrollArea::vertical().stick_to_bottom(true).auto_shrink([false; 2]).show(ui, |ui| {
                ui.add_space(10.0);
                let mut prev: Option<(i64, i64)> = None;
                for m in &self.messages {
                    if !q.is_empty() && !m.text.to_lowercase().contains(&q) { continue; }
                    if m.user_id == 0 {
                        ui.horizontal(|ui| {
                            ui.add_space(14.0);
                            ui.label(egui::RichText::new(&m.text).italics().small().color(egui::Color32::GRAY));
                        });
                        ui.add_space(gap);
                        prev = None;
                        continue;
                    }
                    let grouped = self.settings.group
                        && prev.map(|(u, t)| u == m.user_id && m.timestamp - t < 300).unwrap_or(false);
                    let highlight = !me.is_empty() && m.text.to_lowercase().contains(&me);
                    let role = self.members.iter().find(|mm| mm.user_id == m.user_id).map(|mm| mm.role).unwrap_or(0);

                    let row = ui.horizontal_top(|ui| {
                        ui.add_space(10.0);
                        if self.settings.avatars {
                            let (rect, _) = ui.allocate_exact_size(egui::vec2(36.0, 30.0), egui::Sense::hover());
                            if !grouped {
                                ui.painter().circle_filled(rect.center(), 17.0, avatar_color(m.user_id));
                                let ini: String = m.username.chars().next().unwrap_or('?').to_uppercase().collect();
                                ui.painter().text(rect.center(), egui::Align2::CENTER_CENTER, ini,
                                    egui::FontId::proportional(16.0), egui::Color32::WHITE);
                            } else if self.settings.timestamps {
                                ui.painter().text(rect.center(), egui::Align2::CENTER_CENTER, clock(m.timestamp),
                                    egui::FontId::proportional(10.0), egui::Color32::from_rgb(110,114,120));
                            }
                            ui.add_space(6.0);
                        }
                        ui.vertical(|ui| {
                            if !grouped {
                                ui.horizontal(|ui| {
                                    ui.label(egui::RichText::new(&m.username).strong().color(role_color(role)));
                                    if !role_badge(role).is_empty() {
                                        ui.label(egui::RichText::new(role_badge(role)).small().color(role_color(role)));
                                    }
                                    if self.settings.timestamps {
                                        ui.label(egui::RichText::new(time_ago(m.timestamp)).small().color(egui::Color32::GRAY));
                                    }
                                });
                            }
                            let mut txt = egui::RichText::new(&m.text);
                            if highlight { txt = txt.color(egui::Color32::from_rgb(250,166,26)); }
                            let resp = ui.add(egui::Label::new(txt).sense(egui::Sense::click()).wrap(true));
                            if m.user_id == self.my_id || self.my_role >= 1 {
                                let mid = m.id;
                                resp.context_menu(|ui| {
                                    if ui.button("🗑 Eliminar mensaje").clicked() { delete_clicked = Some(mid); ui.close_menu(); }
                                });
                            }
                            resp
                        });
                    });
                    if highlight {
                        let r = row.response.rect.expand(3.0);
                        ui.painter().rect_stroke(r, 4.0, egui::Stroke::new(1.0_f32, egui::Color32::from_rgb(250,166,26).gamma_multiply(0.5)));
                    }
                    ui.add_space(gap);
                    prev = Some((m.user_id, m.timestamp));
                }
                ui.add_space(10.0);
            });
        });
        if let Some(id) = delete_clicked { self.send_command(ClientMsg::DeleteMessage { message_id: id }); }

        // ===== Crear servidor =====
        if self.show_create_server {
            egui::Window::new("Crear servidor").collapsible(false).resizable(false).show(ctx, |ui| {
                ui.set_width(240.0);
                ui.label("Nombre");
                ui.add(egui::TextEdit::singleline(&mut self.new_server_name).desired_width(240.0));
                ui.add_space(6.0);
                ui.label("Icono (emoji)");
                ui.add(egui::TextEdit::singleline(&mut self.new_server_icon).hint_text("🎮").desired_width(60.0));
                ui.add_space(6.0);
                ui.horizontal_wrapped(|ui| {
                    for e in ["🎮","🌍","💻","🎵","📚","⚽","🍕","🚀","🐱","🔥"] {
                        if ui.button(e).clicked() { self.new_server_icon = e.to_string(); }
                    }
                });
                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    if ui.button("Crear 🎉").clicked() && !self.new_server_name.trim().is_empty() {
                        self.send_command(ClientMsg::CreateServer { name: self.new_server_name.clone(), icon: self.new_server_icon.clone() });
                        self.new_server_name.clear(); self.new_server_icon.clear();
                        self.show_create_server = false;
                    }
                    if ui.button("Cancelar").clicked() { self.show_create_server = false; }
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
            .with_inner_size([1320.0, 780.0])
            .with_min_inner_size([940.0, 560.0])
            .with_title("FigDih v0.5"),
        ..Default::default()
    };
    eframe::run_native("FigDih", options, Box::new(|cc| {
        let mut fonts = egui::FontDefinitions::default();

        // Fuente de emojis: archivo externo opcional, si no la embebida
        let emoji_bytes: Vec<u8> = std::fs::read("emoji.ttf")
            .unwrap_or_else(|_| include_bytes!("../assets/NotoEmoji.ttf").to_vec());
        fonts.font_data.insert("figemoji".to_owned(), egui::FontData::from_owned(emoji_bytes));

        // CLAVE: quitar el mini-font de emojis que trae egui y poner el nuestro
        for fam in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
            if let Some(list) = fonts.families.get_mut(&fam) {
                list.retain(|n| n != "emoji-icon-font");
                list.push("figemoji".to_owned());
            }
        }
        cc.egui_ctx.set_fonts(fonts);
        Box::new(FigDihApp::new())
    }))
}