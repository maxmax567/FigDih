use eframe::egui;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tungstenite::Message as WsMessage;

// === Tipos ===
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ServerInfo {
    id: i64,
    name: String,
    icon: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChannelInfo {
    id: i64,
    server_id: i64,
    name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChatMessage {
    id: Option<i64>,
    user: String,
    channel_id: i64,
    text: String,
    timestamp: u64,
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
    Welcome { user: String },
    Message(ChatMessage),
    UserJoined { user: String, channel_id: i64 },
    UserLeft { user: String, channel_id: i64 },
    Servers(Vec<ServerInfo>),
    Channels(Vec<ChannelInfo>),
    Messages(Vec<ChatMessage>),
    Error(String),
}

// === Aplicación GUI ===
struct FigDihApp {
    username: String,
    servers: Vec<ServerInfo>,
    channels: Vec<ChannelInfo>,
    messages: Vec<ChatMessage>,
    selected_server: Option<i64>,
    selected_channel: Option<i64>,
    input_text: String,
    ws_tx: Option<mpsc::UnboundedSender<String>>,
    ws_rx: Option<mpsc::UnboundedReceiver<String>>,
    connected: bool,
    _rt: Arc<Runtime>,
    pending_join: bool,
}

impl FigDihApp {
    fn new() -> Self {
        let (gui_to_ws_tx, mut gui_to_ws_rx) = mpsc::unbounded_channel::<String>();
        let (ws_to_gui_tx, ws_to_gui_rx) = mpsc::unbounded_channel::<String>();
        
        let rt = Arc::new(Runtime::new().unwrap());
        let rt_clone = rt.clone();

        rt_clone.spawn(async move {
            let url = "ws://localhost:3000/ws";
            if let Ok((ws_stream, _)) = connect_async(url).await {
                let (mut write, mut read) = ws_stream.split();

                tokio::spawn(async move {
                    while let Some(msg) = gui_to_ws_rx.recv().await {
                        let _ = write.send(WsMessage::Text(msg)).await;
                    }
                });

                while let Some(Ok(msg)) = read.next().await {
                    if let WsMessage::Text(text) = msg {
                        let _ = ws_to_gui_tx.send(text);
                    }
                }
            } else {
                eprintln!("⚠️ No se pudo conectar al servidor en {}", url);
            }
        });

        let app = Self {
            username: "Usuario".into(),
            servers: Vec::new(),
            channels: Vec::new(),
            messages: Vec::new(),
            selected_server: None,
            selected_channel: None,
            input_text: String::new(),
            ws_tx: Some(gui_to_ws_tx),
            ws_rx: Some(ws_to_gui_rx),
            connected: false,
            _rt: rt,
            pending_join: false,
        };

        // Solicitar servidores al iniciar
        app.send_command(ClientMsg::GetServers);
        app
    }

    fn send_command(&self, cmd: ClientMsg) {
        if let Some(tx) = &self.ws_tx {
            if let Ok(json) = serde_json::to_string(&cmd) {
                let _ = tx.send(json);
            }
        }
    }

    fn send_message(&mut self) {
        if self.input_text.trim().is_empty() || self.selected_channel.is_none() {
            return;
        }

        let msg = ChatMessage {
            id: None,
            user: self.username.clone(),
            channel_id: self.selected_channel.unwrap(),
            text: self.input_text.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
           .unwrap().as_secs(),
        };

        // ✅ Eco local: tu mensaje aparece al instante
        self.messages.push(msg.clone());
        self.send_command(ClientMsg::Send(msg));
        self.input_text.clear();
    }

    fn select_server(&mut self, server_id: i64) {
        self.selected_server = Some(server_id);
        self.selected_channel = None;
        self.messages.clear();
        self.send_command(ClientMsg::GetChannels { server_id });
    }

    fn select_channel(&mut self, channel_id: i64) {
        // Dejar canal anterior
        if let Some(old_channel) = self.selected_channel {
            self.send_command(ClientMsg::Leave { channel_id: old_channel });
        }
        
        self.selected_channel = Some(channel_id);
        self.messages.clear();
        
        // Unirse al nuevo canal
        self.send_command(ClientMsg::Join {
            username: self.username.clone(),
            channel_id,
        });
        
        // Cargar mensajes históricos
        self.send_command(ClientMsg::GetMessages { channel_id, limit: 50 });
        
        self.pending_join = true;
    }
}

impl eframe::App for FigDihApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // 1. Procesar mensajes del servidor (igual que antes)
        if let Some(rx) = &mut self.ws_rx {
            while let Ok(text) = rx.try_recv() {
                if let Ok(server_msg) = serde_json::from_str::<ServerMsg>(&text) {
                    match server_msg {
                        ServerMsg::Welcome { user } => {
                            self.connected = true;
                            self.messages.push(ChatMessage { id: None, user: "🤖 Sistema".into(), channel_id: 0, text: format!("¡Bienvenido {}!", user), timestamp: 0 });
                        }
                        ServerMsg::Message(m) => { if self.selected_channel == Some(m.channel_id) { self.messages.push(m); } }
                        ServerMsg::UserJoined { user, channel_id } => {
                            if self.selected_channel == Some(channel_id) {
                                self.messages.push(ChatMessage { id: None, user: "👥 Sistema".into(), channel_id, text: format!("{} se unió", user), timestamp: 0 });
                            }
                        }
                        ServerMsg::UserLeft { user, channel_id } => {
                            if self.selected_channel == Some(channel_id) {
                                self.messages.push(ChatMessage { id: None, user: "👥 Sistema".into(), channel_id, text: format!("{} dejó el canal", user), timestamp: 0 });
                            }
                        }
                        ServerMsg::Servers(servers) => { self.servers = servers; }
                        ServerMsg::Channels(channels) => { self.channels = channels; }
                        ServerMsg::Messages(mut msgs) => { msgs.reverse(); self.messages = msgs; }
                        _ => {}
                    }
                }
            }
        }

        // 2. UI: Servidores
        let mut server_to_select = None;
        egui::SidePanel::left("servers").default_width(72.0).show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(10.0);
                ui.heading("💬");
                ui.separator();
                ui.add_space(10.0);
                for server in &self.servers {
                    let is_selected = self.selected_server == Some(server.id);
                    let button = egui::Button::new(&server.icon).min_size(egui::vec2(48.0, 48.0)).rounding(egui::Rounding::same(16.0));
                    if is_selected { ui.visuals_mut().widgets.inactive.bg_fill = egui::Color32::from_rgb(88, 101, 242); }
                    if ui.add(button).clicked() { server_to_select = Some(server.id); }
                    ui.add_space(8.0);
                }
            });
        });
        if let Some(id) = server_to_select { self.select_server(id); } // <-- MUTACIÓN FUERA DEL BUCLE

        // 3. UI: Canales
        let mut channel_to_select = None;
        egui::SidePanel::left("channels").default_width(240.0).show(ctx, |ui| {
            if let Some(server_id) = self.selected_server {
                let server_name = self.servers.iter().find(|s| s.id == server_id).map(|s| s.name.clone()).unwrap_or_default();
                ui.heading(&server_name);
                ui.separator();
                ui.add_space(10.0);
                ui.label(egui::RichText::new("CANALES DE TEXTO").small().color(egui::Color32::GRAY));
                ui.add_space(5.0);
                for channel in &self.channels {
                    let is_selected = self.selected_channel == Some(channel.id);
                    let button = egui::Button::new(format!("# {}", channel.name)).min_size(egui::vec2(ui.available_width(), 32.0)).wrap(false);
                    if is_selected { ui.visuals_mut().widgets.inactive.bg_fill = egui::Color32::from_rgb(79, 84, 92); }
                    if ui.add(button).clicked() { channel_to_select = Some(channel.id); }
                }
            }
        });
        if let Some(id) = channel_to_select { self.select_channel(id); } // <-- MUTACIÓN FUERA DEL BUCLE

        // 4. UI: Top, Input y Central (igual que antes, sin cambios de borrow)
        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if let Some(channel_id) = self.selected_channel {
                    let name = self.channels.iter().find(|c| c.id == channel_id).map(|c| c.name.clone()).unwrap_or_default();
                    ui.label(egui::RichText::new(format!("# {}", name)).strong());
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(if self.connected { "🟢" } else { "🔴" });
                    ui.add(egui::TextEdit::singleline(&mut self.username).desired_width(100.0));
                    ui.label("Tu nombre:");
                });
            });
        });

        egui::TopBottomPanel::bottom("input").show(ctx, |ui| {
            if self.selected_channel.is_some() {
                ui.horizontal(|ui| {
                    let text_edit = ui.add(egui::TextEdit::singleline(&mut self.input_text).hint_text("Escribe un mensaje...").desired_width(ui.available_width() - 100.0));
                    if text_edit.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) { self.send_message(); }
                    if ui.add(egui::Button::new("Enviar 🚀").min_size(egui::vec2(80.0, 40.0))).clicked() { self.send_message(); }
                });
            }
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            if self.selected_channel.is_some() {
                egui::ScrollArea::vertical().stick_to_bottom(true).auto_shrink([false; 2]).show(ui, |ui| {
                    ui.add_space(10.0);
                    for msg in &self.messages {
                        ui.horizontal(|ui| {
                            ui.label(egui::RichText::new(&msg.user).strong().color(egui::Color32::from_rgb(88, 101, 242)));
                            if msg.timestamp > 0 {
                                ui.label(egui::RichText::new(chrono_format(msg.timestamp)).small().color(egui::Color32::GRAY));
                            }
                        ui.label(&msg.text);
                    });
                    ui.add_space(5.0);
                }
                });
            } else {
                ui.centered_and_justified(|ui| ui.heading("👋 Selecciona un servidor y canal"));
            }
        });

        ctx.request_repaint_after(std::time::Duration::from_millis(100));
    }
}

fn chrono_format(timestamp: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap().as_secs();
    
    let diff = now - timestamp;
    if diff < 60 {
        "ahora".to_string()
    } else if diff < 3600 {
        format!("hace {}m", diff / 60)
    } else if diff < 86400 {
        format!("hace {}h", diff / 3600)
    } else {
        format!("hace {}d", diff / 86400)
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1200.0, 700.0])
            .with_min_inner_size([800.0, 500.0])
            .with_title("FigDih v2.0 — Chat Ultraligero"),
        ..Default::default()
    };
    eframe::run_native(
        "FigDih",
        options,
        Box::new(|_cc| Box::new(FigDihApp::new())),
    )
}