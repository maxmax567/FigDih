<div align="center">

![FigDih](client/assets/icon.png)

# 💬 FigDih

**Chat en tiempo real escrito en Rust.**

Cliente de escritorio ligero y servidor con persistencia SQLite.

![Rust](https://img.shields.io/badge/Rust-1.75%2B-dea584?logo=rust&logoColor=white)
![egui](https://img.shields.io/badge/GUI-egui-4b8bbe)
![SQLite](https://img.shields.io/badge/DB-SQLite-003b57?logo=sqlite&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-ready-2496ed?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)

</div>

---

## ✨ Características

- 🖥️ **Cliente multiplataforma** construido con `egui` (Windows, Linux, macOS)
- 🌐 **Servidor en Rust** con persistencia en **SQLite**
- 🎨 **Temas** intercambiables, incluyendo un **tema estilo Discord**
- 😀 **Emojis** integrados en el chat
- 🔔 **Notificaciones** de escritorio
- ⚙️ **Menú de configuración** completo
- 🔌 **Reconexión automática** ante caídas de red
- 🐳 **Despliegue con Docker**
- 🚀 **Releases automatizadas** con GitHub Actions

## 📁 Estructura del proyecto

```text
FigDih/
├── client/              # Cliente de escritorio (egui)
│   ├── assets/          # Iconos e imágenes (icon.svg / icon.png)
│   └── src/
├── server/              # Servidor Rust + SQLite
│   └── src/
├── docker/              # Dockerfile del servidor
└── .github/workflows/   # CI y releases
