//! EarthGrid Tray App — system tray icon with health polling.
//! 🌍 Online / 🌑 Offline

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tray_icon::menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
use tray_icon::{Icon, TrayIconBuilder};

use std::path::PathBuf;

const API_BASE: &str = "http://localhost:8400";
const POLL_SECS: u64 = 5;

// Embed icons at compile time
const ICON_ONLINE: &[u8] = include_bytes!("../../assets/tray-online-32.png");
const ICON_OFFLINE: &[u8] = include_bytes!("../../assets/tray-offline-32.png");

#[derive(Clone, PartialEq)]
enum State {
    Online(String),
    Offline,
}

fn icon_dir() -> PathBuf {
    let dir = std::env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
            PathBuf::from(home).join(".cache")
        })
        .join("earthgrid-tray");
    std::fs::create_dir_all(&dir).ok();
    dir
}

fn write_icon(name: &str, data: &[u8]) -> PathBuf {
    let path = icon_dir().join(name);
    std::fs::write(&path, data).expect("Failed to write icon");
    path
}

fn load_icon(png: &[u8]) -> Icon {
    let img = image::load_from_memory(png).expect("bad icon PNG").into_rgba8();
    let (w, h) = img.dimensions();
    Icon::from_rgba(img.into_raw(), w, h).expect("icon create failed")
}

fn make_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(3)))
        .build()
        .into()
}

fn poll_node() -> State {
    let agent = make_agent();
    let health = agent.get(&format!("{}/health", API_BASE)).call();
    match health {
        Ok(r) if r.status().as_u16() < 400 => {
            // Try to get stats
            let stats = agent.get(&format!("{}/status", API_BASE)).call().ok().and_then(|mut r| {
                let json: serde_json::Value = r.body_mut().read_json().ok()?;
                let bytes = json["storage"]["used_bytes"].as_f64().unwrap_or(0.0);
                let peers = json["peers"]["connected"].as_u64().unwrap_or(0);
                Some(format!("{:.1} TB | {} peers", bytes / 1e12, peers))
            });
            State::Online(stats.unwrap_or_else(|| "connected".into()))
        }
        _ => State::Offline,
    }
}

fn main() {
    // Suppress libayatana deprecation warning
    std::env::set_var("G_MESSAGES_DEBUG", "none");

    // GTK must be initialized before creating tray icons on Linux
    #[cfg(target_os = "linux")]
    gtk::init().expect("Failed to init GTK");

    // Write icon files for GNOME AppIndicator (uses file paths, not RGBA)
    let online_path = write_icon("earthgrid-online.png", ICON_ONLINE);
    let offline_path = write_icon("earthgrid-offline.png", ICON_OFFLINE);
    eprintln!("Icons written to: {}", icon_dir().display());

    let icon_online = load_icon(ICON_ONLINE);
    let icon_offline = load_icon(ICON_OFFLINE);

    // Menu
    let title = MenuItem::new("EarthGrid v0.1.0", false, None);
    let status_item = MenuItem::new("Status: checking...", false, None);
    let dashboard = MenuItem::new("Open Dashboard", true, None);
    let quit = MenuItem::new("Quit", true, None);

    let menu = Menu::new();
    let _ = menu.append(&title);
    let _ = menu.append(&PredefinedMenuItem::separator());
    let _ = menu.append(&status_item);
    let _ = menu.append(&PredefinedMenuItem::separator());
    let _ = menu.append(&dashboard);
    let _ = menu.append(&PredefinedMenuItem::separator());
    let _ = menu.append(&quit);

    let dashboard_id = dashboard.id().clone();
    let quit_id = quit.id().clone();

    // Tray (starts offline)
    let tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_icon(icon_offline.clone())
        .with_tooltip("EarthGrid — Offline")
        .with_temp_dir_path(icon_dir())
        .build()
        .expect("Failed to create tray icon");

    // Shared state: background thread writes, main thread reads
    let shared_state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::Offline));
    let bg_state = Arc::clone(&shared_state);

    // Background polling thread (no UI objects — only updates shared state)
    thread::spawn(move || loop {
        let new_state = poll_node();
        *bg_state.lock().unwrap() = new_state;
        thread::sleep(Duration::from_secs(POLL_SECS));
    });

    let menu_rx = MenuEvent::receiver();
    let mut last_state = State::Offline;

    // Use GTK main loop with periodic check (GNOME needs this!)
    #[cfg(target_os = "linux")]
    {
        let shared_for_gtk = Arc::clone(&shared_state);
        gtk::glib::timeout_add_local(Duration::from_millis(200), move || {
            // Check menu events
            if let Ok(event) = menu_rx.try_recv() {
                if event.id == quit_id {
                    std::process::exit(0);
                } else if event.id == dashboard_id {
                    let _ = open::that(&format!("{}/ui", API_BASE));
                }
            }

            // Update UI from shared state
            let current = shared_for_gtk.lock().unwrap().clone();
            if current != last_state {
                match &current {
                    State::Online(info) => {
                        let _ = tray.set_icon(Some(icon_online.clone()));
                        let _ = tray.set_tooltip(Some("EarthGrid — Online"));
                        let _ = status_item.set_text(&format!("Status: Online | {}", info));
                    }
                    State::Offline => {
                        let _ = tray.set_icon(Some(icon_offline.clone()));
                        let _ = tray.set_tooltip(Some("EarthGrid — Offline"));
                        let _ = status_item.set_text("Status: Offline");
                    }
                }
                last_state = current;
            }

            gtk::glib::ControlFlow::Continue
        });

        gtk::main();
    }

    // Non-Linux fallback: simple loop
    #[cfg(not(target_os = "linux"))]
    loop {
        if let Ok(event) = menu_rx.try_recv() {
            if event.id == quit_id {
                std::process::exit(0);
            } else if event.id == dashboard_id {
                let _ = open::that(&format!("{}/ui", API_BASE));
            }
        }

        let current = shared_state.lock().unwrap().clone();
        if current != last_state {
            match &current {
                State::Online(info) => {
                    let _ = tray.set_icon(Some(icon_online.clone()));
                    let _ = tray.set_tooltip(Some("EarthGrid — Online"));
                    let _ = status_item.set_text(&format!("Status: Online | {}", info));
                }
                State::Offline => {
                    let _ = tray.set_icon(Some(icon_offline.clone()));
                    let _ = tray.set_tooltip(Some("EarthGrid — Offline"));
                    let _ = status_item.set_text("Status: Offline");
                }
            }
            last_state = current;
        }

        thread::sleep(Duration::from_millis(100));
    }
}
