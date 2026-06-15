use std::io;
use std::time::Duration;

use crossterm::event::{self, Event as CrosstermEvent, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::prelude::{Frame, Stylize};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use rooms_client::{
    AttachConfig, ClientCommand, ConnectionStatus, InboundMessage, RoomState, Transport, connect,
};
use serde_json::Value;
use tokio::runtime::Builder;
use tokio::sync::mpsc::error::TryRecvError;

const EVENT_LOG_LIMIT: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiModel {
    attach_config: AttachConfig,
    attach_url: String,
    state: RoomState,
    event_log: Vec<String>,
}

impl UiModel {
    pub fn new(attach_config: AttachConfig, attach_url: String) -> Self {
        Self {
            attach_config,
            attach_url,
            state: RoomState::default(),
            event_log: vec!["boot: rooms-tui ready".to_string()],
        }
    }

    pub fn title(&self) -> String {
        format!("rooms-tui · {}", self.attach_config.room)
    }

    pub fn peer_label(&self) -> String {
        match self.attach_config.peer_name.as_deref() {
            Some(name) if !name.trim().is_empty() => {
                format!("{} ({})", self.attach_config.peer_id, name.trim())
            }
            _ => self.attach_config.peer_id.clone(),
        }
    }

    pub fn attach_url(&self) -> &str {
        &self.attach_url
    }

    pub fn state(&self) -> &RoomState {
        &self.state
    }

    pub fn event_log(&self) -> &[String] {
        &self.event_log
    }

    pub fn set_connection_status(&mut self, status: ConnectionStatus) {
        self.state.set_connection_status(status);
        self.push_event(format!("status: {status:?}"));
    }

    pub fn apply_inbound(&mut self, message: InboundMessage) -> Result<(), String> {
        let summary = inbound_summary(&message);
        match self.state.apply_inbound(&message) {
            Ok(()) => {
                self.push_event(summary);
                Ok(())
            }
            Err(err) => {
                self.state.errors.push(err.clone());
                self.state.set_connection_status(ConnectionStatus::Error);
                self.push_event(format!("state error: {err}"));
                Err(err)
            }
        }
    }

    pub fn snapshot_text(&self) -> String {
        let mut lines = vec![
            format!("status: {:?}", self.state.connection_status),
            format!("attach: {}", redacted_attach_url(&self.attach_url)),
            format!(
                "session: {}",
                self.state.session_id.as_deref().unwrap_or("<none>")
            ),
            format!(
                "room: {}",
                self.state
                    .room_id
                    .as_deref()
                    .unwrap_or(self.attach_config.room.as_str())
            ),
            format!("peers: {}", peers_text(&self.state)),
            format!("transcript: {} items", self.state.transcript.len()),
            format!("active: {}", active_turn_text(&self.state)),
            format!("queue: {}", queue_text(&self.state)),
            format!("permissions: {}", permissions_text(&self.state)),
        ];

        if let Some(replay) = &self.state.replay {
            lines.push(format!(
                "replay: {} {} frames gen {}",
                replay.phase, replay.frame_count, replay.generation
            ));
        }
        if !self.state.errors.is_empty() {
            lines.push(format!("errors: {}", self.state.errors.join(" | ")));
        }
        if !self.state.debug_frames.is_empty() {
            lines.push(format!("debug frames: {}", self.state.debug_frames.len()));
        }

        lines.join("\n")
    }

    pub fn recent_events_text(&self) -> String {
        if self.event_log.is_empty() {
            return "no events yet".to_string();
        }

        self.event_log
            .iter()
            .rev()
            .take(14)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn push_event(&mut self, event: impl Into<String>) {
        self.event_log.push(event.into());
        let overflow = self.event_log.len().saturating_sub(EVENT_LOG_LIMIT);
        if overflow > 0 {
            self.event_log.drain(0..overflow);
        }
    }
}

pub fn run_tui(mut model: UiModel) -> io::Result<()> {
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(io::Error::other)?;

    model.set_connection_status(ConnectionStatus::Connecting);
    let mut transport = match runtime.block_on(connect(model.attach_config.clone())) {
        Ok(transport) => {
            model.push_event("transport: websocket connected");
            Some(transport)
        }
        Err(err) => {
            let _ = model.apply_inbound(InboundMessage::Error(err.to_string()));
            None
        }
    };

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal, &mut model, &mut transport);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    if let Some(transport) = transport {
        let Transport {
            inbound,
            outbound,
            task,
        } = transport;
        drop(inbound);
        let _ = outbound.blocking_send(ClientCommand::Shutdown);
        let _ = runtime.block_on(task);
    }

    result
}

fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    model: &mut UiModel,
    transport: &mut Option<Transport>,
) -> io::Result<()> {
    loop {
        drain_transport(model, transport);
        terminal.draw(|frame| render_scaffold(frame, model))?;
        if event::poll(Duration::from_millis(100))? {
            match event::read()? {
                CrosstermEvent::Key(key)
                    if matches!(key.code, KeyCode::Char('q') | KeyCode::Esc) =>
                {
                    break;
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn drain_transport(model: &mut UiModel, transport: &mut Option<Transport>) {
    let Some(transport) = transport else {
        return;
    };

    loop {
        match transport.inbound.try_recv() {
            Ok(message) => {
                let _ = model.apply_inbound(message);
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                if !matches!(
                    model.state.connection_status,
                    ConnectionStatus::Closed | ConnectionStatus::Error
                ) {
                    let _ = model.apply_inbound(InboundMessage::Closed);
                }
                break;
            }
        }
    }
}

pub fn render_scaffold(frame: &mut Frame<'_>, model: &UiModel) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(7),
            Constraint::Length(3),
        ])
        .split(frame.area());

    frame.render_widget(
        Paragraph::new(format!(
            "{}  peer {}  status {:?}",
            model.title(),
            model.peer_label(),
            model.state.connection_status
        ))
        .bold()
        .block(Block::default().borders(Borders::BOTTOM)),
        root[0],
    );

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(root[1]);

    frame.render_widget(
        Paragraph::new(model.snapshot_text())
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title("room snapshot")
                    .borders(Borders::ALL),
            ),
        body[0],
    );

    frame.render_widget(
        Paragraph::new(model.recent_events_text())
            .wrap(Wrap { trim: false })
            .block(Block::default().title("events").borders(Borders::ALL)),
        body[1],
    );

    frame.render_widget(
        Paragraph::new("q/Esc quit · live rooms-client transport · controls land next")
            .block(Block::default().borders(Borders::TOP)),
        root[2],
    );
}

fn redacted_attach_url(url: &str) -> String {
    match url.split_once('?') {
        Some((base, _query)) => format!("{base}?<redacted-query>"),
        None => url.to_string(),
    }
}

fn inbound_summary(message: &InboundMessage) -> String {
    match message {
        InboundMessage::Frame { raw, event } => {
            if let Some(method) = raw.get("method").and_then(Value::as_str) {
                format!("frame: {method}")
            } else if let Some(id) = raw.get("id") {
                if raw.get("error").is_some() {
                    format!("response error: {id}")
                } else {
                    format!("response: {id}")
                }
            } else if let Some(event) = event {
                format!("event: {event:?}")
            } else {
                "frame: <unknown>".to_string()
            }
        }
        InboundMessage::Error(error) => format!("transport error: {error}"),
        InboundMessage::Closed => "transport: closed".to_string(),
    }
}

fn peers_text(state: &RoomState) -> String {
    if state.peers.is_empty() {
        return "none".to_string();
    }

    state
        .peers
        .iter()
        .map(|peer| match peer.peer_name.as_deref() {
            Some(name) if !name.trim().is_empty() => format!("{} ({})", peer.peer_id, name),
            _ => peer.peer_id.clone(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn active_turn_text(state: &RoomState) -> String {
    let Some(turn) = &state.active_turn else {
        return "idle".to_string();
    };

    let mut label = turn.peer_id.clone();
    if let Some(name) = turn
        .peer_name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
    {
        label.push_str(&format!(" ({name})"));
    }
    if !turn.text.is_empty() {
        label.push_str(&format!(" · {}", turn.text));
    }
    if turn.cancelled {
        label.push_str(" · cancelled");
    }
    label
}

fn queue_text(state: &RoomState) -> String {
    if state.queue.is_empty() {
        return "empty".to_string();
    }

    state
        .queue
        .iter()
        .map(|item| format!("{}:{:?}", item.queue_item_id, item.status))
        .collect::<Vec<_>>()
        .join(", ")
}

fn permissions_text(state: &RoomState) -> String {
    if state.pending_permissions.is_empty() {
        return "none".to_string();
    }

    state
        .pending_permissions
        .iter()
        .map(|permission| {
            let marker = if permission.actionable {
                "actionable"
            } else {
                "replayed"
            };
            format!("{} ({marker})", permission.request_id)
        })
        .collect::<Vec<_>>()
        .join(", ")
}
