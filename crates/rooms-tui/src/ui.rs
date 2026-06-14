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

use crate::cli::AttachConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiModel {
    attach_config: AttachConfig,
    attach_url: String,
}

impl UiModel {
    pub fn new(attach_config: AttachConfig, attach_url: String) -> Self {
        Self {
            attach_config,
            attach_url,
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
}

pub fn run_tui(model: UiModel) -> io::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal, &model);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    model: &UiModel,
) -> io::Result<()> {
    loop {
        terminal.draw(|frame| render_scaffold(frame, model))?;
        if event::poll(Duration::from_millis(250))? {
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

pub fn render_scaffold(frame: &mut Frame<'_>, model: &UiModel) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(3),
        ])
        .split(frame.area());

    frame.render_widget(
        Paragraph::new(format!("{}  peer {}", model.title(), model.peer_label()))
            .bold()
            .block(Block::default().borders(Borders::BOTTOM)),
        root[0],
    );

    frame.render_widget(
        Paragraph::new(format!(
            "Rust Rooms client scaffold is live.\n\nattach: {}\n\nNext: websocket bootstrap → session/attach replay → transcript reducer.\n\nPress q or Esc to quit.",
            model.attach_url()
        ))
        .wrap(Wrap { trim: false })
        .block(Block::default().title("status").borders(Borders::ALL)),
        root[1],
    );

    frame.render_widget(
        Paragraph::new("composer placeholder: protocol builders are ready; transport lands next")
            .block(Block::default().borders(Borders::TOP)),
        root[2],
    );
}
