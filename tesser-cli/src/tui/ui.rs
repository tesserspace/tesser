use chrono::{DateTime, Utc};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Wrap},
    Frame,
};
use rust_decimal::Decimal;
use tesser_rpc::conversions::{from_decimal_proto, from_timestamp_proto};
use tesser_rpc::proto;

use super::app::{CommandOverlay, LogCategory, MonitorApp};

pub fn draw(f: &mut Frame<'_>, app: &MonitorApp) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Min(10),
            Constraint::Length(8),
        ])
        .split(f.size());

    render_header(f, layout[0], app);

    let main_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(layout[1]);

    let venue_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(main_chunks[0]);
    let summary_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(venue_chunks[0]);

    render_sub_accounts(f, summary_chunks[0], app);
    render_balances(f, summary_chunks[1], app);
    render_positions(f, venue_chunks[1], app);
    render_orders(f, main_chunks[1], app);

    let footer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(5)])
        .split(layout[2]);

    render_log(f, footer[0], app);
    render_help(f, footer[1]);
    render_overlay(f, f.size(), app);
}

fn render_header(f: &mut Frame<'_>, area: Rect, app: &MonitorApp) {
    let currency = app.reporting_currency().unwrap_or("");
    let equity = format_money(app.equity(), currency);
    let realized = format_money(app.realized_pnl(), currency);
    let initial = format_money(app.initial_equity(), currency);
    let status = app.status();
    let active_algos = status.map(|s| s.active_algorithms).unwrap_or_default();
    let liquidate_only = status.map(|s| s.liquidate_only).unwrap_or(false);
    let data_timestamp = status
        .and_then(|s| s.last_data_timestamp.as_ref())
        .map(|ts| from_timestamp_proto(ts.clone()));

    let mut lines = Vec::new();
    let active_algos_text = active_algos.to_string();
    let snapshot_text = relative_time(app.last_snapshot_at());
    let events_text = relative_time(app.last_event_at());
    let last_data_text = relative_time(data_timestamp);

    lines.push(Line::from(vec![
        label("Equity"),
        value(&equity),
        Span::raw("  "),
        label("Realized PnL"),
        value(&realized),
        Span::raw("  "),
        label("Initial"),
        value(&initial),
        Span::raw("  "),
        label("Active Algos"),
        value(&active_algos_text),
        Span::raw("  "),
        label("Mode"),
        if liquidate_only {
            Span::styled(
                "LIQUIDATE",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )
        } else {
            Span::styled("NORMAL", Style::default().fg(Color::Green))
        },
    ]));

    lines.push(Line::from(vec![
        label("Control"),
        value(app.control_addr()),
        Span::raw("  "),
        label("Snapshot"),
        value(&snapshot_text),
        Span::raw("  "),
        label("Events"),
        value(&events_text),
        Span::raw("  "),
        label("Stream"),
        if app.stream_connected() {
            Span::styled("ONLINE", Style::default().fg(Color::Green))
        } else {
            Span::styled("OFFLINE", Style::default().fg(Color::Yellow))
        },
        Span::raw("  "),
        label("Cancel"),
        if app.cancel_in_progress() {
            Span::styled("RUNNING", Style::default().fg(Color::Yellow))
        } else {
            value("idle")
        },
        Span::raw("  "),
        label("Last Data"),
        value(&last_data_text),
    ]));

    if let Some(err) = app.last_error() {
        lines.push(Line::from(vec![
            Span::styled(
                "Warning: ",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ),
            Span::styled(err, Style::default().fg(Color::Red)),
        ]));
    }

    let header = Paragraph::new(lines)
        .block(
            Block::default()
                .title("Commander Status")
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: true });
    f.render_widget(header, area);
}

fn render_positions(f: &mut Frame<'_>, area: Rect, app: &MonitorApp) {
    let Some(positions) = app.positions() else {
        let block = Paragraph::new("No positions")
            .block(Block::default().title("Positions").borders(Borders::ALL))
            .wrap(Wrap { trim: true });
        f.render_widget(block, area);
        return;
    };

    if positions.is_empty() {
        let block = Paragraph::new("No positions")
            .block(Block::default().title("Positions").borders(Borders::ALL))
            .wrap(Wrap { trim: true });
        f.render_widget(block, area);
        return;
    }

    let rows = positions.iter().map(|pos| {
        let qty = decimal_from_proto(pos.quantity.as_ref());
        let entry = decimal_from_proto(pos.entry_price.as_ref());
        let unreal = decimal_from_proto(pos.unrealized_pnl.as_ref());
        let style = match unreal {
            Some(v) if v > Decimal::ZERO => Style::default().fg(Color::Green),
            Some(v) if v < Decimal::ZERO => Style::default().fg(Color::Red),
            _ => Style::default(),
        };
        Row::new(vec![
            Cell::from(pos.symbol.clone()),
            Cell::from(side_label(pos.side)),
            Cell::from(format_decimal(qty)),
            Cell::from(format_decimal(entry)),
            Cell::from(format_decimal(unreal)),
        ])
        .style(style)
    });

    let widths = [
        Constraint::Length(12),
        Constraint::Length(6),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(12),
    ];
    let table = Table::new(rows, widths)
        .header(
            Row::new(vec!["Symbol", "Side", "Qty", "Entry Px", "Unrealized"])
                .style(Style::default().fg(Color::Gray)),
        )
        .block(Block::default().title("Positions").borders(Borders::ALL))
        .column_spacing(1);
    f.render_widget(table, area);
}

fn render_sub_accounts(f: &mut Frame<'_>, area: Rect, app: &MonitorApp) {
    let Some(accounts) = app.sub_accounts() else {
        let block = Paragraph::new("No venue data").block(
            Block::default()
                .title("Per-Exchange Equity")
                .borders(Borders::ALL),
        );
        f.render_widget(block, area);
        return;
    };

    if accounts.is_empty() {
        let block = Paragraph::new("No venue data").block(
            Block::default()
                .title("Per-Exchange Equity")
                .borders(Borders::ALL),
        );
        f.render_widget(block, area);
        return;
    }

    let rows = accounts.iter().map(|acct| {
        let equity = decimal_from_proto(acct.equity.as_ref());
        Row::new(vec![
            Cell::from(acct.exchange.clone()),
            Cell::from(format_decimal(equity)),
            Cell::from(acct.balances.len().to_string()),
            Cell::from(acct.positions.len().to_string()),
        ])
    });

    let widths = [
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(10),
        Constraint::Length(12),
    ];
    let table = Table::new(rows, widths)
        .header(
            Row::new(vec!["Exchange", "Equity", "Balances", "Positions"])
                .style(Style::default().fg(Color::Gray)),
        )
        .block(
            Block::default()
                .title("Per-Exchange Equity")
                .borders(Borders::ALL),
        )
        .column_spacing(1);
    f.render_widget(table, area);
}

fn render_balances(f: &mut Frame<'_>, area: Rect, app: &MonitorApp) {
    let Some(balances) = app.balances() else {
        let block = Paragraph::new("No balances")
            .block(Block::default().title("Balances").borders(Borders::ALL))
            .wrap(Wrap { trim: true });
        f.render_widget(block, area);
        return;
    };

    if balances.is_empty() {
        let block = Paragraph::new("No balances")
            .block(Block::default().title("Balances").borders(Borders::ALL))
            .wrap(Wrap { trim: true });
        f.render_widget(block, area);
        return;
    }

    let rows = balances.iter().map(|cash| {
        let qty = decimal_from_proto(cash.quantity.as_ref());
        let rate = decimal_from_proto(cash.conversion_rate.as_ref());
        Row::new(vec![
            Cell::from(cash.currency.clone()),
            Cell::from(format_decimal(qty)),
            Cell::from(format_decimal(rate)),
        ])
    });
    let widths = [
        Constraint::Length(10),
        Constraint::Length(16),
        Constraint::Length(16),
    ];
    let table = Table::new(rows, widths)
        .header(
            Row::new(vec!["Currency", "Quantity", "Conv Rate"])
                .style(Style::default().fg(Color::Gray)),
        )
        .block(Block::default().title("Balances").borders(Borders::ALL))
        .column_spacing(1);
    f.render_widget(table, area);
}

fn render_orders(f: &mut Frame<'_>, area: Rect, app: &MonitorApp) {
    let orders = app.orders();
    if orders.is_empty() {
        let block = Paragraph::new("No open orders")
            .block(
                Block::default()
                    .title("Active Orders")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: true });
        f.render_widget(block, area);
        return;
    }

    let rows = orders.iter().map(|order| {
        let qty = decimal_from_proto(order.quantity.as_ref());
        let filled = decimal_from_proto(order.filled_quantity.as_ref());
        let avg = decimal_from_proto(order.avg_fill_price.as_ref());
        Row::new(vec![
            Cell::from(order.id.clone()),
            Cell::from(order.symbol.clone()),
            Cell::from(side_label(order.side)),
            Cell::from(format_decimal(qty)),
            Cell::from(format_decimal(filled)),
            Cell::from(format_decimal(avg)),
            Cell::from(order_status(order.status).to_string()),
        ])
    });

    let widths = [
        Constraint::Length(12),
        Constraint::Length(10),
        Constraint::Length(6),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Min(10),
    ];
    let table = Table::new(rows, widths)
        .header(
            Row::new(vec![
                "ID", "Symbol", "Side", "Qty", "Filled", "Avg Px", "Status",
            ])
            .style(Style::default().fg(Color::Gray)),
        )
        .block(
            Block::default()
                .title("Active Orders")
                .borders(Borders::ALL),
        )
        .column_spacing(1);
    f.render_widget(table, area);
}

fn render_log(f: &mut Frame<'_>, area: Rect, app: &MonitorApp) {
    let capacity = area.height.saturating_sub(2).max(1) as usize;
    let mut rows: Vec<_> = app.log().rev().take(capacity.max(1)).cloned().collect();
    rows.reverse();

    let items: Vec<ListItem> = if rows.is_empty() {
        vec![ListItem::new(Line::from(Span::styled(
            "Waiting for events...",
            Style::default().fg(Color::DarkGray),
        )))]
    } else {
        rows.into_iter()
            .map(|entry| {
                let content = vec![
                    Span::styled(
                        format!("[{}] ", entry.timestamp_short()),
                        Style::default().fg(Color::Gray),
                    ),
                    Span::styled(entry.message.clone(), color_for(entry.category)),
                ];
                ListItem::new(Line::from(content))
            })
            .collect()
    };

    let list = List::new(items)
        .block(Block::default().title("Live Log").borders(Borders::ALL))
        .highlight_symbol("")
        .highlight_style(Style::default());
    f.render_widget(list, area);
}

fn render_help(f: &mut Frame<'_>, area: Rect) {
    let lines = vec![
        Line::from(vec![
            key_hint("q"),
            Span::raw(" Quit   "),
            key_hint("Ctrl+C"),
            Span::raw(" Immediate quit   "),
            key_hint("m"),
            Span::raw(" Toggle command palette"),
        ]),
        Line::from(vec![
            Span::styled("In palette: ", Style::default().fg(Color::Gray)),
            key_hint("c"),
            Span::raw(" start Cancel-All   "),
            key_hint("Esc"),
            Span::raw(" close palette"),
        ]),
        Line::from(vec![
            Span::styled("Confirm Cancel-All: ", Style::default().fg(Color::Gray)),
            Span::raw("type "),
            Span::styled("cancel all", Style::default().fg(Color::Yellow)),
            Span::raw(" then press "),
            key_hint("Enter"),
        ]),
    ];
    let help = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Help & Shortcuts"),
        )
        .wrap(Wrap { trim: true });
    f.render_widget(help, area);
}

fn render_overlay(f: &mut Frame<'_>, area: Rect, app: &MonitorApp) {
    match app.overlay() {
        CommandOverlay::Hidden => {}
        CommandOverlay::Palette => {
            let chunk = centered_rect(60, 30, area);
            let block = Block::default()
                .title("Command Palette")
                .borders(Borders::ALL)
                .style(Style::default().bg(Color::Black));
            let lines = vec![
                Line::from("Press 'c' to initiate Cancel All."),
                Line::from("Press Esc (or 'm') to close this panel."),
            ];
            let paragraph = Paragraph::new(lines)
                .alignment(Alignment::Left)
                .wrap(Wrap { trim: true })
                .block(block);
            f.render_widget(paragraph, chunk);
        }
        CommandOverlay::Confirm { .. } => {
            let chunk = centered_rect(70, 35, area);
            let mut lines = vec![
                Line::from("Type 'cancel all' and press Enter to confirm."),
                Line::from("This action will cancel every open order and running algo."),
                Line::from(""),
            ];
            let input = app.confirmation_buffer().unwrap_or_default();
            lines.push(Line::from(vec![
                Span::styled("> ", Style::default().fg(Color::Gray)),
                Span::styled(input.to_string(), Style::default().fg(Color::White)),
            ]));
            if let Some(err) = app.overlay_error() {
                lines.push(Line::from(Span::styled(
                    err,
                    Style::default().fg(Color::Red),
                )));
            }
            let block = Block::default()
                .title("Confirm Cancel All")
                .borders(Borders::ALL)
                .style(Style::default().bg(Color::Black));
            let paragraph = Paragraph::new(lines)
                .alignment(Alignment::Left)
                .wrap(Wrap { trim: true })
                .block(block);
            f.render_widget(paragraph, chunk);
        }
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    let vertical = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1]);
    vertical[1]
}

fn label(text: &str) -> Span<'_> {
    Span::styled(format!("{text}: "), Style::default().fg(Color::DarkGray))
}

fn value(text: &str) -> Span<'_> {
    Span::styled(text.to_string(), Style::default().fg(Color::White))
}

fn format_money(value: Option<Decimal>, currency: &str) -> String {
    match value {
        Some(amount) => {
            if currency.is_empty() {
                amount.normalize().to_string()
            } else {
                format!("{} {}", amount.normalize(), currency)
            }
        }
        None => "--".to_string(),
    }
}

fn relative_time(ts: Option<DateTime<Utc>>) -> String {
    match ts {
        Some(value) => {
            let delta = Utc::now() - value;
            if delta.num_seconds() <= 0 {
                "just now".to_string()
            } else if delta.num_seconds() < 60 {
                format!("{}s ago", delta.num_seconds())
            } else if delta.num_minutes() < 60 {
                format!("{}m ago", delta.num_minutes())
            } else {
                format!("{}h ago", delta.num_hours())
            }
        }
        None => "--".to_string(),
    }
}

fn decimal_from_proto(proto: Option<&proto::Decimal>) -> Option<Decimal> {
    proto.map(|inner| from_decimal_proto(inner.clone()))
}

fn format_decimal(value: Option<Decimal>) -> String {
    value
        .map(|d| d.normalize().to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn side_label(value: i32) -> &'static str {
    match proto::Side::try_from(value).unwrap_or(proto::Side::Unspecified) {
        proto::Side::Buy => "BUY",
        proto::Side::Sell => "SELL",
        proto::Side::Unspecified => "NA",
    }
}

fn order_status(value: i32) -> &'static str {
    match proto::OrderStatus::try_from(value).unwrap_or(proto::OrderStatus::Unspecified) {
        proto::OrderStatus::PendingNew => "PENDING",
        proto::OrderStatus::Accepted => "ACCEPTED",
        proto::OrderStatus::PartiallyFilled => "PARTIAL",
        proto::OrderStatus::Filled => "FILLED",
        proto::OrderStatus::Canceled => "CANCELLED",
        proto::OrderStatus::Rejected => "REJECTED",
        proto::OrderStatus::Unspecified => "UNKNOWN",
    }
}

fn color_for(category: LogCategory) -> Style {
    match category {
        LogCategory::Signal => Style::default().fg(Color::Yellow),
        LogCategory::Fill => Style::default().fg(Color::Green),
        LogCategory::Order => Style::default().fg(Color::Blue),
        LogCategory::Info => Style::default().fg(Color::Gray),
        LogCategory::Error => Style::default().fg(Color::Red),
    }
}

fn key_hint(label: &str) -> Span<'_> {
    Span::styled(
        format!("[{label}]"),
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
}
