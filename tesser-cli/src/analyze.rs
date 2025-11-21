use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tesser_data::analytics::{
    analyze_execution, ExecutionAnalysisRequest, ExecutionReport, ExecutionStats,
};

/// Runs the execution analysis workflow and renders a textual report.
pub fn run_execution(request: ExecutionAnalysisRequest) -> Result<()> {
    let report = analyze_execution(&request)?;
    render_report(&report);
    Ok(())
}

fn render_report(report: &ExecutionReport) {
    println!("=== Execution Analysis ===");
    println!(
        "Period        : {} -> {}",
        format_period(report.period_start),
        format_period(report.period_end)
    );
    println!(
        "Orders        : {} analyzed ({} skipped)",
        report.totals.order_count, report.skipped_orders
    );
    println!("Fills         : {}", report.totals.fill_count);
    println!(
        "Arrival Cover : {} ({})",
        report.totals.orders_with_arrival,
        arrival_percent(&report.totals)
    );
    if report.totals.order_count == 0 {
        println!("No executions found for the requested window.");
        return;
    }

    println!(
        "Volume        : {}",
        format_decimal(&report.totals.filled_quantity, 4)
    );
    println!(
        "Notional      : {}",
        format_decimal(&report.totals.notional, 2)
    );
    println!(
        "Fees          : {}",
        format_decimal(&report.totals.total_fees, 6)
    );
    println!(
        "Shortfall     : {}",
        format_decimal(&report.totals.implementation_shortfall, 6)
    );
    println!(
        "Avg Slippage  : {} bps",
        format_optional(report.totals.avg_slippage_bps, 2)
    );

    if report.per_algo.is_empty() {
        return;
    }

    println!("\nAlgo breakdown:");
    println!(
        "{:<12} {:>8} {:>12} {:>14} {:>12} {:>14} {:>14}",
        "Algo", "Orders", "Quantity", "Notional", "Fees", "Shortfall", "Slippage(bps)"
    );
    for stats in &report.per_algo {
        println!(
            "{:<12} {:>8} {:>12} {:>14} {:>12} {:>14} {:>14}",
            stats.label,
            stats.order_count,
            format_decimal(&stats.filled_quantity, 4),
            format_decimal(&stats.notional, 2),
            format_decimal(&stats.total_fees, 6),
            format_decimal(&stats.implementation_shortfall, 6),
            format_optional(stats.avg_slippage_bps, 2)
        );
    }
}

fn format_period(ts: Option<DateTime<Utc>>) -> String {
    ts.map(|value| value.to_rfc3339())
        .unwrap_or_else(|| "-".to_string())
}

fn format_decimal(value: &Decimal, scale: u32) -> String {
    if value.is_zero() {
        return "0".to_string();
    }
    value.round_dp(scale).normalize().to_string()
}

fn format_optional(value: Option<Decimal>, scale: u32) -> String {
    value
        .map(|v| format_decimal(&v, scale))
        .unwrap_or_else(|| "-".to_string())
}

fn arrival_percent(stats: &ExecutionStats) -> String {
    if stats.order_count == 0 {
        return "-".to_string();
    }
    let pct = stats.orders_with_arrival as f64 / stats.order_count as f64 * 100.0;
    format!("{pct:.0}%")
}
