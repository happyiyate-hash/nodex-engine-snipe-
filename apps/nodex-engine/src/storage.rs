use serde::Serialize;
use sqlx::{Row, SqlitePool};
use std::{path::PathBuf, process::Stdio};
use tokio::process::Command;

#[derive(Debug, Serialize)]
pub struct DiskUsage {
    pub filesystem: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub used_percent: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DataCategory {
    pub logical_bytes: Option<u64>,
    pub records: Option<i64>,
    pub physical_bytes: Option<u64>,
    pub note: String,
}

#[derive(Debug, Serialize)]
pub struct StorageOverview {
    pub disk: Option<DiskUsage>,
    pub database: DatabaseUsage,
    pub categories: Categories,
    pub system_journal: JournalUsage,
    pub updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct DatabaseUsage {
    pub path: Option<String>,
    pub database_bytes: u64,
    pub wal_bytes: u64,
    pub shm_bytes: u64,
    pub total_file_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct JournalUsage {
    pub available: bool,
    pub reported_size_bytes: Option<u64>,
    pub raw_report: Option<String>,
    pub note: String,
}

#[derive(Debug, Serialize)]
pub struct Categories {
    pub tokens: DataCategory,
    pub launches: DataCategory,
    pub events: DataCategory,
    pub discovery: DataCategory,
    pub chart_data: DataCategory,
    pub paper_trading: DataCategory,
    pub engine_logs: DataCategory,
    pub system_logs: DataCategory,
    pub database: DataCategory,
    pub other: DataCategory,
}

async fn table_usage(pool: &SqlitePool, table: &str, bytes_sql: &str) -> DataCategory {
    // SQL fragments are compile-time constants at every call site; no user input is interpolated.
    let bytes = sqlx::query_scalar::<_, i64>(bytes_sql).fetch_one(pool).await.ok();
    let count_sql = format!("SELECT COUNT(*) FROM {table}");
    let records = sqlx::query_scalar::<_, i64>(&count_sql).fetch_one(pool).await.ok();
    DataCategory {
        logical_bytes: bytes.and_then(|value| u64::try_from(value).ok()),
        records,
        physical_bytes: None,
        note: "Logical payload size inside SQLite; included in the database file size and not additive to disk usage.".into(),
    }
}

async fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn parse_df(output: &str) -> Option<DiskUsage> {
    let line = output.lines().last()?;
    let columns: Vec<&str> = line.split_whitespace().collect();
    if columns.len() < 6 {
        return None;
    }
    Some(DiskUsage {
        filesystem: columns[5].to_owned(),
        total_bytes: columns[0].parse().ok()?,
        used_bytes: columns[1].parse().ok()?,
        available_bytes: columns[2].parse().ok()?,
        used_percent: columns[4].to_owned(),
    })
}

fn parse_human_bytes(report: &str) -> Option<u64> {
    let words: Vec<&str> = report.split_whitespace().collect();
    let index = words.iter().position(|word| *word == "up")?;
    let value = words.get(index + 1)?.trim_end_matches(',');
    let split = value.find(|ch: char| !ch.is_ascii_digit() && ch != '.')?;
    let number: f64 = value[..split].parse().ok()?;
    let multiplier = match value[split..].trim_end_matches('B').to_ascii_uppercase().as_str() {
        "K" | "KB" | "KI" | "KIB" => 1024_f64,
        "M" | "MB" | "MI" | "MIB" => 1024_f64.powi(2),
        "G" | "GB" | "GI" | "GIB" => 1024_f64.powi(3),
        "T" | "TB" | "TI" | "TIB" => 1024_f64.powi(4),
        _ => 1_f64,
    };
    Some((number * multiplier) as u64)
}

async fn database_file_usage(pool: &SqlitePool) -> DatabaseUsage {
    let path = sqlx::query("PRAGMA database_list")
        .fetch_all(pool)
        .await
        .ok()
        .and_then(|rows| rows.into_iter().find_map(|row| {
            let name: String = row.try_get("name").ok()?;
            if name == "main" {
                row.try_get::<String, _>("file").ok().filter(|value| !value.is_empty())
            } else { None }
        }));
    let file_bytes = |suffix: &str| -> u64 {
        let Some(path) = path.as_ref() else { return 0 };
        let candidate = PathBuf::from(format!("{path}{suffix}"));
        std::fs::metadata(candidate).map(|meta| meta.len()).unwrap_or(0)
    };
    let database_bytes = file_bytes("");
    let wal_bytes = file_bytes("-wal");
    let shm_bytes = file_bytes("-shm");
    DatabaseUsage {
        path,
        database_bytes,
        wal_bytes,
        shm_bytes,
        total_file_bytes: database_bytes.saturating_add(wal_bytes).saturating_add(shm_bytes),
    }
}

pub async fn inspect(pool: &SqlitePool) -> StorageOverview {
    let disk = command_output("df", &["-B1", "--output=size,used,avail,pcent,target", "/"])
        .await
        .and_then(|output| parse_df(&output));

    let tokens = table_usage(pool, "token_states",
        "SELECT COALESCE(SUM(length(payload_json)),0) FROM token_states").await;
    let launches = table_usage(pool, "token_launches",
        "SELECT COALESCE(SUM(length(payload_json)),0) FROM token_launches").await;
    let events = table_usage(pool, "solana_raw_events",
        "SELECT COALESCE(SUM(length(event_json)),0) FROM solana_raw_events").await;
    let chart_data = table_usage(pool, "chart_candles",
        "SELECT COALESCE(SUM(length(payload_json)),0) FROM chart_candles").await;
    let ledger = table_usage(pool, "paper_ledger",
        "SELECT COALESCE(SUM(length(id)+length(kind)+COALESCE(length(reference),0)),0) FROM paper_ledger").await;
    let positions = table_usage(pool, "portfolio_positions",
        "SELECT COALESCE(SUM(length(payload_json)),0) FROM portfolio_positions").await;
    let paper_bytes = ledger.logical_bytes.unwrap_or(0).saturating_add(positions.logical_bytes.unwrap_or(0));
    let paper_records = ledger.records.unwrap_or(0).saturating_add(positions.records.unwrap_or(0));
    let database = database_file_usage(pool).await;

    let journal_report = command_output("journalctl", &["--disk-usage"]).await;
    let journal_size = journal_report.as_deref().and_then(parse_human_bytes);
    let journal = JournalUsage {
        available: journal_report.is_some(),
        reported_size_bytes: journal_size,
        raw_report: journal_report,
        note: "systemd journal size is system-wide. Linux does not expose reliable per-service disk attribution for journald.".into(),
    };

    StorageOverview {
        disk,
        categories: Categories {
            tokens,
            launches: launches.clone(),
            events,
            discovery: DataCategory {
                logical_bytes: launches.logical_bytes,
                records: launches.records,
                physical_bytes: None,
                note: "Discovery history is represented by launch records; this overlaps the Launches category.".into(),
            },
            chart_data,
            paper_trading: DataCategory {
                logical_bytes: Some(paper_bytes),
                records: Some(paper_records),
                physical_bytes: None,
                note: "Logical paper-ledger and position payload sizes; included in SQLite database size.".into(),
            },
            engine_logs: DataCategory {
                logical_bytes: None, records: None, physical_bytes: None,
                note: "NodeX currently writes tracing output to stdout, captured by systemd. Its bytes cannot be isolated from journald's system-wide total.".into(),
            },
            system_logs: DataCategory {
                logical_bytes: journal_size, records: None, physical_bytes: None,
                note: "System-wide systemd journal size; not additive to database category.".into(),
            },
            database: DataCategory {
                logical_bytes: Some(database.total_file_bytes), records: None,
                physical_bytes: Some(database.total_file_bytes),
                note: "Physical SQLite database, WAL and SHM files.".into(),
            },
            other: DataCategory {
                logical_bytes: None, records: None, physical_bytes: None,
                note: "Not yet attributed; compare root filesystem usage against measured application files.".into(),
            },
        },
        database,
        system_journal: journal,
        updated_at: chrono::Utc::now().to_rfc3339(),
    }
}

/// Rotate and vacuum archived systemd journal files. This is deliberately gated
/// by an explicit deployment setting and the service's own OS permissions.
pub async fn cleanup_system_journal() -> Result<String, String> {
    if std::env::var("NODEX_ALLOW_SYSTEM_JOURNAL_CLEANUP").as_deref() != Ok("true") {
        return Err("System journal cleanup is disabled. Set NODEX_ALLOW_SYSTEM_JOURNAL_CLEANUP=true after reviewing its system-wide effect.".into());
    }
    let rotate = Command::new("journalctl").arg("--rotate").output().await
        .map_err(|error| format!("Could not execute journalctl: {error}"))?;
    if !rotate.status.success() {
        return Err(format!("journalctl --rotate failed: {}", String::from_utf8_lossy(&rotate.stderr).trim()));
    }
    let vacuum = Command::new("journalctl").arg("--vacuum-size=200M").output().await
        .map_err(|error| format!("Could not execute journalctl: {error}"))?;
    if !vacuum.status.success() {
        return Err(format!("journalctl vacuum failed; check service permissions: {}", String::from_utf8_lossy(&vacuum.stderr).trim()));
    }
    Ok(String::from_utf8_lossy(&vacuum.stdout).trim().to_owned())
}
