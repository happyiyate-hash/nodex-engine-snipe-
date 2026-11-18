use serde::Serialize;
use sqlx::{Row, SqlitePool};
use std::{path::PathBuf, process::Stdio};
use tokio::process::Command;

#[derive(Debug, Serialize)]
pub struct DiskUsage {
    pub filesystem: String,
    pub mountpoint: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
    pub used_percent: String,
}

#[derive(Debug, Serialize)]
pub struct StorageOverview {
    pub disk: Option<DiskUsage>,
    pub disk_error: Option<String>,
    pub database: DatabaseUsage,
    pub developer_training: DeveloperTrainingUsage,
    pub system_journal: JournalUsage,
    pub updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct DeveloperTrainingUsage {
    pub developers: u64,
    pub tokens: u64,
    pub observations: u64,
    pub trades: u64,
    /// SQLite page allocation for training tables and their indexes, when dbstat is available.
    pub allocated_bytes: Option<u64>,
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
    // Command output is: size, used, available, percent, source, target.
    // Keep the expected field count in sync with the df --output arguments.
    if columns.len() != 6 {
        return None;
    }
    Some(DiskUsage {
        filesystem: columns[4].to_owned(),
        mountpoint: columns[5].to_owned(),
        total_bytes: columns[0].parse().ok()?,
        used_bytes: columns[1].parse().ok()?,
        available_bytes: columns[2].parse().ok()?,
        used_percent: columns[3].trim_end_matches('%').to_owned(),
    })
}

fn parse_human_bytes(report: &str) -> Option<u64> {
    let words: Vec<&str> = report.split_whitespace().collect();
    let index = words.iter().position(|word| *word == "up")?;
    let value = words.get(index + 1)?.trim_end_matches(',');
    let split = value.find(|ch: char| !ch.is_ascii_digit() && ch != '.')?;
    let number: f64 = value[..split].parse().ok()?;
    let multiplier = match value[split..]
        .trim_end_matches('B')
        .to_ascii_uppercase()
        .as_str()
    {
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
        .and_then(|rows| {
            rows.into_iter().find_map(|row| {
                let name: String = row.try_get("name").ok()?;
                if name == "main" {
                    row.try_get::<String, _>("file")
                        .ok()
                        .filter(|value| !value.is_empty())
                } else {
                    None
                }
            })
        });
    let file_bytes = |suffix: &str| -> u64 {
        let Some(path) = path.as_ref() else { return 0 };
        let candidate = PathBuf::from(format!("{path}{suffix}"));
        std::fs::metadata(candidate)
            .map(|meta| meta.len())
            .unwrap_or(0)
    };
    let database_bytes = file_bytes("");
    let wal_bytes = file_bytes("-wal");
    let shm_bytes = file_bytes("-shm");
    DatabaseUsage {
        path,
        database_bytes,
        wal_bytes,
        shm_bytes,
        total_file_bytes: database_bytes
            .saturating_add(wal_bytes)
            .saturating_add(shm_bytes),
    }
}

async fn developer_training_usage(pool: &SqlitePool) -> DeveloperTrainingUsage {
    async fn count(pool: &SqlitePool, table: &str) -> u64 {
        let query = format!("SELECT COUNT(*) FROM {table}");
        sqlx::query_scalar::<_, i64>(&query)
            .fetch_one(pool)
            .await
            .unwrap_or(0)
            .max(0) as u64
    }
    let developers = count(pool, "training_developers").await;
    let tokens = count(pool, "training_tokens").await;
    let observations = count(pool, "training_token_observations").await;
    let trades = count(pool, "training_trade_events").await;
    let page_allocation = sqlx::query_scalar::<_, i64>(
        r#"SELECT COALESCE(SUM(pgsize), 0) FROM dbstat
           WHERE name IN (
             'training_developers', 'training_tokens', 'training_token_observations',
             'training_trade_events', 'creator_reputations', 'creator_intelligence'
           )
           OR name LIKE 'idx_training_%'
           OR name IN ('idx_creator_reputations_rank', 'idx_creator_intelligence_score')"#,
    )
    .fetch_one(pool)
    .await
    .ok()
    .map(|bytes| bytes.max(0) as u64);
    let payload_estimate = sqlx::query_scalar::<_, i64>(
        r#"SELECT
          COALESCE((SELECT SUM(length(CAST(creator_address AS BLOB)) + length(CAST(first_seen_at AS BLOB)) + length(CAST(last_seen_at AS BLOB)) + length(CAST(updated_at AS BLOB))) FROM training_developers), 0) +
          COALESCE((SELECT SUM(length(CAST(mint_address AS BLOB)) + length(CAST(creator_address AS BLOB)) + length(CAST(source AS BLOB)) + length(CAST(launch_signature AS BLOB)) + length(CAST(launched_at AS BLOB)) + COALESCE(length(CAST(last_observed_at AS BLOB)),0) + length(CAST(lifecycle_status AS BLOB)) + COALESCE(length(CAST(graduated_at AS BLOB)),0) + length(CAST(created_at AS BLOB)) + length(CAST(updated_at AS BLOB))) FROM training_tokens), 0) +
          COALESCE((SELECT SUM(length(CAST(mint_address AS BLOB)) + length(CAST(creator_address AS BLOB)) + length(CAST(observation_key AS BLOB)) + length(CAST(observed_at AS BLOB)) + COALESCE(length(CAST(evidence_json AS BLOB)),0)) FROM training_token_observations), 0) +
          COALESCE((SELECT SUM(length(CAST(event_key AS BLOB)) + length(CAST(signature AS BLOB)) + length(CAST(mint_address AS BLOB)) + COALESCE(length(CAST(creator_address AS BLOB)),0) + length(CAST(venue AS BLOB)) + length(CAST(trader_address AS BLOB)) + length(CAST(side AS BLOB)) + COALESCE(length(CAST(evidence_json AS BLOB)),0) + length(CAST(recorded_at AS BLOB))) FROM training_trade_events), 0) +
          COALESCE((SELECT SUM(length(CAST(creator_address AS BLOB)) + length(CAST(sample_status AS BLOB)) + length(CAST(updated_at AS BLOB))) FROM creator_reputations), 0) +
          COALESCE((SELECT SUM(length(CAST(creator_address AS BLOB)) + length(CAST(display_label AS BLOB)) + length(CAST(source AS BLOB)) + length(CAST(updated_at AS BLOB))) FROM creator_intelligence), 0)"#,
    )
    .fetch_one(pool)
    .await
    .ok()
    .map(|bytes| bytes.max(0) as u64);
    // dbstat includes table and index pages. If SQLite was built without dbstat,
    // fall back to the measured byte length of persisted training payload fields.
    let allocated_bytes = page_allocation.or(payload_estimate);
    DeveloperTrainingUsage {
        developers,
        tokens,
        observations,
        trades,
        allocated_bytes,
    }
}

pub async fn inspect(pool: &SqlitePool) -> StorageOverview {
    // The root mount is backed by the EC2 root EBS volume. Include both source
    // and mountpoint so the UI can identify exactly which filesystem was read.
    let disk_output = command_output(
        "df",
        &["-B1", "--output=size,used,avail,pcent,source,target", "/"],
    )
    .await;
    let disk = disk_output.as_deref().and_then(parse_df);
    let disk_error = if disk.is_some() {
        None
    } else if disk_output.is_none() {
        Some("The df command failed or is unavailable on the server.".to_owned())
    } else {
        Some("The df output did not match the expected six-column format.".to_owned())
    };

    let database = database_file_usage(pool).await;
    let developer_training = developer_training_usage(pool).await;

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
        disk_error,
        database,
        developer_training,
        system_journal: journal,
        updated_at: chrono::Utc::now().to_rfc3339(),
    }
}

/// Rotate and vacuum archived systemd journal files. This is deliberately gated
/// by an explicit deployment setting and the service's own OS permissions.
pub async fn cleanup_system_journal() -> Result<String, String> {
    if std::env::var("NODEX_ALLOW_SYSTEM_JOURNAL_CLEANUP")
        .ok()
        .as_deref()
        != Some("true")
    {
        return Err("System journal cleanup is disabled. Set NODEX_ALLOW_SYSTEM_JOURNAL_CLEANUP=true after reviewing its system-wide effect.".into());
    }
    let rotate = Command::new("journalctl")
        .arg("--rotate")
        .output()
        .await
        .map_err(|error| format!("Could not execute journalctl: {error}"))?;
    if !rotate.status.success() {
        return Err(format!(
            "journalctl --rotate failed: {}",
            String::from_utf8_lossy(&rotate.stderr).trim()
        ));
    }
    let vacuum = Command::new("journalctl")
        .arg("--vacuum-size=200M")
        .output()
        .await
        .map_err(|error| format!("Could not execute journalctl: {error}"))?;
    if !vacuum.status.success() {
        return Err(format!(
            "journalctl vacuum failed; check service permissions: {}",
            String::from_utf8_lossy(&vacuum.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&vacuum.stdout).trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_root_filesystem_capacity_and_mountpoint() {
        let output = "  1B-blocks       Used       Avail Use% Filesystem       Mounted on\n85819633664 7597551616 78222082048   9% /dev/nvme0n1p1 /\n";
        let disk = parse_df(output).expect("valid df output should parse");
        assert_eq!(disk.total_bytes, 85_819_633_664);
        assert_eq!(disk.used_bytes, 7_597_551_616);
        assert_eq!(disk.available_bytes, 78_222_082_048);
        assert_eq!(disk.used_percent, "9");
        assert_eq!(disk.filesystem, "/dev/nvme0n1p1");
        assert_eq!(disk.mountpoint, "/");
    }

    #[test]
    fn rejects_df_output_with_missing_columns() {
        let output =
            "1B-blocks Used Avail Use% Mounted on\n85819633664 7597551616 78222082048 9% /\n";
        assert!(parse_df(output).is_none());
    }
}
