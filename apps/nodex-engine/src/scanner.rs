//! Launch discovery and security-scanning pipeline.
//!
//! This module owns the launch worker queue and per-token scan lifecycle.
//! Shared application services and existing scan helpers remain in the parent
//! module during this incremental extraction.

use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn start_launch_processor(
    mut receiver: broadcast::Receiver<serde_json::Value>,
    events: EventBroadcaster,
    rpc: SolanaRpc,
    security_rpc: Option<SolanaRpc>,
    database: sqlx::SqlitePool,
    pump_fun_program_id: String,
    control: Arc<RwLock<EngineControl>>,
    trading_settings: Arc<RwLock<TradingSettings>>,
    market_monitor: Arc<RwLock<Option<MarketMonitor>>>,
    token_registry: TokenRegistry,
) {
    const LAUNCH_QUEUE_CAPACITY: usize = 256;
    const LAUNCH_WORKER_COUNT: usize = 4;
    let seen_signatures = Arc::new(Mutex::new(HashSet::<String>::new()));
    // Prevent concurrent discovery/metadata/scan pipelines for the same mint.
    let inflight_mints = Arc::new(Mutex::new(HashSet::<String>::new()));
    let (work_tx, work_rx) = tokio::sync::mpsc::channel::<(String, u64)>(LAUNCH_QUEUE_CAPACITY);
    let work_rx = Arc::new(Mutex::new(work_rx));

    for _ in 0..LAUNCH_WORKER_COUNT {
        let work_rx = work_rx.clone();
        let seen_signatures = seen_signatures.clone();
        let rpc = rpc.clone();
        let events = events.clone();
        let program_id = pump_fun_program_id.clone();
        let control = control.clone();
        let trading_settings = trading_settings.clone();
        let market_monitor = market_monitor.clone();
        let token_registry = token_registry.clone();
        let inflight_mints = inflight_mints.clone();
        let security_rpc = security_rpc.clone();
        let database = database.clone();
        tokio::spawn(async move {
            loop {
                let work = { work_rx.lock().await.recv().await };
                let Some((signature, slot_hint)) = work else {
                    break;
                };
                if control.read().await.paused {
                    continue;
                }
                let mut transaction = None;
                let mut last_error = None;
                for attempt in 0..4 {
                    match rpc.get_transaction(&signature).await {
                        Ok(Some(value)) => {
                            transaction = Some(value);
                            break;
                        }
                        Ok(None) => {
                            if attempt < 3 {
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    300 * (attempt + 1),
                                ))
                                .await;
                            }
                        }
                        Err(error) => {
                            last_error = Some(error.to_string());
                            if attempt < 3 {
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    300 * (attempt + 1),
                                ))
                                .await;
                            }
                        }
                    }
                }
                let Some(transaction) = transaction else {
                    let message = last_error.unwrap_or_else(|| {
                        "RPC returned null for this confirmed transaction after retries".to_string()
                    });
                    let _ = events.publish(json!({"type":"diagnostic","data":{
                        "severity":"warning","component":"solana_get_transaction",
                        "message":message,"signature":signature,"slot_hint":slot_hint,
                        "retry_exhausted":true,"received_at":chrono::Utc::now().to_rfc3339()
                    }}));
                    // Allow reconnect/backfill to retry transactions that RPC could not return.
                    seen_signatures.lock().await.remove(&signature);
                    continue;
                };
                let decoded = match decode_pump_fun_create(&transaction, &signature, &program_id) {
                    Ok(Some(decoded)) => decoded,
                    Ok(None) => continue,
                    Err(error) => {
                        let _ = events.send(json!({"type":"diagnostic","data":{
                            "severity":"error","component":"pump_fun_decoder",
                            "message":error.to_string(),"signature":signature,
                            "received_at":chrono::Utc::now().to_rfc3339()
                        }}));
                        continue;
                    }
                };
                let launch = detect_pump_fun_launch(CreateInstruction {
                    program_id: program_id.clone(),
                    configured_pump_fun_program_id: program_id.clone(),
                    mint_address: decoded.mint_address,
                    creator_address: Some(decoded.creator_address.clone()),
                    signature: signature.clone(),
                    slot: if decoded.slot == 0 {
                        slot_hint
                    } else {
                        decoded.slot
                    },
                });
                let Some(launch) = launch else { continue };
                // Unknown/unpreferred creators remain visible and monitored. Strict
                // preferred-owner mode is an entry restriction, not a discovery filter.
                let whitelist_settings = trading_settings.read().await;
                let whitelist_only_mode = whitelist_settings.whitelist_only_mode;
                let creator_is_preferred = launch
                    .creator_address
                    .as_deref()
                    .map(|address| {
                        whitelist_settings
                            .preferred_owners
                            .iter()
                            .any(|owner| owner.address == address)
                    })
                    .unwrap_or(false);
                if whitelist_only_mode && !creator_is_preferred {
                    let _ = events.send(json!({
                        "type":"creator_watchlist_status",
                        "data":{
                            "mint_address":launch.mint_address.clone(),
                            "creator_address":launch.creator_address.clone(),
                            "status":"monitor_only",
                            "preferred_owner":false,
                            "reason":"Creator is not in the preferred-owner list; discovery and market monitoring continue, but strict whitelist mode blocks paper entry."
                        }
                    }));
                }
                drop(whitelist_settings);
                let mint_address = launch.mint_address.clone();
                {
                    let mut inflight = inflight_mints.lock().await;
                    if !inflight.insert(mint_address.clone()) {
                        continue;
                    }
                }
                // Durable scan cache: duplicate launch notifications must not repeat RPC or
                // RugCheck work. Diagnostic-only results expire so incomplete policy can be
                // revisited; verified/rejected decisions remain terminal until explicit revalidation.
                if let Some(existing) = token_registry.get(&mint_address) {
                    let cache = &existing.data["security"]["scan_cache"];
                    let cache_status = cache["status"].as_str().unwrap_or_default();
                    let retry_after = cache["retry_after"]
                        .as_str()
                        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok());
                    let now = chrono::Utc::now();
                    let terminal = matches!(cache_status, "verified" | "rejected");
                    let cached = cache_status == "completed_diagnostic"
                        && retry_after.map(|time| time > now).unwrap_or(true);
                    let waiting = cache_status == "retry_wait"
                        && retry_after.map(|time| time > now).unwrap_or(false);
                    let scan_started_at = cache["started_at"]
                        .as_str()
                        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok());
                    let scan_in_progress = cache_status == "scanning"
                        && scan_started_at
                            .map(|time| now.signed_duration_since(time).num_seconds() < 120)
                            .unwrap_or(false);
                    if terminal || cached || waiting || scan_in_progress {
                        inflight_mints.lock().await.remove(&mint_address);
                        continue;
                    }
                }
                let scan_attempts = token_registry
                    .get(&mint_address)
                    .and_then(|state| state.data["security"]["scan_cache"]["attempts"].as_u64())
                    .unwrap_or(0);

                // Resolve identity before announcing discovery or starting security analysis.
                let metadata_result = tokio::time::timeout(
                    Duration::from_secs(10),
                    resolve_discovery_metadata(&rpc, &mint_address, &events),
                )
                .await;
                let metadata = match metadata_result {
                    Ok(Ok(metadata)) => metadata,
                    Ok(Err(error)) => {
                        tracing::warn!(%error,mint_address=%mint_address,"Initial token metadata resolution failed; continuing with partial discovery");
                        let mut partial = DiscoveryMetadata::partial();
                        partial.errors.push(token_diagnostic(
                            &mint_address,
                            "metadata_resolution",
                            "solana_rpc",
                            error.to_string(),
                            provider_error_details(&format!("{error:#}")),
                        ));
                        partial
                    }
                    Err(_) => {
                        let mut partial = DiscoveryMetadata::partial();
                        let message =
                            "Initial token metadata resolution timed out after 10 seconds.";
                        tracing::warn!(mint_address=%mint_address,"{message}");
                        partial.errors.push(token_diagnostic(
                            &mint_address,
                            "metadata_resolution",
                            "solana_rpc",
                            message,
                            json!({"kind":"timeout","timeout_seconds":10}),
                        ));
                        partial
                    }
                };

                let creator_address = launch.creator_address.clone();
                let mut identity = serde_json::Map::new();
                identity.insert("mint_address".into(), json!(mint_address));
                if let Some(value) = metadata.name.as_ref() {
                    identity.insert("name".into(), json!(value));
                }
                if let Some(value) = metadata.symbol.as_ref() {
                    identity.insert("symbol".into(), json!(value));
                }
                if let Some(value) = metadata.decimals {
                    identity.insert("decimals".into(), json!(value));
                }
                if let Some(value) = metadata.metadata_uri.as_ref() {
                    identity.insert("metadata_uri".into(), json!(value));
                }
                if let Some(value) = metadata.logo_url.as_ref() {
                    identity.insert("logo_url".into(), json!(value));
                }

                let initial_state = token_registry.upsert_patch(
                    &mint_address,
                    json!({
                        "identity": identity,
                        "discovery": {
                            "source": "pump_fun",
                            "creator_address": creator_address,
                            "creator_preferred": creator_is_preferred,
                            "whitelist_only_mode": whitelist_only_mode,
                            "signature": launch.signature,
                            "slot": launch.slot,
                            "detected_at": launch.detected_at,
                            "metadata_status": metadata.status
                        },
                        "security": {
                            // A retry is a new scan generation, not a continuation of
                            // the previous terminal verdict. Clear the old decision before
                            // publishing the new Scanning state.
                            "status": "scanning",
                            "decision": "PENDING",
                            "verification_status": "PENDING",
                            "decision_reason": "A new security scan attempt is in progress.",
                            "scanned_at": null,
                            "scan_cache": {
                                "status": "scanning",
                                "attempts": scan_attempts,
                                "scan_attempt": scan_attempts + 1,
                                "started_at": chrono::Utc::now().to_rfc3339()
                            }
                        },
                        "activity": {
                            "status": "scanning",
                            "label": "Scanning",
                            "scan_attempt": scan_attempts + 1,
                            "progress": 0,
                            "stage": "initializing",
                            "sub_event": "Scanning",
                            "updated_at": chrono::Utc::now().to_rfc3339()
                        },
                        "market": {"status": "not_started"}
                    }),
                );
                match initial_state {
                    Ok(snapshot) => {
                        // Persist the in-progress lifecycle before expensive provider calls.
                        // On restart, a stale "scanning" state can be retried instead of lost.
                        if let Err(error) =
                            nodex_persistence::save_token_state(&database, &snapshot).await
                        {
                            let _ = events.send(json!({"type":"token_diagnostic","data":token_diagnostic(
                                &mint_address,"token_scan_cache","sqlite",error.to_string(),
                                json!({"kind":"database_persistence_error","message":error.to_string()})
                            )}));
                        }
                        let _ = events.send(json!({
                            "type":"new_token_detected",
                            "data":{
                                "mint_address":mint_address,
                                "name":metadata.name,
                                "symbol":metadata.symbol,
                                "decimals":metadata.decimals,
                                "logo_url":metadata.logo_url,
                                "metadata_uri":metadata.metadata_uri,
                                "metadata_status":metadata.status,
                                "scan_status":"pending",
                                "discovery":snapshot.data.get("discovery").cloned().unwrap_or(serde_json::Value::Null)
                            }
                        }));
                        let mut legacy_launch =
                            serde_json::to_value(&launch).unwrap_or_else(|_| json!({}));
                        if let Some(object) = legacy_launch.as_object_mut() {
                            object.insert("name".into(), json!(metadata.name));
                            object.insert("symbol".into(), json!(metadata.symbol));
                            object.insert("decimals".into(), json!(metadata.decimals));
                            object.insert("logo_url".into(), json!(metadata.logo_url));
                            object.insert("metadata_uri".into(), json!(metadata.metadata_uri));
                            object.insert("metadata_status".into(), json!(metadata.status));
                        }
                        // Keep the legacy event enriched so existing clients also receive
                        // metadata before the scanner begins.
                        let _ = events.send(json!({"type":"launch_detected","data":legacy_launch}));
                        for diagnostic in &metadata.errors {
                            let _ =
                                events.send(json!({"type":"token_diagnostic","data":diagnostic}));
                        }
                        let _ = events.send(json!({"type":"token_scan_started","data":{
                            "mint_address":mint_address,
                            "status":"scanning",
                            "scan_attempt":scan_attempts + 1,
                            "state_version":snapshot.version,
                            "received_at":chrono::Utc::now().to_rfc3339()
                        }}));
                    }
                    Err(error) => {
                        tracing::error!(%error, mint_address=%mint_address, "Could not create in-memory pending token state");
                        inflight_mints.lock().await.remove(&mint_address);
                        continue;
                    }
                }

                let scan_rpc = security_rpc.clone().unwrap_or_else(|| rpc.clone());
                let scan_events = events.clone();
                let scan_mint = mint_address.clone();
                let approved_monitor = market_monitor.clone();
                let approved_program_id = program_id.clone();
                let approved_registry = token_registry.clone();
                let approved_database = database.clone();
                let inflight_mints_for_scan = inflight_mints.clone();
                let trading_settings_for_scan = trading_settings.clone();
                tokio::spawn(async move {
                    let scan_settings = trading_settings_for_scan.read().await.clone();
                    let progress_events = scan_events.clone();
                    let progress_mint = scan_mint.clone();
                    let scan_attempt = scan_attempts + 1;
                    let scan_result = match tokio::time::timeout(
                        Duration::from_secs(45),
                        scan_rpc.scan_token_security_with_progress_mode(
                            &scan_mint,
                            scan_settings.diagnostic_mode,
                            move |stage, progress, message| {
                                let _ = progress_events.send(
                                    json!({"type":"token_scan_progress","data":{
                                        "mint_address":progress_mint,
                                        "scan_attempt":scan_attempt,
                                        "stage":stage,
                                        "progress":progress,
                                        "message":message,
                                        "received_at":chrono::Utc::now().to_rfc3339()
                                    }}),
                                );
                            },
                        ),
                    )
                    .await
                    {
                        Ok(Ok(report)) => Ok(report),
                        Ok(Err(error)) => Err(error.to_string()),
                        Err(_) => Err("Security scan timed out after 45 seconds".to_string()),
                    };
                    match scan_result {
                        Ok(mut report) => {
                            // The final execution gate is calculated after every provider and creator-history check has been appended.
                            // Keep the pre-scan identity authoritative when the scanner has no better value.
                            if report.metadata.name.is_none() {
                                report.metadata.name = metadata.name.clone();
                            }
                            if report.metadata.symbol.is_none() {
                                report.metadata.symbol = metadata.symbol.clone();
                            }
                            if report.metadata.uri.is_none() {
                                report.metadata.uri = metadata.metadata_uri.clone();
                            }
                            let mut identity_patch = serde_json::Map::new();
                            if let Some(name) = report.metadata.name.as_ref() {
                                identity_patch.insert("name".into(), json!(name));
                            }
                            if let Some(symbol) = report.metadata.symbol.as_ref() {
                                identity_patch.insert("symbol".into(), json!(symbol));
                            }
                            if let Some(uri) = report.metadata.uri.as_ref() {
                                identity_patch.insert("metadata_uri".into(), json!(uri));
                            }
                            if let Some(decimals) = report.decimals {
                                identity_patch.insert("decimals".into(), json!(decimals));
                            }
                            if let Some(supply) = report.supply_raw.as_ref() {
                                identity_patch.insert("total_supply_raw".into(), json!(supply));
                            }
                            if let Some(logo) = metadata.logo_url.as_ref() {
                                identity_patch.insert("logo_url".into(), json!(logo));
                            }
                            if !identity_patch.is_empty() {
                                let _ = approved_registry
                                    .upsert_patch(&scan_mint, json!({"identity":identity_patch}));
                            }

                            // Creator history is optional enrichment. It is not a launch
                            // verification prerequisite and must not delay the first market subscription.
                            if let Some(details) = report.rugcheck.error_details.clone() {
                                let message =
                                    report.rugcheck.error.clone().unwrap_or_else(|| {
                                        "RugCheck provider request failed.".into()
                                    });
                                let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"rugcheck","rugcheck",message,details)}));
                            }
                            for check in &report.checks {
                                if check.name == "holder_distribution"
                                    && check.detail.contains("NODEX_PROVIDER_ERROR_JSON:")
                                {
                                    let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"holder_distribution","solana_rpc",check.detail.clone(),provider_error_details(&check.detail))}));
                                }
                            }
                            // Read the latest persisted policy after all evidence is collected.
                            let current_policy = trading_settings_for_scan.read().await.clone();
                            // "Verified" in the launch feed means the explicit core launch gate
                            // passed. Keep all_checks_verified strict for policy/reporting: holder,
                            // creator, liquidity and sellability enrichments are still incomplete.
                            let all_checks_verified = !report.checks.is_empty()
                                && report
                                    .checks
                                    .iter()
                                    .all(|check| check.status.eq_ignore_ascii_case("verified"));
                            let report_value =
                                serde_json::to_value(&report).unwrap_or_else(|_| json!({}));
                            let verification_gate_passed =
                                core_verification_gate_passed(&report_value);
                            let execution_gate = json!({
                                "execution_enabled": current_policy.execution_enabled,
                                "block_unverified_trades": current_policy.block_unverified_trades,
                                "diagnostic_mode": current_policy.diagnostic_mode,
                                "all_checks_verified": all_checks_verified,
                                "eligible": current_policy.execution_enabled
                                    && !current_policy.diagnostic_mode
                                    && !report.diagnostic_only
                                    && (!current_policy.block_unverified_trades || all_checks_verified),
                                "reason": if !current_policy.execution_enabled {
                                    "Execution master switch is OFF or execution module is not connected"
                                } else if current_policy.diagnostic_mode || report.diagnostic_only {
                                    "Diagnostic-only scan results block execution"
                                } else if current_policy.block_unverified_trades && !all_checks_verified {
                                    "Security checks are incomplete or not all VERIFIED"
                                } else {
                                    "Qualified by current runtime policy"
                                }
                            });
                            let mut report_payload =
                                serde_json::to_value(&report).unwrap_or_else(|_| json!({}));
                            report_payload["execution_gate"] = execution_gate;
                            report_payload["all_checks_verified"] = json!(all_checks_verified);
                            report_payload["verification_gate_passed"] =
                                json!(verification_gate_passed);
                            report_payload["verification_status"] =
                                json!(if verification_gate_passed {
                                    "VERIFIED"
                                } else {
                                    "UNVERIFIED"
                                });
                            let decision = report.decision.clone();
                            let mut terminal_status = match decision.as_str() {
                                "REJECTED" => "rejected",
                                "APPROVED" => "completed",
                                _ => "unverified",
                            };
                            // Persist the terminal lifecycle status alongside the verdict.
                            // A report can carry a stale intermediate "scanning" field.
                            report_payload["status"] = json!(if verification_gate_passed {
                                "verified"
                            } else if decision.eq_ignore_ascii_case("REJECTED") {
                                "rejected"
                            } else {
                                "unverified"
                            });
                            match decision.as_str() {
                                "APPROVED" => {
                                    let launch_saved = match nodex_persistence::save_launch(
                                        &approved_database,
                                        &launch,
                                    )
                                    .await
                                    {
                                        Ok(true) => true,
                                        Ok(false) => true,
                                        Err(error) => {
                                            let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                                "severity":"error","component":"launch_persistence",
                                                "message":error.to_string(),"mint_address":scan_mint
                                            }}));
                                            false
                                        }
                                    };
                                    let approved_state = if launch_saved {
                                        approved_registry.upsert_patch(
                                            &scan_mint,
                                            json!({
                                                "security":report_payload.clone(),
                                                "market":{"status":"starting"}
                                            }),
                                        )
                                    } else {
                                        Err("Approved launch could not be persisted")
                                    };
                                    let approval_persisted = match approved_state {
                                        Ok(snapshot) => nodex_persistence::save_token_state(
                                            &approved_database,
                                            &snapshot,
                                        )
                                        .await
                                        .is_ok(),
                                        Err(error) => {
                                            let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                                "severity":"error","component":"token_approval_registry",
                                                "message":error,"mint_address":scan_mint
                                            }}));
                                            false
                                        }
                                    };
                                    if approval_persisted {
                                        let _ = scan_events.send(json!({"type":"token_scan_update","data":report_payload.clone()}));
                                        if let Some(snapshot) = approved_registry.get(&scan_mint) {
                                            let _ = scan_events.send(json!({"type":"token_state_updated","data":snapshot}));
                                        }
                                    } else {
                                        terminal_status = "unverified";
                                        let message="Security approval could not be persisted to SQLite; token is retained as unverified.";
                                        report_payload["decision"] = json!("UNVERIFIED");
                                        report_payload["status"] = json!("unverified");
                                        report_payload["decision_reason"] = json!(message);
                                        if let Ok(snapshot) = approved_registry.upsert_patch(
                                            &scan_mint,
                                            json!({"security":report_payload.clone(),"market":{"status":"not_started"}}),
                                        ) {
                                            let _ = nodex_persistence::save_token_state(&approved_database, &snapshot).await;
                                            let _ = scan_events.send(json!({"type":"token_state_updated","data":snapshot}));
                                        }
                                        let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"token_persistence","sqlite",message,json!({"kind":"database_persistence_error","message":message}))}));
                                    }
                                }
                                "REJECTED" => {
                                    // Rejected launches remain visible for investigation.
                                    // Their explicit rejected security status keeps them out of
                                    // the approved-token market and paper-entry paths.
                                    let mut security_state = report_payload.clone();
                                    if let Some(object) = security_state.as_object_mut() {
                                        object.insert(
                                            "status".into(),
                                            json!(if verification_gate_passed {
                                                "verified"
                                            } else {
                                                "rejected"
                                            }),
                                        );
                                        object.insert(
                                            "verification_status".into(),
                                            json!(if verification_gate_passed {
                                                "VERIFIED"
                                            } else {
                                                "UNVERIFIED"
                                            }),
                                        );
                                    }
                                    match approved_registry.upsert_patch(
                                        &scan_mint,
                                        json!({
                                            "security":security_state,
                                            "market":{"status":"not_started"}
                                        }),
                                    ) {
                                        Ok(snapshot) => {
                                            if let Err(error) = nodex_persistence::save_token_state(
                                                &approved_database,
                                                &snapshot,
                                            )
                                            .await
                                            {
                                                let message = format!(
                                                    "Rejected token state could not be persisted: {error}"
                                                );
                                                let _ = scan_events.send(json!({
                                                    "type":"token_diagnostic",
                                                    "data":token_diagnostic(
                                                        &scan_mint,
                                                        "token_persistence",
                                                        "sqlite",
                                                        message.clone(),
                                                        json!({"kind":"database_persistence_error","message":message})
                                                    )
                                                }));
                                            } else {
                                                let _ = scan_events.send(json!({
                                                    "type":"token_state_updated",
                                                    "data":snapshot
                                                }));
                                            }
                                        }
                                        Err(error) => {
                                            let _ = scan_events.send(json!({
                                                "type":"token_diagnostic",
                                                "data":token_diagnostic(
                                                    &scan_mint,
                                                    "token_state",
                                                    "nodex_token_registry",
                                                    error.to_string(),
                                                    json!({"kind":"token_registry_error","message":error.to_string()})
                                                )
                                            }));
                                        }
                                    }
                                    if let Err(error) =
                                        nodex_persistence::save_launch(&approved_database, &launch)
                                            .await
                                    {
                                        let _ = scan_events.send(json!({
                                            "type":"token_diagnostic",
                                            "data":token_diagnostic(
                                                &scan_mint,
                                                "launch_persistence",
                                                "sqlite",
                                                error.to_string(),
                                                json!({"kind":"database_persistence_error","message":error.to_string()})
                                            )
                                        }));
                                    }
                                    let _ = scan_events.send(json!({
                                        "type":"token_scan_update",
                                        "data":report.clone()
                                    }));
                                    let _ = scan_events.send(json!({
                                        "type":"token_approval_pending",
                                        "data":{
                                            "mint_address":scan_mint,
                                            "decision":"REJECTED",
                                            "reason":report.decision_reason.clone()
                                        }
                                    }));
                                }
                                _ => {
                                    let mut security_state =
                                        serde_json::to_value(&report).unwrap_or_else(|_| json!({}));
                                    if let Some(object) = security_state.as_object_mut() {
                                        object.insert("status".into(), json!("unverified"));
                                    }
                                    match approved_registry.upsert_patch(&scan_mint,json!({"security":security_state,"market":{"status":"not_started"}})){
                                        Ok(snapshot)=>{
                                            if let Err(error)=nodex_persistence::save_token_state(&approved_database,&snapshot).await{
                                                terminal_status="unverified";
                                                let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"token_persistence","sqlite",error.to_string(),json!({"kind":"database_persistence_error","message":error.to_string()}))}));
                                            }else{let _=scan_events.send(json!({"type":"token_state_updated","data":snapshot}));}
                                        }
                                        Err(error)=>{let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"token_state","nodex_token_registry",error.to_string(),json!({"kind":"token_registry_error","message":error.to_string()}))}));}
                                    }
                                    if let Err(error) =
                                        nodex_persistence::save_launch(&approved_database, &launch)
                                            .await
                                    {
                                        let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"launch_persistence","sqlite",error.to_string(),json!({"kind":"database_persistence_error","message":error.to_string()}))}));
                                    }
                                    let _ = scan_events.send(
                                        json!({"type":"token_scan_update","data":report.clone()}),
                                    );
                                    let _=scan_events.send(json!({"type":"token_approval_pending","data":{"mint_address":scan_mint,"decision":decision,"reason":"Token remains visible but isolated until an explicit APPROVED decision is produced."}}));
                                }
                            }
                            let _ = scan_events.send(json!({"type":"token_scan_completed","data":{
                                "mint_address":scan_mint,"scan_attempt":scan_attempt,"decision":decision,"status":terminal_status,
                                "progress":100,
                                "message":report_payload.get("decision_reason").cloned().unwrap_or(json!("Security scan finished.")),
                                "diagnostic_only":report.diagnostic_only,
                                "all_checks_verified":all_checks_verified,
                                "verification_gate_passed":verification_gate_passed,
                                "received_at":chrono::Utc::now().to_rfc3339()
                            }}));
                            // Record a durable terminal/intermediate scan state independently of
                            // the security verdict. A completed diagnostic scan is cached briefly,
                            // never promoted to verified; rate limits receive a shorter retry window.
                            let scanned_at = chrono::Utc::now();
                            let diagnostic_only = report.diagnostic_only;
                            let provider_limited = report
                                .rugcheck
                                .error
                                .as_deref()
                                .map(|value| {
                                    let value = value.to_ascii_lowercase();
                                    value.contains("429")
                                        || value.contains("rate limit")
                                        || value.contains("too many")
                                        || value.contains("max usage")
                                })
                                .unwrap_or(false)
                                || report.checks.iter().any(|check| {
                                    let value = check.detail.to_ascii_lowercase();
                                    value.contains("429")
                                        || value.contains("rate limit")
                                        || value.contains("too many")
                                        || value.contains("max usage")
                                });
                            let cache_status = if verification_gate_passed {
                                "verified"
                            } else if decision == "REJECTED" {
                                "rejected"
                            } else {
                                "completed_diagnostic"
                            };
                            let retry_seconds = if provider_limited {
                                (30_u64.saturating_mul(
                                    2_u64.saturating_pow(scan_attempts.min(5) as u32),
                                ))
                                .min(900)
                                    + (scan_mint.bytes().map(u64::from).sum::<u64>() % 11)
                            } else {
                                900
                            };
                            let retry_after = if cache_status == "completed_diagnostic" {
                                Some(
                                    (scanned_at + chrono::Duration::seconds(retry_seconds as i64))
                                        .to_rfc3339(),
                                )
                            } else {
                                None
                            };
                            if let Ok(snapshot) = approved_registry.upsert_patch(
                                &scan_mint,
                                json!({
                                    "security": {
                                        "scan_cache": {
                                            "status": cache_status,
                                            "verified": cache_status == "verified",
                                            "diagnostic_only": diagnostic_only,
                                            "attempts": scan_attempts + 1,
                                            "scan_attempt": scan_attempt,
                                            "scanned_at": scanned_at.to_rfc3339(),
                                            "retry_after": retry_after,
                                            "provider_rate_limited": provider_limited
                                        }
                                    }
                                }),
                            ) {
                                if let Err(error) = nodex_persistence::save_token_state(
                                    &approved_database,
                                    &snapshot,
                                )
                                .await
                                {
                                    let _ = scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(
                                        &scan_mint,"token_scan_cache","sqlite",error.to_string(),
                                        json!({"kind":"database_persistence_error","message":error.to_string()})
                                    )}));
                                }
                            }
                        }
                        Err(error) => {
                            let error_details = provider_error_details(&error);
                            let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"security_scan","solana_rpc",error.clone(),error_details.clone())}));
                            let _=scan_events.send(json!({"type":"diagnostic","data":{"severity":"error","component":"token_security_scanner","message":error,"mint_address":scan_mint,"received_at":chrono::Utc::now().to_rfc3339()}}));
                            let failure_check = json!({
                                "name":"security_scan",
                                "status":"unverified",
                                "severity":"critical",
                                "detail":error
                            });
                            let provider_response = json!({
                                "provider":"solana_rpc",
                                "method":"security_scan",
                                "response":error_details
                            });
                            let _=scan_events.send(json!({"type":"token_scan_failed","data":{
                                "mint_address":scan_mint,
                                "scan_attempt":scan_attempt,
                                "decision":"UNVERIFIED",
                                "decision_reason":"Diagnostic-only mode is active. This scan failed before provider evidence could be fully collected; no token is being verified.",
                                "diagnostic_only":true,
                                "diagnostic_assessment":{"decision":"UNVERIFIED","decision_reason":error},
                                "error_details":error_details,
                                "provider_responses":[provider_response.clone()],
                                "checks":[failure_check.clone()],
                                "progress":100,
                                "stage":"failed",
                                "message":error,
                                "status":"failed",
                                "received_at":chrono::Utc::now().to_rfc3339()
                            }}));
                            let previous_attempts = approved_registry
                                .get(&scan_mint)
                                .and_then(|state| {
                                    state.data["security"]["scan_cache"]["attempts"].as_u64()
                                })
                                .unwrap_or(0);
                            let attempts = previous_attempts.saturating_add(1);
                            let lower_error = error.to_ascii_lowercase();
                            let rate_limited = lower_error.contains("429")
                                || lower_error.contains("rate limit")
                                || lower_error.contains("too many")
                                || lower_error.contains("max usage");
                            let base_seconds = if rate_limited { 30_u64 } else { 15_u64 };
                            let backoff_seconds = (base_seconds
                                .saturating_mul(2_u64.saturating_pow(attempts.min(6) as u32)))
                            .min(900)
                                + (scan_mint.bytes().map(u64::from).sum::<u64>() % 11);
                            let retry_after = (chrono::Utc::now()
                                + chrono::Duration::seconds(backoff_seconds as i64))
                            .to_rfc3339();
                            match approved_registry.upsert_patch(&scan_mint,json!({"security":{
                                "status":"failed",
                                "scan_cache":{
                                    "status":"retry_wait",
                                    "verified":false,
                                    "attempts":attempts,
                                    "scan_attempt":attempts,
                                    "last_error":error,
                                    "rate_limited":rate_limited,
                                    "retry_after":retry_after
                                },
                                "decision":"UNVERIFIED",
                                "decision_reason":"Diagnostic-only mode is active. This scan failed before provider evidence could be fully collected; no token is being verified.",
                                "diagnostic_only":true,
                                "diagnostic_assessment":{"decision":"UNVERIFIED","decision_reason":error},
                                "error_details":error_details,
                                "provider_responses":[provider_response],
                                "checks":[failure_check],
                                "progress":100,
                                "stage":"failed",
                                "message":error
                            },"market":{"status":"not_started"}})){
                                Ok(snapshot)=>{
                                    if let Err(persist_error)=nodex_persistence::save_token_state(&approved_database,&snapshot).await{
                                        let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"token_persistence","sqlite",persist_error.to_string(),json!({"kind":"database_persistence_error","message":persist_error.to_string()}))}));
                                    }else{let _=scan_events.send(json!({"type":"token_state_updated","data":snapshot}));}
                                }
                                Err(state_error)=>{let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"token_state","nodex_token_registry",state_error.to_string(),json!({"kind":"token_registry_error","message":state_error.to_string()}))}));}
                            }
                            if let Err(persist_error) =
                                nodex_persistence::save_launch(&approved_database, &launch).await
                            {
                                let _=scan_events.send(json!({"type":"token_diagnostic","data":token_diagnostic(&scan_mint,"launch_persistence","sqlite",persist_error.to_string(),json!({"kind":"database_persistence_error","message":persist_error.to_string()}))}));
                            }
                        }
                    }
                    // Fail closed: only an explicit APPROVED verdict with all core
                    // verification checks passed may enter market monitoring.
                    let approved_for_market_monitoring =
                        approved_registry.get(&scan_mint).is_some_and(|state| {
                            token_approved_for_market_monitoring(&state.data["security"])
                        });

                    if !approved_for_market_monitoring {
                        match approved_registry.upsert_patch(
                            &scan_mint,
                            json!({"market":{
                                "status":"not_started",
                                "reason":"Security verification is not approved; market monitoring is restricted to verified tokens."
                            }}),
                        ) {
                            Ok(snapshot) => {
                                if let Err(error) =
                                    nodex_persistence::save_token_state(&approved_database, &snapshot)
                                        .await
                                {
                                    let _ = scan_events.send(json!({
                                        "type":"token_diagnostic",
                                        "data":token_diagnostic(
                                            &scan_mint,
                                            "market_monitoring_gate",
                                            "sqlite",
                                            error.to_string(),
                                            json!({"kind":"database_persistence_error","message":error.to_string()})
                                        )
                                    }));
                                } else {
                                    let _ = scan_events.send(json!({
                                        "type":"token_state_updated",
                                        "data":snapshot
                                    }));
                                }
                            }
                            Err(error) => {
                                let _ = scan_events.send(json!({
                                    "type":"token_diagnostic",
                                    "data":token_diagnostic(
                                        &scan_mint,
                                        "market_monitoring_gate",
                                        "nodex_token_registry",
                                        error.to_string(),
                                        json!({"kind":"token_registry_error","message":error.to_string()})
                                    )
                                }));
                            }
                        }
                    } else if let Some(monitor) = approved_monitor.read().await.clone() {
                        match monitor
                            .subscribe_pump_fun_bonding_curve(&scan_mint, &approved_program_id)
                            .await
                        {
                            Ok(account_address) => {
                                let _ = scan_events.send(
                                    json!({"type":"market_subscription_requested","data":{
                                        "mint_address":scan_mint,"account_address":account_address,
                                        "venue":"pump_fun_bonding_curve","status":"pending",
                                        "received_at":chrono::Utc::now().to_rfc3339()
                                    }}),
                                );
                            }
                            Err(error) => {
                                let _ = scan_events.send(
                                    json!({"type":"market_subscription_failed","data":{
                                        "mint_address":scan_mint,
                                        "venue":"pump_fun_bonding_curve",
                                        "status":"failed",
                                        "reason":error.to_string(),
                                        "message":"Market data unavailable",
                                        "received_at":chrono::Utc::now().to_rfc3339()
                                    }}),
                                );
                                let _ = scan_events.send(json!({"type":"diagnostic","data":{
                                    "severity":"warning","component":"market_monitor_registration",
                                    "message":error.to_string(),"mint_address":scan_mint,
                                    "received_at":chrono::Utc::now().to_rfc3339()
                                }}));
                            }
                        }
                    } else {
                        let _ =
                            scan_events.send(json!({"type":"market_subscription_failed","data":{
                                "mint_address":scan_mint,
                                "venue":"pump_fun_bonding_curve",
                                "status":"failed",
                                "reason":"Market monitor is unavailable",
                                "message":"Market data unavailable",
                                "received_at":chrono::Utc::now().to_rfc3339()
                            }}));
                    }
                    inflight_mints_for_scan.lock().await.remove(&scan_mint);
                });
            }
        });
    }

    // Requeue transient diagnostic scans when their persisted retry deadline expires.
    {
        let retry_registry = token_registry.clone();
        let retry_work_tx = work_tx.clone();
        let retry_events = events.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut scheduled = HashMap::<String, tokio::time::Instant>::new();
            loop {
                interval.tick().await;
                let now = chrono::Utc::now();
                for state in retry_registry.snapshot() {
                    let mint = state.mint_address.clone();
                    let cache = &state.data["security"]["scan_cache"];
                    let cache_status = cache["status"].as_str().unwrap_or_default();
                    if cache_status == "scanning" {
                        scheduled.remove(&mint);
                        continue;
                    }
                    if !matches!(cache_status, "retry_wait" | "completed_diagnostic") {
                        scheduled.remove(&mint);
                        continue;
                    }
                    if scheduled
                        .get(&mint)
                        .is_some_and(|queued_at| queued_at.elapsed() < Duration::from_secs(30))
                    {
                        continue;
                    }
                    scheduled.remove(&mint);
                    let retry_at = cache["retry_after"]
                        .as_str()
                        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                        .map(|value| value.with_timezone(&chrono::Utc));
                    if retry_at.as_ref().map(|deadline| deadline <= &now) != Some(true) {
                        continue;
                    }
                    let signature = state.data["discovery"]["signature"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    if signature.is_empty() {
                        continue;
                    }
                    let slot = state.data["discovery"]["slot"].as_u64().unwrap_or(0);
                    match retry_work_tx.try_send((signature, slot)) {
                        Ok(()) => {
                            scheduled.insert(mint.clone(), tokio::time::Instant::now());
                            let _ = retry_events.send(
                                json!({"type":"token_scan_retry_scheduled","data":{
                                    "mint_address":mint,
                                    "scan_attempt":cache["attempts"].as_u64().unwrap_or(0) + 1,
                                    "retry_after":retry_at.as_ref().map(|value| value.to_rfc3339()),
                                    "received_at":now.to_rfc3339()
                                }}),
                            );
                        }
                        Err(error) => {
                            let _ = retry_events.send(
                                json!({"type":"token_diagnostic","data":token_diagnostic(
                                    &mint,"token_scan_retry","launch_queue",
                                    format!("Could not enqueue scheduled security retry: {error}"),
                                    json!({"kind":"retry_queue_error","message":error.to_string()})
                                )}),
                            );
                        }
                    }
                }
            }
        });
    }

    // The producer owns the Solana notification stream. It validates the program tag,
    // extracts the confirmed transaction signature/slot, deduplicates, then hands work
    // to the bounded decoder queue. RPC decoding stays in the worker pool above.
    tokio::spawn(async move {
        loop {
            let notification = match receiver.recv().await {
                Ok(value) => value,
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    let _ = events.send(json!({"type":"diagnostic","data":{
                        "severity":"warning",
                        "component":"pump_fun_launch_queue",
                        "message":"Launch notification consumer lagged; stream backfill may replay missed signatures.",
                        "dropped_notifications":count,
                        "received_at":chrono::Utc::now().to_rfc3339()
                    }}));
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };

            if notification
                .get("method")
                .and_then(serde_json::Value::as_str)
                != Some("logsNotification")
                || notification
                    .get("nodex_program_id")
                    .and_then(serde_json::Value::as_str)
                    != Some(pump_fun_program_id.as_str())
                || {
                    let current = control.read().await;
                    current.paused || !current.token_discovery_enabled
                }
            {
                continue;
            }

            let Some(value) = notification.pointer("/params/result/value") else {
                continue;
            };
            if !value.get("err").is_some_and(serde_json::Value::is_null) {
                continue;
            }
            let Some(signature) = value
                .get("signature")
                .and_then(serde_json::Value::as_str)
                .filter(|signature| !signature.is_empty())
                .map(str::to_owned)
            else {
                continue;
            };
            let slot = notification
                .pointer("/params/result/context/slot")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();

            {
                let mut seen = seen_signatures.lock().await;
                if !seen.insert(signature.clone()) {
                    continue;
                }
                if seen.len() > 20_000 {
                    // Keep this bounded without retaining an unbounded process-lifetime set.
                    seen.clear();
                    seen.insert(signature.clone());
                }
            }

            if let Err(error) = work_tx.try_send((signature.clone(), slot)) {
                // Do not permanently suppress a signature that could not enter the queue.
                seen_signatures.lock().await.remove(&signature);
                let _ = events.send(json!({"type":"diagnostic","data":{
                    "severity":"warning",
                    "component":"pump_fun_launch_queue",
                    "message":format!("Could not enqueue launch transaction: {error}"),
                    "signature":signature,
                    "queue_capacity":LAUNCH_QUEUE_CAPACITY,
                    "received_at":chrono::Utc::now().to_rfc3339()
                }}));
            }
        }
    });
}
