mod run;

use std::time::Instant;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    client::{Empty, HostClient, TypedCall, TypedReply},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{history, identity, invalid, list_models, state, valid_effort};

const NAMESPACE: &str = "pelican-schedule";
const KEY: &str = "plan";
const MINUTE_MS: u64 = 60_000;
const LEASE_MS: u64 = 135_000;
const MAX_MODEL_MS: u64 = 90_000;

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Settings {
    enabled: bool,
    client_key_id: String,
    model: String,
    effort: String,
    cron: String,
    timezone: String,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Plan {
    settings: Settings,
    key_name: Option<String>,
    armed_after_slot_ms: u64,
    last_claimed_slot_ms: Option<u64>,
    pending: Option<Claim>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Claim {
    slot_ms: u64,
    started_at_ms: u64,
    client_key_id: String,
    key_name: Option<String>,
    model: String,
    effort: String,
}

struct Rule {
    cron: Cron,
    timezone: Tz,
}

impl Rule {
    fn parse(settings: &Settings) -> Result<Self, PluginFault> {
        if settings.cron.len() > 120 || settings.cron.split_ascii_whitespace().count() != 5 {
            return Err(invalid_cron());
        }
        let cron = settings.cron.parse().map_err(|_| invalid_cron())?;
        let timezone = settings
            .timezone
            .parse()
            .map_err(|_| PluginFault::new(ErrorCode::InvalidInput, "pelican schedule timezone"))?;
        Ok(Self { cron, timezone })
    }

    fn next_after(&self, after_ms: u64) -> Option<u64> {
        let start = DateTime::<Utc>::from_timestamp_millis(after_ms.try_into().ok()?)?
            .with_timezone(&self.timezone);
        let next = self.cron.find_next_occurrence(&start, false).ok()?;
        next.timestamp_millis().try_into().ok()
    }
}

impl Plan {
    fn next_slot(&self, now_ms: u64) -> Result<Option<u64>, PluginFault> {
        if !self.settings.enabled {
            return Ok(None);
        }
        // Only the current minute and future minutes are eligible, even after downtime.
        let anchor = minute(now_ms)
            .saturating_sub(1000)
            .max(self.armed_after_slot_ms)
            .max(self.last_claimed_slot_ms.unwrap_or(0));
        Ok(Rule::parse(&self.settings)?
            .next_after(anchor)
            .filter(|slot| {
                *slot > anchor && *slot >= minute(now_ms) && slot.is_multiple_of(MINUTE_MS)
            }))
    }

    fn view(&self, now_ms: u64) -> Result<Value, PluginFault> {
        Ok(json!({
            "settings": self.settings,
            "nextRunAtMs": self.next_slot(now_ms)?,
            "runningSinceMs": self.pending.as_ref().map(|claim| claim.started_at_ms),
        }))
    }
}

pub(super) async fn status(host: &HostClient) -> Result<Value, PluginFault> {
    match read(host).await?.0 {
        Some(plan) => plan.view(now_ms()),
        None => Ok(json!({"settings": null, "nextRunAtMs": null, "runningSinceMs": null})),
    }
}

pub(super) async fn save(host: &HostClient, payload: &[u8]) -> Result<Value, PluginFault> {
    if payload.len() > 4096 {
        return Err(invalid());
    }
    let mut settings: Settings = serde_json::from_slice(payload).map_err(|_| invalid())?;
    if !identity::valid_text(&settings.client_key_id, 200)
        || !identity::valid_text(&settings.model, 512)
        || !valid_effort(&settings.effort)
    {
        return Err(invalid());
    }
    settings.cron = settings
        .cron
        .split_ascii_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    settings.timezone = settings.timezone.trim().to_owned();
    let rule = Rule::parse(&settings)?;
    let key = identity::keys(host)
        .await?
        .into_iter()
        .find(|key| key.id == settings.client_key_id && key.enabled)
        .ok_or_else(|| PluginFault::new(ErrorCode::PermissionDenied, "pelican schedule key"))?;
    if !list_models(host, &key.id)
        .await?
        .iter()
        .any(|model| model == &settings.model)
    {
        return Err(PluginFault::new(
            ErrorCode::InvalidInput,
            "pelican schedule model",
        ));
    }
    let now = now_ms();
    if rule.next_after(minute(now)).is_none() {
        return Err(invalid_cron());
    }
    let (previous, version) = read(host).await?;
    let (last_claimed_slot_ms, pending) = previous
        .map(|plan| (plan.last_claimed_slot_ms, plan.pending))
        .unwrap_or_default();
    let plan = Plan {
        settings,
        key_name: Some(key.name).filter(|name| identity::valid_text(name, 512)),
        armed_after_slot_ms: minute(now),
        last_claimed_slot_ms,
        pending,
    };
    write(host, version, &plan).await?;
    plan.view(now)
}

pub(super) async fn pause(host: &HostClient, payload: &[u8]) -> Result<Value, PluginFault> {
    if payload.len() > 64
        || !serde_json::from_slice::<Value>(payload)
            .is_ok_and(|value| value.as_object().is_some_and(serde_json::Map::is_empty))
    {
        return Err(invalid());
    }
    let (Some(mut plan), version) = read(host).await? else {
        return Ok(json!({"settings": null, "nextRunAtMs": null, "runningSinceMs": null}));
    };
    if plan.settings.enabled {
        plan.settings.enabled = false;
        write(host, version, &plan).await?;
    }
    plan.view(now_ms())
}

pub(crate) async fn reconcile(call: TypedCall<Empty>) -> Result<TypedReply<Empty>, PluginFault> {
    let started = Instant::now();
    let host = &call.host;
    let (Some(mut plan), version) = read(host).await? else {
        return Ok(done());
    };
    let now = now_ms();
    if let Some(claim) = plan.pending {
        if now.saturating_sub(claim.started_at_ms) >= LEASE_MS {
            // If the final result was already saved, append keeps that record by ID.
            let mut record = run::record_for(&claim);
            record.issue = Some(history::Issue::Interrupted);
            history::append(host, record).await?;
            release(host, claim.slot_ms).await?;
        }
        return Ok(done());
    }
    let Some(slot_ms) = plan.next_slot(now)?.filter(|slot| *slot <= now) else {
        return Ok(done());
    };
    let claim = Claim {
        slot_ms,
        started_at_ms: now,
        client_key_id: plan.settings.client_key_id.clone(),
        key_name: plan.key_name.clone(),
        model: plan.settings.model.clone(),
        effort: plan.settings.effort.clone(),
    };
    plan.last_claimed_slot_ms = Some(slot_ms);
    plan.pending = Some(claim.clone());
    match write(host, version, &plan).await {
        Ok(()) => {}
        Err(error) if error.code == ErrorCode::Conflict => return Ok(done()),
        Err(error) => return Err(error),
    }
    // Commit the slot before sending the request. A crash or lost reply is not a retry signal.
    let budget_ms = call.context.timeout_ms;
    let reserve_ms = 10_000.min(budget_ms / 4);
    let remaining_ms = budget_ms.saturating_sub(started.elapsed().as_millis() as u64 + reserve_ms);
    let record = run::execute(host, &claim, remaining_ms.min(MAX_MODEL_MS)).await;
    history::append(host, record).await?;
    release(host, slot_ms).await?;
    Ok(done())
}

async fn release(host: &HostClient, slot_ms: u64) -> Result<(), PluginFault> {
    for _ in 0..4 {
        let (Some(mut plan), version) = read(host).await? else {
            return Ok(());
        };
        if plan
            .pending
            .as_ref()
            .is_none_or(|claim| claim.slot_ms != slot_ms)
        {
            return Ok(());
        }
        plan.pending = None;
        match write(host, version, &plan).await {
            Ok(()) => return Ok(()),
            Err(error) if error.code == ErrorCode::Conflict => continue,
            Err(error) => return Err(error),
        }
    }
    Err(PluginFault::new(
        ErrorCode::Conflict,
        "pelican schedule changed",
    ))
}

async fn read(host: &HostClient) -> Result<(Option<Plan>, Option<u64>), PluginFault> {
    state::get(host, NAMESPACE, KEY).await
}

async fn write(host: &HostClient, version: Option<u64>, plan: &Plan) -> Result<(), PluginFault> {
    state::put(host, NAMESPACE, KEY, version, plan).await
}

fn now_ms() -> u64 {
    Utc::now().timestamp_millis().try_into().unwrap_or(0)
}

fn minute(ms: u64) -> u64 {
    ms / MINUTE_MS * MINUTE_MS
}

fn invalid_cron() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, "pelican schedule cron")
}

fn done() -> TypedReply<Empty> {
    TypedReply::new(Empty {})
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(stamp: &str) -> u64 {
        DateTime::parse_from_rfc3339(stamp)
            .unwrap()
            .timestamp_millis() as u64
    }

    fn plan() -> Plan {
        Plan {
            settings: Settings {
                enabled: true,
                client_key_id: "fixture-key".into(),
                model: "fixture-model".into(),
                effort: "high".into(),
                cron: "0 9 * * 1-5".into(),
                timezone: "Asia/Shanghai".into(),
            },
            key_name: None,
            armed_after_slot_ms: ms("2026-11-18T00:59:00Z"),
            last_claimed_slot_ms: None,
            pending: None,
        }
    }

    #[test]
    fn cron_uses_the_plan_timezone_and_skips_missed_minutes() {
        let plan = plan();
        let wednesday = ms("2026-11-18T01:00:00Z");
        let thursday = ms("2026-11-19T01:00:00Z");
        assert_eq!(plan.next_slot(wednesday - 1).unwrap(), Some(wednesday));
        assert_eq!(plan.next_slot(wednesday + 59_999).unwrap(), Some(wednesday));
        assert_eq!(plan.next_slot(wednesday + 60_000).unwrap(), Some(thursday));
        assert_eq!(
            plan.next_slot(ms("2026-11-20T01:01:00Z")).unwrap(),
            Some(ms("2026-11-23T01:00:00Z"))
        );
    }

    #[test]
    fn arming_and_claiming_prevent_same_slot_dispatch_including_clock_rollback() {
        let mut plan = plan();
        let wednesday = ms("2026-11-18T01:00:00Z");
        let thursday = ms("2026-11-19T01:00:00Z");
        plan.armed_after_slot_ms = wednesday;
        assert_eq!(plan.next_slot(wednesday).unwrap(), Some(thursday));
        plan.last_claimed_slot_ms = Some(thursday);
        assert_eq!(
            plan.next_slot(wednesday).unwrap(),
            Some(ms("2026-11-20T01:00:00Z"))
        );
    }
}
