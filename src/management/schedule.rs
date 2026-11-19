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

use super::{
    default_verbosity, history, identity, invalid, is_auto_verbosity, list_models, refine, state,
    valid_effort, valid_verbosity,
};

const NAMESPACE: &str = "pelican-schedule";
const KEY: &str = "plan";
const MINUTE_MS: u64 = 60_000;
const LEASE_MS: u64 = 420_000;
const MAX_MODEL_MS: u64 = 300_000;
const CAS_RETRIES: usize = 12;

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Settings {
    enabled: bool,
    client_key_id: String,
    model: String,
    effort: String,
    #[serde(default = "default_verbosity", skip_serializing_if = "is_auto_verbosity")]
    verbosity: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_enqueue: Option<PendingEnqueue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    history_save_failure: Option<SaveFailure>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PendingEnqueue {
    slot_ms: u64,
    started_at_ms: u64,
    client_key_id: String,
    key_name: Option<String>,
    model: String,
    effort: String,
    #[serde(default = "default_verbosity", skip_serializing_if = "is_auto_verbosity")]
    verbosity: String,
}

impl PendingEnqueue {
    fn into_task(self) -> refine::ScheduleTask {
        refine::ScheduleTask {
            slot_ms: self.slot_ms,
            started_at_ms: self.started_at_ms,
            client_key_id: self.client_key_id,
            key_name: self.key_name,
            model: self.model,
            effort: self.effort,
            verbosity: if self.verbosity.is_empty() {
                default_verbosity()
            } else {
                self.verbosity
            },
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SaveFailure {
    slot_ms: u64,
    reason: SaveFailureReason,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SaveFailureReason {
    Full,
    TooLarge,
    Unconfirmed,
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

    fn due_slot_in_window(&self, start_ms: u64, end_ms: u64) -> Result<Option<u64>, PluginFault> {
        if !self.settings.enabled {
            return Ok(None);
        }
        let end_minute = minute(end_ms);
        let mut anchor = minute(start_ms)
            .saturating_sub(1000)
            .max(self.armed_after_slot_ms)
            .max(self.last_claimed_slot_ms.unwrap_or(0));
        let rule = Rule::parse(&self.settings)?;
        let mut chosen = None;
        for _ in 0..16 {
            let Some(slot) = rule.next_after(anchor) else {
                break;
            };
            if slot > end_minute {
                break;
            }
            if slot > anchor && slot >= minute(start_ms) && slot.is_multiple_of(MINUTE_MS) {
                chosen = Some(slot);
            }
            anchor = slot;
        }
        Ok(chosen)
    }

    fn view(&self, now_ms: u64, active_since_ms: Option<u64>) -> Result<Value, PluginFault> {
        Ok(json!({
            "settings": self.settings,
            "nextRunAtMs": self.next_slot(now_ms)?,
            "runningSinceMs": self.pending.as_ref().map(|claim| claim.started_at_ms).or(active_since_ms),
            "historySaveFailure": self.history_save_failure,
        }))
    }
}

pub(super) async fn status(host: &HostClient) -> Result<Value, PluginFault> {
    let active_since = refine::active_scheduled_since_ms(host).await?;
    match read(host).await?.0 {
        Some(plan) => plan.view(now_ms(), active_since),
        None => Ok(json!({
            "settings": null,
            "nextRunAtMs": null,
            "runningSinceMs": active_since
        })),
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
        || !valid_verbosity(&settings.verbosity)
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
    let (last_claimed_slot_ms, pending, pending_enqueue, history_save_failure) = previous
        .map(|plan| {
            (
                plan.last_claimed_slot_ms,
                plan.pending,
                plan.pending_enqueue,
                plan.history_save_failure,
            )
        })
        .unwrap_or_default();
    let plan = Plan {
        settings,
        key_name: Some(key.name).filter(|name| identity::valid_text(name, 512)),
        armed_after_slot_ms: minute(now),
        last_claimed_slot_ms,
        pending,
        pending_enqueue,
        history_save_failure,
    };
    write(host, version, &plan).await?;
    let active_since = refine::active_scheduled_since_ms(host).await?;
    plan.view(now, active_since)
}

pub(super) async fn pause(host: &HostClient, payload: &[u8]) -> Result<Value, PluginFault> {
    if payload.len() > 64
        || !serde_json::from_slice::<Value>(payload)
            .is_ok_and(|value| value.as_object().is_some_and(serde_json::Map::is_empty))
    {
        return Err(invalid());
    }
    let active_since = refine::active_scheduled_since_ms(host).await?;
    let (Some(mut plan), version) = read(host).await? else {
        return Ok(json!({
            "settings": null,
            "nextRunAtMs": null,
            "runningSinceMs": active_since
        }));
    };
    if plan.settings.enabled {
        plan.settings.enabled = false;
        write(host, version, &plan).await?;
    }
    plan.view(now_ms(), active_since)
}

pub(crate) async fn reconcile(call: TypedCall<Empty>) -> Result<TypedReply<Empty>, PluginFault> {
    let started = Instant::now();
    let host = &call.host;
    let (plan, _) = read(host).await?;
    let now = now_ms();
    if let Some(claim) = plan.as_ref().and_then(|plan| plan.pending.as_ref()) {
        if now.saturating_sub(claim.started_at_ms) >= LEASE_MS {
            // Legacy v0.7.0 pending slot recovery: record interrupted without replaying.
            let mut record = record_for(claim);
            record.issue = Some(history::Issue::Interrupted);
            finish(host, claim.slot_ms, record).await?;
        }
        return Ok(done());
    }
    let budget_ms = call.context.timeout_ms;
    let reserve_ms = 10_000.min(budget_ms / 4);
    let model_budget = || {
        budget_ms
            .saturating_sub(started.elapsed().as_millis() as u64 + reserve_ms)
            .min(MAX_MODEL_MS)
    };
    let t0 = now_ms();
    flush_pending_enqueue(host).await?;
    enqueue_due_slot(host, t0, t0).await?;
    let budget = model_budget();
    refine::reconcile(host, (budget > 0).then_some(budget)).await?;
    let t1 = now_ms();
    if minute(t1) > minute(t0) {
        enqueue_due_slot(host, t0, t1).await?;
    }
    Ok(done())
}

async fn flush_pending_enqueue(host: &HostClient) -> Result<(), PluginFault> {
    let Some(plan) = read(host).await?.0 else {
        return Ok(());
    };
    let Some(pending) = plan.pending_enqueue else {
        return Ok(());
    };
    let slot_ms = pending.slot_ms;
    refine::enqueue_scheduled(host, pending.into_task()).await?;
    clear_pending_enqueue(host, slot_ms).await
}

async fn clear_pending_enqueue(host: &HostClient, slot_ms: u64) -> Result<(), PluginFault> {
    for _ in 0..CAS_RETRIES {
        let (Some(mut plan), version) = read(host).await? else {
            return Ok(());
        };
        if plan
            .pending_enqueue
            .as_ref()
            .is_none_or(|pending| pending.slot_ms != slot_ms)
        {
            return Ok(());
        }
        plan.pending_enqueue = None;
        match write(host, version, &plan).await {
            Ok(()) => return Ok(()),
            Err(error) if error.code == ErrorCode::Conflict => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn enqueue_due_slot(
    host: &HostClient,
    window_start_ms: u64,
    window_end_ms: u64,
) -> Result<(), PluginFault> {
    let (plan, version) = read(host).await?;
    let Some(mut plan) = plan else {
        return Ok(());
    };
    let Some(slot_ms) = plan.due_slot_in_window(window_start_ms, window_end_ms)? else {
        return Ok(());
    };
    if slot_ms > window_end_ms {
        return Ok(());
    }
    let pending = PendingEnqueue {
        slot_ms,
        started_at_ms: now_ms().max(slot_ms),
        client_key_id: plan.settings.client_key_id.clone(),
        key_name: plan.key_name.clone(),
        model: plan.settings.model.clone(),
        effort: plan.settings.effort.clone(),
        verbosity: plan.settings.verbosity.clone(),
    };
    // Record pending_enqueue together with last_claimed_slot_ms so a failure before enqueue completes is recovered next tick.
    plan.last_claimed_slot_ms = Some(slot_ms);
    plan.pending_enqueue = Some(pending.clone());
    match write(host, version, &plan).await {
        Ok(()) => {}
        Err(error) if error.code == ErrorCode::Conflict => return Ok(()),
        Err(error) => return Err(error),
    }
    refine::enqueue_scheduled(host, pending.into_task()).await?;
    clear_pending_enqueue(host, slot_ms).await
}

pub(super) async fn record_save_outcome(
    host: &HostClient,
    slot_ms: u64,
    reason: Option<SaveFailureReason>,
) -> Result<(), PluginFault> {
    let failure = reason.map(|reason| SaveFailure { slot_ms, reason });
    for _ in 0..CAS_RETRIES {
        let (Some(mut plan), version) = read(host).await? else {
            return Ok(());
        };
        if plan.history_save_failure.as_ref().map(|item| item.slot_ms)
            == failure.as_ref().map(|item| item.slot_ms)
            && plan.history_save_failure.as_ref().map(|item| item.reason as u8)
                == failure.as_ref().map(|item| item.reason as u8)
        {
            return Ok(());
        }
        plan.history_save_failure = failure;
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

fn record_for(claim: &Claim) -> history::Record {
    history::Record {
        id: history::scheduled_id(claim.slot_ms),
        started_at_ms: claim.started_at_ms,
        model: claim.model.clone(),
        effort: claim.effort.clone(),
        verbosity: default_verbosity(),
        trigger: history::Trigger::Scheduled,
        client_key_id: Some(claim.client_key_id.clone()),
        client_key_name: claim.key_name.clone(),
        scheduled_at_ms: Some(claim.slot_ms),
        ..history::Record::default()
    }
}

async fn finish(
    host: &HostClient,
    slot_ms: u64,
    record: history::Record,
) -> Result<(), PluginFault> {
    let failure = history::append(host, record)
        .await
        .err()
        .map(|error| SaveFailure {
            slot_ms,
            reason: match error.message.as_str() {
                history::FULL => SaveFailureReason::Full,
                history::TOO_LARGE => SaveFailureReason::TooLarge,
                _ => SaveFailureReason::Unconfirmed,
            },
        });
    // A failed archive write does not extend the claim or retry the model call.
    release(host, slot_ms, failure).await
}

async fn release(
    host: &HostClient,
    slot_ms: u64,
    failure: Option<SaveFailure>,
) -> Result<(), PluginFault> {
    for _ in 0..CAS_RETRIES {
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
        plan.history_save_failure = failure;
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
    state::put(host, NAMESPACE, KEY, version, plan)
        .await
        .map(|_| ())
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
                verbosity: "auto".into(),
                cron: "0 9 * * 1-5".into(),
                timezone: "Asia/Shanghai".into(),
            },
            key_name: None,
            armed_after_slot_ms: ms("2026-11-18T00:59:00Z"),
            last_claimed_slot_ms: None,
            pending: None,
            pending_enqueue: None,
            history_save_failure: None,
        }
    }

    #[test]
    fn due_slot_in_window_catches_cron_crossed_during_long_manual_run() {
        let mut daily = plan();
        daily.settings.cron = "0 9 * * *".into();
        let nine_am = ms("2026-11-18T01:00:00Z");
        daily.armed_after_slot_ms = nine_am - 120_000;
        let t0 = nine_am - 15_000;
        let t1 = nine_am + 165_000;
        assert_eq!(daily.next_slot(t1).unwrap(), Some(ms("2026-11-19T01:00:00Z")));
        assert_eq!(daily.due_slot_in_window(t0, t1).unwrap(), Some(nine_am));

        daily.armed_after_slot_ms = nine_am + 60_000;
        assert_eq!(daily.due_slot_in_window(t0, t1).unwrap(), None);
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
