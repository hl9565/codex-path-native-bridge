use std::{
    hash::{BuildHasher, RandomState},
    time::Instant,
};

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
pub(super) const MAX_PLANS: usize = 10;
pub(super) const MISSING: &str = "pelican schedule missing";
pub(super) const FULL: &str = "pelican schedule full";
pub(super) const CLAIMED: &str = "pelican schedule claim pending";

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Settings {
    enabled: bool,
    client_key_id: String,
    model: String,
    effort: String,
    #[serde(default = "default_verbosity")]
    verbosity: String,
    cron: String,
    timezone: String,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
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

#[derive(Deserialize, Serialize)]
struct PlanEntry {
    id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    name: String,
    #[serde(flatten)]
    plan: Plan,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PlanStore {
    plans: Vec<PlanEntry>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StoredSchedule {
    Multi(PlanStore),
    Legacy(Box<Plan>),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SaveRequest {
    id: Option<String>,
    name: Option<String>,
    #[serde(flatten)]
    settings: Settings,
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
    #[serde(
        default = "default_verbosity",
        skip_serializing_if = "is_auto_verbosity"
    )]
    verbosity: String,
}

impl PendingEnqueue {
    fn into_task(self, plan_id: String) -> refine::ScheduleTask {
        refine::ScheduleTask {
            plan_id,
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

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SaveFailure {
    slot_ms: u64,
    reason: SaveFailureReason,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
}

impl PlanStore {
    async fn response(
        &self,
        host: &HostClient,
        saved_id: Option<String>,
    ) -> Result<Value, PluginFault> {
        let active = refine::active_scheduled_since_ms(host).await?;
        let now = now_ms();
        let plans = self
            .plans
            .iter()
            .map(|entry| {
                let plan = &entry.plan;
                let running_since = plan
                    .pending
                    .as_ref()
                    .map(|claim| claim.started_at_ms)
                    .or_else(|| {
                        active
                            .as_ref()
                            .filter(|(id, _)| id == &entry.id)
                            .map(|(_, since)| *since)
                    });
                Ok(json!({
                    "id": entry.id,
                    "name": entry.name,
                    "settings": plan.settings,
                    "keyName": plan.key_name,
                    "nextRunAtMs": plan.next_slot(now)?,
                    "runningSinceMs": running_since,
                    "historySaveFailure": plan.history_save_failure,
                }))
            })
            .collect::<Result<Vec<_>, PluginFault>>()?;
        let mut response = json!({ "plans": plans, "maxPlans": MAX_PLANS });
        if let Some(id) = saved_id {
            response["savedId"] = json!(id);
        }
        Ok(response)
    }

    fn find_mut(&mut self, id: &str) -> Option<&mut PlanEntry> {
        self.plans.iter_mut().find(|entry| entry.id == id)
    }

    fn claim_due_slots(
        &mut self,
        window_start_ms: u64,
        window_end_ms: u64,
    ) -> Result<bool, PluginFault> {
        let mut claimed = false;
        for entry in &mut self.plans {
            let plan = &mut entry.plan;
            if plan.pending.is_some() || plan.pending_enqueue.is_some() {
                continue;
            }
            let Some(slot_ms) = plan.due_slot_in_window(window_start_ms, window_end_ms)? else {
                continue;
            };
            // Persist the slot and its exact settings in the same CAS write.
            plan.last_claimed_slot_ms = Some(slot_ms);
            plan.pending_enqueue = Some(PendingEnqueue {
                slot_ms,
                started_at_ms: window_end_ms,
                client_key_id: plan.settings.client_key_id.clone(),
                key_name: plan.key_name.clone(),
                model: plan.settings.model.clone(),
                effort: plan.settings.effort.clone(),
                verbosity: plan.settings.verbosity.clone(),
            });
            claimed = true;
        }
        Ok(claimed)
    }
}

pub(super) async fn status(host: &HostClient) -> Result<Value, PluginFault> {
    read(host).await?.0.response(host, None).await
}

pub(super) async fn save(host: &HostClient, payload: &[u8]) -> Result<Value, PluginFault> {
    if payload.len() > 4096 {
        return Err(invalid());
    }
    let SaveRequest {
        id,
        name,
        mut settings,
    } = serde_json::from_slice(payload).map_err(|_| invalid())?;
    let name = name.map(|name| name.trim().to_owned());
    let id = id.as_deref().filter(|id| !id.is_empty());
    if id.is_some_and(|id| !history::valid_plan_id(id))
        || name
            .as_ref()
            .is_some_and(|name| name.chars().count() > 64 || name.chars().any(char::is_control))
        || !identity::valid_text(&settings.client_key_id, 200)
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
    if !list_models(host, &key.id).await?.contains(&settings.model) {
        return Err(PluginFault::new(
            ErrorCode::InvalidInput,
            "pelican schedule model",
        ));
    }
    let key_name = Some(key.name).filter(|name| identity::valid_text(name, 512));
    let (store, saved_id) = update(host, |store| {
        let armed = minute(now_ms());
        if rule.next_after(armed).is_none() {
            return Err(invalid_cron());
        }
        if let Some(id) = id {
            let entry = store.find_mut(id).ok_or_else(missing)?;
            if let Some(name) = &name {
                entry.name.clone_from(name);
            }
            entry.plan.settings.clone_from(&settings);
            entry.plan.key_name.clone_from(&key_name);
            entry.plan.armed_after_slot_ms = armed;
            return Ok((true, id.to_owned()));
        }
        if store.plans.len() >= MAX_PLANS {
            return Err(PluginFault::new(ErrorCode::Conflict, FULL));
        }
        let id = loop {
            let candidate = format!("{:08x}", RandomState::new().hash_one(armed) as u32);
            if store.plans.iter().all(|entry| entry.id != candidate) {
                break candidate;
            }
        };
        store.plans.push(PlanEntry {
            id: id.clone(),
            name: name.clone().unwrap_or_default(),
            plan: Plan {
                settings: settings.clone(),
                key_name: key_name.clone(),
                armed_after_slot_ms: armed,
                last_claimed_slot_ms: None,
                pending: None,
                pending_enqueue: None,
                history_save_failure: None,
            },
        });
        Ok((true, id))
    })
    .await?;
    store.response(host, Some(saved_id)).await
}

pub(super) async fn pause(host: &HostClient, payload: &[u8]) -> Result<Value, PluginFault> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Toggle {
        id: Option<String>,
        #[serde(default)]
        enabled: bool,
    }
    if payload.len() > 128 {
        return Err(invalid());
    }
    let Toggle { id, enabled } = serde_json::from_slice(payload).map_err(|_| invalid())?;
    if id.as_deref().is_some_and(|id| !history::valid_plan_id(id)) || (enabled && id.is_none()) {
        return Err(invalid());
    }
    let (store, ()) = update(host, |store| {
        let entries = match &id {
            Some(id) => std::slice::from_mut(store.find_mut(id).ok_or_else(missing)?),
            None => store.plans.as_mut_slice(),
        };
        let mut changed = false;
        for entry in entries {
            if entry.plan.settings.enabled != enabled {
                entry.plan.settings.enabled = enabled;
                if enabled {
                    entry.plan.armed_after_slot_ms = minute(now_ms());
                }
                changed = true;
            }
        }
        Ok((changed, ()))
    })
    .await?;
    store.response(host, None).await
}

pub(super) async fn delete(host: &HostClient, payload: &[u8]) -> Result<Value, PluginFault> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Delete {
        id: String,
    }
    if payload.len() > 128 {
        return Err(invalid());
    }
    let Delete { id } = serde_json::from_slice(payload).map_err(|_| invalid())?;
    if !history::valid_plan_id(&id) {
        return Err(invalid());
    }
    let (store, ()) = update(host, |store| {
        let index = store
            .plans
            .iter()
            .position(|entry| entry.id == id)
            .ok_or_else(missing)?;
        let plan = &store.plans[index].plan;
        // Let the handoff finish before removing the durable claim.
        if plan.pending.is_some() || plan.pending_enqueue.is_some() {
            return Err(PluginFault::new(ErrorCode::Conflict, CLAIMED));
        }
        store.plans.remove(index);
        Ok((true, ()))
    })
    .await?;
    store.response(host, None).await
}

pub(crate) async fn reconcile(call: TypedCall<Empty>) -> Result<TypedReply<Empty>, PluginFault> {
    let started = Instant::now();
    let host = &call.host;
    let (store, ()) = update(host, |_| Ok((false, ()))).await?;
    let now = now_ms();
    let mut has_live_legacy_claim = false;
    for entry in &store.plans {
        if let Some(claim) = &entry.plan.pending {
            if now.saturating_sub(claim.started_at_ms) >= LEASE_MS {
                recover_claim(host, &entry.id, claim).await?;
            } else {
                has_live_legacy_claim = true;
            }
        }
    }
    if has_live_legacy_claim {
        return Ok(done());
    }
    flush_pending_enqueue(host, store).await?;
    let t0 = now_ms();
    enqueue_due_slots(host, t0, t0).await?;
    let budget_ms = call.context.timeout_ms;
    let reserve_ms = 10_000.min(budget_ms / 4);
    let budget = budget_ms
        .saturating_sub(started.elapsed().as_millis() as u64 + reserve_ms)
        .min(MAX_MODEL_MS);
    let result = refine::reconcile(host, (budget > 0).then_some(budget)).await;
    let t1 = now_ms();
    if minute(t1) > minute(t0) {
        enqueue_due_slots(host, t0, t1).await?;
    }
    result?;
    Ok(done())
}

pub(super) async fn has_pending_enqueue(
    host: &HostClient,
    plan_id: &str,
    slot_ms: u64,
) -> Result<bool, PluginFault> {
    Ok(read(host).await?.0.plans.iter().any(|entry| {
        entry.id == plan_id
            && entry
                .plan
                .pending_enqueue
                .as_ref()
                .is_some_and(|pending| pending.slot_ms == slot_ms)
    }))
}

async fn flush_pending_enqueue(host: &HostClient, store: PlanStore) -> Result<(), PluginFault> {
    for entry in store.plans {
        let Some(pending) = entry.plan.pending_enqueue else {
            continue;
        };
        let slot_ms = pending.slot_ms;
        match refine::enqueue_scheduled(host, pending.into_task(entry.id.clone())).await {
            Ok(()) => {}
            // A full queue can drain below. Keep the claim for the next tick.
            Err(error) if error.message == refine::QUEUE_FULL => continue,
            Err(error) => return Err(error),
        }
        update(host, |store| {
            let Some(entry) = store.find_mut(&entry.id) else {
                return Ok((false, ()));
            };
            if entry
                .plan
                .pending_enqueue
                .as_ref()
                .is_none_or(|pending| pending.slot_ms != slot_ms)
            {
                return Ok((false, ()));
            }
            entry.plan.pending_enqueue = None;
            Ok((true, ()))
        })
        .await?;
    }
    Ok(())
}

async fn enqueue_due_slots(
    host: &HostClient,
    window_start_ms: u64,
    window_end_ms: u64,
) -> Result<(), PluginFault> {
    let (store, claimed) = update(host, |store| {
        let claimed = store.claim_due_slots(window_start_ms, window_end_ms)?;
        Ok((claimed, claimed))
    })
    .await?;
    if claimed {
        flush_pending_enqueue(host, store).await?;
    }
    Ok(())
}

pub(super) async fn record_save_outcome(
    host: &HostClient,
    plan_id: &str,
    slot_ms: u64,
    reason: Option<SaveFailureReason>,
) -> Result<(), PluginFault> {
    let failure = reason.map(|reason| SaveFailure { slot_ms, reason });
    update(host, |store| {
        let Some(entry) = store.find_mut(plan_id) else {
            return Ok((false, ()));
        };
        let warning = &mut entry.plan.history_save_failure;
        if *warning == failure || warning.is_some_and(|warning| warning.slot_ms > slot_ms) {
            return Ok((false, ()));
        }
        *warning = failure;
        Ok((true, ()))
    })
    .await?;
    Ok(())
}

async fn recover_claim(host: &HostClient, plan_id: &str, claim: &Claim) -> Result<(), PluginFault> {
    let record = history::Record {
        id: if plan_id == history::DEFAULT_PLAN_ID {
            history::scheduled_id(claim.slot_ms)
        } else {
            history::scheduled_plan_id(plan_id, claim.slot_ms)
        },
        started_at_ms: claim.started_at_ms,
        model: claim.model.clone(),
        effort: claim.effort.clone(),
        verbosity: default_verbosity(),
        trigger: history::Trigger::Scheduled,
        client_key_id: Some(claim.client_key_id.clone()),
        client_key_name: claim.key_name.clone(),
        scheduled_at_ms: Some(claim.slot_ms),
        issue: Some(history::Issue::Interrupted),
        ..history::Record::default()
    };
    let failure = history::append(host, record)
        .await
        .err()
        .map(|error| SaveFailure {
            slot_ms: claim.slot_ms,
            reason: match error.message.as_str() {
                history::FULL => SaveFailureReason::Full,
                history::TOO_LARGE => SaveFailureReason::TooLarge,
                _ => SaveFailureReason::Unconfirmed,
            },
        });
    update(host, |store| {
        let Some(entry) = store.find_mut(plan_id) else {
            return Ok((false, ()));
        };
        let plan = &mut entry.plan;
        if plan
            .pending
            .as_ref()
            .is_none_or(|pending| pending.slot_ms != claim.slot_ms)
        {
            return Ok((false, ()));
        }
        plan.pending = None;
        plan.history_save_failure = failure;
        Ok((true, ()))
    })
    .await?;
    Ok(())
}

async fn read(host: &HostClient) -> Result<(PlanStore, Option<u64>, bool), PluginFault> {
    let (stored, version) = state::get(host, NAMESPACE, KEY).await?;
    let (store, legacy) = match stored {
        Some(StoredSchedule::Multi(store)) => (store, false),
        Some(StoredSchedule::Legacy(plan)) => (
            PlanStore {
                plans: vec![PlanEntry {
                    id: history::DEFAULT_PLAN_ID.into(),
                    name: String::new(),
                    plan: *plan,
                }],
            },
            true,
        ),
        None => (PlanStore::default(), false),
    };
    Ok((store, version, legacy))
}

async fn update<T>(
    host: &HostClient,
    mut change: impl FnMut(&mut PlanStore) -> Result<(bool, T), PluginFault>,
) -> Result<(PlanStore, T), PluginFault> {
    for _ in 0..CAS_RETRIES {
        let (mut store, version, legacy) = read(host).await?;
        let (changed, result) = change(&mut store)?;
        if !changed && !legacy {
            return Ok((store, result));
        }
        match state::put(host, NAMESPACE, KEY, version, &store).await {
            Ok(_) => return Ok((store, result)),
            Err(error) if error.code == ErrorCode::Conflict => continue,
            Err(error) => return Err(error),
        }
    }
    Err(PluginFault::new(
        ErrorCode::Conflict,
        "pelican schedule changed",
    ))
}

fn missing() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, MISSING)
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
    fn claims_each_plan_due_during_a_long_manual_run() {
        let nine_am = ms("2026-11-18T01:00:00Z");
        let t0 = nine_am - 15_000;
        let t1 = nine_am + 165_000;
        let plans = [
            ("first", "0 9 * * *"),
            ("second", "1 9 * * *"),
            ("rearmed", "0 9 * * *"),
        ]
        .into_iter()
        .map(|(id, cron)| {
            let mut plan = plan();
            plan.settings.cron = cron.into();
            plan.settings.model = id.into();
            plan.armed_after_slot_ms = if id == "rearmed" {
                nine_am + 60_000
            } else {
                nine_am - 120_000
            };
            PlanEntry {
                id: id.into(),
                name: String::new(),
                plan,
            }
        })
        .collect();
        let mut store = PlanStore { plans };
        assert!(!store.claim_due_slots(t0, t0).unwrap());
        assert!(store.claim_due_slots(t0, t1).unwrap());
        for (entry, slot) in store.plans.iter_mut().zip([nine_am, nine_am + 60_000]) {
            assert_eq!(entry.plan.last_claimed_slot_ms, Some(slot));
            let pending = entry.plan.pending_enqueue.take().unwrap();
            assert_eq!(pending.slot_ms, slot);
            assert_eq!(pending.model, entry.id);
            assert_eq!(pending.started_at_ms, t1);
        }
        assert!(store.plans[2].plan.last_claimed_slot_ms.is_none());
        assert!(!store.claim_due_slots(t0, t1).unwrap());
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
