use chrono::Utc;
use gateway_plugin_sdk::{ErrorCode, PluginFault, client::HostClient};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    default_verbosity,
    generation,
    history::{
        self, Issue, Outcome, Record, RefineAction, RefineRound, Refinement, RefinementEnd, Trigger,
    },
    identity, invalid, is_auto_verbosity, list_models,
    schedule::{self, SaveFailureReason},
    state, valid_effort, valid_verbosity,
};

const NAMESPACE: &str = "pelican-refine";
const KEY: &str = "job";
const MAX_ROUNDS: usize = 1;
const LEGACY_MAX_ROUNDS: usize = 3;
const MAX_MANUAL_QUEUE: usize = 8;
const MAX_QUEUE: usize = MAX_MANUAL_QUEUE + schedule::MAX_PLANS;
pub(super) const QUEUE_FULL: &str = "pelican refine queue full";
const CAS_RETRIES: usize = 12;
const MAX_SAVE_ATTEMPTS: u8 = 2;
const LEASE_MS: u64 = 420_000;
const MAX_SOURCE_JSON: usize = 850 * 1024;
const PART_BYTES: usize = 48 * 1024 - 2;
const MAX_PARTS: usize = 20;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StartRequest {
    id: String,
    client_key_id: String,
    model: String,
    effort: String,
    #[serde(default = "default_verbosity")]
    verbosity: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Selection {
    id: String,
    client_key_id: String,
    model: String,
    effort: String,
    #[serde(default = "default_verbosity", skip_serializing_if = "is_auto_verbosity")]
    verbosity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trigger: Option<Trigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scheduled_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scheduled_plan_id: Option<String>,
}

impl Selection {
    fn plan_id(&self) -> Option<&str> {
        if self.effective_trigger() != Trigger::Scheduled {
            return None;
        }
        self.scheduled_plan_id
            .as_deref()
            .or_else(|| history::scheduled_id_parts(&self.id).map(|(plan_id, _)| plan_id))
    }

    fn effective_trigger(&self) -> Trigger {
        if let Some(trigger) = self.trigger {
            return trigger;
        }
        if history::valid_refinement_id(&self.id) {
            Trigger::Refined
        } else if self.scheduled_at_ms.is_some() {
            Trigger::Scheduled
        } else {
            Trigger::Manual
        }
    }

    fn effective_verbosity(&self) -> String {
        if self.verbosity.is_empty() {
            default_verbosity()
        } else {
            self.verbosity.clone()
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueuedTask {
    selection: Selection,
    started_at_ms: u64,
    client_key_name: Option<String>,
}

impl QueuedTask {
    fn into_job(self) -> Job {
        Job {
            record: Record {
                started_at_ms: self.started_at_ms,
                client_key_name: self.client_key_name,
                ..Record::default()
            },
            selection: self.selection,
            rounds: Vec::new(),
            initial: None,
            latest: None,
            phase: Phase::Queued,
            stop_requested: false,
            save_attempts: 0,
        }
    }

    fn view(&self) -> Value {
        json!({
            "id": self.selection.id,
            "trigger": self.selection.effective_trigger(),
            "model": self.selection.model,
            "effort": self.selection.effort,
            "verbosity": self.selection.effective_verbosity(),
            "startedAtMs": self.started_at_ms,
        })
    }
}

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueueState {
    active: Option<Job>,
    queue: Vec<QueuedTask>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StoredState {
    Queue(QueueState),
    Legacy(Box<Job>),
}

impl QueueState {
    fn response(&self) -> Value {
        let queue: Vec<Value> = self.queue.iter().map(QueuedTask::view).collect();
        json!({
            "job": self.active.as_ref().map(Job::view),
            "queue": queue,
            "queuedCount": self.queue.len(),
            "maxRounds": MAX_ROUNDS,
        })
    }

    fn has_id(&self, id: &str) -> bool {
        self.active.as_ref().is_some_and(|job| job.selection.id == id)
            || self.queue.iter().any(|item| item.selection.id == id)
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Job {
    selection: Selection,
    record: Record,
    rounds: Vec<RefineRound>,
    initial: Option<Document>,
    latest: Option<Document>,
    phase: Phase,
    stop_requested: bool,
    #[serde(default, skip_serializing_if = "is_zero_u8")]
    save_attempts: u8,
}

fn is_zero_u8(value: &u8) -> bool {
    *value == 0
}

#[derive(Clone, Copy, Deserialize, Serialize)]
struct Document {
    round: usize,
    parts: usize,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum Phase {
    Queued,
    Running {
        since_ms: u64,
    },
    Saving {
        end: RefinementEnd,
    },
    Done {
        end: RefinementEnd,
        archive: Archive,
    },
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Archive {
    Saved,
    Full,
    TooLarge,
    Unconfirmed,
}

impl Job {
    fn view(&self) -> Value {
        let (status, end, archive) = match self.phase {
            Phase::Queued => ("queued", None, None),
            Phase::Running { .. } => ("running", None, None),
            Phase::Saving { end } => ("saving", Some(end), None),
            Phase::Done { end, archive } => ("done", Some(end), Some(archive)),
        };
        json!({
            "id": self.selection.id,
            "trigger": self.selection.effective_trigger(),
            "model": self.selection.model,
            "effort": self.selection.effort,
            "verbosity": self.selection.effective_verbosity(),
            "status": status,
            "rounds": self.rounds.len(),
            "end": end,
            "archive": archive,
            "stopRequested": self.stop_requested,
            "hasDraft": self.latest.is_some(),
            "hasComparison": self.latest.is_some_and(|draft| draft.round > 1),
        })
    }

    fn seed(&self) -> Record {
        Record {
            id: self.selection.id.clone(),
            started_at_ms: self.record.started_at_ms,
            model: self.selection.model.clone(),
            effort: self.selection.effort.clone(),
            verbosity: self.selection.effective_verbosity(),
            client_key_id: Some(self.selection.client_key_id.clone()),
            client_key_name: self.record.client_key_name.clone(),
            trigger: self.selection.effective_trigger(),
            scheduled_at_ms: self.selection.scheduled_at_ms,
            ..Record::default()
        }
    }

    fn is_busy(&self) -> bool {
        !matches!(self.phase, Phase::Done { .. })
    }
}

pub(super) struct ScheduleTask {
    pub plan_id: String,
    pub slot_ms: u64,
    pub started_at_ms: u64,
    pub client_key_id: String,
    pub key_name: Option<String>,
    pub model: String,
    pub effort: String,
    pub verbosity: String,
}

pub(super) async fn status(host: &HostClient) -> Result<Value, PluginFault> {
    Ok(read_state(host).await?.0.response())
}

pub(super) async fn active_scheduled_since_ms(
    host: &HostClient,
) -> Result<Option<(String, u64)>, PluginFault> {
    let (state, _) = read_state(host).await?;
    Ok(state.active.and_then(|job| {
        let since_ms = match job.phase {
            Phase::Running { since_ms } => since_ms,
            Phase::Saving { .. } => job.record.started_at_ms,
            _ => return None,
        };
        Some((job.selection.plan_id()?.to_owned(), since_ms))
    }))
}

pub(super) async fn start(host: &HostClient, payload: &[u8]) -> Result<Value, PluginFault> {
    if payload.len() > 4096 {
        return Err(invalid());
    }
    let req: StartRequest = serde_json::from_slice(payload).map_err(|_| invalid())?;
    if !history::valid_manual_id(&req.id) {
        return Err(invalid());
    }
    let trigger = Trigger::Manual;
    if !identity::valid_text(&req.client_key_id, 200)
        || !identity::valid_text(&req.model, 512)
        || !valid_effort(&req.effort)
        || !valid_verbosity(&req.verbosity)
    {
        return Err(invalid());
    }
    let initial_state = read_state(host).await?.0;
    if initial_state.has_id(&req.id) {
        return Ok(initial_state.response());
    }
    if history::contains(host, &req.id).await? {
        return Err(conflict());
    }
    let key = identity::keys(host)
        .await?
        .into_iter()
        .find(|key| key.id == req.client_key_id && key.enabled)
        .ok_or_else(|| PluginFault::new(ErrorCode::PermissionDenied, "pelican refine key"))?;
    if !list_models(host, &key.id).await?.contains(&req.model) {
        return Err(PluginFault::new(
            ErrorCode::InvalidInput,
            "pelican refine model",
        ));
    }
    let task = QueuedTask {
        selection: Selection {
            id: req.id.clone(),
            client_key_id: req.client_key_id,
            model: req.model,
            effort: req.effort,
            verbosity: req.verbosity,
            trigger: Some(trigger),
            scheduled_at_ms: None,
            scheduled_plan_id: None,
        },
        started_at_ms: now_ms(),
        client_key_name: Some(key.name).filter(|name| identity::valid_text(name, 512)),
    };
    for _ in 0..CAS_RETRIES {
        let (mut state, version) = read_state(host).await?;
        if state.has_id(&req.id) {
            return Ok(state.response());
        }
        if state
            .active
            .as_ref()
            .is_none_or(|job| !job.is_busy())
            && state.queue.is_empty()
        {
            state.active = Some(task.clone().into_job());
        } else {
            let manual_queued = state
                .queue
                .iter()
                .filter(|item| item.selection.effective_trigger() != Trigger::Scheduled)
                .count();
            if manual_queued >= MAX_MANUAL_QUEUE || state.queue.len() >= MAX_QUEUE {
                return Err(queue_full());
            }
            state.queue.push(task.clone());
        }
        match write_state(host, version, &state).await {
            Ok(_) => return Ok(state.response()),
            Err(error) if error.code == ErrorCode::Conflict => continue,
            Err(error) => return Err(error),
        }
    }
    Err(conflict())
}

pub(super) async fn enqueue_scheduled(
    host: &HostClient,
    task: ScheduleTask,
) -> Result<(), PluginFault> {
    let id = history::scheduled_plan_id(&task.plan_id, task.slot_ms);
    let legacy_id =
        (task.plan_id == history::DEFAULT_PLAN_ID).then(|| history::scheduled_id(task.slot_ms));
    let queued = QueuedTask {
        selection: Selection {
            id: id.clone(),
            client_key_id: task.client_key_id,
            model: task.model,
            effort: task.effort,
            verbosity: task.verbosity,
            trigger: Some(Trigger::Scheduled),
            scheduled_at_ms: Some(task.slot_ms),
            scheduled_plan_id: Some(task.plan_id.clone()),
        },
        started_at_ms: task.started_at_ms.max(task.slot_ms),
        client_key_name: task.key_name,
    };
    for _ in 0..CAS_RETRIES {
        let (mut state, version) = read_state(host).await?;
        if state.has_id(&id) || legacy_id.as_ref().is_some_and(|id| state.has_id(id)) {
            return Ok(());
        }
        let same_plan = |selection: &Selection| selection.plan_id() == Some(task.plan_id.as_str());
        let active_same_plan = state
            .active
            .as_ref()
            .is_some_and(|job| job.is_busy() && same_plan(&job.selection));
        let queued_same_plan = state.queue.iter().any(|item| same_plan(&item.selection));
        if active_same_plan || queued_same_plan || history::contains(host, &id).await? {
            return Ok(());
        }
        if let Some(legacy_id) = &legacy_id
            && history::contains(host, legacy_id).await?
        {
            return Ok(());
        }
        if !schedule::has_pending_enqueue(host, &task.plan_id, task.slot_ms).await? {
            return Ok(());
        }
        let scheduled_queued = state
            .queue
            .iter()
            .filter(|item| item.selection.effective_trigger() == Trigger::Scheduled)
            .count();
        if state.active.as_ref().is_none_or(|job| !job.is_busy()) && state.queue.is_empty() {
            state.active = Some(queued.clone().into_job());
        } else if scheduled_queued < schedule::MAX_PLANS && state.queue.len() < MAX_QUEUE {
            state.queue.push(queued.clone());
        } else {
            return Err(queue_full());
        }
        match write_state(host, version, &state).await {
            Ok(_) => return Ok(()),
            Err(error) if error.code == ErrorCode::Conflict => continue,
            Err(error) => return Err(error),
        }
    }
    Err(conflict())
}

pub(super) async fn stop(host: &HostClient, payload: &[u8]) -> Result<Value, PluginFault> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Stop {
        id: String,
    }
    if payload.len() > 128 {
        return Err(invalid());
    }
    let Stop { id } = serde_json::from_slice(payload).map_err(|_| invalid())?;
    if !valid_job_id(&id) {
        return Err(invalid());
    }
    for _ in 0..CAS_RETRIES {
        let (mut state, version) = read_state(host).await?;
        if let Some(job) = state.active.as_mut()
            && job.selection.id == id
        {
            if !matches!(job.phase, Phase::Queued | Phase::Running { .. }) || job.stop_requested {
                return Ok(state.response());
            }
            job.stop_requested = true;
            match write_state(host, version, &state).await {
                Ok(_) => return Ok(state.response()),
                Err(error) if error.code == ErrorCode::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
        if let Some(index) = state.queue.iter().position(|item| item.selection.id == id) {
            state.queue.remove(index);
            match write_state(host, version, &state).await {
                Ok(_) => return Ok(state.response()),
                Err(error) if error.code == ErrorCode::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
        return Err(missing());
    }
    Err(conflict())
}

pub(super) async fn document(host: &HostClient, query: &str) -> Result<Value, PluginFault> {
    let (id, version) = query
        .strip_prefix("id=")
        .and_then(|query| query.split_once("&version="))
        .ok_or_else(invalid)?;
    let record = load_draft_record(host, id, version).await?;
    Ok(json!({ "record": record }))
}

pub(super) async fn draft_source(
    host: &HostClient,
    id: &str,
    version: &str,
) -> Result<Option<String>, PluginFault> {
    let document = {
        if !valid_job_id(id) || !matches!(version, "first" | "latest") {
            return Err(invalid());
        }
        let Some(job) = read_state(host).await?.0.active else {
            return Ok(None);
        };
        if job.selection.id != id {
            return Ok(None);
        }
        let Some(doc) = (match version {
            "first" => job.initial,
            _ => job.latest,
        }) else {
            return Ok(None);
        };
        doc
    };
    let source = load_document(host, document).await?;
    if read_state(host)
        .await?
        .0
        .active
        .is_none_or(|current| current.selection.id != id)
    {
        return Ok(None);
    }
    Ok(Some(source))
}

async fn load_draft_record(
    host: &HostClient,
    id: &str,
    version: &str,
) -> Result<Record, PluginFault> {
    if !valid_job_id(id) || !matches!(version, "first" | "latest") {
        return Err(invalid());
    }
    let Some(job) = read_state(host).await?.0.active else {
        return Err(missing());
    };
    if job.selection.id != id {
        return Err(missing());
    }
    let document = match version {
        "first" => job.initial,
        _ => job.latest,
    }
    .ok_or_else(missing)?;
    let source = load_document(host, document).await?;
    // Starting another task can reuse these fixed staging keys.
    if read_state(host)
        .await?
        .0
        .active
        .is_none_or(|current| current.selection.id != id)
    {
        return Err(missing());
    }
    let mut record = job.record;
    if record.id.is_empty() {
        record.id = job.selection.id.clone();
        record.model = job.selection.model.clone();
        record.effort = job.selection.effort.clone();
        record.verbosity = job.selection.effective_verbosity();
        record.trigger = job.selection.effective_trigger();
        record.scheduled_at_ms = job.selection.scheduled_at_ms;
    }
    if record.verbosity.is_empty() {
        record.verbosity = job.selection.effective_verbosity();
    }
    if document.round == 1 && job.latest.is_some_and(|latest| latest.round != 1) {
        let first = job.rounds.first().ok_or_else(invalid)?;
        record.model = first.model.clone();
        record.duration_ms = first.duration_ms;
        record.first_content_ms = first.first_content_ms;
        record.input_tokens = first.input_tokens;
        record.output_tokens = first.output_tokens;
        record.cached_tokens = first.cached_tokens;
        record.reasoning_tokens = first.reasoning_tokens;
        record.account_id = None;
        record.account_name = None;
    }
    record.source = Some(source);
    Ok(record)
}

// A call to reconcile can consume at most one round. Before dispatch, commit
// the claim. An unknown response after restart must not send that round again.
pub(super) async fn reconcile(host: &HostClient, budget: Option<u64>) -> Result<(), PluginFault> {
    let (mut state, mut version) = read_state(host).await?;
    if state
        .active
        .as_ref()
        .is_none_or(|job| !job.is_busy())
    {
        if state.queue.is_empty() {
            return Ok(());
        }
        let next = state.queue.remove(0);
        state.active = Some(next.into_job());
        match write_state(host, version, &state).await {
            Ok(next_version) => version = Some(next_version),
            Err(error) if error.code == ErrorCode::Conflict => return Ok(()),
            Err(error) => return Err(error),
        }
    }
    let Some(mut job) = state.active.clone() else {
        return Ok(());
    };
    match job.phase {
        Phase::Done { .. } => return Ok(()),
        Phase::Saving { end } => return archive(host, job, end).await,
        Phase::Running { since_ms } => {
            if now_ms().saturating_sub(since_ms) >= LEASE_MS {
                let mut interrupted = job.seed();
                interrupted.issue = Some(Issue::Interrupted);
                job.rounds.push(RefineRound::from_record(
                    &interrupted,
                    RefineAction::Interrupted,
                ));
                if job.latest.is_none() {
                    job.record = interrupted;
                }
                return finalize(host, job, RefinementEnd::Interrupted).await;
            }
            return Ok(());
        }
        Phase::Queued if job.stop_requested => {
            return finalize(host, job, RefinementEnd::Stopped).await;
        }
        Phase::Queued => {}
    }
    let Some(budget) = budget.filter(|budget| *budget > 0) else {
        return Ok(());
    };
    if job.rounds.len() >= MAX_ROUNDS || job.latest.is_some() {
        return finalize(host, job, RefinementEnd::LimitReached).await;
    }
    let since_ms = now_ms();
    job.phase = Phase::Running { since_ms };
    state.active = Some(job.clone());
    match write_state(host, version, &state).await {
        Ok(_) => {}
        Err(error) if error.code == ErrorCode::Conflict => return Ok(()),
        Err(error) => return Err(error),
    }
    let round = job.rounds.len() + 1;
    let seed = job.seed();
    let mut measured = generation::draft(host, seed, budget).await;
    let source = measured.source.take();
    let mut complete = measured.outcome == Outcome::Generated
        && source.as_deref().is_some_and(generation::complete_draft);
    if let Some(source) = source {
        match stage_document(host, round, &source).await {
            Ok(document) => {
                job.initial = Some(document);
                job.latest = Some(document);
            }
            Err(issue) => {
                measured.source_omitted = true;
                measured.issue = Some(issue);
                measured.outcome = Outcome::Failed;
                complete = false;
            }
        }
    }
    if !complete && measured.outcome == Outcome::Generated {
        measured.outcome = Outcome::Incomplete;
    }
    job.rounds.push(RefineRound::from_record(
        &measured,
        if complete {
            RefineAction::Drafted
        } else {
            RefineAction::Failed
        },
    ));
    job.record = measured;
    let end = if complete {
        RefinementEnd::Accepted
    } else {
        RefinementEnd::DraftFailed
    };
    complete_round(host, job, since_ms, end).await
}

async fn complete_round(
    host: &HostClient,
    mut job: Job,
    since_ms: u64,
    end: RefinementEnd,
) -> Result<(), PluginFault> {
    // The Stop button or queue submission can write during the stream. Merge both
    // without changing the active claim identity or losing a completed draft.
    for _ in 0..CAS_RETRIES {
        let (mut state, version) = read_state(host).await?;
        let Some(current) = state.active.as_ref() else {
            return Err(conflict());
        };
        if current.selection.id != job.selection.id
            || !matches!(current.phase, Phase::Running { since_ms: active } if active == since_ms)
        {
            return Err(conflict());
        }
        job.stop_requested = current.stop_requested;
        let end = if job.stop_requested {
            RefinementEnd::Stopped
        } else {
            end
        };
        job.phase = Phase::Saving { end };
        state.active = Some(job.clone());
        match write_state(host, version, &state).await {
            Ok(_) => return archive(host, job, end).await,
            Err(error) if error.code == ErrorCode::Conflict => continue,
            Err(error) => return Err(error),
        }
    }
    Err(conflict())
}

async fn commit_active_job(
    host: &HostClient,
    job: &Job,
    merge_stop: bool,
) -> Result<Job, PluginFault> {
    let mut updated = job.clone();
    for _ in 0..CAS_RETRIES {
        let (mut state, version) = read_state(host).await?;
        let Some(active) = state.active.as_ref() else {
            return Err(conflict());
        };
        if active.selection.id != updated.selection.id {
            return Err(conflict());
        }
        if merge_stop {
            updated.stop_requested |= active.stop_requested;
        }
        state.active = Some(updated.clone());
        match write_state(host, version, &state).await {
            Ok(_) => return Ok(updated),
            Err(error) if error.code == ErrorCode::Conflict => continue,
            Err(error) => return Err(error),
        }
    }
    Err(conflict())
}

async fn finalize(
    host: &HostClient,
    mut job: Job,
    end: RefinementEnd,
) -> Result<(), PluginFault> {
    job.phase = Phase::Saving { end };
    let committed = commit_active_job(host, &job, true).await?;
    archive(host, committed, end).await
}

enum AppendOutcome {
    Final(Archive, Option<SaveFailureReason>),
    RetryLater,
}

async fn try_append_record(host: &HostClient, record: &Record, attempts: u8) -> AppendOutcome {
    for immediate_try in 0..2 {
        match history::append(host, record.clone()).await {
            Ok(()) => return AppendOutcome::Final(Archive::Saved, None),
            Err(error) if error.message == history::FULL => {
                return AppendOutcome::Final(Archive::Full, Some(SaveFailureReason::Full));
            }
            Err(error) if error.message == history::TOO_LARGE => {
                return AppendOutcome::Final(Archive::TooLarge, Some(SaveFailureReason::TooLarge));
            }
            Err(error) if error.code == ErrorCode::InvalidInput => {
                return AppendOutcome::Final(
                    Archive::Unconfirmed,
                    Some(SaveFailureReason::Unconfirmed),
                );
            }
            Err(_) if immediate_try == 0 => continue,
            Err(_) if attempts < MAX_SAVE_ATTEMPTS => return AppendOutcome::RetryLater,
            Err(_) => {
                return AppendOutcome::Final(
                    Archive::Unconfirmed,
                    Some(SaveFailureReason::Unconfirmed),
                );
            }
        }
    }
    AppendOutcome::Final(Archive::Unconfirmed, Some(SaveFailureReason::Unconfirmed))
}

async fn archive(host: &HostClient, mut job: Job, end: RefinementEnd) -> Result<(), PluginFault> {
    let mut record = if job.rounds.is_empty() {
        job.seed()
    } else {
        job.record.clone()
    };
    if let Some(latest) = job.latest {
        match load_document(host, latest).await {
            Ok(source) => record.source = Some(source),
            Err(error)
                if error.code == ErrorCode::InvalidInput && error.message == history::MISSING =>
            {
                record.source = None;
                record.source_omitted = true;
                record.outcome = Outcome::Failed;
                record.issue.get_or_insert(Issue::ModelFailed);
            }
            Err(error) => return Err(error),
        }
    }
    if matches!(end, RefinementEnd::Stopped) && record.source.is_none() {
        record.outcome = Outcome::Stopped;
    }
    let keep_refinement =
        job.selection.effective_trigger() == Trigger::Refined || job.rounds.len() > 1;
    if keep_refinement {
        record.first_content_ms = job.rounds.first().and_then(|run| run.first_content_ms);
        record.duration_ms = now_ms()
            .saturating_sub(record.started_at_ms)
            .max(record.first_content_ms.unwrap_or(0));
        record.input_tokens = total(&job.rounds, |run| run.input_tokens);
        record.output_tokens = total(&job.rounds, |run| run.output_tokens);
        record.cached_tokens = total(&job.rounds, |run| run.cached_tokens);
        record.reasoning_tokens = total(&job.rounds, |run| run.reasoning_tokens);
        record.refinement = Some(Refinement {
            end,
            rounds: job.rounds.clone(),
        });
    } else {
        record.refinement = None;
        if let Some(first_ms) = record.first_content_ms {
            record.duration_ms = record.duration_ms.max(first_ms);
        }
    }
    let (archive, failure_reason) = match try_append_record(host, &record, job.save_attempts).await {
        AppendOutcome::Final(archive, failure_reason) => (archive, failure_reason),
        AppendOutcome::RetryLater => {
            job.save_attempts = job.save_attempts.saturating_add(1);
            job.phase = Phase::Saving { end };
            commit_active_job(host, &job, true).await?;
            return Ok(());
        }
    };
    if let Some(plan_id) = job.selection.plan_id()
        && let Some(slot_ms) = job.selection.scheduled_at_ms
    {
        schedule::record_save_outcome(host, plan_id, slot_ms, failure_reason).await?;
    }
    job.phase = Phase::Done { end, archive };
    commit_active_job(host, &job, false).await?;
    Ok(())
}

fn total(rounds: &[RefineRound], field: impl Fn(&RefineRound) -> Option<u64>) -> Option<u64> {
    let mut sum = 0u64;
    let mut any = false;
    for value in rounds.iter().filter_map(field) {
        sum = sum.checked_add(value)?;
        any = true;
    }
    any.then_some(sum)
}

async fn stage_document(
    host: &HostClient,
    round: usize,
    mut source: &str,
) -> Result<Document, Issue> {
    if source.len() > generation::MAX_SOURCE_BYTES
        || source.contains('\0')
        || serde_json::to_vec(source)
            .map_err(|_| Issue::OutputLimit)?
            .len()
            > MAX_SOURCE_JSON
    {
        return Err(Issue::OutputLimit);
    }
    let mut parts = 0;
    while !source.is_empty() {
        if parts >= MAX_PARTS {
            return Err(Issue::OutputLimit);
        }
        let (part, remaining) = source.split_at(state::json_prefix(source, PART_BYTES));
        let key = format!("draft-{round}-{parts}");
        let (_, version) = state::get::<String>(host, NAMESPACE, &key)
            .await
            .map_err(|_| Issue::ModelFailed)?;
        state::put(host, NAMESPACE, &key, version, part)
            .await
            .map_err(|_| Issue::ModelFailed)?;
        parts += 1;
        source = remaining;
    }
    Ok(Document { round, parts })
}

async fn load_document(host: &HostClient, document: Document) -> Result<String, PluginFault> {
    if !(1..=LEGACY_MAX_ROUNDS).contains(&document.round) || !(1..=MAX_PARTS).contains(&document.parts)
    {
        return Err(invalid());
    }
    let mut source = String::new();
    for part in 0..document.parts {
        let key = format!("draft-{}-{part}", document.round);
        let (value, _) = state::get::<String>(host, NAMESPACE, &key).await?;
        source.push_str(&value.ok_or_else(missing)?);
    }
    Ok(source)
}

pub(super) fn valid_job_id(id: &str) -> bool {
    history::valid_id(id)
}

async fn read_state(host: &HostClient) -> Result<(QueueState, Option<u64>), PluginFault> {
    let (stored, version) = state::get::<StoredState>(host, NAMESPACE, KEY).await?;
    let state = match stored {
        Some(StoredState::Queue(state)) => state,
        Some(StoredState::Legacy(job)) => QueueState {
            active: Some(*job),
            queue: Vec::new(),
        },
        None => QueueState::default(),
    };
    Ok((state, version))
}

async fn write_state(
    host: &HostClient,
    version: Option<u64>,
    state: &QueueState,
) -> Result<u64, PluginFault> {
    state::put(host, NAMESPACE, KEY, version, state).await
}

fn now_ms() -> u64 {
    Utc::now().timestamp_millis().try_into().unwrap_or(0)
}

fn missing() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, history::MISSING)
}

fn conflict() -> PluginFault {
    PluginFault::new(ErrorCode::Conflict, "pelican refine job")
}

fn queue_full() -> PluginFault {
    PluginFault::new(ErrorCode::Conflict, QUEUE_FULL)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_v070_job_deserializes_with_refined_trigger() {
        let legacy = json!({
            "selection": {
                "id": "refine-00000000000000000000000000000001",
                "clientKeyId": "fixture-key",
                "model": "fixture-model",
                "effort": "low"
            },
            "record": Record::default(),
            "rounds": [],
            "initial": null,
            "latest": null,
            "phase": {"state": "queued"},
            "stopRequested": false
        });
        let stored: StoredState = serde_json::from_value(legacy).unwrap();
        match stored {
            StoredState::Legacy(job) => {
                assert_eq!(job.selection.effective_trigger(), Trigger::Refined);
                assert_eq!(job.seed().trigger, Trigger::Refined);
            }
            StoredState::Queue(_) => panic!("must not parse v0.7.0 job as QueueState"),
        }
    }
}
