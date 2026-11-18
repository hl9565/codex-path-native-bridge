use chrono::Utc;
use gateway_plugin_sdk::{ErrorCode, PluginFault, client::HostClient};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    generation::{self, Edit},
    history::{
        self, Issue, Outcome, Record, RefineAction, RefineRound, Refinement, RefinementEnd, Trigger,
    },
    identity, invalid, list_models, state, valid_effort,
};

const NAMESPACE: &str = "pelican-refine";
const KEY: &str = "job";
const MAX_ROUNDS: usize = 3;
const LEASE_MS: u64 = 135_000;
const MAX_SOURCE_JSON: usize = 850 * 1024;
const PART_BYTES: usize = 48 * 1024 - 2;
const MAX_PARTS: usize = 20;

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Selection {
    id: String,
    client_key_id: String,
    model: String,
    effort: String,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Job {
    selection: Selection,
    record: Record,
    rounds: Vec<RefineRound>,
    initial: Option<Document>,
    latest: Option<Document>,
    phase: Phase,
    stop_requested: bool,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
struct Document {
    round: usize,
    parts: usize,
}

#[derive(Deserialize, Serialize)]
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
            "id": self.selection.id, "model": self.selection.model, "effort": self.selection.effort,
            "status": status, "rounds": self.rounds.len(), "end": end, "archive": archive,
            "stopRequested": self.stop_requested,
            "hasDraft": self.latest.is_some(),
            "hasComparison": self.latest.is_some_and(|draft| draft.round > 1),
        })
    }

    fn response(&self) -> Value {
        json!({ "job": self.view(), "maxRounds": MAX_ROUNDS })
    }

    fn seed(&self) -> Record {
        Record {
            id: self.selection.id.clone(),
            started_at_ms: self.record.started_at_ms,
            model: self.selection.model.clone(),
            effort: self.selection.effort.clone(),
            client_key_id: Some(self.selection.client_key_id.clone()),
            client_key_name: self.record.client_key_name.clone(),
            trigger: Trigger::Refined,
            ..Record::default()
        }
    }
}

pub(super) async fn status(host: &HostClient) -> Result<Value, PluginFault> {
    Ok(match read(host).await?.0 {
        Some(job) => job.response(),
        None => json!({ "job": null, "maxRounds": MAX_ROUNDS }),
    })
}

pub(super) async fn start(host: &HostClient, payload: &[u8]) -> Result<Value, PluginFault> {
    if payload.len() > 4096 {
        return Err(invalid());
    }
    let selection: Selection = serde_json::from_slice(payload).map_err(|_| invalid())?;
    if !history::valid_refinement_id(&selection.id)
        || !identity::valid_text(&selection.client_key_id, 200)
        || !identity::valid_text(&selection.model, 512)
        || !valid_effort(&selection.effort)
    {
        return Err(invalid());
    }
    let (current, version) = read(host).await?;
    if let Some(current) = current {
        if current.selection.id == selection.id {
            return Ok(current.response());
        }
        if !matches!(current.phase, Phase::Done { .. }) {
            return Err(conflict());
        }
    }
    if history::contains(host, &selection.id).await? {
        return Err(conflict());
    }
    let key = identity::keys(host)
        .await?
        .into_iter()
        .find(|key| key.id == selection.client_key_id && key.enabled)
        .ok_or_else(|| PluginFault::new(ErrorCode::PermissionDenied, "pelican refine key"))?;
    if !list_models(host, &key.id).await?.contains(&selection.model) {
        return Err(PluginFault::new(
            ErrorCode::InvalidInput,
            "pelican refine model",
        ));
    }
    let job = Job {
        record: Record {
            started_at_ms: now_ms(),
            client_key_name: Some(key.name).filter(|name| identity::valid_text(name, 512)),
            ..Record::default()
        },
        selection,
        rounds: Vec::new(),
        initial: None,
        latest: None,
        phase: Phase::Queued,
        stop_requested: false,
    };
    write(host, version, &job).await?;
    Ok(job.response())
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
    if !history::valid_refinement_id(&id) {
        return Err(invalid());
    }
    for _ in 0..4 {
        let (Some(mut job), version) = read(host).await? else {
            return Err(missing());
        };
        if job.selection.id != id {
            return Err(missing());
        }
        if !matches!(job.phase, Phase::Queued | Phase::Running { .. }) || job.stop_requested {
            return Ok(job.response());
        }
        job.stop_requested = true;
        match write(host, version, &job).await {
            Ok(_) => return Ok(job.response()),
            Err(error) if error.code == ErrorCode::Conflict => continue,
            Err(error) => return Err(error),
        }
    }
    Err(conflict())
}

pub(super) async fn document(host: &HostClient, query: &str) -> Result<Value, PluginFault> {
    let (id, version) = query
        .strip_prefix("id=")
        .and_then(|query| query.split_once("&version="))
        .ok_or_else(invalid)?;
    if !history::valid_refinement_id(id) || !matches!(version, "first" | "latest") {
        return Err(invalid());
    }
    let (Some(job), _) = read(host).await? else {
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
    if read(host)
        .await?
        .0
        .is_none_or(|current| current.selection.id != id)
    {
        return Err(missing());
    }
    let mut record = job.record;
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
    Ok(json!({ "record": record }))
}

// A call to reconcile can consume at most one round. Before dispatch, commit
// the claim. An unknown response after restart must not send that round again.
pub(super) async fn reconcile(host: &HostClient, budget: Option<u64>) -> Result<(), PluginFault> {
    let (Some(mut job), version) = read(host).await? else {
        return Ok(());
    };
    match job.phase {
        Phase::Done { .. } => return Ok(()),
        Phase::Saving { end } => return archive(host, job, version, end).await,
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
                return finalize(host, job, version, RefinementEnd::Interrupted).await;
            }
            return Ok(());
        }
        Phase::Queued if job.stop_requested => {
            return finalize(host, job, version, RefinementEnd::Stopped).await;
        }
        Phase::Queued => {}
    }
    let Some(budget) = budget.filter(|budget| *budget > 0) else {
        return Ok(());
    };
    if job.rounds.len() >= MAX_ROUNDS {
        return finalize(host, job, version, RefinementEnd::LimitReached).await;
    }
    // Loading the existing draft is read-only. Do it before claiming the call.
    let old = if let Some(document) = job.latest {
        Some(load_document(host, document).await?)
    } else {
        None
    };
    let since_ms = now_ms();
    job.phase = Phase::Running { since_ms };
    match write(host, version, &job).await {
        Ok(_) => {}
        Err(error) if error.code == ErrorCode::Conflict => return Ok(()),
        Err(error) => return Err(error),
    }
    let round = job.rounds.len() + 1;
    let seed = job.seed();
    let end = if let Some(source) = old {
        let (mut measured, edit) =
            generation::review(host, seed, &source, round == MAX_ROUNDS, budget).await;
        let edit = edit.map(|edit| match edit {
            Edit::Replace(html) if html.trim() == source.trim() => Edit::Keep,
            other => other,
        });
        match edit {
            Ok(Edit::Keep) => {
                job.rounds
                    .push(RefineRound::from_record(&measured, RefineAction::Accepted));
                Some(RefinementEnd::Accepted)
            }
            Ok(Edit::Replace(source)) => match stage_document(host, round, &source).await {
                Ok(document) => {
                    measured.outcome = Outcome::Generated;
                    job.rounds
                        .push(RefineRound::from_record(&measured, RefineAction::Revised));
                    job.record = measured;
                    job.latest = Some(document);
                    (round == MAX_ROUNDS).then_some(RefinementEnd::LimitReached)
                }
                Err(issue) => {
                    measured.issue = Some(issue);
                    job.rounds
                        .push(RefineRound::from_record(&measured, RefineAction::Failed));
                    Some(RefinementEnd::ReviewFailed)
                }
            },
            Err(_) => {
                job.rounds
                    .push(RefineRound::from_record(&measured, RefineAction::Failed));
                Some(RefinementEnd::ReviewFailed)
            }
        }
    } else {
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
        (!complete).then_some(RefinementEnd::DraftFailed)
    };
    complete_round(host, job, since_ms, end).await
}

async fn complete_round(
    host: &HostClient,
    mut job: Job,
    since_ms: u64,
    end: Option<RefinementEnd>,
) -> Result<(), PluginFault> {
    // The Stop button can write during the stream. Merge it without changing
    // the identity of the claimed request or losing a completed draft.
    for _ in 0..4 {
        let (Some(current), version) = read(host).await? else {
            return Err(conflict());
        };
        if current.selection.id != job.selection.id
            || !matches!(current.phase, Phase::Running { since_ms: active } if active == since_ms)
        {
            return Err(conflict());
        }
        job.stop_requested = current.stop_requested;
        let end = end.or(job.stop_requested.then_some(RefinementEnd::Stopped));
        job.phase = match end {
            Some(end) => Phase::Saving { end },
            None => Phase::Queued,
        };
        match write(host, version, &job).await {
            Ok(version) => {
                return if let Some(end) = end {
                    archive(host, job, Some(version), end).await
                } else {
                    Ok(())
                };
            }
            Err(error) if error.code == ErrorCode::Conflict => continue,
            Err(error) => return Err(error),
        }
    }
    Err(conflict())
}

async fn finalize(
    host: &HostClient,
    mut job: Job,
    version: Option<u64>,
    end: RefinementEnd,
) -> Result<(), PluginFault> {
    job.phase = Phase::Saving { end };
    let version = write(host, version, &job).await?;
    archive(host, job, Some(version), end).await
}

async fn archive(
    host: &HostClient,
    mut job: Job,
    version: Option<u64>,
    end: RefinementEnd,
) -> Result<(), PluginFault> {
    let mut record = if job.rounds.is_empty() {
        job.seed()
    } else {
        job.record.clone()
    };
    if let Some(latest) = job.latest {
        record.source = Some(load_document(host, latest).await?);
    }
    if matches!(end, RefinementEnd::Stopped) && record.source.is_none() {
        record.outcome = Outcome::Stopped;
    }
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
    let archive = match history::append(host, record).await {
        Ok(()) => Archive::Saved,
        Err(error) if error.message == history::FULL => Archive::Full,
        Err(error) if error.message == history::TOO_LARGE => Archive::TooLarge,
        Err(_) => Archive::Unconfirmed,
    };
    job.phase = Phase::Done { end, archive };
    write(host, version, &job).await.map(|_| ())
}

fn total(rounds: &[RefineRound], field: impl Fn(&RefineRound) -> Option<u64>) -> Option<u64> {
    if rounds.is_empty() {
        return None;
    }
    rounds
        .iter()
        .try_fold(0u64, |total, run| total.checked_add(field(run)?))
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
    if !(1..=MAX_ROUNDS).contains(&document.round) || !(1..=MAX_PARTS).contains(&document.parts) {
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

async fn read(host: &HostClient) -> Result<(Option<Job>, Option<u64>), PluginFault> {
    state::get(host, NAMESPACE, KEY).await
}

async fn write(host: &HostClient, version: Option<u64>, job: &Job) -> Result<u64, PluginFault> {
    state::put(host, NAMESPACE, KEY, version, job).await
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
