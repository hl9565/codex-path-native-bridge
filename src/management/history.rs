mod archive;

use gateway_plugin_sdk::{ErrorCode, PluginFault, client::HostClient};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{identity, invalid, state, valid_effort};

pub(super) use archive::append;

const PAGE_SIZE: usize = 20;
const PREVIEW_BYTES: usize = 16 * 1024;
const MAX_RECORD_BYTES: usize = 900 * 1024;
pub(super) const FULL: &str = "pelican history storage is full";
pub(super) const TOO_LARGE: &str = "pelican history record is too large";
pub(super) const MISSING: &str = "pelican history record was not found";

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Record {
    pub id: String,
    pub started_at_ms: u64,
    pub model: String,
    pub effort: String,
    pub outcome: Outcome,
    pub duration_ms: u64,
    pub first_content_ms: Option<u64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub source: Option<String>,
    pub source_omitted: bool,
    #[serde(default)]
    pub trigger: Trigger,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_key_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_key_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<Issue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refinement: Option<Refinement>,
}

#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Outcome {
    Generated,
    NoSvg,
    Incomplete,
    Stopped,
    #[default]
    Failed,
}

#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Trigger {
    #[default]
    Manual,
    Scheduled,
    Refined,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Issue {
    Interrupted,
    Timeout,
    KeyUnavailable,
    ModelFailed,
    OutputLimit,
    ReviewInvalid,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Refinement {
    pub end: RefinementEnd,
    pub rounds: Vec<RefineRound>,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum RefinementEnd {
    Accepted,
    LimitReached,
    DraftFailed,
    ReviewFailed,
    Interrupted,
    Stopped,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum RefineAction {
    Drafted,
    Revised,
    Accepted,
    Failed,
    Interrupted,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RefineRound {
    pub action: RefineAction,
    pub model: String,
    pub duration_ms: u64,
    pub first_content_ms: Option<u64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub gateway_request_id: Option<String>,
    pub issue: Option<Issue>,
}

impl RefineRound {
    pub fn from_record(record: &Record, action: RefineAction) -> Self {
        Self {
            action,
            model: record.model.clone(),
            duration_ms: record.duration_ms,
            first_content_ms: record.first_content_ms,
            input_tokens: record.input_tokens,
            output_tokens: record.output_tokens,
            cached_tokens: record.cached_tokens,
            reasoning_tokens: record.reasoning_tokens,
            gateway_request_id: record.gateway_request_id.clone(),
            issue: record.issue,
        }
    }
}

pub(super) fn scheduled_id(slot_ms: u64) -> String {
    format!("scheduled-{slot_ms}")
}

impl Record {
    fn is_valid(&self) -> bool {
        let id_valid = match (self.trigger, self.scheduled_at_ms) {
            (Trigger::Manual, None) => valid_manual_id(&self.id) && self.refinement.is_none(),
            (Trigger::Scheduled, Some(slot)) => {
                self.id == scheduled_id(slot)
                    && slot <= self.started_at_ms
                    && slot.is_multiple_of(60_000)
                    && self.refinement.is_none()
            }
            (Trigger::Refined, None) => valid_refinement_id(&self.id) && self.refinement.is_some(),
            _ => false,
        };
        id_valid
            && self.started_at_ms <= 8_640_000_000_000_000
            && identity::valid_text(&self.model, 512)
            && valid_effort(&self.effort)
            && self
                .first_content_ms
                .is_none_or(|time| time <= self.duration_ms)
            && (!self.source_omitted || self.source.is_none())
            && self
                .client_key_id
                .as_deref()
                .is_none_or(|id| identity::valid_text(id, 200))
            && self
                .gateway_request_id
                .as_deref()
                .is_none_or(|id| identity::valid_text(id, 256))
    }

    pub(super) async fn attach_account(&mut self, host: &HostClient) {
        if let (Some(key_id), Some(request_id)) = (&self.client_key_id, &self.gateway_request_id)
            && let Some(account) = identity::account(host, key_id, request_id).await
        {
            self.account_id = Some(account.id);
            self.account_name = account.name;
        }
    }
}

pub(super) async fn list(host: &HostClient, query: &str) -> Result<Value, PluginFault> {
    let mut offset = None;
    let mut snapshot = None;
    if !query.is_empty() {
        for part in query.split('&') {
            let (name, value) = part.split_once('=').ok_or_else(invalid)?;
            let field = match name {
                "offset" => &mut offset,
                "snapshot" => &mut snapshot,
                _ => return Err(invalid()),
            };
            if field.is_some() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid());
            }
            *field = Some(value.parse::<usize>().map_err(|_| invalid())?);
        }
    }
    let offset = offset.unwrap_or(0);
    if !offset.is_multiple_of(PAGE_SIZE) {
        return Err(invalid());
    }
    let (records, total, snapshot) = archive::page(host, offset, snapshot).await?;
    Ok(json!({ "records": records, "total": total, "snapshot": snapshot }))
}

pub(super) async fn detail(host: &HostClient, query: &str) -> Result<Value, PluginFault> {
    let id = query
        .strip_prefix("id=")
        .filter(|id| valid_id(id))
        .ok_or_else(invalid)?;
    let record = archive::detail(host, id)
        .await?
        .ok_or_else(|| PluginFault::new(ErrorCode::InvalidInput, MISSING))?;
    Ok(json!({ "record": record }))
}

pub(super) async fn save_manual(host: &HostClient, payload: &[u8]) -> Result<Value, PluginFault> {
    if payload.len() > MAX_RECORD_BYTES {
        return Err(too_large());
    }
    let mut record: Record = serde_json::from_slice(payload).map_err(|_| invalid())?;
    if record.trigger != Trigger::Manual || !record.is_valid() {
        return Err(invalid());
    }
    // Keys and accounts in CPR history are populated from host data, not a submitted label.
    record.account_id = None;
    record.account_name = None;
    record.issue = None;
    record.client_key_name = match &record.client_key_id {
        Some(id) => identity::key_name(host, id).await,
        None => None,
    };
    record.attach_account(host).await;
    append(host, record).await?;
    Ok(json!({ "saved": true }))
}

pub(super) fn valid_refinement_id(id: &str) -> bool {
    id.strip_prefix("refine-").is_some_and(valid_manual_id)
}

fn valid_manual_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_id(id: &str) -> bool {
    valid_manual_id(id)
        || valid_refinement_id(id)
        || id.strip_prefix("scheduled-").is_some_and(|slot| {
            slot.parse::<u64>()
                .is_ok_and(|slot| id == scheduled_id(slot) && slot.is_multiple_of(60_000))
        })
}

pub(super) async fn contains(host: &HostClient, id: &str) -> Result<bool, PluginFault> {
    Ok(archive::detail(host, id).await?.is_some())
}

fn too_large() -> PluginFault {
    PluginFault::new(ErrorCode::InvalidInput, TOO_LARGE)
}

fn list_entry(mut record: Record, complete: bool) -> Result<Value, PluginFault> {
    let has_source = record
        .source
        .as_ref()
        .is_some_and(|source| !source.is_empty());
    if !complete
        || serde_json::to_vec(&record.source)
            .map_err(|_| invalid())?
            .len()
            > PREVIEW_BYTES
    {
        record.source = None;
    }
    let mut value = serde_json::to_value(record).map_err(|_| invalid())?;
    value["hasSource"] = json!(has_source);
    Ok(value)
}
