use gateway_plugin_sdk::{ErrorCode, PluginFault, client::HostClient};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{identity, invalid, state, valid_effort};

const NAMESPACE: &str = "pelican-history";
const KEY: &str = "recent";
const MAX_RECORDS: usize = 20;
const MAX_BYTES: usize = 240 * 1024;

#[derive(Clone, Deserialize, Serialize)]
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
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Outcome {
    Generated,
    NoSvg,
    Incomplete,
    Stopped,
    Failed,
}

#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Trigger {
    #[default]
    Manual,
    Scheduled,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Issue {
    Interrupted,
    Timeout,
    KeyUnavailable,
    ModelFailed,
    OutputLimit,
}

pub(super) fn scheduled_id(slot_ms: u64) -> String {
    format!("scheduled-{slot_ms}")
}

impl Record {
    fn is_valid(&self) -> bool {
        let id_valid = match (self.trigger, self.scheduled_at_ms) {
            (Trigger::Manual, None) => {
                self.id.len() == 32 && self.id.bytes().all(|byte| byte.is_ascii_hexdigit())
            }
            (Trigger::Scheduled, Some(slot)) => {
                self.id == scheduled_id(slot)
                    && slot <= self.started_at_ms
                    && slot.is_multiple_of(60_000)
            }
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

pub(super) async fn list(host: &HostClient) -> Result<Value, PluginFault> {
    let (records, _) = read(host).await?;
    Ok(json!({ "records": records }))
}

pub(super) async fn save_manual(host: &HostClient, payload: &[u8]) -> Result<Value, PluginFault> {
    if payload.len() > MAX_BYTES {
        return Err(invalid());
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
    append(host, record).await
}

pub(super) async fn append(host: &HostClient, mut record: Record) -> Result<Value, PluginFault> {
    if !record.is_valid() {
        return Err(invalid());
    }
    if serde_json::to_vec(&record).map_err(|_| invalid())?.len() > MAX_BYTES - 2 {
        record.source = None;
        record.source_omitted = true;
    }

    // A management page can be open in multiple tabs. Merge against the current
    // version on each conflict so a later write cannot erase another run.
    for _ in 0..4 {
        let (mut records, version) = read(host).await?;
        if records.iter().any(|saved| saved.id == record.id) {
            return Ok(json!({ "records": records }));
        }
        records.push(record.clone());
        records.sort_unstable_by(|a, b| {
            b.started_at_ms
                .cmp(&a.started_at_ms)
                .then_with(|| b.id.cmp(&a.id))
        });
        records.truncate(MAX_RECORDS);
        while serde_json::to_vec(&records).map_err(|_| invalid())?.len() > MAX_BYTES {
            records.pop();
        }

        match state::put(host, NAMESPACE, KEY, version, &records).await {
            Ok(_) => return Ok(json!({ "records": records })),
            Err(error) if error.code == ErrorCode::Conflict => continue,
            Err(error) => return Err(error),
        }
    }

    Err(PluginFault::new(
        ErrorCode::Conflict,
        "pelican history version changed",
    ))
}

async fn read(host: &HostClient) -> Result<(Vec<Record>, Option<u64>), PluginFault> {
    let (records, version) = state::get(host, NAMESPACE, KEY).await?;
    Ok((records.unwrap_or_default(), version))
}
