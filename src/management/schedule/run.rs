use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::{
        host::{
            self, ModelEventBatch, ModelExecuteRequest, ModelOperation, ModelStreamCloseRequest,
            ModelStreamReadRequest, ModelStreamReadResult, ModelStreamResult,
        },
        model::{CanonicalEvent, FinishReason, Usage},
    },
    client::{HostClient, SessionError},
};
use serde_json::json;
use tokio::time::timeout;

use super::{
    Claim,
    history::{self, Issue, Outcome, Record, Trigger},
    identity,
};
use crate::management::{PROMPT, callback};

const MAX_SOURCE_BYTES: usize = 512 * 1024;
const MAX_BATCH_BYTES: u32 = 1024 * 1024;

pub(super) fn record_for(claim: &Claim) -> Record {
    Record {
        id: history::scheduled_id(claim.slot_ms),
        started_at_ms: claim.started_at_ms,
        model: claim.model.clone(),
        effort: claim.effort.clone(),
        outcome: Outcome::Failed,
        duration_ms: 0,
        first_content_ms: None,
        input_tokens: None,
        output_tokens: None,
        cached_tokens: None,
        reasoning_tokens: None,
        source: None,
        source_omitted: false,
        trigger: Trigger::Scheduled,
        client_key_id: Some(claim.client_key_id.clone()),
        client_key_name: claim.key_name.clone(),
        account_id: None,
        account_name: None,
        gateway_request_id: None,
        scheduled_at_ms: Some(claim.slot_ms),
        issue: None,
    }
}

pub(super) async fn execute(host: &HostClient, claim: &Claim, budget_ms: u64) -> Record {
    let mut run = Capture::new(claim);
    let result = if budget_ms == 0 {
        Err(Issue::Timeout)
    } else {
        timeout(Duration::from_millis(budget_ms), run.read(host, claim))
            .await
            .unwrap_or(Err(Issue::Timeout))
    };
    run.record.duration_ms = run.elapsed_ms();
    if let Some(stream) = run.stream.take() {
        let close = ModelStreamCloseRequest { stream };
        let _ = timeout(
            Duration::from_millis(750),
            callback::<_, serde_json::Value>(host, host::MODEL_STREAM_CLOSE, close),
        )
        .await;
    }
    let mut record = run.finish(result);
    let (name, ()) = tokio::join!(
        identity::key_name(host, &claim.client_key_id),
        record.attach_account(host),
    );
    if let Some(name) = name {
        record.client_key_name = Some(name);
    }
    record
}

struct Capture {
    record: Record,
    started: Instant,
    stream: Option<String>,
    parts: BTreeMap<u32, String>,
    source_bytes: usize,
    terminal: Option<FinishReason>,
}

impl Capture {
    fn new(claim: &Claim) -> Self {
        Self {
            record: record_for(claim),
            started: Instant::now(),
            stream: None,
            parts: BTreeMap::new(),
            source_bytes: 0,
            terminal: None,
        }
    }

    fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    async fn read(&mut self, host: &HostClient, claim: &Claim) -> Result<(), Issue> {
        let request = ModelExecuteRequest {
            client_key_id: Some(claim.client_key_id.clone()),
            model: claim.model.clone(),
            protocol: "openai".into(),
            operation: ModelOperation::Generate,
            provider: None,
            account_id: None,
            previous_response_id: None,
        };
        let mut body = json!({
            "model": claim.model, "stream": true, "store": false, "tools": [],
            "input": [{"role": "user", "content": [{"type": "input_text", "text": PROMPT}]}],
        });
        if claim.effort != "auto" {
            body["reasoning"] = json!({"effort": claim.effort});
        }
        let params = serde_json::to_value(request).map_err(|_| Issue::ModelFailed)?;
        let payload = serde_json::to_vec(&body).map_err(|_| Issue::ModelFailed)?;
        let reply = host
            .call(host::MODEL_EXECUTE_STREAM, params, payload)
            .await
            .map_err(|error| model_issue(&error.into_plugin_fault()))?;
        let result: ModelStreamResult =
            serde_json::from_value(reply.result).map_err(|_| Issue::ModelFailed)?;
        self.record.gateway_request_id =
            Some(result.request_id).filter(|id| identity::valid_text(id, 256));
        self.stream = Some(result.stream.clone());
        if !reply.payload.is_empty() {
            return Err(Issue::ModelFailed);
        }
        let params = serde_json::to_value(ModelStreamReadRequest {
            stream: result.stream,
            maximum_bytes: MAX_BATCH_BYTES,
        })
        .map_err(|_| Issue::ModelFailed)?;
        loop {
            let reply = host
                .call(host::MODEL_STREAM_READ, params.clone(), Vec::new())
                .await
                .map_err(|error: SessionError| model_issue(&error.into_plugin_fault()))?;
            let read: ModelStreamReadResult =
                serde_json::from_value(reply.result).map_err(|_| Issue::ModelFailed)?;
            if read.end {
                self.stream = None;
            }
            if reply.payload.is_empty() {
                return if read.end && read.events == 0 {
                    Ok(())
                } else {
                    Err(Issue::ModelFailed)
                };
            }
            let batch = ModelEventBatch::decode(&reply.payload).map_err(|_| Issue::ModelFailed)?;
            if batch.events.len() != read.events as usize || (!read.end && read.events == 0) {
                return Err(Issue::ModelFailed);
            }
            for event in batch.events {
                // A canonical fact and its raw Responses event describe the same bytes.
                for fact in event.facts {
                    self.consume(fact)?;
                }
            }
            if read.end {
                return Ok(());
            }
        }
    }

    fn consume(&mut self, fact: CanonicalEvent) -> Result<(), Issue> {
        match fact {
            CanonicalEvent::TextDelta { index, text } if !text.is_empty() => {
                if self.source_bytes.saturating_add(text.len()) > MAX_SOURCE_BYTES {
                    return Err(Issue::OutputLimit);
                }
                if self.record.first_content_ms.is_none() {
                    self.record.first_content_ms = Some(self.elapsed_ms());
                }
                self.source_bytes += text.len();
                self.parts.entry(index).or_default().push_str(&text);
            }
            CanonicalEvent::Started { model, .. } => self.model(model),
            CanonicalEvent::Completed { model, reason, .. } => {
                self.model(model);
                self.terminal = Some(reason);
            }
            CanonicalEvent::Usage {
                usage:
                    Usage {
                        input_tokens,
                        output_tokens,
                        cached_tokens,
                        reasoning_tokens,
                        ..
                    },
            } => {
                self.record.input_tokens = input_tokens.or(self.record.input_tokens);
                self.record.output_tokens = output_tokens.or(self.record.output_tokens);
                self.record.cached_tokens = cached_tokens.or(self.record.cached_tokens);
                self.record.reasoning_tokens = reasoning_tokens.or(self.record.reasoning_tokens);
            }
            _ => {}
        }
        Ok(())
    }

    fn model(&mut self, model: Option<String>) {
        if let Some(model) = model.filter(|value| identity::valid_text(value, 512)) {
            self.record.model = model;
        }
    }

    fn finish(mut self, result: Result<(), Issue>) -> Record {
        let source: String = self.parts.into_values().collect();
        self.record.outcome = match (result, self.terminal) {
            (Err(issue), _) => {
                self.record.issue = Some(issue);
                Outcome::Failed
            }
            (Ok(()), Some(FinishReason::Stop)) if has_svg(&source) => Outcome::Generated,
            (Ok(()), Some(FinishReason::Stop)) => Outcome::NoSvg,
            (Ok(()), _) => Outcome::Incomplete,
        };
        self.record.source = (!source.is_empty()).then_some(source);
        self.record
    }
}

fn has_svg(source: &str) -> bool {
    let source = source.to_ascii_lowercase();
    let Some((end, _)) = source.rmatch_indices("</svg").find(|(index, _)| {
        source[index + 5..]
            .trim_start_matches(|c: char| c.is_ascii_whitespace())
            .starts_with('>')
    }) else {
        return false;
    };
    source[..end].match_indices("<svg").any(|(start, _)| {
        let rest = &source[start + 4..];
        rest.as_bytes()
            .first()
            .is_some_and(|byte| *byte == b'>' || byte.is_ascii_whitespace())
    })
}

fn model_issue(error: &PluginFault) -> Issue {
    match error.code {
        ErrorCode::PermissionDenied => Issue::KeyUnavailable,
        ErrorCode::Timeout => Issue::Timeout,
        ErrorCode::Cancelled => Issue::Interrupted,
        _ => Issue::ModelFailed,
    }
}
