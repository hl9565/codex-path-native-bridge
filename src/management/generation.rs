mod audit;

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
use serde_json::{Value, json};
use tokio::time::timeout;

pub(super) use audit::{complete_draft, extract_document_html};

use super::{
    PROMPT, callback,
    history::{Issue, Outcome, Record},
    identity,
};

pub(super) const MAX_SOURCE_BYTES: usize = 512 * 1024;
const MAX_BATCH_BYTES: u32 = 1024 * 1024;

pub(super) async fn draft(host: &HostClient, record: Record, budget_ms: u64) -> Record {
    let body = request_body(&record);
    let (run, result) = execute(host, record, body, budget_ms).await;
    run.finish_text(result)
}

fn request_body(record: &Record) -> Value {
    let mut body = json!({
        "model": record.model, "stream": true, "store": false, "tools": [],
        "input": [{"role": "user", "content": [{"type": "input_text", "text": PROMPT}]}],
    });
    if record.effort != "auto" {
        body["reasoning"] = json!({"effort": record.effort});
    }
    if !record.verbosity.is_empty() && record.verbosity != "auto" {
        body["text"] = json!({"verbosity": record.verbosity});
    }
    body
}

async fn execute(
    host: &HostClient,
    record: Record,
    body: Value,
    budget_ms: u64,
) -> (Capture, Result<(), Issue>) {
    let key_id = record.client_key_id.clone();
    let mut run = Capture::new(record);
    let result = if budget_ms == 0 {
        Err(Issue::Timeout)
    } else {
        timeout(Duration::from_millis(budget_ms), run.read(host, &body))
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
    if let Some(key_id) = key_id {
        let (name, ()) = tokio::join!(
            identity::key_name(host, &key_id),
            run.record.attach_account(host),
        );
        if let Some(name) = name {
            run.record.client_key_name = Some(name);
        }
    }
    (run, result)
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
    fn new(record: Record) -> Self {
        Self {
            record,
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

    async fn read(&mut self, host: &HostClient, body: &Value) -> Result<(), Issue> {
        let request = ModelExecuteRequest {
            client_key_id: Some(
                self.record
                    .client_key_id
                    .clone()
                    .ok_or(Issue::KeyUnavailable)?,
            ),
            model: self.record.model.clone(),
            protocol: "openai".into(),
            operation: ModelOperation::Generate,
            provider: None,
            account_id: None,
            previous_response_id: None,
        };
        let params = serde_json::to_value(request).map_err(|_| Issue::ModelFailed)?;
        let payload = serde_json::to_vec(body).map_err(|_| Issue::ModelFailed)?;
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

    fn finish_text(mut self, result: Result<(), Issue>) -> Record {
        let source: String = self.parts.into_values().collect();
        self.record.outcome = match (result, self.terminal) {
            (Err(issue), _) => {
                self.record.issue = Some(issue);
                Outcome::Failed
            }
            (Ok(()), Some(FinishReason::Stop)) if audit::has_svg(&source) => Outcome::Generated,
            (Ok(()), Some(FinishReason::Stop)) => Outcome::NoSvg,
            (Ok(()), _) => Outcome::Incomplete,
        };
        self.record.source = (!source.is_empty()).then_some(source);
        self.record
    }
}

fn model_issue(error: &PluginFault) -> Issue {
    match error.code {
        ErrorCode::PermissionDenied => Issue::KeyUnavailable,
        ErrorCode::Timeout => Issue::Timeout,
        ErrorCode::Cancelled => Issue::Interrupted,
        _ => Issue::ModelFailed,
    }
}
