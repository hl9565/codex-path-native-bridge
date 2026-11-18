use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;
use gateway_plugin_sdk::call::{
    data::ACCOUNTS_LIST,
    host::{self, ModelEventBatch},
    model::{CanonicalEvent, ExecutionEvent, FinishReason, Usage},
    services::CALL_METHOD,
};

use super::{ErrorCode, Host, PluginFault, Stage, Value, json, management_call, rpc_call, start};

const KEY: &str = "fixture-key";
const REQUEST: &str = "gateway-request-123";
const ACCOUNT: &str = "routed-account";
const SVG: &str = "<svg viewBox='0 0 12 12'><circle r='2'/></svg>";
const ARCHIVE: &str = "pelican-archive";
const INDEX: &str = "pelican-archive-index";

#[derive(Default)]
struct Fixture {
    state: BTreeMap<(String, String), (u64, Value)>,
    issued_slots: BTreeSet<u64>,
    reject_next_claim: bool,
    reject_generation: bool,
    usage_key_mismatch: bool,
    data_writes_before_full: Option<usize>,
    interrupt_publish: bool,
}

impl Fixture {
    fn plan(&mut self) -> &mut Value {
        &mut self
            .state
            .get_mut(&("pelican-schedule".into(), "plan".into()))
            .unwrap()
            .1
    }

    fn callback(
        &mut self,
        method: &str,
        params: Value,
        payload: &[u8],
    ) -> Result<(Value, Vec<u8>), PluginFault> {
        let result = match method {
            host::STATE_GET => {
                assert!(payload.is_empty());
                let record = self.state.get(&state_key(&params));
                json!({ "record": record.map(|(version, value)| json!({
                    "value": value, "version": version, "schema_version": 1,
                })) })
            }
            host::STATE_PUT => {
                assert!(payload.is_empty());
                let key = state_key(&params);
                let previous = self.state.get(&key).map(|(version, _)| *version);
                if params["expected_version"].as_u64() != previous {
                    return Err(PluginFault::new(
                        ErrorCode::Conflict,
                        "state version conflicts",
                    ));
                }
                if key.0 == "pelican-schedule"
                    && params["value"]["pending"].is_object()
                    && std::mem::take(&mut self.reject_next_claim)
                {
                    return Err(PluginFault::new(ErrorCode::Conflict, "fixture claim lost"));
                }
                if key.0 == INDEX
                    && key.1 == "head"
                    && params["value"]["pending"].is_null()
                    && std::mem::take(&mut self.interrupt_publish)
                {
                    return Err(PluginFault::new(
                        ErrorCode::Upstream,
                        "lost final index write",
                    ));
                }
                if key.0 == ARCHIVE {
                    assert!(serde_json::to_vec(&params["value"]).unwrap().len() <= 48 * 1024);
                    if let Some(writes) = self.data_writes_before_full.as_mut() {
                        if *writes == 0 {
                            return Err(PluginFault::new(
                                ErrorCode::Capacity,
                                "state quota is exhausted",
                            ));
                        }
                        *writes -= 1;
                    }
                }
                let version = previous.unwrap_or(0) + 1;
                self.state.insert(key, (version, params["value"].clone()));
                json!({"version": version})
            }
            host::STATE_DELETE => {
                assert!(payload.is_empty());
                let key = state_key(&params);
                assert_eq!(key.0, ARCHIVE);
                assert!(
                    key.1.contains("-part-"),
                    "committed records must never be deleted"
                );
                assert_eq!(
                    self.state[&key].0,
                    params["expected_version"].as_u64().unwrap()
                );
                json!({"deleted": self.state.remove(&key).is_some()})
            }
            host::KEYS_LIST => {
                json!({"keys": [{"id": KEY, "name": "Fixture Key", "enabled": true}], "next_cursor": null})
            }
            host::MODELS_LIST => {
                assert_eq!(params["client_key_id"], KEY);
                json!({"models": ["fixture-model"]})
            }
            host::MODEL_EXECUTE_STREAM => {
                let slot = self.plan()["pending"]["slotMs"]
                    .as_u64()
                    .expect("claim precedes dispatch");
                assert!(
                    self.issued_slots.insert(slot),
                    "the same slot dispatched twice"
                );
                assert_eq!(params["client_key_id"], KEY);
                assert_eq!(params["model"], "fixture-model");
                assert_eq!(params["protocol"], "openai");
                assert_eq!(params["operation"], "generate");
                assert!(params["account_id"].is_null());
                let page = include_str!("../../web/pelican.html");
                let prompt = page
                    .split_once("const FIXED_PROMPT = '")
                    .unwrap()
                    .1
                    .split_once('\'')
                    .unwrap()
                    .0;
                assert_eq!(
                    serde_json::from_slice::<Value>(payload).unwrap(),
                    json!({
                        "model": "fixture-model", "stream": true, "store": false, "tools": [],
                        "reasoning": {"effort": "high"},
                        "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
                    })
                );
                if self.reject_generation {
                    return Err(PluginFault::new(
                        ErrorCode::PermissionDenied,
                        "fixture Key disabled",
                    ));
                }
                json!({"stream": "fixture-stream", "request_id": REQUEST})
            }
            host::MODEL_STREAM_READ => {
                assert_eq!(params["stream"], "fixture-stream");
                let events = ModelEventBatch {
                    events: vec![
                        ExecutionEvent::canonical(CanonicalEvent::TextDelta {
                            index: 0,
                            text: SVG.into(),
                        }),
                        ExecutionEvent::canonical(CanonicalEvent::Usage {
                            usage: Usage {
                                input_tokens: Some(7),
                                output_tokens: Some(19),
                                reasoning_tokens: Some(3),
                                ..Usage::default()
                            },
                        }),
                        ExecutionEvent::canonical(CanonicalEvent::Completed {
                            id: "fixture-response".into(),
                            model: Some("resolved-model".into()),
                            reason: FinishReason::Stop,
                        }),
                    ],
                };
                return Ok((json!({"end": true, "events": 3}), events.encode().unwrap()));
            }
            CALL_METHOD => {
                assert_eq!(
                    params,
                    json!({"operation": "usage.records.get", "input": {"id": REQUEST}})
                );
                json!({"Ok": {"id": REQUEST, "data": {
                    "client_key_ref": if self.usage_key_mismatch { "different-key" } else { KEY },
                    "provider": "openai", "account_ref": ACCOUNT,
                    "requested_model": "fixture-model", "model": "resolved-model", "protocol": "openai",
                    "operation": "generate", "endpoint": "/v1/responses", "client_transport": "http",
                    "service_tier": null, "started_at_ms": 12000, "completed_at_ms": 12100,
                    "outcome": "succeeded", "tokens": {}, "cost": {"kind": "unavailable"},
                }, "billing_snapshot": null, "provider_observation": null }})
            }
            ACCOUNTS_LIST => {
                assert!(
                    !self.usage_key_mismatch,
                    "an unrelated usage row must not resolve an account"
                );
                assert_eq!(
                    serde_json::from_slice::<Value>(payload).unwrap()["provider_id"],
                    "openai"
                );
                let data = json!({"schema_version": 1, "next_cursor": null, "accounts": [{
                    "account_id": ACCOUNT, "provider_id": "openai", "name": "Routed Fixture Account",
                    "email": "excluded@example.test", "group_ids": [], "enabled": true, "updated_at_ms": 1000,
                }]});
                return Ok((json!({}), serde_json::to_vec(&data).unwrap()));
            }
            _ => panic!("unexpected Pelican host callback {method}"),
        };
        Ok((result, Vec::new()))
    }

    async fn management(
        &mut self,
        host: &mut Host,
        path: &str,
        input: Option<&Value>,
    ) -> (u16, Value) {
        management_call(host, path, input, &mut |method, params, payload| {
            self.callback(method, params, payload)
        })
        .await
    }

    async fn tick(&mut self, host: &mut Host) {
        let (result, payload) = rpc_call(
            host,
            "plugin.reconcile",
            Stage::Maintenance,
            json!({}),
            Vec::new(),
            &mut |method, params, payload| self.callback(method, params, payload),
        )
        .await;
        assert_eq!(result, json!({}));
        assert!(payload.is_empty());
    }
}

fn settings(enabled: bool) -> Value {
    json!({"enabled": enabled, "clientKeyId": KEY, "model": "fixture-model", "effort": "high",
        "cron": "* * * * *", "timezone": "UTC"})
}

fn state_key(params: &Value) -> (String, String) {
    (
        params["namespace"].as_str().unwrap().into(),
        params["key"].as_str().unwrap().into(),
    )
}

fn record(id: u32, source: &str) -> Value {
    json!({
        "id": format!("{id:032x}"), "startedAtMs": 1_700_000_000_000_u64 + u64::from(id),
        "model": "fixture-model", "effort": "low", "outcome": "generated", "trigger": "manual",
        "durationMs": 100, "firstContentMs": 20, "source": source, "sourceOmitted": false,
    })
}

#[tokio::test]
async fn archive_pages_survive_restart_and_open_legacy_and_chunked_source() {
    let mut host = start().await;
    let mut fixture = Fixture::default();
    let legacy_key = ("pelican-history".into(), "recent".into());
    let mut old = record(0, SVG);
    old.as_object_mut().unwrap().remove("trigger");
    let legacy = (1, json!([old]));
    fixture.state.insert(legacy_key.clone(), legacy.clone());

    let large_source = format!(
        "<svg><!--{}--></svg>",
        "\u{9e48}\"\\\n\u{1f}".repeat(25_000)
    );
    for id in 1..=130 {
        let source = if id == 1 { &large_source } else { SVG };
        assert_eq!(
            fixture
                .management(&mut host, "/history", Some(&record(id, source)))
                .await,
            (200, json!({"saved": true})),
            "save {id}",
        );
    }
    let (_, first) = fixture.management(&mut host, "/history", None).await;
    assert_eq!(first["snapshot"], 130);
    assert_eq!(first["total"], 131);
    assert_eq!(first["records"].as_array().unwrap().len(), 20);
    assert_eq!(first["records"][0]["id"], record(130, SVG)["id"]);
    assert_eq!(first["records"][19]["id"], record(111, SVG)["id"]);

    host.shutdown().await;
    let mut host = start().await;
    assert_eq!(
        fixture
            .management(&mut host, "/history", Some(&record(131, SVG)))
            .await
            .0,
        200
    );
    let (_, second) = fixture
        .management(&mut host, "/history?offset=20&snapshot=130", None)
        .await;
    assert_eq!(second["total"], 131);
    assert_eq!(second["records"][0]["id"], record(110, SVG)["id"]);
    let (_, last) = fixture
        .management(&mut host, "/history?offset=120&snapshot=130", None)
        .await;
    assert_eq!(last["records"].as_array().unwrap().len(), 11);
    assert_eq!(last["records"][9]["id"], record(1, SVG)["id"]);
    assert!(last["records"][9]["source"].is_null());
    assert_eq!(last["records"][9]["hasSource"], true);
    assert_eq!(last["records"][10]["trigger"], "manual");
    for (id, source) in [(0, SVG), (1, large_source.as_str())] {
        let path = format!("/history/record?id={id:032x}");
        let (status, detail) = fixture.management(&mut host, &path, None).await;
        assert_eq!(status, 200);
        assert_eq!(detail["record"]["source"], source);
    }
    let mut invalid = record(132, SVG);
    invalid["apiKey"] = json!("a-placeholder-secret");
    let before = fixture.state.clone();
    assert_eq!(
        fixture
            .management(&mut host, "/history", Some(&invalid))
            .await
            .0,
        400
    );
    assert_eq!(
        fixture
            .management(&mut host, "/history", Some(&record(0, "duplicate")))
            .await
            .0,
        200
    );
    assert_eq!(fixture.state, before);
    assert_eq!(fixture.state[&legacy_key], legacy);
    host.shutdown().await;
}

#[tokio::test]
async fn quota_keeps_older_results_and_a_scheduled_save_failure_releases_the_claim() {
    let mut host = start().await;
    let mut fixture = Fixture::default();
    assert_eq!(
        fixture
            .management(&mut host, "/history", Some(&record(1, SVG)))
            .await
            .0,
        200
    );
    fixture.data_writes_before_full = Some(1);
    let big = record(2, &"x".repeat(75_000));
    assert_eq!(
        fixture
            .management(&mut host, "/history", Some(&big))
            .await
            .0,
        507
    );
    let (_, page) = fixture.management(&mut host, "/history", None).await;
    assert_eq!(page["total"], 1);
    assert_eq!(page["records"][0]["source"], SVG);
    assert_eq!(
        fixture.state.keys().filter(|key| key.0 == ARCHIVE).count(),
        1
    );
    assert_eq!(
        fixture
            .management(
                &mut host,
                "/history/record?id=00000000000000000000000000000002",
                None
            )
            .await
            .0,
        404
    );

    assert_eq!(
        fixture
            .management(&mut host, "/schedule", Some(&settings(true)))
            .await
            .0,
        200
    );
    fixture.plan()["armedAfterSlotMs"] = json!(0);
    fixture.tick(&mut host).await;
    assert_eq!(fixture.issued_slots.len(), 1);
    let issued = *fixture.issued_slots.first().unwrap();
    let (_, plan) = fixture.management(&mut host, "/schedule", None).await;
    assert!(plan["runningSinceMs"].is_null());
    assert!(plan["nextRunAtMs"].as_u64().unwrap() > issued);
    assert_eq!(
        plan["historySaveFailure"],
        json!({"slotMs": issued, "reason": "full"})
    );
    let (_, page_after_schedule) = fixture.management(&mut host, "/history", None).await;
    assert_eq!(page, page_after_schedule);
    host.shutdown().await;
}

#[tokio::test]
async fn retry_after_interrupted_publication_recovers_the_original_without_a_duplicate() {
    let mut host = start().await;
    let mut fixture = Fixture {
        interrupt_publish: true,
        ..Fixture::default()
    };
    let original = record(1, SVG);
    assert_eq!(
        fixture
            .management(&mut host, "/history", Some(&original))
            .await
            .0,
        502
    );
    let (_, unpublished) = fixture.management(&mut host, "/history", None).await;
    assert_eq!(unpublished["total"], 0);
    host.shutdown().await;

    let mut host = start().await;
    let replacement = record(1, "payload from another attempt");
    assert_eq!(
        fixture
            .management(&mut host, "/history", Some(&replacement))
            .await
            .0,
        200
    );
    let (_, page) = fixture.management(&mut host, "/history", None).await;
    assert_eq!(page["total"], 1);
    assert_eq!(page["records"][0]["source"], SVG);
    host.shutdown().await;
}

#[tokio::test]
async fn history_accepts_only_the_matching_gateway_account() {
    let mut host = start().await;
    let mut fixture = Fixture::default();
    let mut record = json!({
        "id": "20000000000000000000000000000000", "startedAtMs": 1_700_000_001_000_u64,
        "model": "resolved-model", "effort": "low", "outcome": "generated", "durationMs": 100,
        "source": SVG, "sourceOmitted": false, "clientKeyId": KEY, "gatewayRequestId": REQUEST,
        "clientKeyName": "Unverified Label", "accountId": "not-from-host", "accountName": "Not From Host",
    });
    let (status, saved) = fixture
        .management(&mut host, "/history", Some(&record))
        .await;
    assert_eq!(status, 200);
    assert_eq!(saved, json!({"saved": true}));
    let (_, data) = fixture.management(&mut host, "/history", None).await;
    assert_eq!(data["records"][0]["clientKeyName"], "Fixture Key");
    assert_eq!(data["records"][0]["accountId"], ACCOUNT);
    assert_eq!(data["records"][0]["accountName"], "Routed Fixture Account");
    assert!(!data.to_string().contains("excluded@example.test"));

    fixture.usage_key_mismatch = true;
    record["id"] = json!("30000000000000000000000000000000");
    let (status, _) = fixture
        .management(&mut host, "/history", Some(&record))
        .await;
    assert_eq!(status, 200);
    let (_, data) = fixture.management(&mut host, "/history", None).await;
    assert!(data["records"][0]["accountId"].is_null());
    assert!(data["records"][0]["accountName"].is_null());
    assert_eq!(data["records"][0]["clientKeyName"], "Fixture Key");
    host.shutdown().await;
}

#[tokio::test]
async fn scheduled_generation_is_claimed_persisted_and_paused_by_the_host() {
    let mut host = start().await;
    let mut fixture = Fixture::default();
    let (_, status) = fixture.management(&mut host, "/schedule", None).await;
    assert!(status["settings"].is_null());
    let mut invalid = settings(true);
    invalid["cron"] = json!("60 * * * *");
    assert_eq!(
        fixture
            .management(&mut host, "/schedule", Some(&invalid))
            .await
            .0,
        400
    );
    invalid = settings(true);
    invalid["timezone"] = json!("Not/A_Timezone");
    assert_eq!(
        fixture
            .management(&mut host, "/schedule", Some(&invalid))
            .await
            .0,
        400
    );
    assert!(fixture.state.is_empty());
    assert_eq!(
        fixture
            .management(&mut host, "/schedule", Some(&settings(false)))
            .await
            .0,
        200
    );
    fixture.tick(&mut host).await;
    assert!(fixture.issued_slots.is_empty());
    let (status, saved) = fixture
        .management(&mut host, "/schedule", Some(&settings(true)))
        .await;
    assert_eq!(status, 200);
    assert!(saved["nextRunAtMs"].as_u64().is_some());
    assert!(
        fixture.issued_slots.is_empty(),
        "saving a rule does not execute it"
    );

    // The fixture makes an enabled plan eligible now without waiting for the wall clock minute.
    fixture.plan()["armedAfterSlotMs"] = json!(0);
    fixture.reject_next_claim = true;
    fixture.tick(&mut host).await;
    assert!(
        fixture.issued_slots.is_empty(),
        "a failed CAS must not dispatch"
    );
    fixture.tick(&mut host).await;
    assert_eq!(fixture.issued_slots.len(), 1);
    let issued_slot = *fixture.issued_slots.first().unwrap();
    let (_, history) = fixture.management(&mut host, "/history", None).await;
    let record = &history["records"][0];
    assert_eq!(record["trigger"], "scheduled");
    assert_eq!(record["scheduledAtMs"], issued_slot);
    assert_eq!(record["gatewayRequestId"], REQUEST);
    assert_eq!(record["accountName"], "Routed Fixture Account");
    assert_eq!(record["clientKeyName"], "Fixture Key");
    assert_eq!(record["model"], "resolved-model");
    assert_eq!(record["outcome"], "generated");
    assert_eq!(record["outputTokens"], 19);
    assert_eq!(record["source"], SVG);
    host.shutdown().await;

    let mut restarted = start().await;
    fixture.tick(&mut restarted).await; // The fixture rejects a duplicate dispatch of the same minute.
    let (_, paused) = fixture
        .management(&mut restarted, "/schedule/pause", Some(&json!({})))
        .await;
    assert_eq!(paused["settings"]["enabled"], false);
    assert!(paused["nextRunAtMs"].is_null());
    let dispatched = fixture.issued_slots.len();
    fixture.tick(&mut restarted).await;
    assert_eq!(fixture.issued_slots.len(), dispatched);
    restarted.shutdown().await;
}

#[tokio::test]
async fn rejected_and_abandoned_slots_record_failure_without_replaying() {
    let mut host = start().await;
    let mut fixture = Fixture {
        reject_generation: true,
        ..Fixture::default()
    };
    assert_eq!(
        fixture
            .management(&mut host, "/schedule", Some(&settings(true)))
            .await
            .0,
        200
    );
    fixture.plan()["armedAfterSlotMs"] = json!(0);
    fixture.tick(&mut host).await;
    let (_, history) = fixture.management(&mut host, "/history", None).await;
    assert_eq!(fixture.issued_slots.len(), 1);
    assert_eq!(history["records"][0]["outcome"], "failed");
    assert_eq!(history["records"][0]["issue"], "key_unavailable");

    let abandoned = (Utc::now().timestamp_millis() as u64 / 60_000 - 5) * 60_000;
    fixture.plan()["pending"] = json!({"slotMs": abandoned, "startedAtMs": abandoned,
        "clientKeyId": KEY, "keyName": "Fixture Key", "model": "fixture-model", "effort": "high"});
    fixture.plan()["settings"]["enabled"] = json!(false);
    fixture.tick(&mut host).await;
    fixture.tick(&mut host).await;
    assert_eq!(fixture.issued_slots.len(), 1);
    let (_, history) = fixture.management(&mut host, "/history", None).await;
    let records = history["records"].as_array().unwrap();
    assert_eq!(records.len(), 2);
    let interrupted = records
        .iter()
        .find(|record| record["scheduledAtMs"] == abandoned)
        .unwrap();
    assert_eq!(interrupted["issue"], "interrupted");
    assert!(fixture.plan()["pending"].is_null());
    host.shutdown().await;
}
