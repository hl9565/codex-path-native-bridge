//! Exercises the compiled plugin process over the real RPC framing.
//!
//! The fixture plays the host side of CPR's plugin protocol using the same SDK
//! frame codecs, then checks that HTTP middleware calls preserve every field
//! except the exact path this plugin is contracted to rewrite.

use std::{process::Stdio, time::Duration};

use gateway_plugin_sdk::{
    CallContext, Capability, Contributions, ErrorCode, Frame, FrameError, Handshake, Manifest,
    Message, PROTOCOL_VERSION, PluginFault, Stage,
    call::{
        management::ManagementResponse,
        middleware::{MiddlewareHeader, http as wire},
    },
    client::{read_frame, write_frame},
};
use serde_json::{Value, json};
use tokio::{
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};

#[path = "rpc/pelican.rs"]
mod pelican;

const BIN: &str = env!("CARGO_BIN_EXE_cpr-omp-native-bridge");
const STEP_TIMEOUT: Duration = Duration::from_secs(10);
const INSTANCE: &str = "test-instance";
const INCARNATION: &str = "test-incarnation";

fn manifest() -> Manifest {
    Manifest::from_author_slice(include_bytes!("../plugin.json")).expect("author manifest")
}

fn plugin_id() -> String {
    manifest().plugin_id().expect("plugin id")
}

fn contributions() -> Contributions {
    manifest().contributes
}

fn header(name: &str, value: &[u8]) -> MiddlewareHeader {
    MiddlewareHeader {
        name: name.into(),
        value: value.to_vec(),
    }
}

fn hello(protocol_version: u32, contributes: Contributions) -> Frame {
    Frame::control(Message::Hello {
        handshake: Handshake {
            protocol_version,
            artifact_sha256: "0".repeat(64),
            plugin_id: plugin_id(),
            instance_id: INSTANCE.into(),
            generation: 1,
            incarnation: INCARNATION.into(),
            configuration: json!({}),
            contributes,
        },
    })
}

struct Host {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    next_call_id: u64,
}

async fn spawn() -> Host {
    let mut child = Command::new(BIN)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("plugin process starts");
    let stdin = child.stdin.take().expect("plugin stdin");
    let stdout = child.stdout.take().expect("plugin stdout");
    Host {
        child,
        stdin,
        stdout,
        next_call_id: 1,
    }
}

async fn start() -> Host {
    let mut host = spawn().await;
    write_frame(&mut host.stdin, &hello(PROTOCOL_VERSION, contributions()))
        .await
        .expect("Hello write");
    let ready = timeout(STEP_TIMEOUT, read_frame(&mut host.stdout))
        .await
        .expect("Ready timed out")
        .expect("Ready frame");
    assert!(ready.payload.is_empty());
    match ready.message {
        Message::Ready {
            protocol_version,
            incarnation,
            call_budgets,
        } => {
            assert_eq!(protocol_version, PROTOCOL_VERSION);
            assert_eq!(incarnation, INCARNATION);
            assert!(call_budgets.is_empty());
        }
        other => panic!("expected Ready, got {other:?}"),
    }
    host
}

impl Host {
    async fn read(&mut self) -> Frame {
        timeout(STEP_TIMEOUT, read_frame(&mut self.stdout))
            .await
            .expect("plugin response timed out")
            .expect("plugin frame is valid")
    }

    async fn send(&mut self, frame: Frame) {
        write_frame(&mut self.stdin, &frame)
            .await
            .expect("plugin frame write");
    }

    async fn shutdown(mut self) {
        self.send(Frame::control(Message::Shutdown)).await;
        let status = timeout(STEP_TIMEOUT, self.child.wait())
            .await
            .expect("plugin shutdown timed out")
            .expect("plugin wait");
        assert!(status.success(), "plugin exited with {status}");
    }
}

struct Case {
    name: &'static str,
    method: &'static str,
    uri: &'static str,
    expected_uri: &'static str,
    version: wire::Version,
    headers: Vec<MiddlewareHeader>,
    settings: Value,
    timeout_ms: Option<u64>,
    body: wire::Body,
    response: wire::Response,
}

fn response(
    status: u16,
    headers: Vec<MiddlewareHeader>,
    body: wire::Body,
    source: &str,
) -> wire::Response {
    wire::Response {
        status,
        version: wire::Version::Http11,
        headers,
        body,
        response: Some(source.into()),
        session: false,
    }
}

fn cases() -> Vec<Case> {
    let sse = vec![
        header("content-type", b"text/event-stream"),
        header("x-next", b"1"),
    ];
    let upgrade = vec![
        header("upgrade", b"websocket"),
        header("connection", b"Upgrade"),
        header("sec-websocket-key", b"dGhlIHNhbXBsZSBub25jZQ=="),
    ];
    let request_headers = vec![
        header("content-type", b"application/json"),
        header("x-raw", b"\x00\xff"),
        header("x-raw", b"second"),
    ];

    vec![
        Case {
            name: "post alias with empty body",
            method: "POST",
            uri: "/v1/codex/responses",
            expected_uri: "/v1/responses",
            version: wire::Version::Http11,
            headers: request_headers.clone(),
            settings: json!({"compression": true, "trace": "off"}),
            timeout_ms: Some(30_000),
            body: wire::Body::Empty,
            response: response(
                200,
                sse.clone(),
                wire::Body::Handle {
                    handle: "resp-body-1".into(),
                },
                "resp-1",
            ),
        },
        Case {
            name: "post alias with exact query bytes",
            method: "POST",
            uri: "/v1/codex/responses?stream=true&a=%2F&b=1&b=2&empty=&q=?/v1/codex/responses",
            expected_uri: "/v1/responses?stream=true&a=%2F&b=1&b=2&empty=&q=?/v1/codex/responses",
            version: wire::Version::Http11,
            headers: Vec::new(),
            settings: Value::Null,
            timeout_ms: None,
            body: wire::Body::Empty,
            response: response(200, sse.clone(), wire::Body::Empty, "resp-2"),
        },
        Case {
            name: "get alias with websocket upgrade headers",
            method: "GET",
            uri: "/v1/codex/responses",
            expected_uri: "/v1/responses",
            version: wire::Version::Http11,
            headers: upgrade,
            settings: json!({"upgrade": "websocket"}),
            timeout_ms: Some(60_000),
            body: wire::Body::Empty,
            response: response(101, Vec::new(), wire::Body::Empty, "ws-1"),
        },
        Case {
            name: "get absolute uri preserves authority and query",
            method: "GET",
            uri: "https://Example.COM:8443/v1/codex/responses?token=%2b+",
            expected_uri: "https://Example.COM:8443/v1/responses?token=%2b+",
            version: wire::Version::Http2,
            headers: vec![header("host", b"Example.COM:8443")],
            settings: Value::Null,
            timeout_ms: Some(1_000),
            body: wire::Body::Empty,
            response: response(200, Vec::new(), wire::Body::Empty, "resp-4"),
        },
        Case {
            name: "post absolute http uri",
            method: "POST",
            uri: "http://example.com/v1/codex/responses",
            expected_uri: "http://example.com/v1/responses",
            version: wire::Version::Http11,
            headers: Vec::new(),
            settings: Value::Null,
            timeout_ms: None,
            body: wire::Body::Empty,
            response: response(200, sse, wire::Body::Empty, "resp-5"),
        },
        Case {
            name: "lowercase method unchanged",
            method: "post",
            uri: "/v1/codex/responses",
            expected_uri: "/v1/codex/responses",
            version: wire::Version::Http11,
            headers: Vec::new(),
            settings: Value::Null,
            timeout_ms: None,
            body: wire::Body::Empty,
            response: response(204, Vec::new(), wire::Body::Empty, "resp-6"),
        },
        Case {
            name: "trailing slash unchanged",
            method: "GET",
            uri: "/v1/codex/responses/",
            expected_uri: "/v1/codex/responses/",
            version: wire::Version::Http11,
            headers: Vec::new(),
            settings: Value::Null,
            timeout_ms: None,
            body: wire::Body::Empty,
            response: response(404, Vec::new(), wire::Body::Empty, "resp-7"),
        },
        Case {
            name: "suffix path unchanged",
            method: "POST",
            uri: "/v1/codex/responses-extra",
            expected_uri: "/v1/codex/responses-extra",
            version: wire::Version::Http11,
            headers: Vec::new(),
            settings: Value::Null,
            timeout_ms: None,
            body: wire::Body::Empty,
            response: response(404, Vec::new(), wire::Body::Empty, "resp-8"),
        },
        Case {
            name: "case sensitive path unchanged",
            method: "POST",
            uri: "/v1/Codex/responses",
            expected_uri: "/v1/Codex/responses",
            version: wire::Version::Http2,
            headers: Vec::new(),
            settings: Value::Null,
            timeout_ms: None,
            body: wire::Body::Empty,
            response: response(404, Vec::new(), wire::Body::Empty, "resp-9"),
        },
        Case {
            name: "alias in query unchanged",
            method: "POST",
            uri: "/v1/models?path=/v1/codex/responses",
            expected_uri: "/v1/models?path=/v1/codex/responses",
            version: wire::Version::Http11,
            headers: Vec::new(),
            settings: Value::Null,
            timeout_ms: None,
            body: wire::Body::Empty,
            response: response(200, Vec::new(), wire::Body::Empty, "resp-10"),
        },
        Case {
            name: "percent encoded path unchanged",
            method: "POST",
            uri: "/v1/codex/%72esponses",
            expected_uri: "/v1/codex/%72esponses",
            version: wire::Version::Http11,
            headers: Vec::new(),
            settings: Value::Null,
            timeout_ms: None,
            body: wire::Body::Empty,
            response: response(404, Vec::new(), wire::Body::Empty, "resp-11"),
        },
        Case {
            name: "head method unchanged",
            method: "HEAD",
            uri: "/v1/codex/responses",
            expected_uri: "/v1/codex/responses",
            version: wire::Version::Http11,
            headers: Vec::new(),
            settings: Value::Null,
            timeout_ms: None,
            body: wire::Body::Empty,
            response: response(405, Vec::new(), wire::Body::Empty, "resp-12"),
        },
        Case {
            name: "options star unchanged",
            method: "OPTIONS",
            uri: "*",
            expected_uri: "*",
            version: wire::Version::Http11,
            headers: Vec::new(),
            settings: Value::Null,
            timeout_ms: None,
            body: wire::Body::Empty,
            response: response(204, Vec::new(), wire::Body::Empty, "resp-13"),
        },
        Case {
            name: "post alias preserves lazy request and response handles",
            method: "POST",
            uri: "/v1/codex/responses?stream=true",
            expected_uri: "/v1/responses?stream=true",
            version: wire::Version::Http11,
            headers: vec![header("content-type", b"application/json")],
            settings: json!({"request": "stream"}),
            timeout_ms: Some(120_000),
            body: wire::Body::Handle {
                handle: "req-body-1".into(),
            },
            response: response(
                200,
                vec![header("content-type", b"text/event-stream")],
                wire::Body::Handle {
                    handle: "resp-body-3".into(),
                },
                "resp-14",
            ),
        },
        Case {
            name: "absolute uri without path unchanged",
            method: "POST",
            uri: "https://example.com",
            expected_uri: "https://example.com",
            version: wire::Version::Http11,
            headers: Vec::new(),
            settings: Value::Null,
            timeout_ms: None,
            body: wire::Body::Empty,
            response: response(200, Vec::new(), wire::Body::Empty, "resp-15"),
        },
    ]
}

fn call_frame(id: u64, request: wire::Request) -> Frame {
    let request_id = format!("req-{id}");
    let call = wire::Call {
        settings_sources: json!({"chain": "test"}),
        request_id: request_id.clone(),
        call_id: format!("call-{id}"),
        parent_call_id: None,
        request,
    };
    Frame {
        message: Message::Call {
            id,
            method: "middleware.handle".into(),
            context: call_context(id, Stage::Http),
            params: serde_json::to_value(call).expect("call params"),
        },
        payload: Vec::new(),
    }
}

fn call_context(id: u64, stage: Stage) -> CallContext {
    CallContext {
        request: None,
        call_id: id,
        instance_id: INSTANCE.into(),
        generation: 1,
        incarnation: INCARNATION.into(),
        stage,
        timeout_ms: 5_000,
        resource_stream: false,
        resource_scope_id: format!("scope-{id}"),
        request_id: Some(format!("req-{id}")),
        attempt_id: None,
        account_id: None,
        credential_revision: None,
    }
}

#[tokio::test]
async fn actual_process_handshake_and_path_next_preservation() {
    let cases = cases();
    assert_eq!(cases.len(), 15);
    let mut host = start().await;

    for case in &cases {
        let id = host.next_call_id;
        host.next_call_id += 2;
        let request = wire::Request {
            settings: case.settings.clone(),
            method: case.method.into(),
            uri: case.uri.into(),
            version: case.version,
            headers: case.headers.clone(),
            timeout_ms: case.timeout_ms,
            body: case.body.clone(),
        };
        host.send(call_frame(id, request)).await;
        host.send(Frame::control(Message::Credit {
            id,
            bytes: 64 * 1024,
            frames: 1,
        }))
        .await;

        let mut next_callbacks = 0;
        let mut result = None;
        loop {
            let frame = host.read().await;
            match frame.message {
                Message::Callback {
                    id: callback_id,
                    parent_id,
                    method,
                    params,
                } => {
                    assert_eq!(parent_id, id, "{}: callback parent", case.name);
                    assert_eq!(method, "host.middleware.next", "{}: method", case.name);
                    assert_eq!(next_callbacks, 0, "{}: next called twice", case.name);
                    assert!(frame.payload.is_empty(), "{}: next payload", case.name);
                    next_callbacks += 1;
                    let forwarded: wire::Request =
                        serde_json::from_value(params).expect("next request decodes");
                    assert_eq!(forwarded.uri, case.expected_uri, "{}: uri", case.name);
                    assert_eq!(
                        forwarded.method, case.method,
                        "{}: method preserved",
                        case.name
                    );
                    assert_eq!(
                        serde_json::to_value(forwarded.version).unwrap(),
                        serde_json::to_value(case.version).unwrap(),
                        "{}: version preserved",
                        case.name
                    );
                    assert_eq!(
                        serde_json::to_value(&forwarded.headers).unwrap(),
                        serde_json::to_value(&case.headers).unwrap(),
                        "{}: headers preserved",
                        case.name
                    );
                    assert_eq!(
                        forwarded.settings, case.settings,
                        "{}: settings preserved",
                        case.name
                    );
                    assert_eq!(
                        forwarded.timeout_ms, case.timeout_ms,
                        "{}: timeout preserved",
                        case.name
                    );
                    assert_eq!(
                        serde_json::to_value(&forwarded.body).unwrap(),
                        serde_json::to_value(&case.body).unwrap(),
                        "{}: request body handle preserved",
                        case.name
                    );
                    host.send(Frame {
                        message: Message::Result {
                            id: callback_id,
                            result: serde_json::to_value(&case.response).unwrap(),
                        },
                        payload: Vec::new(),
                    })
                    .await;
                }
                Message::Suspend { .. } => {}
                Message::Resume {
                    id: resume_id,
                    epoch,
                } => {
                    assert_eq!(resume_id, id, "{}: resume id", case.name);
                    host.send(Frame::control(Message::Resumed {
                        id: resume_id,
                        epoch,
                    }))
                    .await;
                }
                Message::CallbackConsumed { .. } => {}
                Message::Result {
                    id: result_id,
                    result: value,
                } if result_id == id => {
                    assert!(frame.payload.is_empty(), "{}: result payload", case.name);
                    assert_eq!(
                        result.replace(value),
                        None,
                        "{}: duplicate result",
                        case.name
                    );
                }
                Message::End { id: end_id, error } if end_id == id => {
                    assert!(error.is_none(), "{}: stream error {error:?}", case.name);
                    assert!(result.is_some(), "{}: result before end", case.name);
                    break;
                }
                Message::Stream { .. } => {
                    panic!("{}: unexpected response stream frame", case.name);
                }
                Message::Error {
                    id: error_id,
                    error,
                } if error_id == id => {
                    panic!(
                        "{}: plugin error {:?}: {}",
                        case.name, error.code, error.message
                    );
                }
                other => panic!("{}: unexpected frame {other:?}", case.name),
            }
        }

        assert_eq!(
            next_callbacks, 1,
            "{}: exactly one next callback",
            case.name
        );
        let returned: wire::Response =
            serde_json::from_value(result.take().unwrap()).expect("response decodes");
        assert_eq!(
            returned.status, case.response.status,
            "{}: status",
            case.name
        );
        assert_eq!(
            serde_json::to_value(returned.version).unwrap(),
            serde_json::to_value(case.response.version).unwrap(),
            "{}: response version",
            case.name
        );
        assert_eq!(
            serde_json::to_value(&returned.headers).unwrap(),
            serde_json::to_value(&case.response.headers).unwrap(),
            "{}: response headers",
            case.name
        );
        assert_eq!(
            serde_json::to_value(&returned.body).unwrap(),
            serde_json::to_value(&case.response.body).unwrap(),
            "{}: response body handle",
            case.name
        );
        assert_eq!(
            returned.response, case.response.response,
            "{}: response source",
            case.name
        );
        assert!(!returned.session, "{}: response session", case.name);
    }

    host.shutdown().await;
}

async fn history_call(
    host: &mut Host,
    input: Option<&Value>,
    callback: &mut impl FnMut(&str, Value) -> Result<Value, PluginFault>,
) -> (u16, Value) {
    management_call(host, "/history", input, &mut |method, params, payload| {
        assert!(payload.is_empty());
        callback(method, params).map(|result| (result, Vec::new()))
    })
    .await
}

async fn management_call(
    host: &mut Host,
    path: &str,
    input: Option<&Value>,
    callback: &mut impl FnMut(&str, Value, &[u8]) -> Result<(Value, Vec<u8>), PluginFault>,
) -> (u16, Value) {
    let (result, payload) = rpc_call(
        host,
        "management.handle",
        Stage::Management,
        json!({
            "method": if input.is_some() { "POST" } else { "GET" },
            "path": path.trim_start_matches('/'),
            "query": "",
            "content_type": input.map(|_| "application/json"),
        }),
        input
            .map(|value| serde_json::to_vec(value).unwrap())
            .unwrap_or_default(),
        callback,
    )
    .await;
    let metadata: ManagementResponse = serde_json::from_value(result).unwrap();
    (metadata.status, serde_json::from_slice(&payload).unwrap())
}

async fn rpc_call(
    host: &mut Host,
    method: &str,
    stage: Stage,
    params: Value,
    payload: Vec<u8>,
    callback: &mut impl FnMut(&str, Value, &[u8]) -> Result<(Value, Vec<u8>), PluginFault>,
) -> (Value, Vec<u8>) {
    let id = host.next_call_id;
    host.next_call_id += 2;
    let mut context = call_context(id, stage);
    if stage == Stage::Maintenance {
        context.request_id = None;
    }
    host.send(Frame {
        message: Message::Call {
            id,
            method: method.into(),
            context,
            params,
        },
        payload,
    })
    .await;
    loop {
        let frame = host.read().await;
        match frame.message {
            Message::Callback {
                id: callback_id,
                parent_id,
                method,
                params,
            } => {
                assert_eq!(parent_id, id);
                let reply = match callback(&method, params, &frame.payload) {
                    Ok((result, payload)) => Frame {
                        message: Message::Result {
                            id: callback_id,
                            result,
                        },
                        payload,
                    },
                    Err(error) => Frame::control(Message::Error {
                        id: callback_id,
                        error,
                    }),
                };
                host.send(reply).await;
            }
            Message::Suspend { .. } | Message::CallbackConsumed { .. } => {}
            Message::Resume {
                id: resume_id,
                epoch,
            } => {
                assert_eq!(resume_id, id);
                host.send(Frame::control(Message::Resumed { id, epoch }))
                    .await;
            }
            Message::Result {
                id: result_id,
                result,
            } if result_id == id => {
                return (result, frame.payload);
            }
            other => panic!("unexpected {method} frame {other:?}"),
        }
    }
}

#[tokio::test]
async fn management_history_survives_another_write_without_losing_either_record() {
    let mut host = start().await;
    let record = |id: &str, start: u64, source: &str| {
        json!({
            "id": id, "startedAtMs": start, "model": "fixture-model", "effort": "low",
            "outcome": "generated", "trigger": "manual", "durationMs": 100, "firstContentMs": 20,
            "inputTokens": 10, "outputTokens": 12, "cachedTokens": null, "reasoningTokens": null,
            "source": source, "sourceOmitted": false,
        })
    };
    let older = record(
        "10000000000000000000000000000000",
        1_700_000_000_000,
        "<svg><!-- older --></svg>",
    );
    let newer = record(
        "20000000000000000000000000000000",
        1_700_000_001_000,
        "<svg><!-- newer --></svg>",
    );
    let mut version = None;
    let mut saved = json!([]);
    let mut call_store = |method: &str, params: Value| {
        assert_eq!(params["namespace"], "pelican-history");
        assert_eq!(params["key"], "recent");
        match method {
            "host.state.get" => Ok(json!({
                "record": version.map(|version| json!({"value": saved, "version": version, "schema_version": 1})),
            })),
            "host.state.put" => {
                assert_eq!(params["expected_version"].as_u64(), version);
                if version.is_none() {
                    version = Some(1);
                    let mut legacy_record = older.clone();
                    legacy_record.as_object_mut().unwrap().remove("trigger");
                    saved = json!([legacy_record]);
                    Err(PluginFault::new(
                        ErrorCode::Conflict,
                        "another tab saved a record",
                    ))
                } else {
                    assert_eq!(version, Some(1), "a duplicate run must not write again");
                    saved = params["value"].clone();
                    version = Some(2);
                    Ok(json!({"version": 2}))
                }
            }
            other => panic!("unexpected history host callback {other}"),
        }
    };
    assert_eq!(
        history_call(&mut host, None, &mut call_store).await,
        (200, json!({"records": []}))
    );
    let expected = (200, json!({"records": [newer, older]}));
    assert_eq!(
        history_call(&mut host, Some(&newer), &mut call_store).await,
        expected
    );
    assert_eq!(
        history_call(&mut host, Some(&newer), &mut call_store).await,
        expected
    );
    assert_eq!(
        history_call(&mut host, None, &mut call_store).await,
        expected
    );
    let mut invalid = newer.clone();
    invalid["apiKey"] = json!("a-placeholder-secret");
    let (status, _) = history_call(&mut host, Some(&invalid), &mut |_: &str, _: Value| {
        panic!("invalid records must not reach the state store")
    })
    .await;
    assert_eq!(status, 400);
    host.shutdown().await;
}

#[tokio::test]
async fn process_rejects_mismatched_contribution() {
    let mut contributes = contributions();
    let declaration = contributes
        .get_mut(&Capability::Middleware)
        .expect("middleware contribution");
    declaration.version = 5;
    let mut host = spawn().await;
    host.send(hello(PROTOCOL_VERSION, contributes)).await;
    expect_no_ready(&mut host).await;
}

#[tokio::test]
async fn process_rejects_rpc_v6_handshake() {
    let mut host = spawn().await;
    host.send(hello(6, contributions())).await;
    expect_no_ready(&mut host).await;
}

async fn expect_no_ready(host: &mut Host) {
    match timeout(STEP_TIMEOUT, read_frame(&mut host.stdout)).await {
        Ok(Err(FrameError::Io(_))) => {}
        Ok(Ok(frame)) => panic!("plugin answered a rejected handshake: {frame:?}"),
        Ok(Err(error)) => panic!("unexpected frame error: {error}"),
        Err(_) => panic!("plugin did not exit after a rejected handshake"),
    }
    let status = timeout(STEP_TIMEOUT, host.child.wait())
        .await
        .expect("rejected handshake process did not exit")
        .expect("plugin wait");
    assert_eq!(
        status.code(),
        Some(1),
        "rejected handshake exit status: {status}"
    );
}
