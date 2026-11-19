mod generation;
mod history;
mod identity;
mod refine;
mod schedule;
mod state;

pub(crate) use schedule::reconcile;

use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::{
        host::{self, ModelListRequest, ModelListResult},
        management::{
            ManagementCallback, ManagementPage, ManagementRegistration, ManagementRequest,
            ManagementResource, ManagementResponse, ManagementRoute,
        },
        middleware::MiddlewareHeader,
    },
    client::{HostClient, SessionError, TypedCall, TypedReply},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

const PROMPT: &str = "用svg绘制一个2D鹈鹏骑自行车的动画的html，不要用skill，不用测试";

pub(super) fn registration() -> ManagementRegistration {
    let route = |method: &str, path: &str| ManagementRoute {
        method: method.into(),
        path: path.into(),
        request_content_types: if method == "POST" {
            vec!["application/json".into()]
        } else {
            Vec::new()
        },
        response_content_types: vec!["application/json".into()],
    };
    ManagementRegistration {
        routes: vec![
            route("GET", "bootstrap"),
            route("POST", "models"),
            route("GET", "history"),
            route("POST", "history"),
            route("GET", "history/record"),
            route("GET", "schedule"),
            route("POST", "schedule"),
            route("POST", "schedule/pause"),
            route("POST", "schedule/delete"),
            route("GET", "refine"),
            route("POST", "refine"),
            route("POST", "refine/stop"),
            route("GET", "refine/draft"),
        ],
        resources: vec![ManagementResource {
            path: "web/pelican.html".into(),
            public: false,
        }],
        pages: vec![ManagementPage {
            id: "pelican".into(),
            title: "鹈鹕".into(),
            description: Some("测试 SVG 动画生成，支持后台队列与定时任务。".into()),
            entry: "web/pelican.html".into(),
            icon: None,
        }],
        callbacks: vec![ManagementCallback {
            path: "preview".into(),
            response_content_types: vec!["text/html; charset=utf-8".into()],
        }],
    }
}

pub(crate) async fn callback_handle(
    call: TypedCall<ManagementRequest>,
) -> Result<TypedReply<ManagementResponse>, PluginFault> {
    if call.request.method != "GET"
        || call.request.path != "preview"
        || !call.payload.is_empty()
    {
        return Ok(html_reply(400, "预览请求方式或路径无效。"));
    }
    let query = match parse_preview_query(&call.request.query) {
        Ok(query) => query,
        Err(()) => return Ok(html_reply(400, "预览链接参数无效，请返回插件页重新打开。")),
    };
    let source = match query {
        PreviewQuery::History { id } => {
            if !history::valid_id(&id) {
                return Ok(html_reply(400, "预览记录标识无效，请返回插件页重新打开。"));
            }
            history::load_source(&call.host, &id).await
        }
        PreviewQuery::Draft { job, version } => {
            if !refine::valid_job_id(&job) {
                return Ok(html_reply(400, "预览记录标识无效，请返回插件页重新打开。"));
            }
            match refine::draft_source(&call.host, &job, &version).await {
                Ok(None) if version == "latest" && history::valid_id(&job) => {
                    history::load_source(&call.host, &job).await
                }
                other => other,
            }
        }
    };
    let html = match source {
        Ok(Some(source)) => generation::extract_document_html(&source),
        Ok(None) => None,
        Err(_) => {
            return Ok(html_reply(502, "暂时无法读取该记录源码，请返回插件页重试。"));
        }
    };
    match html {
        Some(html) => Ok(html_document_reply(200, html.into_bytes())),
        None => Ok(html_reply(
            404,
            "未找到可播放的动画源码，可能记录不存在或尚未生成，请返回插件页重试。",
        )),
    }
}

enum PreviewQuery {
    History { id: String },
    Draft { job: String, version: String },
}

fn parse_preview_query(query: &str) -> Result<PreviewQuery, ()> {
    if query.is_empty() {
        return Err(());
    }
    let mut id = None;
    let mut job = None;
    let mut version = None;
    for part in query.split('&').filter(|part| !part.is_empty()) {
        let (raw_name, raw_value) = part.split_once('=').ok_or(())?;
        let name = percent_decode(raw_name).ok_or(())?;
        let value = percent_decode(raw_value).ok_or(())?;
        match name.as_str() {
            "state" => {}
            "id" => {
                if id.is_some() || value.is_empty() {
                    return Err(());
                }
                id = Some(value);
            }
            "job" => {
                if job.is_some() || value.is_empty() {
                    return Err(());
                }
                job = Some(value);
            }
            "version" => {
                if version.is_some() || !matches!(value.as_str(), "first" | "latest") {
                    return Err(());
                }
                version = Some(value);
            }
            _ => return Err(()),
        }
    }
    match (id, job, version) {
        (Some(id), None, None) => Ok(PreviewQuery::History { id }),
        (None, Some(job), Some(version)) => Ok(PreviewQuery::Draft { job, version }),
        _ => Err(()),
    }
}

fn percent_decode(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = hex_nibble(*bytes.get(i + 1)?)?;
            let lo = hex_nibble(*bytes.get(i + 2)?)?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn html_reply(status: u16, message: &str) -> TypedReply<ManagementResponse> {
    let body = format!(
        "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
         <title>鹈鹕预览</title></head><body style=\"margin:0;padding:32px;\
         font-family:system-ui,sans-serif;background:#0f172a;color:#e2e8f0\">\
         <p>{message}</p></body></html>"
    );
    html_document_reply(status, body.into_bytes())
}

fn html_document_reply(status: u16, payload: Vec<u8>) -> TypedReply<ManagementResponse> {
    TypedReply::new(ManagementResponse {
        status,
        content_type: "text/html; charset=utf-8".into(),
        headers: vec![MiddlewareHeader {
            name: "cache-control".into(),
            value: b"no-store".to_vec(),
        }],
    })
    .with_payload(payload)
}

pub(super) async fn handle(
    call: TypedCall<ManagementRequest>,
) -> Result<TypedReply<ManagementResponse>, PluginFault> {
    let result = match (call.request.method.as_str(), call.request.path.as_str()) {
        ("GET", "bootstrap") => bootstrap(&call).await,
        ("POST", "models") => models(&call).await,
        ("GET", "history") => history::list(&call.host, &call.request.query).await,
        ("GET", "history/record") => history::detail(&call.host, &call.request.query).await,
        ("POST", "history") => history::save_manual(&call.host, &call.payload).await,
        ("GET", "schedule") => schedule::status(&call.host).await,
        ("POST", "schedule") => schedule::save(&call.host, &call.payload).await,
        ("POST", "schedule/pause") => schedule::pause(&call.host, &call.payload).await,
        ("POST", "schedule/delete") => schedule::delete(&call.host, &call.payload).await,
        ("GET", "refine") => refine::status(&call.host).await,
        ("POST", "refine") => refine::start(&call.host, &call.payload).await,
        ("POST", "refine/stop") => refine::stop(&call.host, &call.payload).await,
        ("GET", "refine/draft") => refine::document(&call.host, &call.request.query).await,
        _ => return reply(404, json!({"message": "页面接口不存在"})),
    };
    match result {
        Ok(value) => reply(200, value),
        Err(error) => {
            let (status, message) = match error.code {
                ErrorCode::Capacity if error.message == history::FULL => {
                    (507, "生成记录的存储已满，本次结果未保存。")
                }
                ErrorCode::InvalidInput if error.message == history::TOO_LARGE => {
                    (413, "本次结果太大，未保存到生成记录。")
                }
                ErrorCode::InvalidInput if error.message == history::MISSING => {
                    (404, "未找到这条生成记录。")
                }
                ErrorCode::InvalidInput if error.message == schedule::MISSING => {
                    (404, "未找到该定时计划。")
                }
                ErrorCode::InvalidInput => (
                    400,
                    match error.message.as_str() {
                        "pelican schedule cron" => {
                            "Cron 无效或没有未来的运行时间，请填写五段规则。"
                        }
                        "pelican schedule timezone" => {
                            "时区无效，请填写 IANA 时区，例如 Asia/Shanghai。"
                        }
                        "pelican schedule model" | "pelican refine model" => {
                            "所选 Key 当前无法使用此模型。"
                        }
                        _ => "请求参数无效",
                    },
                ),
                ErrorCode::PermissionDenied => (403, "没有访问权限，或所选 Client Key 不可用"),
                ErrorCode::Conflict if error.message == schedule::FULL => {
                    (409, "定时计划已达上限，请删除一个计划后再新增。")
                }
                ErrorCode::Conflict if error.message == schedule::CLAIMED => {
                    (409, "本轮计划正在提交，请稍后再删除。")
                }
                ErrorCode::Conflict if call.request.path.starts_with("schedule") => {
                    (409, "定时计划刚刚发生变化，请刷新后重试。")
                }
                ErrorCode::Conflict if error.message == refine::QUEUE_FULL => {
                    (409, "当前排队任务较多，请等待已有任务完成后再试。")
                }
                ErrorCode::Conflict if call.request.path.starts_with("refine") => {
                    (409, "当前有多轮任务或任务状态已变化，请查看最新进度。")
                }
                ErrorCode::Conflict => (409, "生成记录正在更新，请稍后重试"),
                ErrorCode::Timeout => (504, "读取超时，请稍后重试"),
                ErrorCode::Cancelled => (499, "请求已取消"),
                ErrorCode::Capacity => (503, "宿主繁忙或 Key 列表超过页面容量"),
                _ => (502, "读取失败，请检查插件状态和 CPR 请求记录"),
            };
            reply(status, json!({"message": message}))
        }
    }
}

async fn bootstrap(call: &TypedCall<ManagementRequest>) -> Result<Value, PluginFault> {
    let keys = identity::keys(&call.host).await?;
    Ok(json!({"prompt": PROMPT, "keys": keys}))
}

async fn models(call: &TypedCall<ManagementRequest>) -> Result<Value, PluginFault> {
    if call.payload.len() > 8 * 1024 {
        return Err(invalid());
    }
    let body: Value = serde_json::from_slice(&call.payload).map_err(|_| invalid())?;
    let object = body.as_object().ok_or_else(invalid)?;
    if object.len() != 1 {
        return Err(invalid());
    }
    let key = object
        .get("clientKeyId")
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    if !identity::valid_text(key, 200) {
        return Err(invalid());
    }
    Ok(json!({"models": list_models(&call.host, key).await?}))
}

async fn list_models(host: &HostClient, key: &str) -> Result<Vec<String>, PluginFault> {
    let request = ModelListRequest {
        client_key_id: key.into(),
        protocol: "openai".into(),
        client_version: concat!("pelican-test/", env!("CARGO_PKG_VERSION")).into(),
    };
    let result: ModelListResult = callback(host, host::MODELS_LIST, request).await?;
    Ok(result.models)
}

fn valid_effort(effort: &str) -> bool {
    matches!(
        effort,
        "auto" | "none" | "minimal" | "low" | "medium" | "high" | "xhigh"
    )
}

pub(super) fn default_verbosity() -> String {
    "auto".into()
}

pub(super) fn is_auto_verbosity(verbosity: &str) -> bool {
    verbosity.is_empty() || verbosity == "auto"
}

pub(super) fn valid_verbosity(verbosity: &str) -> bool {
    matches!(verbosity, "auto" | "low" | "medium" | "high")
}

async fn callback<I: Serialize, O: DeserializeOwned>(
    host: &HostClient,
    method: &str,
    input: I,
) -> Result<O, PluginFault> {
    let params = serde_json::to_value(input).map_err(|_| invalid())?;
    let response = host
        .call(method, params, Vec::new())
        .await
        .map_err(SessionError::into_plugin_fault)?;
    if !response.payload.is_empty() {
        return Err(invalid());
    }
    serde_json::from_value(response.result).map_err(|_| invalid())
}

fn invalid() -> PluginFault {
    PluginFault::new(
        ErrorCode::InvalidInput,
        "invalid pelican management request or host reply",
    )
}

fn reply(status: u16, body: Value) -> Result<TypedReply<ManagementResponse>, PluginFault> {
    let payload = serde_json::to_vec(&body).map_err(|_| invalid())?;
    Ok(TypedReply::new(ManagementResponse {
        status,
        content_type: "application/json".into(),
        headers: vec![MiddlewareHeader {
            name: "cache-control".into(),
            value: b"no-store".to_vec(),
        }],
    })
    .with_payload(payload))
}
