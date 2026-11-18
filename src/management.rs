mod history;
mod identity;
mod schedule;
mod state;

pub(crate) use schedule::reconcile;

use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::{
        host::{self, ModelListRequest, ModelListResult},
        management::{
            ManagementPage, ManagementRegistration, ManagementRequest, ManagementResource,
            ManagementResponse, ManagementRoute,
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
            route("GET", "schedule"),
            route("POST", "schedule"),
            route("POST", "schedule/pause"),
        ],
        resources: vec![ManagementResource {
            path: "web/pelican.html".into(),
            public: false,
        }],
        pages: vec![ManagementPage {
            id: "pelican".into(),
            title: "鹈鹕".into(),
            description: Some("生成 SVG 动画，点击记录中的缩略图回看。".into()),
            entry: "web/pelican.html".into(),
            icon: None,
        }],
        callbacks: Vec::new(),
    }
}

pub(super) async fn handle(
    call: TypedCall<ManagementRequest>,
) -> Result<TypedReply<ManagementResponse>, PluginFault> {
    let result = match (call.request.method.as_str(), call.request.path.as_str()) {
        ("GET", "bootstrap") => bootstrap(&call).await,
        ("POST", "models") => models(&call).await,
        ("GET", "history") => history::list(&call.host).await,
        ("POST", "history") => history::save_manual(&call.host, &call.payload).await,
        ("GET", "schedule") => schedule::status(&call.host).await,
        ("POST", "schedule") => schedule::save(&call.host, &call.payload).await,
        ("POST", "schedule/pause") => schedule::pause(&call.host, &call.payload).await,
        _ => return reply(404, json!({"message": "页面接口不存在"})),
    };
    match result {
        Ok(value) => reply(200, value),
        Err(error) => {
            let (status, message) = match error.code {
                ErrorCode::InvalidInput => (
                    400,
                    match error.message.as_str() {
                        "pelican schedule cron" => {
                            "Cron 无效或没有未来的运行时间，请填写五段规则。"
                        }
                        "pelican schedule timezone" => {
                            "时区无效，请填写 IANA 时区，例如 Asia/Shanghai。"
                        }
                        "pelican schedule model" => "所选 Key 当前无法使用此模型。",
                        _ => "请求参数无效",
                    },
                ),
                ErrorCode::PermissionDenied => (403, "没有访问权限，或所选 Client Key 不可用"),
                ErrorCode::Conflict if call.request.path.starts_with("schedule") => {
                    (409, "定时计划刚刚发生变化，请刷新后重试。")
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
        client_version: "codex-path-native-bridge/0.4.0".into(),
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
