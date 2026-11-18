use std::time::Duration;

use gateway_plugin_sdk::{
    ErrorCode, PluginFault,
    call::{
        data::AccountFactsQuery,
        host::{self, ClientKey, KeyListRequest, KeyListResult},
        services::usage::{GetRecord, GetUsageRecord, UsageOutcome},
    },
    client::HostClient,
};
use tokio::time::{sleep, timeout};

use super::{callback, invalid};

pub(super) fn valid_text(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

pub(super) async fn keys(host: &HostClient) -> Result<Vec<ClientKey>, PluginFault> {
    let mut keys = Vec::new();
    let mut cursor = None;
    for _ in 0..64 {
        let request = KeyListRequest {
            cursor: cursor.clone(),
            limit: 200,
        };
        let page: KeyListResult = callback(host, host::KEYS_LIST, request).await?;
        keys.extend(page.keys);
        if keys.len() > 10_000 {
            return Err(PluginFault::new(
                ErrorCode::Capacity,
                "too many client keys",
            ));
        }
        match page.next_cursor {
            None => return Ok(keys),
            Some(next) if cursor.as_ref().is_none_or(|previous| &next != previous) => {
                cursor = Some(next);
            }
            _ => return Err(invalid()),
        }
    }
    Err(PluginFault::new(
        ErrorCode::Capacity,
        "too many client key pages",
    ))
}

pub(super) async fn key_name(host: &HostClient, key_id: &str) -> Option<String> {
    timeout(Duration::from_secs(1), keys(host))
        .await
        .ok()?
        .ok()?
        .into_iter()
        .find(|key| key.id == key_id)
        .map(|key| key.name)
        .filter(|name| valid_text(name, 512))
}

pub(super) struct Account {
    pub id: String,
    pub name: Option<String>,
}

pub(super) async fn account(host: &HostClient, key_id: &str, request_id: &str) -> Option<Account> {
    let (id, provider) = timeout(Duration::from_secs(2), async {
        // The final usage row can become visible slightly after the response finishes.
        for delay_ms in [0, 150, 400] {
            if delay_ms != 0 {
                sleep(Duration::from_millis(delay_ms)).await;
            }
            match host
                .service::<GetRecord>(GetUsageRecord {
                    id: request_id.into(),
                })
                .await
            {
                Ok(record) if record.id == request_id && record.data.client_key_ref == key_id => {
                    if record.data.outcome != UsageOutcome::Running {
                        return record
                            .data
                            .account_ref
                            .filter(|id| valid_text(id, 256))
                            .map(|id| (id, record.data.provider));
                    }
                }
                Err(error) if error.kind == "not_found" => {}
                _ => return None,
            }
        }
        None
    })
    .await
    .ok()??;

    let name = timeout(Duration::from_secs(1), account_name(host, &id, provider))
        .await
        .ok()
        .flatten();
    Some(Account { id, name })
}

async fn account_name(host: &HostClient, id: &str, provider_id: Option<String>) -> Option<String> {
    let mut cursor = None;
    for _ in 0..32 {
        let page = host
            .account_facts(AccountFactsQuery {
                provider_id: provider_id.clone(),
                cursor: cursor.clone(),
                limit: 200,
            })
            .await
            .ok()?;
        if let Some(account) = page
            .accounts
            .into_iter()
            .find(|account| account.account_id == id)
        {
            return Some(account.name).filter(|name| valid_text(name, 512));
        }
        let next = page.next_cursor?;
        if cursor.as_ref().is_some_and(|previous| &next <= previous) {
            return None;
        }
        cursor = Some(next);
    }
    None
}
