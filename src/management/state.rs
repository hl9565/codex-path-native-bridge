use gateway_plugin_sdk::{
    PluginFault,
    call::host::{
        self, StateDeleteRequest, StateDeleteResult, StateGetRequest, StateGetResult,
        StatePutRequest, StatePutResult,
    },
    client::HostClient,
};
use serde::{Serialize, de::DeserializeOwned};

use super::{callback, invalid};

pub(super) async fn get<T: DeserializeOwned>(
    host: &HostClient,
    namespace: &str,
    key: &str,
) -> Result<(Option<T>, Option<u64>), PluginFault> {
    let request = StateGetRequest {
        namespace: namespace.into(),
        key: key.into(),
    };
    let result: StateGetResult = callback(host, host::STATE_GET, request).await?;
    match result.record {
        Some(record) => Ok((
            Some(serde_json::from_value(record.value).map_err(|_| invalid())?),
            Some(record.version),
        )),
        None => Ok((None, None)),
    }
}

pub(super) async fn put<T: Serialize + ?Sized>(
    host: &HostClient,
    namespace: &str,
    key: &str,
    expected_version: Option<u64>,
    value: &T,
) -> Result<u64, PluginFault> {
    let request = StatePutRequest {
        namespace: namespace.into(),
        key: key.into(),
        expected_version,
        value: serde_json::to_value(value).map_err(|_| invalid())?,
    };
    Ok(
        callback::<_, StatePutResult>(host, host::STATE_PUT, request)
            .await?
            .version,
    )
}

pub(super) async fn delete(
    host: &HostClient,
    namespace: &str,
    key: &str,
    expected_version: u64,
) -> Result<(), PluginFault> {
    let request = StateDeleteRequest {
        namespace: namespace.into(),
        key: key.into(),
        expected_version,
    };
    callback::<_, StateDeleteResult>(host, host::STATE_DELETE, request).await?;
    Ok(())
}

// The host carries state values in a bounded JSON metadata frame. A raw byte
// split does not account for escapes or guarantee a UTF-8 character boundary.
pub(super) fn json_prefix(text: &str, budget: usize) -> usize {
    let mut spent = 0;
    for (index, ch) in text.char_indices() {
        spent += match ch {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
            '\0'..='\u{1f}' => 6,
            _ => ch.len_utf8(),
        };
        if spent > budget {
            return index;
        }
    }
    text.len()
}
