use gateway_plugin_sdk::{
    PluginFault,
    call::host::{self, StateGetRequest, StateGetResult, StatePutRequest, StatePutResult},
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
) -> Result<(), PluginFault> {
    let request = StatePutRequest {
        namespace: namespace.into(),
        key: key.into(),
        expected_version,
        value: serde_json::to_value(value).map_err(|_| invalid())?,
    };
    callback::<_, StatePutResult>(host, host::STATE_PUT, request).await?;
    Ok(())
}
