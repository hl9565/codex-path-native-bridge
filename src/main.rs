mod management;

use gateway_plugin_sdk::Manifest;
use gateway_plugin_sdk::client::{
    PluginBuilder, PluginSession, SessionConfig, SessionError, methods,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = Manifest::from_author_slice(include_bytes!("../plugin.json"))?;
    let session = PluginSession::accept(
        tokio::io::stdin(),
        tokio::io::stdout(),
        SessionConfig::default(),
    )
    .await?;
    if session.handshake().plugin_id != manifest.plugin_id()?
        || session.handshake().contributes != manifest.contributes
        || !session
            .handshake()
            .configuration
            .as_object()
            .is_some_and(|value| value.is_empty())
    {
        return Err(SessionError::Handshake.into());
    }
    let plugin = PluginBuilder::from_manifest(manifest)?
        .management(management::registration(), management::handle)?
        .on(methods::RECONCILE, management::reconcile)?
        .build()?;
    session.run(plugin).await?;
    Ok(())
}
