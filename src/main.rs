mod management;

use gateway_plugin_sdk::{CallBudgets, Manifest, Stage};
use gateway_plugin_sdk::client::{
    PluginBuilder, PluginSession, SessionConfig, SessionError, methods,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = Manifest::from_author_slice(include_bytes!("../plugin.json"))?;
    let mut session = PluginSession::accept(
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
        .on(methods::MANAGEMENT_CALLBACK, management::callback_handle)?
        .on(methods::RECONCILE, management::reconcile)?
        .build()?;
    session.set_call_budgets(CallBudgets::from([(Stage::Maintenance, 360_000)]))?;
    session.run(plugin).await?;
    Ok(())
}
