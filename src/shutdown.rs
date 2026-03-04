use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::ShutdownError;

pub fn spawn_shutdown_listener(token: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = wait_for_shutdown_signal().await {
            tracing::error!(error = %err, "shutdown listener failed");
        }
        token.cancel();
    })
}

pub async fn wait_for_shutdown_signal() -> Result<(), ShutdownError> {
    #[cfg(unix)]
    {
        let mut term_signal =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(ShutdownError::SigTerm)?;

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.map_err(ShutdownError::Signal)?;
                tracing::info!("received SIGINT (ctrl-c)");
            }
            _ = term_signal.recv() => {
                tracing::info!("received SIGTERM");
            }
        }

        Ok(())
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(ShutdownError::Signal)?;
        tracing::info!("received shutdown signal");
        Ok(())
    }
}
