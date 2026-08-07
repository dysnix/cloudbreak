// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::{fmt, future::Future, io};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownSignal {
    Interrupt,
    Terminate,
}

impl fmt::Display for ShutdownSignal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Interrupt => "SIGINT",
            Self::Terminate => "SIGTERM",
        })
    }
}

/// Wait for an interactive interrupt or the termination signal used by container runtimes.
pub async fn wait_for_shutdown() -> io::Result<ShutdownSignal> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

        select_signal(tokio::signal::ctrl_c(), async move {
            terminate.recv().await;
        })
        .await
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok(ShutdownSignal::Interrupt)
    }
}

#[cfg(unix)]
async fn select_signal<I, T>(interrupt: I, terminate: T) -> io::Result<ShutdownSignal>
where
    I: Future<Output = io::Result<()>>,
    T: Future<Output = ()>,
{
    tokio::select! {
        result = interrupt => {
            result?;
            Ok(ShutdownSignal::Interrupt)
        }
        _ = terminate => Ok(ShutdownSignal::Terminate),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::future::{pending, ready};

    use super::{ShutdownSignal, select_signal};

    #[tokio::test]
    async fn selects_interrupt() {
        assert_eq!(
            select_signal(ready(Ok(())), pending()).await.unwrap(),
            ShutdownSignal::Interrupt
        );
    }

    #[tokio::test]
    async fn selects_terminate() {
        assert_eq!(
            select_signal(pending(), ready(())).await.unwrap(),
            ShutdownSignal::Terminate
        );
    }
}
