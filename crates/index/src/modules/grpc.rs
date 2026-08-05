// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_core::{EnvironmentInfo, IndexConfig};
use futures::StreamExt;
use sea_orm::DatabaseConnection;
use std::{
    collections::HashMap,
    ops::Add,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::mpsc::Sender,
    task::JoinHandle,
    time::{Instant, timeout},
};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::{
    geyser::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks,
        SubscribeRequestFilterSlots, SubscribeUpdate, subscribe_update::UpdateOneof,
    },
    tonic::codec::CompressionEncoding,
};

use crate::metrics;

async fn store_grpc_version(version_json: &str, db: &DatabaseConnection) {
    let grpc_version = serde_json::from_str::<serde_json::Value>(version_json)
        .ok()
        .and_then(|v| {
            v.get("version")
                .and_then(|inner| inner.get("version").and_then(|s| s.as_str()))
                .or_else(|| v.get("version").and_then(|s| s.as_str()))
                .map(str::to_string)
        });

    match grpc_version {
        Some(grpc_version) => {
            if let Err(e) = EnvironmentInfo::upsert_grpc_version(db, &grpc_version).await {
                tracing::error!("Failed to upsert grpc version: {:?}", e);
            }
        }
        None => tracing::error!(
            "Failed to parse grpc version from response: {}",
            version_json
        ),
    }
}

/// Creates a persistent Yellowstone GRPC connection with automatic reconnection.
/// Spawns a background task to handle the stream and forwards updates to the buffer channel.
/// Automatically reconnects on stream timeouts , stream `None` or errors (only after exceeding
///  the `max_grpc_errors` count). It also resets the is_startup flag when the connection is lost.
///
/// Reconnection is governed by a single give-up window: `reconnect_failed_since` records when we
/// started failing to (re)connect/subscribe and is only cleared once we successfully connect AND
/// subscribe. Connect/subscribe failures log an error and `continue`; the loop backs off by
/// `config.grpc.reconnect_backoff` between attempts and panics once the failing window exceeds
/// `config.grpc.reconnect_give_up`.
pub fn subscribe_grpc_with_reconnection(
    config: IndexConfig,
    buffer_channel_tx: Sender<SubscribeUpdate>,
    buffer_channel_rx_len: Arc<Mutex<usize>>,
    last_slot_received: Arc<Mutex<u64>>,
    cancel: Arc<AtomicBool>,
    db: DatabaseConnection,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _guard = metrics::TokioTaskCounterGuard::new("grpc");
        let mut log_first_message = true;
        let mut reconnect_failed_since: Option<Instant> = None;
        let mut is_reconnect = false;

        let give_up = config.grpc.reconnect_give_up;
        let backoff = config.grpc.reconnect_backoff;
        let from_slot_retain = config.grpc.reconnect_from_slot_retain;

        loop {
            if cancel.load(Ordering::SeqCst) {
                tracing::info!("GRPC subscription cancelled");
                return;
            }

            // Centralized reconnection backoff / give-up: `reconnect_failed_since` is `Some` while
            // we are failing to (re)connect or subscribe and only cleared once both succeed.
            if let Some(started) = reconnect_failed_since {
                if started.elapsed() >= give_up {
                    tracing::error!(
                        "Failed to (re)connect to Yellowstone GRPC after {:?}",
                        started.elapsed()
                    );
                    panic!(
                        "Failed to (re)connect to Yellowstone GRPC after {:?}",
                        started.elapsed()
                    );
                }
                tokio::time::sleep(backoff).await;
            }

            let grpc_timeout = Duration::from_secs(config.grpc.timeout);

            let mut client = match GeyserGrpcClient::build_from_shared(config.grpc.endpoint.clone())
                .expect("Failed to build GeyserGrpcClient")
                .x_token(Some(config.grpc.x_token.clone().unwrap_or_default()))
                .expect("Failed to set x-token")
                .max_decoding_message_size(usize::MAX)
                .accept_compressed(CompressionEncoding::Zstd)
                .connect_timeout(grpc_timeout)
                .timeout(grpc_timeout)
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .expect("Failed to set tls config")
                .tcp_keepalive(Some(Duration::from_secs(10)))
                .http2_keep_alive_interval(Duration::from_secs(10))
                .keep_alive_timeout(Duration::from_secs(10))
                // .http2_adaptive_window(true)
                // .initial_stream_window_size(8 * 1024 * 1024) // 8MB
                // .initial_connection_window_size(8 * 1024 * 1024) // 8MB
                .connect()
                .await
            {
                Ok(mut c) => {
                    match c.get_version().await {
                        Ok(response) => store_grpc_version(&response.version, &db).await,
                        Err(e) => tracing::error!("Failed to get grpc version: {:?}", e),
                    }
                    c
                }
                Err(e) => {
                    reconnect_failed_since.get_or_insert_with(Instant::now);
                    tracing::error!("Failed to connect to Yellowstone GRPC: {:?}", e);
                    metrics::increment_grpc_errors();
                    continue;
                }
            };

            // let account_include = config
            //     .programs
            //     .include
            //     .iter()
            //     .map(|pubkey| pubkey.0.to_string())
            //     .collect();

            // tracing::debug!("Account include: {:?}", account_include);

            // Replay from the last received slot on reconnect, but only while we have been failing
            // for less than `from_slot_retain`; after that, drop it since the server may no longer
            // have that slot buffered.
            let keep_from_slot =
                reconnect_failed_since.is_none_or(|started| started.elapsed() < from_slot_retain);
            let from_slot = if is_reconnect && keep_from_slot {
                let last = *last_slot_received
                    .lock()
                    .expect("Failed to lock last_slot_received");
                (last != 0).then_some(last + 1)
            } else {
                None
            };

            let blocks_subscribe_request: SubscribeRequest = SubscribeRequest {
                accounts: HashMap::new(),
                slots: HashMap::from([(
                    "accounts_slots".to_string(),
                    SubscribeRequestFilterSlots {
                        filter_by_commitment: Some(false),
                        interslot_updates: Some(false),
                    },
                )]),
                transactions: HashMap::new(),
                transactions_status: HashMap::new(),
                blocks: HashMap::from([(
                    "accounts_blocks".to_string(),
                    SubscribeRequestFilterBlocks {
                        account_include: vec![],
                        include_transactions: Some(false),
                        include_accounts: Some(true),
                        include_entries: Some(false),
                    },
                )]),
                blocks_meta: HashMap::new(),
                entry: HashMap::new(),
                commitment: Some(CommitmentLevel::Confirmed as i32),
                accounts_data_slice: Vec::new(),
                ping: None,
                from_slot,
            };

            let (_sub_tx, stream) = match client
                .subscribe_with_request(Some(blocks_subscribe_request))
                .await
            {
                Ok(subscription) => {
                    if let Some(slot) = from_slot {
                        tracing::info!(
                            "Reconnected to Yellowstone GRPC replaying from slot {}",
                            slot
                        );
                    }
                    subscription
                }
                Err(e) => {
                    reconnect_failed_since.get_or_insert_with(Instant::now);
                    tracing::error!(
                        "Failed to subscribe to Yellowstone GRPC (from_slot {:?}): {:?}",
                        from_slot,
                        e
                    );
                    metrics::increment_grpc_errors();
                    continue;
                }
            };

            let buffer_channel_rx_len_clone = buffer_channel_rx_len.clone();
            let mut grpc_current_errors = 0;

            let buffer_channel_tx_clone = buffer_channel_tx.clone();
            let last_slot_received = last_slot_received.clone();
            let cancel_clone = cancel.clone();

            let handle = tokio::spawn(async move {
                let _guard = metrics::TokioTaskCounterGuard::new("grpc");

                let mut stream = std::pin::pin!(stream);

                let mut last_block_received_at = Instant::now();
                let mut received_block = false;

                let mut buffer_channel_size =
                    buffer_channel_tx_clone.max_capacity() - buffer_channel_tx_clone.capacity();

                // Add a timeout in case we stop receiving updates for 30 more seconds than the grpc timeout
                // If we reach it, we break the loop and try to reconnect
                while let Some(update) =
                    timeout(grpc_timeout.add(Duration::from_secs(30)), stream.next())
                        .await
                        .unwrap_or_else(|elapsed| {
                            tracing::error!(
                                "GRPC timeout: {:?} - grpc_errors_count: {}",
                                elapsed,
                                grpc_current_errors,
                            );
                            metrics::increment_grpc_timeout_errors();

                            // If the timeout is reached, we return None to break the loop
                            None
                        })
                {
                    if cancel_clone.load(Ordering::SeqCst) {
                        tracing::info!("GRPC subscription cancelled mid-stream");
                        return received_block;
                    }

                    metrics::GRPC_TOTAL_UPDATES_RECEIVED.inc();

                    if Instant::now().duration_since(last_block_received_at)
                        > Duration::from_secs(30)
                    {
                        tracing::error!("No block received in the last 30 seconds");
                        grpc_current_errors += 1;
                        metrics::increment_grpc_errors();

                        if grpc_current_errors >= config.grpc.max_grpc_errors {
                            break;
                        }
                    }

                    buffer_channel_size =
                        buffer_channel_tx_clone.max_capacity() - buffer_channel_tx_clone.capacity();

                    metrics::GRPC_BUFFER_CHANNEL_SIZE_SENDER.set(buffer_channel_size as i64);

                    match update {
                        Ok(update) => {
                            if let Some(UpdateOneof::Block(block)) = &update.update_oneof {
                                last_block_received_at = Instant::now();
                                received_block = true;

                                if log_first_message {
                                    tracing::info!(
                                        "Starting a new indexer service run - slot: {}",
                                        block.slot
                                    );
                                    log_first_message = false;
                                }
                            }

                            buffer_channel_tx_clone
                                .send(update)
                                .await
                                .expect("Failed to send update to buffer channel");
                        }
                        Err(e) => {
                            tracing::error!(
                                "GRPC error: {:?} buffer_channel_size: {} (sender: {}) - grpc_errors_count: {}",
                                e,
                                *buffer_channel_rx_len_clone
                                    .lock()
                                    .expect("Failed to lock buffer_channel_rx_len"),
                                buffer_channel_size,
                                grpc_current_errors,
                            );
                            grpc_current_errors += 1;
                            metrics::increment_grpc_errors();

                            if grpc_current_errors >= config.grpc.max_grpc_errors {
                                break;
                            }
                        }
                    }
                }

                tracing::error!(
                    "Breaking out of grpc subscription loop at slot: {} - buffer_channel_size: {} (sender: {})",
                    *last_slot_received
                        .lock()
                        .expect("Failed to lock last_slot_received"),
                    *buffer_channel_rx_len_clone
                        .lock()
                        .expect("Failed to lock buffer_channel_rx_len"),
                    buffer_channel_size,
                );

                received_block
            });

            match handle.await {
                Ok(received_block) => {
                    tracing::debug!("GRPC subscription handle finished");
                    if received_block {
                        reconnect_failed_since = Some(Instant::now());
                    } else {
                        reconnect_failed_since.get_or_insert_with(Instant::now);
                    }
                }
                Err(e) => {
                    tracing::error!("GRPC subscription handle panicked: {:?}", e);
                    reconnect_failed_since.get_or_insert_with(Instant::now);
                }
            }

            is_reconnect = true;
        }
    })
}
