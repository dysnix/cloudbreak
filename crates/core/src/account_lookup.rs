// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_entity::account_lookup;
use rust_decimal::Decimal;
use sea_orm::{
    ActiveValue::{NotSet, Set},
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect,
    prelude::Expr,
    sea_query::{Alias, OnConflict},
};
use std::collections::HashSet;

pub const CONFIRMED_COMMITMENT: i32 = 1;
pub const FINALIZED_COMMITMENT: i32 = 2;

pub async fn upsert<C: ConnectionTrait>(
    db: &C,
    rows: Vec<account_lookup::ActiveModel>,
) -> Result<(), sea_orm::DbErr> {
    if rows.is_empty() {
        return Ok(());
    }

    account_lookup::Entity::insert_many(rows)
        .on_conflict(
            OnConflict::columns([
                account_lookup::Column::Pubkey,
                account_lookup::Column::Commitment,
            ])
            .update_columns([
                account_lookup::Column::Present,
                account_lookup::Column::Owner,
                account_lookup::Column::Lamports,
                account_lookup::Column::AccountSlot,
                account_lookup::Column::Executable,
                account_lookup::Column::RentEpoch,
                account_lookup::Column::Data,
                account_lookup::Column::WriteVersion,
                account_lookup::Column::UpdatedOn,
            ])
            .action_cond_where(
                Condition::any()
                    .add(
                        Expr::col((Alias::new("excluded"), account_lookup::Column::AccountSlot))
                            .gt(Expr::col((
                                account_lookup::Entity,
                                account_lookup::Column::AccountSlot,
                            ))),
                    )
                    .add(
                        Condition::all()
                            .add(
                                Expr::col((
                                    Alias::new("excluded"),
                                    account_lookup::Column::AccountSlot,
                                ))
                                .eq(Expr::col((
                                    account_lookup::Entity,
                                    account_lookup::Column::AccountSlot,
                                ))),
                            )
                            .add(
                                Expr::col((
                                    Alias::new("excluded"),
                                    account_lookup::Column::WriteVersion,
                                ))
                                .gte(Expr::col((
                                    account_lookup::Entity,
                                    account_lookup::Column::WriteVersion,
                                ))),
                            ),
                    ),
            )
            .to_owned(),
        )
        .exec_without_returning(db)
        .await?;

    Ok(())
}

/// Updates rows that are already tracked without expanding the lookup to every
/// account seen by the indexer. API misses use [`upsert`] to opt a pubkey into
/// the cache; live confirmed/finalized maintenance uses this function.
pub async fn upsert_existing<C: ConnectionTrait>(
    db: &C,
    rows: Vec<account_lookup::ActiveModel>,
) -> Result<(), sea_orm::DbErr> {
    if rows.is_empty() {
        return Ok(());
    }

    let pubkeys = rows
        .iter()
        .map(|row| row.pubkey.as_ref().clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let commitments = rows
        .iter()
        .map(|row| *row.commitment.as_ref())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let existing = account_lookup::Entity::find()
        .select_only()
        .column(account_lookup::Column::Pubkey)
        .column(account_lookup::Column::Commitment)
        .filter(account_lookup::Column::Pubkey.is_in(pubkeys))
        .filter(account_lookup::Column::Commitment.is_in(commitments))
        .into_tuple::<(Vec<u8>, i32)>()
        .all(db)
        .await?
        .into_iter()
        .collect::<HashSet<_>>();
    let rows = rows
        .into_iter()
        .filter(|row| existing.contains(&(row.pubkey.as_ref().clone(), *row.commitment.as_ref())))
        .collect();

    upsert(db, rows).await
}

pub fn tombstone(pubkey: Vec<u8>, commitment: i32, slot: i64) -> account_lookup::ActiveModel {
    account_lookup::ActiveModel {
        pubkey: Set(pubkey),
        commitment: Set(commitment),
        present: Set(false),
        owner: Set(Vec::new()),
        lamports: Set(0),
        account_slot: Set(slot),
        executable: Set(false),
        rent_epoch: Set(Decimal::ZERO),
        data: Set(Vec::new()),
        write_version: Set(0),
        updated_on: NotSet,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{ColumnTrait, Database, EntityTrait, QueryFilter};

    fn live_row(pubkey: Vec<u8>, commitment: i32, slot: i64) -> account_lookup::ActiveModel {
        account_lookup::ActiveModel {
            pubkey: Set(pubkey),
            commitment: Set(commitment),
            present: Set(true),
            owner: Set(vec![2; 32]),
            lamports: Set(slot),
            account_slot: Set(slot),
            executable: Set(false),
            rent_epoch: Set(Decimal::ZERO),
            data: Set(vec![slot as u8]),
            write_version: Set(1),
            updated_on: NotSet,
        }
    }

    #[tokio::test]
    async fn keeps_commitments_separate_and_rejects_older_updates() {
        let Ok(database_url) = std::env::var("CLOUDBREAK_TEST_DATABASE_URL") else {
            return;
        };
        let db = Database::connect(database_url).await.unwrap();
        let pubkey = vec![0xa8; 32];
        let untracked_pubkey = vec![0xb9; 32];
        account_lookup::Entity::delete_many()
            .filter(
                account_lookup::Column::Pubkey.is_in([pubkey.clone(), untracked_pubkey.clone()]),
            )
            .exec(&db)
            .await
            .unwrap();

        upsert(
            &db,
            vec![tombstone(pubkey.clone(), FINALIZED_COMMITMENT, 10)],
        )
        .await
        .unwrap();
        upsert_existing(
            &db,
            vec![live_row(untracked_pubkey.clone(), FINALIZED_COMMITMENT, 12)],
        )
        .await
        .unwrap();
        upsert(&db, vec![live_row(pubkey.clone(), FINALIZED_COMMITMENT, 9)])
            .await
            .unwrap();
        upsert(
            &db,
            vec![live_row(pubkey.clone(), CONFIRMED_COMMITMENT, 12)],
        )
        .await
        .unwrap();
        upsert_existing(
            &db,
            vec![live_row(pubkey.clone(), FINALIZED_COMMITMENT, 11)],
        )
        .await
        .unwrap();

        let rows = account_lookup::Entity::find()
            .filter(account_lookup::Column::Pubkey.eq(pubkey.clone()))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            account_lookup::Entity::find()
                .filter(account_lookup::Column::Pubkey.eq(untracked_pubkey.clone()))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            rows.iter()
                .find(|row| row.commitment == CONFIRMED_COMMITMENT)
                .unwrap()
                .account_slot,
            12
        );
        let finalized = rows
            .iter()
            .find(|row| row.commitment == FINALIZED_COMMITMENT)
            .unwrap();
        assert!(finalized.present);
        assert_eq!(finalized.account_slot, 11);

        account_lookup::Entity::delete_many()
            .filter(account_lookup::Column::Pubkey.is_in([pubkey, untracked_pubkey]))
            .exec(&db)
            .await
            .unwrap();
    }
}
