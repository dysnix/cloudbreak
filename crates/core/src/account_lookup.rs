// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use cloudbreak_entity::account_lookup;
use rust_decimal::Decimal;
use sea_orm::{
    ActiveValue::{NotSet, Set},
    Condition, ConnectionTrait, DatabaseBackend, EntityTrait, Statement, Value,
    prelude::Expr,
    sea_query::{Alias, OnConflict},
};

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

/// Marks already-cached keys dirty without inserting unqueried accounts. The
/// API omits dirty rows from point lookups and refreshes them from canonical
/// storage. Moving the marker to the update slot also makes tip races safe: an
/// API request at an older tip cannot overwrite the future dirty marker.
pub async fn mark_dirty_existing<C: ConnectionTrait>(
    db: &C,
    pubkeys: &[Vec<u8>],
    commitment: i32,
    slot: i64,
) -> Result<(), sea_orm::DbErr> {
    if pubkeys.is_empty() {
        return Ok(());
    }

    let pubkeys = pubkeys
        .iter()
        .cloned()
        .map(|pubkey| Value::Bytes(Some(Box::new(pubkey))))
        .collect();

    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        r#"
        UPDATE account_lookup
        SET present = FALSE,
            owner = '\x'::bytea,
            lamports = 0,
            account_slot = $3::bigint,
            executable = FALSE,
            rent_epoch = 0,
            data = '\x'::bytea,
            write_version = -1,
            updated_on = CURRENT_TIMESTAMP
        WHERE pubkey = ANY($1::bytea[])
          AND commitment = $2::integer
          AND account_slot <= $3::bigint
        "#,
        vec![
            Value::Array(
                sea_orm::sea_query::ArrayType::Bytes,
                Some(Box::new(pubkeys)),
            ),
            Value::Int(Some(commitment)),
            Value::BigInt(Some(slot)),
        ],
    ))
    .await?;
    Ok(())
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
        upsert(&db, vec![live_row(pubkey.clone(), FINALIZED_COMMITMENT, 9)])
            .await
            .unwrap();
        upsert(
            &db,
            vec![live_row(pubkey.clone(), CONFIRMED_COMMITMENT, 12)],
        )
        .await
        .unwrap();
        upsert(
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

        mark_dirty_existing(
            &db,
            &[pubkey.clone(), untracked_pubkey.clone()],
            FINALIZED_COMMITMENT,
            12,
        )
        .await
        .unwrap();
        let dirty = account_lookup::Entity::find_by_id((pubkey.clone(), FINALIZED_COMMITMENT))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(!dirty.present);
        assert_eq!(dirty.account_slot, 12);
        assert_eq!(dirty.write_version, -1);
        assert!(
            account_lookup::Entity::find()
                .filter(account_lookup::Column::Pubkey.eq(untracked_pubkey.clone()))
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );

        account_lookup::Entity::delete_many()
            .filter(account_lookup::Column::Pubkey.is_in([pubkey, untracked_pubkey]))
            .exec(&db)
            .await
            .unwrap();
    }
}
