// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

//! `SeaORM` entity for the unpartitioned current-account lookup table.

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "account_lookup")]
pub struct Model {
    #[sea_orm(
        primary_key,
        auto_increment = false,
        column_type = "VarBinary(StringLen::None)"
    )]
    pub pubkey: Vec<u8>,
    #[sea_orm(primary_key, auto_increment = false)]
    pub commitment: i32,
    pub present: bool,
    #[sea_orm(column_type = "VarBinary(StringLen::None)")]
    pub owner: Vec<u8>,
    pub lamports: i64,
    pub account_slot: i64,
    pub executable: bool,
    #[sea_orm(column_type = "Decimal(Some((20, 0)))")]
    pub rent_epoch: Decimal,
    #[sea_orm(column_type = "VarBinary(StringLen::None)")]
    pub data: Vec<u8>,
    pub write_version: i64,
    pub updated_on: DateTimeWithTimeZone,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
