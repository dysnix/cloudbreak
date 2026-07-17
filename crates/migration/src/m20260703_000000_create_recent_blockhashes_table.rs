use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();

        connection
            .execute_unprepared(
                r#"
                CREATE TABLE IF NOT EXISTS recent_blockhashes (
                    slot      BIGINT NOT NULL PRIMARY KEY,
                    blockhash TEXT   NOT NULL
                );
                CREATE INDEX IF NOT EXISTS recent_blockhashes_blockhash_idx
                    ON recent_blockhashes (blockhash);
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS recent_blockhashes;")
            .await?;

        Ok(())
    }
}
