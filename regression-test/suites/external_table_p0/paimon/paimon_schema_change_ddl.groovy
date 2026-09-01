// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("paimon_schema_change_ddl", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "paimon_schema_change_ddl"
    String dbName = "paimon_schema_change_ddl_db"
    String tableName = "paimon_alter_table"
    String partitionTableName = "paimon_alter_partition_table"

    def schemaId = { String table ->
        def rows = sql """
            SELECT MAX(schema_id)
            FROM `${table}\$schemas`
        """
        assertEquals(1, rows.size())
        return (rows[0][0] as Number).longValue()
    }

    def columnNames = { String table ->
        return sql("DESC `${table}`").collect { row -> row[0].toString() }
    }

    def assertColumnOrder = { String table, List<String> expected ->
        assertEquals(expected, columnNames(table))
    }

    def assertColumnAbsent = { String table, String column ->
        assertFalse(columnNames(table).any { name -> name.equalsIgnoreCase(column) })
    }

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    sql """
        CREATE CATALOG `${catalogName}` PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        )
    """
    sql """SWITCH `${catalogName}`"""
    sql """DROP DATABASE IF EXISTS `${dbName}` FORCE"""
    sql """CREATE DATABASE `${dbName}`"""
    sql """USE `${dbName}`"""
    sql """SET show_column_comment_in_describe = true"""

    try {
        // This suite intentionally contains no INSERT. ALTER correctness is
        // verified from Doris metadata and Paimon's schema history table.
        // Use strict type evolution so narrowing conversions are covered as
        // deterministic failures; Paimon permits explicit casts by default.
        sql """
            CREATE TABLE `${tableName}` (
                id INT NOT NULL COMMENT 'identifier',
                required_value BIGINT NOT NULL,
                score INT NULL DEFAULT '1' COMMENT 'initial score',
                `MixedCase` STRING NULL,
                obsolete STRING NULL,
                amount DECIMAL(8, 2) NULL
            ) ENGINE=paimon
            PROPERTIES (
                'primary-key' = 'id',
                'disable-explicit-type-casting' = 'true'
            )
        """

        assertColumnOrder(
                tableName,
                ["id", "required_value", "score", "MixedCase", "obsolete", "amount"])
        qt_paimon_alter_initial_desc """DESC `${tableName}`"""
        qt_paimon_alter_initial_schema """
            SELECT schema_id, fields, partition_keys, primary_keys
            FROM `${tableName}\$schemas`
            ORDER BY schema_id DESC
            LIMIT 1
        """

        // ADD COLUMN: default, comment and AFTER position are committed as one
        // Paimon schema version.
        long beforeSchemaId = schemaId(tableName)
        sql """
            ALTER TABLE `${tableName}`
            ADD COLUMN added_after STRING NULL DEFAULT 'unknown'
                COMMENT 'added column' AFTER score
        """
        assertEquals(beforeSchemaId + 1, schemaId(tableName))
        assertColumnOrder(
                tableName,
                [
                    "id", "required_value", "score", "added_after",
                    "MixedCase", "obsolete", "amount"
                ])
        qt_paimon_alter_add_column_desc """DESC `${tableName}`"""
        qt_paimon_alter_add_column_schema """
            SELECT schema_id, fields
            FROM `${tableName}\$schemas`
            ORDER BY schema_id DESC
            LIMIT 1
        """

        // ADD COLUMNS is one Doris clause and one atomic Paimon schema commit.
        // TINYINT and SMALLINT also cover the narrow integer type mapping.
        beforeSchemaId = schemaId(tableName)
        sql """
            ALTER TABLE `${tableName}` ADD COLUMN (
                tiny_col TINYINT NULL,
                small_col SMALLINT NULL COMMENT 'small column',
                profile STRUCT<city:STRING, zip:INT> NULL
            )
        """
        assertEquals(beforeSchemaId + 1, schemaId(tableName))
        qt_paimon_alter_add_columns_desc """DESC `${tableName}`"""

        // FIRST position.
        sql """ALTER TABLE `${tableName}` ADD COLUMN first_col BIGINT NULL FIRST"""
        assertColumnOrder(
                tableName,
                [
                    "first_col", "id", "required_value", "score", "added_after",
                    "MixedCase", "obsolete", "amount", "tiny_col", "small_col", "profile"
                ])
        qt_paimon_alter_add_first_desc """DESC `${tableName}`"""

        // Doris resolves column names case-insensitively but sends the canonical
        // remote field name to Paimon.
        sql """ALTER TABLE `${tableName}` RENAME COLUMN mixedcase display_name"""
        assertColumnOrder(
                tableName,
                [
                    "first_col", "id", "required_value", "score", "added_after",
                    "display_name", "obsolete", "amount", "tiny_col", "small_col", "profile"
                ])
        qt_paimon_alter_rename_column_desc """DESC `${tableName}`"""

        // MODIFY COLUMN: widening type, nullability, default, comment and
        // position changes are committed together.
        beforeSchemaId = schemaId(tableName)
        sql """
            ALTER TABLE `${tableName}`
            MODIFY COLUMN score BIGINT NULL DEFAULT '10'
                COMMENT 'updated score' FIRST
        """
        assertEquals(beforeSchemaId + 1, schemaId(tableName))
        assertColumnOrder(
                tableName,
                [
                    "score", "first_col", "id", "required_value", "added_after",
                    "display_name", "obsolete", "amount", "tiny_col", "small_col", "profile"
                ])

        // Additional supported widening conversions and NOT NULL -> NULL.
        sql """ALTER TABLE `${tableName}` MODIFY COLUMN small_col INT NULL"""
        sql """ALTER TABLE `${tableName}` MODIFY COLUMN amount DECIMAL(12, 2) NULL"""
        sql """ALTER TABLE `${tableName}` MODIFY COLUMN required_value BIGINT NULL"""

        // Omitting DEFAULT and COMMENT in a full MODIFY definition removes
        // their existing values.
        sql """ALTER TABLE `${tableName}` MODIFY COLUMN added_after STRING NULL"""
        qt_paimon_alter_modify_column_desc """DESC `${tableName}`"""
        qt_paimon_alter_modify_column_schema """
            SELECT schema_id, fields
            FROM `${tableName}\$schemas`
            ORDER BY schema_id DESC
            LIMIT 1
        """

        sql """ALTER TABLE `${tableName}` DROP COLUMN obsolete"""
        assertColumnAbsent(tableName, "obsolete")
        qt_paimon_alter_drop_column_desc """DESC `${tableName}`"""

        sql """
            ALTER TABLE `${tableName}` ORDER BY (
                id, display_name, score, required_value, small_col,
                tiny_col, added_after, amount, profile, first_col
            )
        """
        assertColumnOrder(
                tableName,
                [
                    "id", "display_name", "score", "required_value", "small_col",
                    "tiny_col", "added_after", "amount", "profile", "first_col"
                ])
        qt_paimon_alter_reorder_columns_desc """DESC `${tableName}`"""
        qt_paimon_alter_final_schema """
            SELECT schema_id, fields, partition_keys, primary_keys
            FROM `${tableName}\$schemas`
            ORDER BY schema_id DESC
            LIMIT 1
        """

        // A failed ADD COLUMNS must not publish the valid prefix of the batch.
        beforeSchemaId = schemaId(tableName)
        test {
            sql """
                ALTER TABLE `${tableName}` ADD COLUMN (
                    batch_ok INT NULL,
                    batch_bad INT NOT NULL DEFAULT '1'
                )
            """
            exception "cannot specify NOT NULL"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))
        assertColumnAbsent(tableName, "batch_ok")
        assertColumnAbsent(tableName, "batch_bad")
        qt_paimon_alter_failed_batch_schema """
            SELECT schema_id, fields
            FROM `${tableName}\$schemas`
            ORDER BY schema_id DESC
            LIMIT 1
        """

        // Multiple Doris ALTER clauses cannot be committed atomically by an
        // external catalog, so they are rejected before the first mutation.
        beforeSchemaId = schemaId(tableName)
        test {
            sql """
                ALTER TABLE `${tableName}`
                ADD COLUMN multi_a INT NULL,
                ADD COLUMN multi_b INT NULL
            """
            exception "External table does not support multiple ALTER clauses"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))
        assertColumnAbsent(tableName, "multi_a")
        assertColumnAbsent(tableName, "multi_b")

        // Paimon SDK schema validation.
        beforeSchemaId = schemaId(tableName)
        test {
            sql """
                ALTER TABLE `${tableName}`
                ADD COLUMN required_col INT NOT NULL DEFAULT '1'
            """
            exception "cannot specify NOT NULL"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))

        test {
            sql """ALTER TABLE `${tableName}` MODIFY COLUMN score INT NULL"""
            exception "cannot be converted"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))

        test {
            sql """
                ALTER TABLE `${tableName}`
                MODIFY COLUMN added_after STRING NOT NULL DEFAULT 'unknown'
            """
            exception "Cannot update column type from nullable to non nullable"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))

        test {
            sql """ALTER TABLE `${tableName}` DROP COLUMN id"""
            exception "Cannot drop partition key or primary key"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))

        // Doris/Paimon adapter validation which cannot be delegated to the SDK.
        test {
            sql """ALTER TABLE `${tableName}` ADD COLUMN ID INT NULL"""
            exception "conflicts with an existing Paimon column"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))

        test {
            sql """ALTER TABLE `${tableName}` RENAME COLUMN display_name ID"""
            exception "conflicts with an existing Paimon column"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))

        test {
            sql """ALTER TABLE `${tableName}` ADD COLUMN agg_col INT SUM NULL"""
            exception "does not support aggregation method"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))

        test {
            sql """ALTER TABLE `${tableName}` ADD COLUMN auto_col BIGINT AUTO_INCREMENT"""
            exception "does not support AUTO_INCREMENT"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))

        test {
            sql """ALTER TABLE `${tableName}` ADD COLUMN generated_col INT AS (score + 1)"""
            exception "cannot be a generated column in a Paimon table"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))

        test {
            sql """ALTER TABLE `${tableName}` ADD COLUMN bad_position INT NULL AFTER missing_col"""
            exception "does not exist in Paimon table"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))

        test {
            sql """
                ALTER TABLE `${tableName}` ORDER BY (
                    id, display_name, score
                )
            """
            exception "must contain every Paimon column exactly once"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))

        test {
            sql """
                ALTER TABLE `${tableName}` ORDER BY (
                    id, display_name, score, required_value, small_col,
                    tiny_col, added_after, amount, profile, id
                )
            """
            exception "Duplicate column in reorder columns"
        }
        assertEquals(beforeSchemaId, schemaId(tableName))

        // Partition and primary-key constraints are delegated to Paimon.
        sql """
            CREATE TABLE `${partitionTableName}` (
                id INT NOT NULL,
                pt STRING NOT NULL,
                payload INT NULL
            ) ENGINE=paimon
            PARTITION BY (pt) ()
            PROPERTIES (
                'primary-key' = 'id,pt'
            )
        """
        qt_paimon_alter_partition_initial_desc """DESC `${partitionTableName}`"""
        qt_paimon_alter_partition_initial_schema """
            SELECT schema_id, fields, partition_keys, primary_keys
            FROM `${partitionTableName}\$schemas`
            ORDER BY schema_id DESC
            LIMIT 1
        """

        long partitionSchemaId = schemaId(partitionTableName)
        test {
            sql """ALTER TABLE `${partitionTableName}` DROP COLUMN pt"""
            exception "Cannot drop partition key or primary key"
        }
        assertEquals(partitionSchemaId, schemaId(partitionTableName))

        test {
            sql """ALTER TABLE `${partitionTableName}` RENAME COLUMN pt partition_col"""
            exception "Cannot rename partition column"
        }
        assertEquals(partitionSchemaId, schemaId(partitionTableName))

        test {
            sql """ALTER TABLE `${partitionTableName}` MODIFY COLUMN pt INT NOT NULL"""
            exception "Cannot update partition column"
        }
        assertEquals(partitionSchemaId, schemaId(partitionTableName))

        // Non-key columns of a partitioned table can still evolve.
        sql """ALTER TABLE `${partitionTableName}` MODIFY COLUMN payload BIGINT NULL"""
        sql """ALTER TABLE `${partitionTableName}` ADD COLUMN extra STRING NULL"""
        assertColumnOrder(partitionTableName, ["id", "pt", "payload", "extra"])
        qt_paimon_alter_partition_final_desc """DESC `${partitionTableName}`"""
        qt_paimon_alter_partition_final_schema """
            SELECT schema_id, fields, partition_keys, primary_keys
            FROM `${partitionTableName}\$schemas`
            ORDER BY schema_id DESC
            LIMIT 1
        """
    } finally {
        sql """DROP TABLE IF EXISTS `${partitionTableName}`"""
        sql """DROP TABLE IF EXISTS `${tableName}`"""
        sql """DROP DATABASE IF EXISTS `${dbName}` FORCE"""
        sql """SWITCH internal"""
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
