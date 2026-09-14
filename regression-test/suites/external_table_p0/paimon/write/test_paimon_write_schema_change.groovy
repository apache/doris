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

suite("test_paimon_write_schema_change", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_schema_change_catalog"
    String dbName = "test_pw_schema_change_db"
    String appendTable = "t_schema_change_append"
    String typeTable = "t_schema_change_types"
    String explicitTypeTable = "t_schema_change_explicit_types"
    String primaryKeyTable = "t_schema_change_pk"

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
        def assertTableEquals = { String table, String columns, String orderBy ->
            spark_paimon """
                REFRESH TABLE paimon.${dbName}.${table}
            """
            def sparkRows = spark_paimon """
                SELECT ${columns}
                FROM paimon.${dbName}.${table}
                ${orderBy}
            """
            def dorisRows = sql """
                SELECT ${columns}
                FROM `${table}`
                ${orderBy}
            """
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        // ------------------------------------------------------------------
        // Append-only partitioned table: every supported column evolution is
        // followed by reading historical rows and writing with the new schema.
        // ------------------------------------------------------------------
        sql """
            CREATE TABLE `${appendTable}` (
                id INT NULL,
                required_value BIGINT NOT NULL,
                name STRING NULL,
                score INT NULL,
                amount DECIMAL(8, 2) NULL,
                obsolete STRING NULL,
                dt STRING NULL
            ) ENGINE=paimon
            PARTITION BY (dt) ()
            PROPERTIES (
                'disable-explicit-type-casting' = 'true'
            )
        """

        sql """
            INSERT INTO `${appendTable}` VALUES
                (1, 100, 'alice', 10, 1.10, 'old-a', '2026-07-01'),
                (2, 200, 'bob', 20, 2.20, 'old-b', '2026-07-02')
        """
        order_qt_sc_append_initial """
            SELECT id, required_value, name, score, amount, obsolete, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, required_value, name, score, amount, obsolete, dt",
                "ORDER BY id")

        // ADD COLUMN with DEFAULT, COMMENT and AFTER. Historical rows remain
        // readable and explicit values can immediately be written.
        sql """
            ALTER TABLE `${appendTable}`
            ADD COLUMN added_after STRING NULL DEFAULT 'unknown'
                COMMENT 'added after score' AFTER score
        """
        order_qt_sc_add_after_before_insert """
            SELECT id, required_value, name, score, added_after, amount, obsolete, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, required_value, name, score, added_after, amount, obsolete, dt",
                "ORDER BY id")

        sql """
            INSERT INTO `${appendTable}`
                (added_after, dt, id, obsolete, amount, name, required_value, score)
            VALUES
                ('added-3', '2026-07-03', 3, 'old-c', 3.30, 'carol', 300, 30),
                (NULL, '2026-07-01', 4, 'old-d', 4.40, 'dave', 400, 40)
        """
        order_qt_sc_add_after_after_insert """
            SELECT id, required_value, name, score, added_after, amount, obsolete, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, required_value, name, score, added_after, amount, obsolete, dt",
                "ORDER BY id")

        sql """
            INSERT INTO `${appendTable}`
                (id, required_value, name, score, amount, obsolete, dt)
            VALUES (100, 10000, 'default-value', 100, 100.00, 'old-default', '2026-07-10')
        """
        order_qt_sc_add_default_omitted """
            SELECT id, added_after FROM `${appendTable}` WHERE id = 100
        """

        // ADD COLUMN FIRST. All historical rows expose NULL for the new column.
        sql """ALTER TABLE `${appendTable}` ADD COLUMN first_col BIGINT NULL FIRST"""
        order_qt_sc_add_first_before_insert """
            SELECT id, first_col, required_value, name, score, added_after, amount, obsolete, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, first_col, required_value, name, score, added_after, amount, obsolete, dt",
                "ORDER BY id")

        sql """
            INSERT INTO `${appendTable}`
                (first_col, id, required_value, name, score, added_after, amount, obsolete, dt)
            VALUES
                (5000, 5, 500, 'erin', 50, 'added-first', 5.50, 'old-e', '2026-07-04')
        """
        order_qt_sc_add_first_after_insert """
            SELECT id, first_col, required_value, name, score, added_after, amount, obsolete, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, first_col, required_value, name, score, added_after, amount, obsolete, dt",
                "ORDER BY id")

        // ADD COLUMNS validates that a batch of columns is visible atomically
        // to both the reader and writer.
        sql """
            ALTER TABLE `${appendTable}` ADD COLUMN (
                tiny_col TINYINT NULL,
                small_col SMALLINT NULL COMMENT 'small integer'
            )
        """
        order_qt_sc_add_columns_before_insert """
            SELECT id, first_col, required_value, name, score, added_after,
                   amount, obsolete, tiny_col, small_col, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                """
                    id, first_col, required_value, name, score, added_after,
                    amount, obsolete, tiny_col, small_col, dt
                """,
                "ORDER BY id")

        sql """
            INSERT INTO `${appendTable}`
                (small_col, tiny_col, dt, id, first_col, required_value,
                 name, score, added_after, amount, obsolete)
            VALUES
                (600, 6, '2026-07-05', 6, 6000, 600,
                 'frank', 60, 'added-columns', 6.60, 'old-f')
        """
        // Omit the newly added nullable columns while keeping added_after
        // explicit because its omitted-default behavior is tracked above.
        sql """
            INSERT INTO `${appendTable}`
                (id, required_value, name, score, added_after, amount, obsolete, dt)
            VALUES
                (60, 6000, 'partial-columns', 600, 'explicit-default-column',
                 60.60, 'old-partial', '2026-07-05')
        """
        order_qt_sc_add_columns_after_insert """
            SELECT id, first_col, required_value, name, score, added_after,
                   amount, obsolete, tiny_col, small_col, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                """
                    id, first_col, required_value, name, score, added_after,
                    amount, obsolete, tiny_col, small_col, dt
                """,
                "ORDER BY id")

        // DROP a populated non-key column.
        sql """ALTER TABLE `${appendTable}` DROP COLUMN obsolete"""
        order_qt_sc_drop_before_insert """
            SELECT id, first_col, required_value, name, score, added_after,
                   amount, tiny_col, small_col, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                """
                    id, first_col, required_value, name, score, added_after,
                    amount, tiny_col, small_col, dt
                """,
                "ORDER BY id")

        sql """
            INSERT INTO `${appendTable}`
                (tiny_col, small_col, dt, id, first_col, required_value,
                 name, score, added_after, amount)
            VALUES
                (7, 700, '2026-07-06', 7, 7000, 700,
                 'grace', 70, 'after-drop', 7.70)
        """
        order_qt_sc_drop_after_insert """
            SELECT id, first_col, required_value, name, score, added_after,
                   amount, tiny_col, small_col, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                """
                    id, first_col, required_value, name, score, added_after,
                    amount, tiny_col, small_col, dt
                """,
                "ORDER BY id")

        // RENAME resolves the old name case-insensitively while preserving the
        // Paimon field id and all historical values.
        sql """ALTER TABLE `${appendTable}` RENAME COLUMN NAME full_name"""
        order_qt_sc_rename_before_insert """
            SELECT id, first_col, required_value, full_name, score, added_after,
                   amount, tiny_col, small_col, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                """
                    id, first_col, required_value, full_name, score, added_after,
                    amount, tiny_col, small_col, dt
                """,
                "ORDER BY id")

        sql """
            INSERT INTO `${appendTable}`
                (full_name, id, score, added_after, dt, amount,
                 required_value, first_col, tiny_col, small_col)
            VALUES
                ('heidi', 8, 80, 'after-rename', '2026-07-02', 8.80,
                 800, 8000, 8, 800)
        """
        order_qt_sc_rename_after_insert """
            SELECT id, first_col, required_value, full_name, score, added_after,
                   amount, tiny_col, small_col, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                """
                    id, first_col, required_value, full_name, score, added_after,
                    amount, tiny_col, small_col, dt
                """,
                "ORDER BY id")

        // MODIFY type: INT -> BIGINT. The new value exceeds the INT range.
        sql """ALTER TABLE `${appendTable}` MODIFY COLUMN score BIGINT NULL"""
        order_qt_sc_modify_bigint_before_insert """
            SELECT id, full_name, score, amount, required_value, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, full_name, score, amount, required_value, dt",
                "ORDER BY id")

        sql """
            INSERT INTO `${appendTable}`
                (id, first_col, required_value, full_name, score,
                 added_after, amount, tiny_col, small_col, dt)
            VALUES
                (9, 9000, 900, 'ivan', CAST(3000000000 AS BIGINT),
                 'after-bigint', 9.90, 9, 900, '2026-07-07')
        """
        order_qt_sc_modify_bigint_after_insert """
            SELECT id, full_name, score, amount, required_value, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, full_name, score, amount, required_value, dt",
                "ORDER BY id")

        // MODIFY type: widen DECIMAL precision without changing the scale.
        sql """ALTER TABLE `${appendTable}` MODIFY COLUMN amount DECIMAL(12, 2) NULL"""
        order_qt_sc_modify_decimal_before_insert """
            SELECT id, full_name, score, amount, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, full_name, score, amount, dt",
                "ORDER BY id")

        sql """
            INSERT INTO `${appendTable}`
                (id, first_col, required_value, full_name, score,
                 added_after, amount, tiny_col, small_col, dt)
            VALUES
                (10, 10000, 1000, 'judy', 100,
                 'after-decimal', 1234567890.12, 10, 1000, '2026-07-08')
        """
        order_qt_sc_modify_decimal_after_insert """
            SELECT id, full_name, score, amount, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, full_name, score, amount, dt",
                "ORDER BY id")

        // MODIFY nullability: NOT NULL -> NULL, then actually write NULL.
        sql """
            ALTER TABLE `${appendTable}`
            MODIFY COLUMN required_value BIGINT NULL
        """
        order_qt_sc_modify_nullable_before_insert """
            SELECT id, full_name, required_value, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, full_name, required_value, dt",
                "ORDER BY id")

        sql """
            INSERT INTO `${appendTable}`
                (id, first_col, required_value, full_name, score,
                 added_after, amount, tiny_col, small_col, dt)
            VALUES
                (11, 11000, NULL, 'kate', 110,
                 'after-nullable', 11.11, 11, 1100, '2026-07-09')
        """
        order_qt_sc_modify_nullable_after_insert """
            SELECT id, full_name, required_value, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, full_name, required_value, dt",
                "ORDER BY id")

        // MODIFY DEFAULT, COMMENT and position together. DESC checks metadata;
        // data checks field-id preservation after moving the column.
        sql """
            ALTER TABLE `${appendTable}`
            MODIFY COLUMN added_after STRING NULL DEFAULT 'changed-default'
                COMMENT 'changed comment' FIRST
        """
        qt_sc_modify_metadata_desc """DESC `${appendTable}`"""
        order_qt_sc_modify_metadata_before_insert """
            SELECT id, full_name, added_after, score, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, full_name, added_after, score, dt",
                "ORDER BY id")

        sql """
            INSERT INTO `${appendTable}`
                (added_after, id, first_col, required_value, full_name,
                 score, amount, tiny_col, small_col, dt)
            VALUES
                ('after-metadata', 12, 12000, 1200, 'leo',
                 120, 12.12, 12, 1200, '2026-07-10')
        """
        sql """
            INSERT INTO `${appendTable}`
                (id, first_col, required_value, full_name,
                 score, amount, tiny_col, small_col, dt)
            VALUES
                (120, 120000, 12000, 'modified-default',
                 1200, 120.00, 12, 1200, '2026-07-10')
        """
        order_qt_sc_modify_default_omitted """
            SELECT id, added_after FROM `${appendTable}` WHERE id = 120
        """
        order_qt_sc_modify_metadata_after_insert """
            SELECT id, full_name, added_after, score, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, full_name, added_after, score, dt",
                "ORDER BY id")

        // Omitting DEFAULT and COMMENT removes both, while AFTER moves the same
        // field again. Explicit writes must continue to map to the correct id.
        sql """
            ALTER TABLE `${appendTable}`
            MODIFY COLUMN added_after STRING NULL AFTER score
        """
        qt_sc_modify_remove_metadata_desc """DESC `${appendTable}`"""
        order_qt_sc_modify_remove_metadata_before_insert """
            SELECT id, full_name, score, added_after, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, full_name, score, added_after, dt",
                "ORDER BY id")

        sql """
            INSERT INTO `${appendTable}`
                (id, first_col, required_value, full_name, score,
                 added_after, amount, tiny_col, small_col, dt)
            VALUES
                (13, 13000, 1300, 'mallory', 130,
                 'after-remove-metadata', 13.13, 13, 1300, '2026-07-11')
        """
        sql """
            INSERT INTO `${appendTable}`
                (id, first_col, required_value, full_name, score,
                 amount, tiny_col, small_col, dt)
            VALUES
                (130, 130000, 13000, 'removed-default', 1300,
                 130.00, 13, 1300, '2026-07-11')
        """
        order_qt_sc_remove_default_omitted """
            SELECT id, added_after FROM `${appendTable}` WHERE id = 130
        """
        order_qt_sc_modify_remove_metadata_after_insert """
            SELECT id, full_name, score, added_after, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                "id, full_name, score, added_after, dt",
                "ORDER BY id")

        // Reorder every column, then use INSERT VALUES without a target list.
        // This catches stale FE and JNI writer physical-column ordering.
        sql """
            ALTER TABLE `${appendTable}` ORDER BY (
                id, full_name, score, amount, required_value,
                added_after, first_col, tiny_col, small_col, dt
            )
        """
        order_qt_sc_reorder_before_insert """
            SELECT id, full_name, score, amount, required_value,
                   added_after, first_col, tiny_col, small_col, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                """
                    id, full_name, score, amount, required_value,
                    added_after, first_col, tiny_col, small_col, dt
                """,
                "ORDER BY id")

        sql """
            INSERT INTO `${appendTable}` VALUES
                (14, 'nick', 140, 14.14, 1400,
                 'after-reorder', 14000, 14, 1400, '2026-07-12')
        """
        order_qt_sc_reorder_after_insert """
            SELECT id, full_name, score, amount, required_value,
                   added_after, first_col, tiny_col, small_col, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                """
                    id, full_name, score, amount, required_value,
                    added_after, first_col, tiny_col, small_col, dt
                """,
                "ORDER BY id")

        // Failed schema changes must be atomic and must not poison a subsequent
        // writer created from the still-current schema.
        test {
            sql """
                ALTER TABLE `${appendTable}` ADD COLUMN (
                    batch_ok INT NULL,
                    batch_bad INT NOT NULL DEFAULT '1'
                )
            """
            exception "cannot specify NOT NULL"
        }
        test {
            sql """ALTER TABLE `${appendTable}` ADD COLUMN ID INT NULL"""
            exception "conflicts with an existing Paimon column"
        }
        test {
            sql """
                ALTER TABLE `${appendTable}`
                ADD COLUMN bad_position INT NULL AFTER missing_col
            """
            exception "does not exist in Paimon table"
        }
        test {
            sql """
                ALTER TABLE `${appendTable}`
                ADD COLUMN multi_a INT NULL,
                ADD COLUMN multi_b INT NULL
            """
            exception "External table does not support multiple ALTER clauses"
        }
        test {
            sql """ALTER TABLE `${appendTable}` DROP COLUMN missing_col"""
            exception "does not exist in Paimon table"
        }
        test {
            sql """
                ALTER TABLE `${appendTable}`
                RENAME COLUMN full_name id
            """
            exception "conflicts with an existing Paimon column"
        }
        test {
            sql """ALTER TABLE `${appendTable}` MODIFY COLUMN score INT NULL"""
            exception "cannot be converted"
        }
        test {
            sql """
                ALTER TABLE `${appendTable}`
                MODIFY COLUMN required_value BIGINT NOT NULL
            """
            exception "nullable to non nullable"
        }
        test {
            sql """ALTER TABLE `${appendTable}` DROP COLUMN dt"""
            exception "Cannot drop partition key or primary key"
        }
        test {
            sql """
                ALTER TABLE `${appendTable}`
                RENAME COLUMN dt partition_col
            """
            exception "Cannot rename partition column"
        }
        test {
            sql """
                ALTER TABLE `${appendTable}`
                MODIFY COLUMN dt INT NULL
            """
            exception "Cannot update partition column"
        }
        test {
            sql """
                ALTER TABLE `${appendTable}` ORDER BY (
                    id, full_name, score
                )
            """
            exception "must contain every Paimon column exactly once"
        }
        test {
            sql """
                ALTER TABLE `${appendTable}` ORDER BY (
                    id, full_name, score, amount, required_value,
                    added_after, first_col, tiny_col, small_col, id
                )
            """
            exception "Duplicate column in reorder columns"
        }

        sql """
            INSERT INTO `${appendTable}` VALUES
                (15, 'olivia', 150, 15.15, NULL,
                 'after-failed-alters', 15000, 15, 1500, '2026-07-13')
        """
        order_qt_sc_after_failed_alters """
            SELECT id, full_name, score, amount, required_value,
                   added_after, first_col, tiny_col, small_col, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                """
                    id, full_name, score, amount, required_value,
                    added_after, first_col, tiny_col, small_col, dt
                """,
                "ORDER BY id")

        // Paimon 1.4.2 has no partition-key evolution in SchemaChange.
        // Doris therefore rejects ADD, DROP and REPLACE before catalog mutation.
        test {
            sql """
                ALTER TABLE `${appendTable}`
                ADD PARTITION KEY bucket(4, id) AS id_bucket
            """
            exception "ADD PARTITION KEY is only supported for Iceberg tables"
        }
        test {
            sql """ALTER TABLE `${appendTable}` DROP PARTITION KEY dt"""
            exception "DROP PARTITION KEY is only supported for Iceberg tables"
        }
        test {
            sql """
                ALTER TABLE `${appendTable}`
                REPLACE PARTITION KEY dt WITH bucket(4, id) AS id_bucket
            """
            exception "REPLACE PARTITION KEY is only supported for Iceberg tables"
        }

        // Rejected partition evolution must not mutate the current schema or
        // prevent a new writer from committing another physical partition.
        sql """
            INSERT INTO `${appendTable}` VALUES
                (16, 'peggy', 160, 16.16, 1600,
                 'after-partition-evolution-failures', 16000, 16, 1600, '2026-07-14')
        """
        order_qt_sc_after_partition_evolution_failures """
            SELECT id, full_name, score, amount, required_value,
                   added_after, first_col, tiny_col, small_col, dt
            FROM `${appendTable}`
            ORDER BY id
        """
        assertTableEquals(
                appendTable,
                """
                    id, full_name, score, amount, required_value,
                    added_after, first_col, tiny_col, small_col, dt
                """,
                "ORDER BY id")

        // Regular writes create new partition values before and after schema
        // evolution. This is dynamic partition creation, not partition evolution.
        def sparkPartitions = spark_paimon """
            SELECT `partition`, record_count
            FROM paimon.${dbName}.`${appendTable}\$partitions`
            ORDER BY `partition`
        """
        def dorisPartitions = sql """
            SELECT `partition`, record_count
            FROM `${appendTable}\$partitions`
            ORDER BY `partition`
        """
        assertSparkDorisResultEquals(sparkPartitions, dorisPartitions)
        order_qt_sc_append_partitions """
            SELECT `partition`, record_count
            FROM `${appendTable}\$partitions`
            ORDER BY `partition`
        """

        // ------------------------------------------------------------------
        // Type-evolution matrix: verify widening conversions on existing data,
        // then write values which cannot fit in the original types.
        // ------------------------------------------------------------------
        sql """
            CREATE TABLE `${typeTable}` (
                id INT NULL,
                c_tiny TINYINT NULL,
                c_small SMALLINT NULL,
                c_int INT NULL,
                c_float FLOAT NULL,
                c_decimal DECIMAL(8, 2) NULL
            ) ENGINE=paimon
            PROPERTIES (
                'disable-explicit-type-casting' = 'true'
            )
        """
        sql """
            INSERT INTO `${typeTable}` VALUES
                (1, 100, 30000, 2000000000, 1.5, 123456.78)
        """
        order_qt_sc_types_initial """
            SELECT * FROM `${typeTable}` ORDER BY id
        """
        assertTableEquals(typeTable, "*", "ORDER BY id")

        sql """ALTER TABLE `${typeTable}` MODIFY COLUMN c_tiny SMALLINT NULL"""
        order_qt_sc_types_tiny_to_small_before_insert """
            SELECT * FROM `${typeTable}` ORDER BY id
        """
        assertTableEquals(typeTable, "*", "ORDER BY id")

        sql """
            INSERT INTO `${typeTable}` VALUES
                (2, 200, 30001, 2000000001, 2.5, 123456.79)
        """
        order_qt_sc_types_tiny_to_small_after_insert """
            SELECT * FROM `${typeTable}` ORDER BY id
        """
        assertTableEquals(typeTable, "*", "ORDER BY id")

        sql """ALTER TABLE `${typeTable}` MODIFY COLUMN c_small INT NULL"""
        order_qt_sc_types_small_to_int_before_insert """
            SELECT * FROM `${typeTable}` ORDER BY id
        """
        assertTableEquals(typeTable, "*", "ORDER BY id")

        sql """
            INSERT INTO `${typeTable}` VALUES
                (3, 201, 40000, 2000000002, 3.5, 123456.80)
        """
        order_qt_sc_types_small_to_int_after_insert """
            SELECT * FROM `${typeTable}` ORDER BY id
        """
        assertTableEquals(typeTable, "*", "ORDER BY id")

        sql """ALTER TABLE `${typeTable}` MODIFY COLUMN c_int BIGINT NULL"""
        order_qt_sc_types_int_to_bigint_before_insert """
            SELECT * FROM `${typeTable}` ORDER BY id
        """
        assertTableEquals(typeTable, "*", "ORDER BY id")

        sql """
            INSERT INTO `${typeTable}` VALUES
                (4, 202, 40001, 3000000000, 4.5, 123456.81)
        """
        order_qt_sc_types_int_to_bigint_after_insert """
            SELECT * FROM `${typeTable}` ORDER BY id
        """
        assertTableEquals(typeTable, "*", "ORDER BY id")

        sql """ALTER TABLE `${typeTable}` MODIFY COLUMN c_float DOUBLE NULL"""
        order_qt_sc_types_float_to_double_before_insert """
            SELECT * FROM `${typeTable}` ORDER BY id
        """
        assertTableEquals(typeTable, "*", "ORDER BY id")

        sql """
            INSERT INTO `${typeTable}` VALUES
                (5, 203, 40002, 3000000001, 1.0E40, 123456.82)
        """
        order_qt_sc_types_float_to_double_after_insert """
            SELECT * FROM `${typeTable}` ORDER BY id
        """
        assertTableEquals(typeTable, "*", "ORDER BY id")

        sql """
            ALTER TABLE `${typeTable}`
            MODIFY COLUMN c_decimal DECIMAL(12, 2) NULL
        """
        order_qt_sc_types_decimal_widen_before_insert """
            SELECT * FROM `${typeTable}` ORDER BY id
        """
        assertTableEquals(typeTable, "*", "ORDER BY id")

        sql """
            INSERT INTO `${typeTable}` VALUES
                (6, 204, 40003, 3000000002, 2.0E40, 1234567890.12)
        """
        order_qt_sc_types_decimal_widen_after_insert """
            SELECT * FROM `${typeTable}` ORDER BY id
        """
        assertTableEquals(typeTable, "*", "ORDER BY id")

        // A narrowing conversion fails with data present; the old writer schema
        // remains usable after the failed ALTER.
        test {
            sql """ALTER TABLE `${typeTable}` MODIFY COLUMN c_int INT NULL"""
            exception "cannot be converted"
        }
        sql """
            INSERT INTO `${typeTable}` VALUES
                (7, 205, 40004, 3000000003, 3.0E40, 1234567890.13)
        """
        order_qt_sc_types_after_failed_narrow """
            SELECT * FROM `${typeTable}` ORDER BY id
        """
        assertTableEquals(typeTable, "*", "ORDER BY id")

        // By default Paimon also permits explicit casts. Use values which fit
        // in the target type to cover BIGINT -> INT and then INT -> STRING.
        sql """
            CREATE TABLE `${explicitTypeTable}` (
                id INT NULL,
                explicit_value BIGINT NULL
            ) ENGINE=paimon
        """
        sql """
            INSERT INTO `${explicitTypeTable}` VALUES
                (1, 100),
                (2, 200)
        """
        order_qt_sc_explicit_types_initial """
            SELECT * FROM `${explicitTypeTable}` ORDER BY id
        """
        assertTableEquals(explicitTypeTable, "*", "ORDER BY id")

        sql """
            ALTER TABLE `${explicitTypeTable}`
            MODIFY COLUMN explicit_value INT NULL
        """
        order_qt_sc_explicit_bigint_to_int_before_insert """
            SELECT * FROM `${explicitTypeTable}` ORDER BY id
        """
        assertTableEquals(explicitTypeTable, "*", "ORDER BY id")

        sql """
            INSERT INTO `${explicitTypeTable}` VALUES
                (3, 300)
        """
        order_qt_sc_explicit_bigint_to_int_after_insert """
            SELECT * FROM `${explicitTypeTable}` ORDER BY id
        """
        assertTableEquals(explicitTypeTable, "*", "ORDER BY id")

        sql """
            ALTER TABLE `${explicitTypeTable}`
            MODIFY COLUMN explicit_value STRING NULL
        """
        order_qt_sc_explicit_int_to_string_before_insert """
            SELECT * FROM `${explicitTypeTable}` ORDER BY id
        """
        assertTableEquals(explicitTypeTable, "*", "ORDER BY id")

        sql """
            INSERT INTO `${explicitTypeTable}` VALUES
                (4, 'after-explicit-cast')
        """
        order_qt_sc_explicit_int_to_string_after_insert """
            SELECT * FROM `${explicitTypeTable}` ORDER BY id
        """
        assertTableEquals(explicitTypeTable, "*", "ORDER BY id")

        // ------------------------------------------------------------------
        // Primary-key table: repeat the core evolutions around merge-tree data
        // and verify key/partition-column restrictions do not affect later writes.
        // ------------------------------------------------------------------
        sql """
            CREATE TABLE `${primaryKeyTable}` (
                id INT NOT NULL,
                dt STRING NOT NULL,
                metric INT NULL,
                legacy STRING NULL
            ) ENGINE=paimon
            PARTITION BY (dt) ()
            PROPERTIES (
                'primary-key' = 'id,dt',
                'bucket' = '2',
                'merge-engine' = 'partial-update'
            )
        """
        sql """
            INSERT INTO `${primaryKeyTable}` VALUES
                (1, '2026-08-01', 10, 'pk-a'),
                (2, '2026-08-01', 20, 'pk-b')
        """
        order_qt_sc_pk_initial """
            SELECT * FROM `${primaryKeyTable}` ORDER BY dt, id
        """
        assertTableEquals(primaryKeyTable, "*", "ORDER BY dt, id")

        sql """
            ALTER TABLE `${primaryKeyTable}`
            ADD COLUMN note STRING NULL DEFAULT 'default-note' AFTER metric
        """
        order_qt_sc_pk_add_before_insert """
            SELECT id, dt, metric, note, legacy
            FROM `${primaryKeyTable}`
            ORDER BY dt, id
        """
        assertTableEquals(
                primaryKeyTable,
                "id, dt, metric, note, legacy",
                "ORDER BY dt, id")

        sql """
            INSERT INTO `${primaryKeyTable}` VALUES
                (1, '2026-08-01', 11, 'updated-after-add', 'pk-a2'),
                (3, '2026-08-02', 30, 'new-after-add', 'pk-c')
        """
        order_qt_sc_pk_add_after_insert """
            SELECT id, dt, metric, note, legacy
            FROM `${primaryKeyTable}`
            ORDER BY dt, id
        """
        assertTableEquals(
                primaryKeyTable,
                "id, dt, metric, note, legacy",
                "ORDER BY dt, id")

        sql """
            ALTER TABLE `${primaryKeyTable}`
            RENAME COLUMN metric metric_value
        """
        order_qt_sc_pk_rename_before_insert """
            SELECT id, dt, metric_value, note, legacy
            FROM `${primaryKeyTable}`
            ORDER BY dt, id
        """
        assertTableEquals(
                primaryKeyTable,
                "id, dt, metric_value, note, legacy",
                "ORDER BY dt, id")

        sql """
            INSERT INTO `${primaryKeyTable}`
                (id, dt, metric_value, note, legacy)
            VALUES
                (2, '2026-08-01', 22, 'updated-after-rename', 'pk-b2'),
                (4, '2026-08-02', 40, 'new-after-rename', 'pk-d')
        """
        order_qt_sc_pk_rename_after_insert """
            SELECT id, dt, metric_value, note, legacy
            FROM `${primaryKeyTable}`
            ORDER BY dt, id
        """
        assertTableEquals(
                primaryKeyTable,
                "id, dt, metric_value, note, legacy",
                "ORDER BY dt, id")

        sql """
            ALTER TABLE `${primaryKeyTable}`
            MODIFY COLUMN metric_value BIGINT NULL
        """
        order_qt_sc_pk_type_before_insert """
            SELECT id, dt, metric_value, note, legacy
            FROM `${primaryKeyTable}`
            ORDER BY dt, id
        """
        assertTableEquals(
                primaryKeyTable,
                "id, dt, metric_value, note, legacy",
                "ORDER BY dt, id")

        sql """
            INSERT INTO `${primaryKeyTable}`
                (id, dt, metric_value, note, legacy)
            VALUES
                (5, '2026-08-03', 3000000000, 'new-after-type', 'pk-e')
        """
        order_qt_sc_pk_type_after_insert """
            SELECT id, dt, metric_value, note, legacy
            FROM `${primaryKeyTable}`
            ORDER BY dt, id
        """
        assertTableEquals(
                primaryKeyTable,
                "id, dt, metric_value, note, legacy",
                "ORDER BY dt, id")

        sql """ALTER TABLE `${primaryKeyTable}` DROP COLUMN legacy"""
        order_qt_sc_pk_drop_before_insert """
            SELECT id, dt, metric_value, note
            FROM `${primaryKeyTable}`
            ORDER BY dt, id
        """
        assertTableEquals(
                primaryKeyTable,
                "id, dt, metric_value, note",
                "ORDER BY dt, id")

        // A PK partial-update writer also distinguishes omitted fields from
        // explicit NULL using the evolved remote schema. A later partial row
        // which omits note applies its schema default again.
        sql """
            INSERT INTO `${primaryKeyTable}`
                (id, dt, metric_value)
            VALUES
                (8, '2026-08-05', 80)
        """
        sql """
            INSERT INTO `${primaryKeyTable}`
                (id, dt, note)
            VALUES
                (8, '2026-08-05', 'explicit-note'),
                (9, '2026-08-05', NULL)
        """
        sql """
            INSERT INTO `${primaryKeyTable}`
                (id, dt, metric_value)
            VALUES
                (8, '2026-08-05', 81)
        """
        order_qt_sc_pk_partial_default """
            SELECT id, dt, metric_value, note
            FROM `${primaryKeyTable}`
            WHERE id IN (8, 9)
            ORDER BY dt, id
        """
        assertTableEquals(
                primaryKeyTable,
                "id, dt, metric_value, note",
                "ORDER BY dt, id")

        sql """
            INSERT INTO `${primaryKeyTable}`
                (note, metric_value, dt, id)
            VALUES
                ('new-after-drop', 60, '2026-08-03', 6)
        """
        order_qt_sc_pk_drop_after_insert """
            SELECT id, dt, metric_value, note
            FROM `${primaryKeyTable}`
            ORDER BY dt, id
        """
        assertTableEquals(
                primaryKeyTable,
                "id, dt, metric_value, note",
                "ORDER BY dt, id")

        // Primary and partition keys cannot be dropped, renamed or have their
        // types changed. All failures must leave the merge-tree writer usable.
        test {
            sql """ALTER TABLE `${primaryKeyTable}` DROP COLUMN id"""
            exception "Cannot drop partition key or primary key"
        }
        test {
            sql """
                ALTER TABLE `${primaryKeyTable}`
                RENAME COLUMN dt partition_col
            """
            exception "Cannot rename partition column"
        }
        test {
            sql """
                ALTER TABLE `${primaryKeyTable}`
                MODIFY COLUMN id BIGINT NOT NULL
            """
            exception "Cannot update primary key"
        }

        sql """
            INSERT INTO `${primaryKeyTable}`
                (id, dt, metric_value, note)
            VALUES
                (7, '2026-08-04', 70, 'after-key-failures')
        """
        order_qt_sc_pk_after_key_failures """
            SELECT id, dt, metric_value, note
            FROM `${primaryKeyTable}`
            ORDER BY dt, id
        """
        assertTableEquals(
                primaryKeyTable,
                "id, dt, metric_value, note",
                "ORDER BY dt, id")
    } finally {
        sql """DROP TABLE IF EXISTS `${primaryKeyTable}`"""
        sql """DROP TABLE IF EXISTS `${explicitTypeTable}`"""
        sql """DROP TABLE IF EXISTS `${typeTable}`"""
        sql """DROP TABLE IF EXISTS `${appendTable}`"""
        sql """DROP DATABASE IF EXISTS `${dbName}` FORCE"""
        sql """SWITCH internal"""
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
