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

suite("test_paimon_create_ddl_write_properties", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_create_props_catalog"
    String dbName = "test_pw_create_props_db"

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

    try {
        def assertTableEquals = { String tableName, String orderBy ->
            spark_paimon """
                REFRESH TABLE paimon.${dbName}.${tableName}
            """
            def sparkRows = spark_paimon """
                SELECT * FROM paimon.${dbName}.${tableName} ${orderBy}
            """
            def dorisRows = sql """
                SELECT * FROM `${tableName}` ${orderBy}
            """
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        def latestSnapshotId = { String tableName ->
            def rows = spark_paimon """
                SELECT MAX(snapshot_id)
                FROM paimon.${dbName}.`${tableName}\$snapshots`
            """
            assertEquals(1, rows.size())
            assertTrue(rows[0][0] != null)
            return rows[0][0].toString()
        }

        // Doris maps location to Paimon's path option. The filesystem catalog
        // deliberately rejects custom table paths, and that SDK validation
        // must be preserved instead of silently ignoring the property.
        test {
            sql """
                CREATE TABLE `t_create_custom_location` (
                    id INT NULL
                ) ENGINE=paimon
                PROPERTIES (
                    'location' =
                        's3://warehouse/wh/${dbName}.db/t_create_custom_location_data'
                )
            """
            exception "does not support specifying the table path"
        }
        qt_create_custom_location_absent """
            SHOW TABLES LIKE 't_create_custom_location'
        """

        // Doris CREATE must preserve the primary/partition keys, table comment
        // and storage/write options. Sequence ordering is verified with a
        // lower-sequence update followed by a higher one.
        sql """
            CREATE TABLE `t_create_sequence` (
                id INT NOT NULL,
                seq BIGINT NOT NULL,
                payload STRING NULL,
                dt STRING NOT NULL
            ) ENGINE=paimon
            PARTITION BY (dt) ()
            PROPERTIES (
                'primary-key' = 'id,dt',
                'bucket' = '2',
                'bucket-key' = 'id',
                'sequence.field' = 'seq',
                'file.format' = 'orc',
                'snapshot.num-retained.min' = '2',
                'snapshot.num-retained.max' = '5',
                'comment' = 'created by Doris with write properties'
            )
        """
        sql """
            INSERT INTO `t_create_sequence` VALUES
                (1, 100, 'newer', 'p1'),
                (2, 10, 'initial-2', 'p1'),
                (3, 5, 'initial-3', 'p2')
        """
        sql """
            INSERT INTO `t_create_sequence` VALUES
                (1, 50, 'older-must-not-win', 'p1'),
                (2, 20, 'updated-2', 'p1')
        """
        order_qt_create_sequence_result """
            SELECT id, seq, payload, dt
            FROM `t_create_sequence`
            ORDER BY dt, id
        """
        qt_create_sequence_schema """
            SELECT partition_keys, primary_keys, comment
            FROM `t_create_sequence\$schemas`
            ORDER BY schema_id DESC
            LIMIT 1
        """
        order_qt_create_sequence_file_format """
            SELECT DISTINCT file_format
            FROM `t_create_sequence\$files`
            ORDER BY file_format
        """
        assertTableEquals("t_create_sequence", "ORDER BY dt, id")

        // Fixed bucket properties must be consumed by the writer, while
        // partial-update accepts arbitrary value-column subsets.
        sql """
            CREATE TABLE `t_create_partial` (
                id INT NOT NULL,
                name STRING NULL,
                score INT NULL,
                note STRING NULL
            ) ENGINE=paimon
            PROPERTIES (
                'primary-key' = 'id',
                'bucket' = '2',
                'bucket-key' = 'id',
                'merge-engine' = 'partial-update'
            )
        """
        sql """
            INSERT INTO `t_create_partial` VALUES
                (1, 'alice', 10, 'initial'),
                (2, 'bob', 20, 'initial')
        """
        sql """INSERT INTO `t_create_partial` (id, score) VALUES (1, 15)"""
        sql """INSERT INTO `t_create_partial` (note, id) VALUES ('updated', 1)"""
        order_qt_create_partial_result """
            SELECT id, name, score, note
            FROM `t_create_partial`
            ORDER BY id
        """
        assertTableEquals("t_create_partial", "ORDER BY id")

        // First-row and aggregation semantics prove that CREATE forwarded the
        // merge-engine and per-field aggregation properties.
        sql """
            CREATE TABLE `t_create_first_row` (
                id INT NOT NULL,
                name STRING NULL,
                score INT NULL
            ) ENGINE=paimon
            PROPERTIES (
                'primary-key' = 'id',
                'bucket' = '1',
                'merge-engine' = 'first-row'
            )
        """
        sql """
            INSERT INTO `t_create_first_row` VALUES
                (1, 'first-1', 10),
                (2, 'first-2', 20)
        """
        sql """
            INSERT INTO `t_create_first_row` VALUES
                (1, 'second-1', 11),
                (3, 'first-3', 30)
        """
        order_qt_create_first_row_result """
            SELECT id, name, score
            FROM `t_create_first_row`
            ORDER BY id
        """
        assertTableEquals("t_create_first_row", "ORDER BY id")

        sql """
            CREATE TABLE `t_create_aggregation` (
                id INT NOT NULL,
                total BIGINT NULL,
                highest INT NULL,
                label STRING NULL
            ) ENGINE=paimon
            PROPERTIES (
                'primary-key' = 'id',
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'fields.total.aggregate-function' = 'sum',
                'fields.highest.aggregate-function' = 'max'
            )
        """
        sql """
            INSERT INTO `t_create_aggregation` VALUES
                (1, 10, 80, 'first'),
                (2, 5, 70, 'stable')
        """
        sql """
            INSERT INTO `t_create_aggregation` VALUES
                (1, 7, 90, 'latest'),
                (2, 3, 60, NULL),
                (3, 4, 50, 'new')
        """
        order_qt_create_aggregation_result """
            SELECT id, total, highest, label
            FROM `t_create_aggregation`
            ORDER BY id
        """
        assertTableEquals("t_create_aggregation", "ORDER BY id")

        // Lookup changelog generation is checked independently from the final
        // table contents, so merely storing the option is not sufficient.
        sql """
            CREATE TABLE `t_create_lookup` (
                id INT NOT NULL,
                name STRING NULL,
                score INT NULL
            ) ENGINE=paimon
            PROPERTIES (
                'primary-key' = 'id',
                'bucket' = '1',
                'changelog-producer' = 'lookup'
            )
        """
        sql """
            INSERT INTO `t_create_lookup` VALUES
                (1, 'old', 10),
                (2, 'stable', 20)
        """
        String lookupBefore = latestSnapshotId("t_create_lookup")
        sql """
            INSERT INTO `t_create_lookup` VALUES
                (1, 'new', 11),
                (3, 'added', 30)
        """
        String lookupAfter = latestSnapshotId("t_create_lookup")
        def lookupChanges = spark_paimon """
            SELECT rowkind, id, name, score
            FROM paimon_incremental_query(
                'paimon.${dbName}.`t_create_lookup\$audit_log`',
                '${lookupBefore}',
                '${lookupAfter}'
            )
            ORDER BY id,
                CASE rowkind WHEN '-U' THEN 0 WHEN '+U' THEN 1 ELSE 2 END
        """
        assertEquals([
                ["-U", 1, "old", 10],
                ["+U", 1, "new", 11],
                ["+I", 3, "added", 30]
        ], lookupChanges)
        order_qt_create_lookup_changelog """
            SELECT rowkind, id, name, score
            FROM `t_create_lookup\$audit_log`
            ORDER BY id,
                CASE rowkind
                    WHEN '+I' THEN 0
                    WHEN '-U' THEN 1
                    WHEN '+U' THEN 2
                    ELSE 3
                END
        """
        order_qt_create_lookup_result """
            SELECT id, name, score
            FROM `t_create_lookup`
            ORDER BY id
        """
        assertTableEquals("t_create_lookup", "ORDER BY id")

        // Dynamic bucket options must affect physical routing after a Doris
        // INSERT, not only appear in metadata.
        sql """
            CREATE TABLE `t_create_dynamic_bucket` (
                pt STRING NOT NULL,
                id INT NOT NULL,
                value STRING NULL
            ) ENGINE=paimon
            PARTITION BY (pt) ()
            PROPERTIES (
                'primary-key' = 'pt,id',
                'bucket' = '-1',
                'dynamic-bucket.target-row-num' = '2',
                'dynamic-bucket.initial-buckets' = '1',
                'dynamic-bucket.max-buckets' = '4'
            )
        """
        sql """
            INSERT INTO `t_create_dynamic_bucket`
            SELECT 'p1', CAST(number AS INT), concat('v', CAST(number AS STRING))
            FROM numbers("number" = "12")
        """
        qt_create_dynamic_bucket_result """
            SELECT COUNT(*), MIN(id), MAX(id)
            FROM `t_create_dynamic_bucket`
        """
        order_qt_create_dynamic_bucket_rows """
            SELECT pt, id, value
            FROM `t_create_dynamic_bucket`
            ORDER BY pt, id
        """
        order_qt_create_dynamic_bucket_files """
            SELECT DISTINCT bucket
            FROM `t_create_dynamic_bucket\$files`
            ORDER BY bucket
        """
        def dynamicBuckets = spark_paimon """
            SELECT DISTINCT bucket
            FROM paimon.${dbName}.`t_create_dynamic_bucket\$files`
            ORDER BY bucket
        """
        assertFalse(dynamicBuckets.isEmpty())
        assertTrue(dynamicBuckets.every { row ->
            int bucket = row[0].toString().toInteger()
            return bucket >= 0 && bucket < 4
        })
        assertTrue(dynamicBuckets.size() > 1)
        assertTableEquals("t_create_dynamic_bucket", "ORDER BY pt, id")

        // Keep one deterministic view of all important CREATE options. This
        // catches property loss or accidental key rewriting in the Doris DDL.
        order_qt_create_write_options """
            SELECT 'aggregation' AS table_name, `key`, value
            FROM `t_create_aggregation\$options`
            WHERE `key` IN (
                'bucket', 'fields.highest.aggregate-function',
                'fields.total.aggregate-function', 'merge-engine'
            )
            UNION ALL
            SELECT 'dynamic_bucket', `key`, value
            FROM `t_create_dynamic_bucket\$options`
            WHERE `key` IN (
                'bucket', 'dynamic-bucket.initial-buckets',
                'dynamic-bucket.max-buckets', 'dynamic-bucket.target-row-num'
            )
            UNION ALL
            SELECT 'first_row', `key`, value
            FROM `t_create_first_row\$options`
            WHERE `key` IN ('bucket', 'merge-engine')
            UNION ALL
            SELECT 'lookup', `key`, value
            FROM `t_create_lookup\$options`
            WHERE `key` IN ('bucket', 'changelog-producer')
            UNION ALL
            SELECT 'partial_update', `key`, value
            FROM `t_create_partial\$options`
            WHERE `key` IN ('bucket', 'bucket-key', 'merge-engine')
            UNION ALL
            SELECT 'sequence', `key`, value
            FROM `t_create_sequence\$options`
            WHERE `key` IN (
                'bucket', 'bucket-key', 'file.format',
                'sequence.field', 'snapshot.num-retained.max',
                'snapshot.num-retained.min'
            )
            ORDER BY table_name, `key`
        """
    } finally {
        [
                "t_create_dynamic_bucket",
                "t_create_lookup",
                "t_create_aggregation",
                "t_create_first_row",
                "t_create_partial",
                "t_create_sequence"
        ].each { tableName ->
            try {
                sql """DROP TABLE IF EXISTS `${tableName}`"""
            } catch (Exception e) {
                logger.info("Failed to drop ${tableName}: ${e.getMessage()}")
            }
        }
        sql """DROP DATABASE IF EXISTS `${dbName}` FORCE"""
        sql """SWITCH internal"""
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
