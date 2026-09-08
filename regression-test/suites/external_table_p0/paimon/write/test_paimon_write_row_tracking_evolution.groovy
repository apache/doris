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

suite("test_paimon_write_row_tracking_evolution", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_row_tracking_catalog"
    String dbName = "test_pw_row_tracking_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_tracking;
        CREATE TABLE paimon.${dbName}.t_tracking (
            id INT, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'bucket' = '-1',
            'row-tracking.enabled' = 'true',
            'compaction.min.file-num' = '2'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_evolution;
        CREATE TABLE paimon.${dbName}.t_evolution (
            id INT, b INT, c INT
        ) USING paimon
        TBLPROPERTIES (
            'bucket' = '-1',
            'row-tracking.enabled' = 'true',
            'data-evolution.enabled' = 'true'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_evolution_source;
        CREATE TABLE paimon.${dbName}.t_evolution_source (
            id INT, b INT, c INT
        ) USING paimon;
    """

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        )
    """
    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    try {
        def trackingRows = { String tableName ->
            return sql("""
                SELECT id, _ROW_ID, _SEQUENCE_NUMBER
                FROM `${tableName}\$row_tracking`
                ORDER BY id
            """)
        }
        def latestSnapshotId = { String tableName ->
            def rows = spark_paimon """
                SELECT max(snapshot_id)
                FROM paimon.${dbName}.`${tableName}\$snapshots`
            """
            return rows[0][0] == null ? 0L : rows[0][0].toString().toLong()
        }

        // Doris assigns row ids through the Paimon committer. Their exact
        // values are not assumed, but they must be unique and stable.
        sql """INSERT INTO t_tracking VALUES
            (1, 'one'), (2, 'two'), (3, 'three')
        """
        def initialTracking = trackingRows("t_tracking")
        assertEquals(3, initialTracking.size())
        assertEquals(3, initialTracking.collect { it[1] }.toSet().size())
        Map<Integer, Long> initialRowIds = initialTracking.collectEntries { row ->
            [(row[0].toString().toInteger()): row[1].toString().toLong()]
        }
        Map<Integer, Long> initialSequences = initialTracking.collectEntries { row ->
            [(row[0].toString().toInteger()): row[2].toString().toLong()]
        }

        // Spark performs row-level changes which Doris does not expose for an
        // append table. This verifies that rows originally written by Doris have
        // valid tracking metadata for every upstream operation.
        spark_paimon_multi """
            UPDATE paimon.${dbName}.t_tracking
                SET payload = 'two-updated' WHERE id = 2;
            DELETE FROM paimon.${dbName}.t_tracking WHERE id = 3;
            MERGE INTO paimon.${dbName}.t_tracking t
            USING (SELECT 1 AS id, 'one-merged' AS payload
                   UNION ALL
                   SELECT 4 AS id, 'four' AS payload) s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET payload = s.payload
            WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (s.id, s.payload);
        """
        sql """refresh table t_tracking"""
        order_qt_row_tracking_after_spark_changes """
            SELECT id, payload FROM t_tracking ORDER BY id
        """
        def changedTracking = trackingRows("t_tracking")
        Map<Integer, Long> changedRowIds = changedTracking.collectEntries { row ->
            [(row[0].toString().toInteger()): row[1].toString().toLong()]
        }
        Map<Integer, Long> changedSequences = changedTracking.collectEntries { row ->
            [(row[0].toString().toInteger()): row[2].toString().toLong()]
        }
        assertEquals(initialRowIds[1], changedRowIds[1])
        assertEquals(initialRowIds[2], changedRowIds[2])
        assertFalse(initialRowIds.values().contains(changedRowIds[4]))
        assertTrue(changedSequences[1] > initialSequences[1])
        assertTrue(changedSequences[2] > initialSequences[2])

        spark_paimon """
            CALL paimon.sys.compact(
                table => '${dbName}.t_tracking',
                compact_strategy => 'full')
        """
        sql """refresh table t_tracking"""
        def compactedTracking = trackingRows("t_tracking")
        Map<Integer, Long> compactedRowIds = compactedTracking.collectEntries { row ->
            [(row[0].toString().toInteger()): row[1].toString().toLong()]
        }
        assertEquals(changedRowIds, compactedRowIds)

        sql """INSERT INTO t_tracking VALUES (5, 'five-after-compact')"""
        order_qt_row_tracking_after_compact_write """
            SELECT id, payload FROM t_tracking ORDER BY id
        """
        def afterDorisReopen = trackingRows("t_tracking")
        assertEquals(4, afterDorisReopen.collect { it[1] }.toSet().size())
        assertFalse(compactedRowIds.values().contains(
                afterDorisReopen.find { it[0].toString().toInteger() == 5 }[1]
                        .toString().toLong()))

        // Data evolution accepts Doris full and partial INSERTs. Spark MERGE
        // then updates only selected columns and keeps the original row ids.
        sql """INSERT INTO t_evolution VALUES (1, 10, 100), (2, 20, 200)"""
        sql """INSERT INTO t_evolution (id, b) VALUES (3, 30)"""
        def evolutionBefore = trackingRows("t_evolution")
        Map<Integer, Long> evolutionRowIds = evolutionBefore.collectEntries { row ->
            [(row[0].toString().toInteger()): row[1].toString().toLong()]
        }
        spark_paimon_multi """
            INSERT INTO paimon.${dbName}.t_evolution_source VALUES
                (1, 11, 111), (2, 22, 222), (4, 44, 444);
            MERGE INTO paimon.${dbName}.t_evolution t
            USING paimon.${dbName}.t_evolution_source s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET b = s.b
            WHEN NOT MATCHED THEN INSERT (id, b, c)
                VALUES (s.id, s.b, s.c);
        """
        sql """refresh table t_evolution"""
        order_qt_data_evolution_after_spark_merge """
            SELECT * FROM t_evolution ORDER BY id
        """
        def evolutionAfter = trackingRows("t_evolution")
        Map<Integer, Long> evolutionAfterIds = evolutionAfter.collectEntries { row ->
            [(row[0].toString().toInteger()): row[1].toString().toLong()]
        }
        assertEquals(evolutionRowIds[1], evolutionAfterIds[1])
        assertEquals(evolutionRowIds[2], evolutionAfterIds[2])
        assertEquals(evolutionRowIds[3], evolutionAfterIds[3])
        assertFalse(evolutionRowIds.values().contains(evolutionAfterIds[4]))

        // Paimon 1.4.2 does not support ordinary UPDATE/DELETE on a data
        // evolution table. Doris currently rejects them at its append-table
        // boundary; either way no Paimon snapshot may be committed.
        long evolutionSnapshot = latestSnapshotId("t_evolution")
        test {
            sql """UPDATE t_evolution SET b = 999 WHERE id = 1"""
            exception "primary-key table"
        }
        assertEquals(evolutionSnapshot, latestSnapshotId("t_evolution"))
        test {
            sql """DELETE FROM t_evolution WHERE id = 1"""
            exception "primary-key table"
        }
        assertEquals(evolutionSnapshot, latestSnapshotId("t_evolution"))
        test {
            sql """
                MERGE INTO t_evolution t
                USING (SELECT 1 AS id, 999 AS b) s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET b = s.b
            """
            exception "primary-key table"
        }
        assertEquals(evolutionSnapshot, latestSnapshotId("t_evolution"))
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
