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

suite("test_paimon_write_changelog_producer", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    String catalogName = "test_pw_changelog_catalog"
    String dbName = "test_pw_changelog_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_input_partial;
        CREATE TABLE paimon.${dbName}.t_input_partial (
            id INT, name STRING, score INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'merge-engine' = 'partial-update',
            'changelog-producer' = 'input'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_lookup;
        CREATE TABLE paimon.${dbName}.t_lookup (
            id INT, name STRING, score INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'changelog-producer' = 'lookup'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_lookup_aggregation;
        CREATE TABLE paimon.${dbName}.t_lookup_aggregation (
            id INT, total BIGINT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'merge-engine' = 'aggregation',
            'fields.total.aggregate-function' = 'sum',
            'changelog-producer' = 'lookup'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_full_compaction;
        CREATE TABLE paimon.${dbName}.t_full_compaction (
            pt STRING, id INT, name STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'pt,id',
            'bucket' = '1',
            'changelog-producer' = 'full-compaction',
            'changelog-producer.row-deduplicate' = 'true'
        );
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
        );
    """
    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    try {
        def latestSnapshotId = { String tableName ->
            def rows = spark_paimon """
                SELECT max(snapshot_id)
                FROM paimon.${dbName}.`${tableName}\$snapshots`
            """
            assertEquals(1, rows.size())
            assertTrue(rows[0][0] != null)
            return rows[0][0].toString()
        }

        def incrementalAuditLog = { tableName, columns, beforeSnapshot, afterSnapshot, orderBy ->
            def rows = spark_paimon """
                SELECT ${columns}
                FROM paimon_incremental_query(
                    'paimon.${dbName}.`${tableName}\$audit_log`',
                    '${beforeSnapshot}',
                    '${afterSnapshot}'
                )
                ${orderBy}
            """
            return rows
        }

        def assertTableEquals = { String tableName, String orderBy ->
            def sparkRows = spark_paimon """
                SELECT * FROM paimon.${dbName}.${tableName} ${orderBy}
            """
            def dorisRows = sql """SELECT * FROM ${tableName} ${orderBy}"""
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        // Input producer preserves the incoming row kind and partial-update payload.
        sql """INSERT INTO t_input_partial VALUES
            (1, 'alice', 10),
            (2, 'bob', 20)
        """
        String inputBefore = latestSnapshotId("t_input_partial")
        sql """INSERT INTO t_input_partial (id, score) VALUES
            (1, 15),
            (3, 30)
        """
        String inputAfter = latestSnapshotId("t_input_partial")
        def inputChanges = incrementalAuditLog(
                "t_input_partial", "rowkind, id, name, score", inputBefore, inputAfter,
                "ORDER BY id")
        assertEquals([
                ["+I", 1, null, 15],
                ["+I", 3, null, 30]
        ], inputChanges)
        assertTableEquals("t_input_partial", "ORDER BY id")

        // Lookup producer resolves previous values and emits complete before/after rows.
        sql """INSERT INTO t_lookup VALUES
            (1, 'old', 10),
            (2, 'stable', 20)
        """
        String lookupBefore = latestSnapshotId("t_lookup")
        sql """INSERT INTO t_lookup VALUES
            (1, 'new', 11),
            (3, 'added', 30)
        """
        String lookupAfter = latestSnapshotId("t_lookup")
        def lookupChanges = incrementalAuditLog(
                "t_lookup", "rowkind, id, name, score", lookupBefore, lookupAfter,
                """ORDER BY id,
                    CASE rowkind WHEN '-U' THEN 0 WHEN '+U' THEN 1 ELSE 2 END""")
        assertEquals([
                ["-U", 1, "old", 10],
                ["+U", 1, "new", 11],
                ["+I", 3, "added", 30]
        ], lookupChanges)
        assertTableEquals("t_lookup", "ORDER BY id")

        // Lookup producer reports the values before and after aggregation.
        sql """INSERT INTO t_lookup_aggregation VALUES
            (1, 10),
            (2, 20)
        """
        String aggregationBefore = latestSnapshotId("t_lookup_aggregation")
        sql """INSERT INTO t_lookup_aggregation VALUES
            (1, 7),
            (3, 30)
        """
        String aggregationAfter = latestSnapshotId("t_lookup_aggregation")
        def aggregationChanges = incrementalAuditLog(
                "t_lookup_aggregation", "rowkind, id, total",
                aggregationBefore, aggregationAfter,
                """ORDER BY id,
                    CASE rowkind WHEN '-U' THEN 0 WHEN '+U' THEN 1 ELSE 2 END""")
        assertEquals([
                ["-U", 1, 10L],
                ["+U", 1, 17L],
                ["+I", 3, 30L]
        ], aggregationChanges)
        assertTableEquals("t_lookup_aggregation", "ORDER BY id")

        // Full-compaction producer must compact every partition/bucket touched by the batch.
        sql """INSERT INTO t_full_compaction VALUES
            ('p1', 1, 'old'),
            ('p2', 2, 'stable')
        """
        String fullCompactionBefore = latestSnapshotId("t_full_compaction")
        sql """INSERT INTO t_full_compaction VALUES
            ('p1', 1, 'new'),
            ('p2', 3, 'added')
        """
        String fullCompactionAfter = latestSnapshotId("t_full_compaction")
        def fullCompactionChanges = incrementalAuditLog(
                "t_full_compaction", "rowkind, pt, id, name",
                fullCompactionBefore, fullCompactionAfter,
                """ORDER BY pt, id,
                    CASE rowkind WHEN '-U' THEN 0 WHEN '+U' THEN 1 ELSE 2 END""")
        assertEquals([
                ["-U", "p1", 1, "old"],
                ["+U", "p1", 1, "new"],
                ["+I", "p2", 3, "added"]
        ], fullCompactionChanges)
        assertTableEquals("t_full_compaction", "ORDER BY pt, id")

        def fullCompactionSnapshot = spark_paimon """
            SELECT commit_kind, changelog_record_count
            FROM paimon.${dbName}.`t_full_compaction\$snapshots`
            WHERE snapshot_id = ${fullCompactionAfter}
        """
        assertEquals([["COMPACT", 3L]], fullCompactionSnapshot)
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
