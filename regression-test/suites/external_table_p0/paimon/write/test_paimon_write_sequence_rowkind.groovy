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

suite("test_paimon_write_sequence_rowkind", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_sequence_rowkind_catalog"
    String dbName = "test_pw_sequence_rowkind_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_sequence_asc;
        CREATE TABLE paimon.${dbName}.t_sequence_asc (
            id INT, seq1 INT, seq2 INT, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '2',
            'sequence.field' = 'seq1,seq2',
            'sequence.field.sort-order' = 'ascending'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_sequence_desc;
        CREATE TABLE paimon.${dbName}.t_sequence_desc (
            id INT, seq INT, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'sequence.field' = 'seq',
            'sequence.field.sort-order' = 'descending'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_rowkind;
        CREATE TABLE paimon.${dbName}.t_rowkind (
            id INT, row_kind STRING, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'rowkind.field' = 'row_kind',
            'changelog-producer' = 'input'
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
        )
    """
    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    try {
        def latestSnapshotId = { String tableName ->
            def rows = spark_paimon """
                SELECT max(snapshot_id)
                FROM paimon.${dbName}.`${tableName}\$snapshots`
            """
            return rows[0][0] == null ? 0L : rows[0][0].toString().toLong()
        }
        def activeFileCount = { String tableName ->
            def rows = spark_paimon """
                SELECT count(*) FROM paimon.${dbName}.`${tableName}\$files`
            """
            return rows[0][0].toString().toLong()
        }
        def assertSparkEquals = { String tableName, String columns, String orderBy ->
            def sparkRows = spark_paimon """
                SELECT ${columns} FROM paimon.${dbName}.${tableName} ${orderBy}
            """
            def dorisRows = sql """SELECT ${columns} FROM ${tableName} ${orderBy}"""
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        // Multi-column ascending sequences compare lexicographically across
        // Doris commits. A NULL tuple loses once a concrete sequence arrives.
        sql """INSERT INTO t_sequence_asc VALUES
            (1, 10, 20, 'base'),
            (2, NULL, NULL, 'null-base')
        """
        sql """INSERT INTO t_sequence_asc VALUES
            (1, 9, 99, 'stale-first-field'),
            (2, 1, 0, 'nonnull-wins')
        """
        sql """INSERT INTO t_sequence_asc VALUES
            (1, 10, 21, 'new-second-field'),
            (2, NULL, NULL, 'null-must-not-return')
        """
        order_qt_sequence_ascending """
            SELECT * FROM t_sequence_asc ORDER BY id
        """

        // Equal sequences fall back to input order. Keep the writer single-task
        // so this oracle checks Paimon's tie rule instead of scheduler ordering.
        sql """set parallel_pipeline_task_num = 1"""
        sql """INSERT INTO t_sequence_asc VALUES
            (3, 7, 7, 'first-equal'),
            (3, 7, 7, 'second-equal')
        """
        order_qt_sequence_equal """
            SELECT id, payload FROM t_sequence_asc WHERE id = 3 ORDER BY id
        """
        assertSparkEquals("t_sequence_asc", "*", "ORDER BY id")

        // Descending order reverses priority: a smaller sequence supersedes the
        // current row while a larger sequence is ignored.
        sql """INSERT INTO t_sequence_desc VALUES (1, 10, 'base')"""
        sql """INSERT INTO t_sequence_desc VALUES (1, 20, 'larger-is-stale')"""
        sql """INSERT INTO t_sequence_desc VALUES (1, 5, 'smaller-wins')"""
        order_qt_sequence_descending """
            SELECT * FROM t_sequence_desc ORDER BY id
        """
        assertSparkEquals("t_sequence_desc", "*", "ORDER BY id")

        // rowkind.field turns ordinary INSERT rows into an input changelog.
        sql """INSERT INTO t_rowkind VALUES
            (1, '+I', 'old-1'),
            (2, '+I', 'old-2')
        """
        long rowkindBefore = latestSnapshotId("t_rowkind")
        sql """INSERT INTO t_rowkind VALUES
            (1, '+U', 'new-1'),
            (2, '-D', 'old-2'),
            (3, '+I', 'new-3')
        """
        long rowkindAfter = latestSnapshotId("t_rowkind")
        order_qt_rowkind_changelog """
            SELECT id, payload FROM t_rowkind ORDER BY id
        """

        def auditRows = spark_paimon """
            SELECT rowkind, id, payload
            FROM paimon_incremental_query(
                'paimon.${dbName}.`t_rowkind\$audit_log`',
                '${rowkindBefore}', '${rowkindAfter}')
            ORDER BY id
        """
        assertEquals([
                ["+U", 1, "new-1"],
                ["-D", 2, "old-2"],
                ["+I", 3, "new-3"]
        ], auditRows)
        assertSparkEquals("t_rowkind", "id, payload", "ORDER BY id")

        // Invalid or omitted row kinds fail atomically. The following valid
        // changelog record must still be accepted by a newly opened writer.
        long beforeInvalidSnapshot = latestSnapshotId("t_rowkind")
        long beforeInvalidFiles = activeFileCount("t_rowkind")
        test {
            sql """INSERT INTO t_rowkind VALUES (9, 'XX', 'invalid')"""
            exception "row kind"
        }
        assertEquals(beforeInvalidSnapshot, latestSnapshotId("t_rowkind"))
        assertEquals(beforeInvalidFiles, activeFileCount("t_rowkind"))

        test {
            sql """INSERT INTO t_rowkind VALUES (9, NULL, 'missing-kind')"""
            exception "cannot be null"
        }
        assertEquals(beforeInvalidSnapshot, latestSnapshotId("t_rowkind"))
        assertEquals(beforeInvalidFiles, activeFileCount("t_rowkind"))

        sql """INSERT INTO t_rowkind VALUES (4, '+I', 'recovered')"""
        order_qt_rowkind_recovered """
            SELECT id, payload FROM t_rowkind WHERE id = 4 ORDER BY id
        """
        assertSparkEquals("t_rowkind", "id, payload", "ORDER BY id")
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
