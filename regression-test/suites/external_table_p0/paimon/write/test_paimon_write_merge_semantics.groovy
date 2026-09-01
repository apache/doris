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

suite("test_paimon_write_merge_semantics", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_merge_semantics_catalog"
    String dbName = "test_pw_merge_semantics_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.t_merge;
        CREATE TABLE paimon.${dbName}.t_merge (
            id INT,
            score INT,
            payload STRUCT<x: INT, y: STRING>,
            status STRING,
            required_value STRING NOT NULL
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '2',
            'bucket-key' = 'id',
            'num-sorted-run.compaction-trigger' = '100'
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

    sql """create database if not exists internal.${dbName}"""
    sql """drop table if exists internal.${dbName}.merge_source"""
    sql """
        create table internal.${dbName}.merge_source (
            id int,
            delta int,
            new_x int,
            new_y string,
            action string,
            required_value string
        ) distributed by hash(id) buckets 1
        properties ('replication_num' = '1')
    """

    try {
        def latestSnapshotId = {
            def rows = spark_paimon """
                SELECT max(snapshot_id)
                FROM paimon.${dbName}.`t_merge\$snapshots`
            """
            return rows[0][0] == null ? 0L : rows[0][0].toString().toLong()
        }
        def activeFileCount = {
            def rows = spark_paimon """
                SELECT count(*) FROM paimon.${dbName}.`t_merge\$files`
            """
            return rows[0][0].toString().toLong()
        }
        def assertCrossEngine = {
            def sparkRows = spark_paimon """
                SELECT id, score, payload.x, payload.y, status, required_value
                FROM paimon.${dbName}.t_merge ORDER BY id
            """
            def dorisRows = sql """
                SELECT id, score, payload.x, payload.y, status, required_value
                FROM t_merge ORDER BY id
            """
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        sql """INSERT INTO t_merge VALUES
            (1, 10, named_struct('x', 1, 'y', 'base-1'), 'old', 'required-1'),
            (2, 20, named_struct('x', 2, 'y', 'base-2'), 'old', 'required-2'),
            (3, 30, named_struct('x', 3, 'y', 'base-3'), 'stable', 'required-3')
        """
        sql """INSERT INTO internal.${dbName}.merge_source VALUES
            (1, 5, 11, 'source-1', 'U', 'required-1-new'),
            (2, 0, 22, 'source-2', 'D', 'required-2-new'),
            (4, 40, 44, 'source-4', 'I', 'required-4')
        """

        long beforeMerge = latestSnapshotId()
        sql """
            MERGE INTO t_merge t
            USING internal.${dbName}.merge_source s
            ON t.id = s.id
            WHEN MATCHED AND s.action = 'U' THEN UPDATE SET
                score = t.score + s.delta,
                payload = named_struct(
                    'x', s.new_x,
                    'y', concat(t.payload.y, '-', s.new_y)),
                status = 'updated',
                required_value = s.required_value
            WHEN MATCHED THEN DELETE
            WHEN NOT MATCHED AND s.action = 'I' THEN INSERT
                (required_value, status, payload, score, id)
                VALUES (
                    s.required_value,
                    'inserted',
                    named_struct('x', s.new_x, 'y', s.new_y),
                    s.delta,
                    s.id)
        """
        assertEquals(beforeMerge + 1L, latestSnapshotId())
        order_qt_merge_semantics_result """
            SELECT id, score, payload.x, payload.y, status, required_value
            FROM t_merge ORDER BY id
        """
        assertCrossEngine()

        // An empty source is a true no-op: it must not publish an empty Paimon
        // snapshot or alter the active file set.
        sql """TRUNCATE TABLE internal.${dbName}.merge_source"""
        long beforeEmptySnapshot = latestSnapshotId()
        long beforeEmptyFiles = activeFileCount()
        sql """
            MERGE INTO t_merge t
            USING internal.${dbName}.merge_source s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET score = t.score + s.delta
            WHEN NOT MATCHED THEN INSERT
                (id, score, payload, status, required_value)
                VALUES (s.id, s.delta,
                    named_struct('x', s.new_x, 'y', s.new_y),
                    'inserted', s.required_value)
        """
        assertEquals(beforeEmptySnapshot, latestSnapshotId())
        assertEquals(beforeEmptyFiles, activeFileCount())

        // P09 failure atomicity: a forbidden key update and a missing required
        // insert column both fail before a snapshot or file becomes visible.
        sql """INSERT INTO internal.${dbName}.merge_source VALUES
            (1, 1, 100, 'invalid-key-update', 'U', 'still-required')
        """
        long beforeFailureSnapshot = latestSnapshotId()
        long beforeFailureFiles = activeFileCount()
        test {
            sql """
                MERGE INTO t_merge t
                USING internal.${dbName}.merge_source s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET id = s.id + 100
            """
            exception "primary-key"
        }
        assertEquals(beforeFailureSnapshot, latestSnapshotId())
        assertEquals(beforeFailureFiles, activeFileCount())

        sql """TRUNCATE TABLE internal.${dbName}.merge_source"""
        sql """INSERT INTO internal.${dbName}.merge_source VALUES
            (9, 9, 9, 'missing-required', 'I', NULL)
        """
        test {
            sql """
                MERGE INTO t_merge t
                USING internal.${dbName}.merge_source s ON t.id = s.id
                WHEN NOT MATCHED THEN INSERT (id, score, payload, status)
                    VALUES (s.id, s.delta,
                        named_struct('x', s.new_x, 'y', s.new_y), 'invalid')
            """
            exception "requires values for every table column"
        }
        assertEquals(beforeFailureSnapshot, latestSnapshotId())
        assertEquals(beforeFailureFiles, activeFileCount())

        // A legal MERGE after both failures proves that no stale writer or
        // transaction state poisoned the table.
        sql """TRUNCATE TABLE internal.${dbName}.merge_source"""
        sql """INSERT INTO internal.${dbName}.merge_source VALUES
            (5, 50, 55, 'recovery', 'I', 'required-5')
        """
        sql """
            MERGE INTO t_merge t
            USING internal.${dbName}.merge_source s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT
                (id, score, payload, status, required_value)
                VALUES (s.id, s.delta,
                    named_struct('x', s.new_x, 'y', s.new_y),
                    'recovered', s.required_value)
        """
        assertEquals(beforeFailureSnapshot + 1L, latestSnapshotId())
        order_qt_merge_semantics_recovered """
            SELECT id, status FROM t_merge WHERE id = 5 ORDER BY id
        """
        assertCrossEngine()
    } finally {
        sql """drop catalog if exists ${catalogName}"""
        sql """drop table if exists internal.${dbName}.merge_source"""
    }
}
