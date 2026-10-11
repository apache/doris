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

suite("test_paimon_write_partition_delete", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_partition_delete_catalog"
    String dbName = "test_pw_partition_delete_db"
    String snapshotsTableSuffix = '$snapshots'

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.t_partition_delete;
        CREATE TABLE paimon.${dbName}.t_partition_delete (
            pt STRING, id INT, score INT, payload STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-1',
            'dynamic-bucket.target-row-num' = '2',
            'write-only' = 'true'
        );
    """

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
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
    sql """SWITCH ${catalogName}"""
    sql """USE ${dbName}"""

    try {
        def latestSnapshot = {
            String query = """
                SELECT snapshot_id, commit_kind
                FROM paimon.${dbName}.`t_partition_delete${snapshotsTableSuffix}`
                ORDER BY snapshot_id DESC LIMIT 1
            """
            def rows = spark_paimon(query)
            return rows.isEmpty() ? [0L, null] : [
                    rows[0][0].toString().toLong(), rows[0][1].toString().toUpperCase()]
        }
        def assertRows = { String tag ->
            "order_qt_${tag}" """
                SELECT pt, id, score, payload
                FROM t_partition_delete ORDER BY id
            """
            def dorisRows = sql """
                SELECT pt, id, score, payload
                FROM t_partition_delete ORDER BY id
            """
            def sparkRows = spark_paimon """
                SELECT pt, id, score, payload
                FROM paimon.${dbName}.t_partition_delete ORDER BY id
            """
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }
        def assertDeleteCommit = { List<Object> before ->
            def after = latestSnapshot()
            assertEquals(before[0] + 1L, after[0])
            assertEquals("APPEND", after[1])
            return after
        }

        sql """INSERT INTO t_partition_delete VALUES
            ('p1', 1, 10, 'p1-a'),
            ('p1', 2, 20, 'p1-b'),
            ('p2', 3, 30, 'p2-a'),
            ('p2', 4, 40, 'p2-b'),
            ('p3', 5, 50, 'p3-a'),
            (NULL, 6, 60, 'default-partition')
        """

        // A predicate covering the complete partition must not affect any
        // other partition, including the default partition represented by NULL.
        def snapshot = latestSnapshot()
        sql """DELETE FROM t_partition_delete WHERE pt = 'p1'"""
        snapshot = assertDeleteCommit(snapshot)
        assertRows("partition_delete_full_partition")

        // A partial-partition predicate is evaluated row by row.
        sql """DELETE FROM t_partition_delete WHERE pt = 'p2' AND score >= 40"""
        snapshot = assertDeleteCommit(snapshot)
        assertRows("partition_delete_partial_partition")

        // A non-convertible partition expression must retain its exact SQL
        // semantics instead of expanding into a full-partition delete.
        sql """DELETE FROM t_partition_delete WHERE upper(pt) = 'P2' AND id = 3"""
        snapshot = assertDeleteCommit(snapshot)
        assertRows("partition_delete_expression")

        // UNKNOWN predicates match no rows and must not create empty commits.
        sql """DELETE FROM t_partition_delete WHERE pt = NULL"""
        assertEquals(snapshot, latestSnapshot())
        sql """DELETE FROM t_partition_delete WHERE pt NOT IN ('p3', NULL)"""
        assertEquals(snapshot, latestSnapshot())
        sql """DELETE FROM t_partition_delete WHERE EXISTS (SELECT 1 WHERE FALSE)"""
        assertEquals(snapshot, latestSnapshot())

        // NOT EXISTS is true here and is combined with a target predicate so
        // only the intended row is removed.
        sql """
            DELETE FROM t_partition_delete
            WHERE id = 5 AND NOT EXISTS (SELECT 1 WHERE FALSE)
        """
        snapshot = assertDeleteCommit(snapshot)
        assertRows("partition_delete_not_exists")

        // IS NULL addresses the default partition explicitly.
        sql """DELETE FROM t_partition_delete WHERE pt IS NULL"""
        snapshot = assertDeleteCommit(snapshot)
        assertRows("partition_delete_default_partition")

        // A second no-match delete proves the empty-table path also avoids a
        // metadata-only snapshot.
        sql """DELETE FROM t_partition_delete WHERE id = 999"""
        assertEquals(snapshot, latestSnapshot())
    } finally {
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
    }
}
