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

suite("test_paimon_write_failures", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    String catalogName = "test_pw_failure_catalog"
    String dbName = "test_pw_failure_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_atomic_append;
        CREATE TABLE paimon.${dbName}.t_atomic_append (
            id INT NOT NULL,
            payload STRING NOT NULL,
            dt STRING NOT NULL
        ) USING paimon
        PARTITIONED BY (dt);

        DROP TABLE IF EXISTS paimon.${dbName}.t_pk_not_null;
        CREATE TABLE paimon.${dbName}.t_pk_not_null (
            id INT NOT NULL,
            payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1'
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
        def assertTableEquals = { String tableName, String orderBy ->
            def sparkRows = spark_paimon """SELECT * FROM paimon.${dbName}.${tableName} ${orderBy}"""
            def dorisRows = sql """SELECT * FROM ${tableName} ${orderBy}"""
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        def assertAtomicAppendState = { long expectedRows, long expectedSnapshots ->
            assertEquals(expectedRows,
                    (sql """SELECT COUNT(*) FROM t_atomic_append""")[0][0] as long)
            assertEquals(expectedSnapshots,
                    (sql """SELECT COUNT(*) FROM t_atomic_append\$snapshots""")[0][0] as long)
        }

        // A failure after an earlier row has already entered the JNI writer must
        // abort the whole statement, including data for a different partition.
        sql """INSERT INTO t_atomic_append VALUES (1, 'baseline', 'p0')"""
        order_qt_failure_atomic_before """
            SELECT id, payload, dt FROM t_atomic_append ORDER BY id
        """
        qt_failure_atomic_snapshot_before """
            SELECT COUNT(*) FROM t_atomic_append\$snapshots
        """

        test {
            sql """INSERT INTO t_atomic_append VALUES
                (2, 'accepted_before_error', 'p1'),
                (3, NULL, 'p2')"""
            exception "Cannot write null to non-null column(payload)"
        }
        assertAtomicAppendState(1L, 1L)
        order_qt_failure_atomic_after """
            SELECT id, payload, dt FROM t_atomic_append ORDER BY id
        """
        qt_failure_atomic_snapshot_after """
            SELECT COUNT(*) FROM t_atomic_append\$snapshots
        """

        // An omitted field without a Paimon default remains NULL and is validated
        // against the real Paimon schema by the Paimon writer.
        test {
            sql """INSERT INTO t_atomic_append (id, dt) VALUES (4, 'p4')"""
            exception "Cannot write null to non-null column(payload)"
        }

        // Partition columns follow the same Paimon nullability contract.
        test {
            sql """INSERT INTO t_atomic_append VALUES (4, 'bad_partition', NULL)"""
            exception "Cannot write null to non-null column(dt)"
        }
        assertAtomicAppendState(1L, 1L)

        // These errors are rejected during target-column and partition binding and
        // therefore must not create a writer or a new Paimon snapshot.
        test {
            sql """INSERT INTO t_atomic_append (id, payload, dt, missing)
                VALUES (5, 'unknown_column', 'p5', 1)"""
            exception "Unknown column 'missing' in target table"
        }
        test {
            sql """INSERT INTO t_atomic_append (id, payload, dt)
                VALUES (5, 'too_few_values')"""
            exception "Column count doesn't match value count"
        }
        test {
            sql """INSERT OVERWRITE TABLE t_atomic_append
                PARTITION (payload = 'not_a_partition') VALUES (5, 'p5')"""
            exception "is not a partition column of Paimon table"
        }
        assertAtomicAppendState(1L, 1L)

        // A successful statement after several failures verifies that failed JNI
        // writers and transactions do not poison subsequent writes.
        sql """INSERT INTO t_atomic_append VALUES (5, 'recovered', 'p5')"""
        assertAtomicAppendState(2L, 2L)
        order_qt_failure_recovered """
            SELECT id, payload, dt FROM t_atomic_append ORDER BY id
        """
        qt_failure_recovered_snapshot """
            SELECT COUNT(*) FROM t_atomic_append\$snapshots
        """

        // A failed overwrite must not publish its replacement files or remove the
        // data referenced by the previous committed snapshot.
        test {
            sql """INSERT OVERWRITE TABLE t_atomic_append VALUES
                (10, 'would_replace', 'p10'),
                (11, NULL, 'p11')"""
            exception "Cannot write null to non-null column(payload)"
        }
        assertAtomicAppendState(2L, 2L)
        order_qt_failure_overwrite_after """
            SELECT id, payload, dt FROM t_atomic_append ORDER BY id
        """
        qt_failure_overwrite_snapshot_after """
            SELECT COUNT(*) FROM t_atomic_append\$snapshots
        """
        assertTableEquals("t_atomic_append", "ORDER BY id")

        // Primary-key nullability is checked before bucket routing. A rejected row
        // must publish no snapshot, and the table remains writable afterwards.
        test {
            sql """INSERT INTO t_pk_not_null VALUES (NULL, 'invalid_key')"""
            exception "Cannot write null to non-null column(id)"
        }
        assertEquals(0L,
                (sql """SELECT COUNT(*) FROM t_pk_not_null\$snapshots""")[0][0] as long)

        sql """INSERT INTO t_pk_not_null VALUES (1, 'valid_after_failure')"""
        order_qt_failure_pk_recovered """
            SELECT id, payload FROM t_pk_not_null ORDER BY id
        """
        qt_failure_pk_snapshot """
            SELECT COUNT(*) FROM t_pk_not_null\$snapshots
        """
        assertTableEquals("t_pk_not_null", "ORDER BY id")
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
