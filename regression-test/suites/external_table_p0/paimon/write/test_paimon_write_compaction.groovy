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

suite("test_paimon_write_compaction", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    String catalogName = "test_pw_compaction_catalog"
    String dbName = "test_pw_compaction_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_pk_auto_compaction;
        CREATE TABLE paimon.${dbName}.t_pk_auto_compaction (
            id INT, name STRING, score INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'bucket-key' = 'id',
            'num-sorted-run.compaction-trigger' = '2',
            'target-file-size' = '1 gb'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_append_auto_compaction;
        CREATE TABLE paimon.${dbName}.t_append_auto_compaction (
            id INT, name STRING
        ) USING paimon
        TBLPROPERTIES (
            'bucket' = '1',
            'bucket-key' = 'id',
            'compaction.min.file-num' = '2',
            'target-file-size' = '1 gb'
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
            def sparkRows = spark_paimon """
                SELECT * FROM paimon.${dbName}.${tableName} ${orderBy}
            """
            def dorisRows = sql """SELECT * FROM ${tableName} ${orderBy}"""
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        def fetchFiles = { String tableName ->
            def rows = spark_paimon """
                SELECT level, record_count, file_source
                FROM paimon.${dbName}.`${tableName}\$files`
                ORDER BY file_path
            """
            return rows
        }

        def fetchSnapshots = { String tableName ->
            def rows = spark_paimon """
                SELECT snapshot_id, commit_kind
                FROM paimon.${dbName}.`${tableName}\$snapshots`
                ORDER BY snapshot_id
            """
            return rows
        }

        // A second primary-key write restores the existing L0 file. Two sorted
        // runs trigger merge-tree compaction, which must merge the updated key
        // and commit the compact increment produced by the JNI writer.
        sql """INSERT INTO t_pk_auto_compaction VALUES
            (1, 'old', 10),
            (2, 'stable', 20)
        """
        def pkFilesBefore = fetchFiles("t_pk_auto_compaction")
        assertEquals(1, pkFilesBefore.size())
        assertEquals(0, pkFilesBefore[0][0].toString().toInteger())
        assertEquals(2L, pkFilesBefore[0][1].toString().toLong())
        assertEquals("APPEND", pkFilesBefore[0][2].toString())

        sql """INSERT INTO t_pk_auto_compaction VALUES
            (1, 'new', 11),
            (3, 'added', 30)
        """
        order_qt_compaction_pk """
            SELECT id, name, score FROM t_pk_auto_compaction ORDER BY id
        """
        def pkRows = sql """SELECT id, name, score FROM t_pk_auto_compaction ORDER BY id"""
        assertEquals([
                [1, "new", 11],
                [2, "stable", 20],
                [3, "added", 30]
        ], pkRows)
        assertTableEquals("t_pk_auto_compaction", "ORDER BY id")

        def pkFilesAfter = fetchFiles("t_pk_auto_compaction")
        assertEquals(1, pkFilesAfter.size())
        assertTrue(pkFilesAfter[0][0].toString().toInteger() > 0)
        assertEquals(3L, pkFilesAfter[0][1].toString().toLong())
        assertEquals("COMPACT", pkFilesAfter[0][2].toString())

        def pkSnapshots = fetchSnapshots("t_pk_auto_compaction")
        assertEquals(3, pkSnapshots.size())
        assertEquals(["APPEND", "APPEND", "COMPACT"],
                pkSnapshots.collect { row -> row[1].toString() })

        // Fixed-bucket append-only tables restore existing files for the bucket.
        // The second write reaches compaction.min.file-num and rewrites both
        // small APPEND files into one COMPACT file without losing duplicates.
        sql """INSERT INTO t_append_auto_compaction VALUES
            (1, 'a'),
            (2, 'b')
        """
        def appendFilesBefore = fetchFiles("t_append_auto_compaction")
        assertEquals(1, appendFilesBefore.size())
        assertEquals(2L, appendFilesBefore[0][1].toString().toLong())
        assertEquals("APPEND", appendFilesBefore[0][2].toString())

        sql """INSERT INTO t_append_auto_compaction VALUES
            (3, 'c'),
            (4, 'd')
        """
        order_qt_compaction_append """
            SELECT id, name FROM t_append_auto_compaction ORDER BY id
        """
        def appendRows = sql """SELECT id, name FROM t_append_auto_compaction ORDER BY id"""
        assertEquals([
                [1, "a"],
                [2, "b"],
                [3, "c"],
                [4, "d"]
        ], appendRows)
        assertTableEquals("t_append_auto_compaction", "ORDER BY id")

        def appendFilesAfter = fetchFiles("t_append_auto_compaction")
        assertEquals(1, appendFilesAfter.size())
        assertEquals(4L, appendFilesAfter[0][1].toString().toLong())
        assertEquals("COMPACT", appendFilesAfter[0][2].toString())

        def appendSnapshots = fetchSnapshots("t_append_auto_compaction")
        assertEquals(3, appendSnapshots.size())
        assertEquals(["APPEND", "APPEND", "COMPACT"],
                appendSnapshots.collect { row -> row[1].toString() })
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
