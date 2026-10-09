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

suite("test_paimon_write_deletion_vector", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_dv_catalog"
    String dbName = "test_pw_dv_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_dv;
        CREATE TABLE paimon.${dbName}.t_dv (
            id INT, payload STRING, score INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'bucket-key' = 'id',
            'deletion-vectors.enabled' = 'true'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_enable_dv;
        CREATE TABLE paimon.${dbName}.t_enable_dv (
            id INT, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'bucket-key' = 'id',
            'deletion-vectors.modifiable' = 'true'
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
    sql """drop table if exists internal.${dbName}.dv_source"""
    sql """
        create table internal.${dbName}.dv_source (
            id int, payload string, score int, action string
        ) distributed by hash(id) buckets 1
        properties ('replication_num' = '1')
    """

    try {
        def deletionVectorEntries = { String tableName ->
            def rows = spark_paimon """
                SELECT coalesce(sum(row_count), 0)
                FROM paimon.${dbName}.`${tableName}\$table_indexes`
                WHERE index_type = 'DELETION_VECTORS'
            """
            return rows[0][0].toString().toLong()
        }
        def assertReaders = { String tag, String tableName, String columns,
                              String orderBy ->
            [true, false].each { boolean forceJni ->
                sql """set force_jni_scanner = ${forceJni}"""
                String reader = forceJni ? "jni" : "native"
                "order_qt_${tag}_${reader}" """
                    SELECT ${columns} FROM ${tableName} ${orderBy}
                """
            }
            def sparkRows = spark_paimon """
                SELECT ${columns} FROM paimon.${dbName}.${tableName} ${orderBy}
            """
            sql """set force_jni_scanner = false"""
            def dorisRows = sql """SELECT ${columns} FROM ${tableName} ${orderBy}"""
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        // Start in MOW mode. Do not set write-only=true here: Paimon implements
        // primary-key deletion vectors during lookup compaction, while write-only
        // deliberately disables that compaction and leaves new level-0 files
        // invisible to the DV-optimized reader until a dedicated compaction runs.
        sql """INSERT INTO t_dv VALUES
            (1, 'old-1', 10),
            (2, 'delete-2', 20),
            (3, 'delete-by-merge-3', 30)
        """
        spark_paimon_multi """
            CALL paimon.sys.compact(
                table => '${dbName}.t_dv',
                compact_strategy => 'full');
        """
        sql """REFRESH CATALOG ${catalogName}"""
        sql """USE ${dbName}"""
        sql """INSERT INTO t_dv VALUES
            (1, 'upsert-1', 11),
            (4, 'insert-4', 40)
        """
        sql """DELETE FROM t_dv WHERE id = 2"""
        boolean dvProducedBeforeCompact = deletionVectorEntries("t_dv") > 0L

        sql """INSERT INTO internal.${dbName}.dv_source VALUES
            (1, 'merged-1', 12, 'U'),
            (3, 'unused-3', 0, 'D'),
            (5, 'inserted-5', 50, 'I')
        """
        sql """
            MERGE INTO t_dv t
            USING internal.${dbName}.dv_source s ON t.id = s.id
            WHEN MATCHED AND s.action = 'D' THEN DELETE
            WHEN MATCHED THEN UPDATE SET payload = s.payload, score = s.score
            WHEN NOT MATCHED THEN INSERT (id, payload, score)
                VALUES (s.id, s.payload, s.score)
        """
        assertReaders("dv_before_compact", "t_dv", "id, payload, score", "ORDER BY id")
        long dvEntriesBeforeCompact = deletionVectorEntries("t_dv")

        // Full compaction must preserve the logical rows while materializing at
        // least part of the accumulated deletion-vector state.
        spark_paimon_multi """
            CALL paimon.sys.compact(
                table => '${dbName}.t_dv',
                compact_strategy => 'full'
            );
        """
        sql """refresh table t_dv"""
        assertReaders("dv_after_compact", "t_dv", "id, payload, score", "ORDER BY id")
        assertTrue(deletionVectorEntries("t_dv") <= dvEntriesBeforeCompact,
                "Full compaction must not increase retained deletion-vector entries")

        // A writer opened after compaction must restore the current index and
        // continue to hide the previous physical row for the same key.
        sql """INSERT INTO t_dv VALUES
            (1, 'post-compact-1', 13),
            (6, 'post-compact-6', 60)
        """
        assertReaders("dv_post_compact_write", "t_dv", "id, payload, score", "ORDER BY id")

        // P08/P11 transition: enable MOW after MOR files already exist. The
        // next Doris statement must reload the changed table options.
        sql """INSERT INTO t_enable_dv VALUES
            (1, 'mor-old-1'),
            (2, 'mor-delete-2')
        """
        spark_paimon_multi """
            CALL paimon.sys.compact(
                table => '${dbName}.t_enable_dv',
                compact_strategy => 'full');
            ALTER TABLE paimon.${dbName}.t_enable_dv SET TBLPROPERTIES (
                'deletion-vectors.enabled' = 'true')
        """
        sql """refresh catalog ${catalogName}"""
        sql """use ${dbName}"""
        sql """INSERT INTO t_enable_dv VALUES (1, 'mow-new-1')"""
        sql """DELETE FROM t_enable_dv WHERE id = 2"""
        // The first MOR-to-MOW lookup compaction may rewrite the old files instead
        // of retaining a non-empty DV index, so validate the stable contract here:
        // both readers must expose the converted update/delete result.
        assertReaders("dv_enabled_after_mor", "t_enable_dv", "id, payload", "ORDER BY id")
        assertTrue(dvProducedBeforeCompact,
                "Doris UPDATE/DELETE must leave a physical deletion vector before compaction")
    } finally {
        sql """set force_jni_scanner = false"""
        sql """drop catalog if exists ${catalogName}"""
        sql """drop table if exists internal.${dbName}.dv_source"""
    }
}
