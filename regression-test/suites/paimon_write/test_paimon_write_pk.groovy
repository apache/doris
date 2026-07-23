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

suite("test_paimon_write_pk", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    String catalogName = "test_pw_pk_catalog"
    String dbName = "test_pw_pk_db"

    // Create Paimon tables via Spark
    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_pk_dedup;
        CREATE TABLE paimon.${dbName}.t_pk_dedup (
            id INT, name STRING, score DOUBLE, ts BIGINT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '2',
            'bucket-key' = 'id'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_pk_bucket4;
        CREATE TABLE paimon.${dbName}.t_pk_bucket4 (
            id INT, name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '4',
            'bucket-key' = 'id'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_pk_composite;
        CREATE TABLE paimon.${dbName}.t_pk_composite (
            user_id INT, event_time BIGINT, event_type STRING, value DOUBLE
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'user_id,event_time',
            'bucket' = '2',
            'bucket-key' = 'user_id'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_pk_string_bucket;
        CREATE TABLE paimon.${dbName}.t_pk_string_bucket (
            user_key STRING, event_id INT, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'user_key,event_id',
            'bucket' = '4',
            'bucket-key' = 'user_key'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_pk_writer_scaling;
        CREATE TABLE paimon.${dbName}.t_pk_writer_scaling (
            id INT, version BIGINT, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'bucket-key' = 'id'
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

    // Prepare an internal OLAP source table for INSERT INTO ... SELECT
    sql """create database if not exists internal.${dbName}"""
    sql """drop table if exists internal.${dbName}.t_source"""
    sql """
        CREATE TABLE internal.${dbName}.t_source (
            id INT, name STRING, score DOUBLE, ts BIGINT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1');
    """
    sql """INSERT INTO internal.${dbName}.t_source VALUES
        (1, 'alice', 95.5, 1000),
        (1, 'alice_updated', 99.0, 2000),
        (2, 'bob', 87.0, 1000),
        (3, 'charlie', 92.3, 1000),
        (3, 'charlie_v2', 88.0, 1500),
        (4, 'diana', 91.0, 1000),
        (5, 'eve', 85.0, 1000)
    """

    try {
        def assertTableEquals = { String tableName, String orderBy ->
            def sparkRows = spark_paimon """SELECT * FROM paimon.${dbName}.${tableName} ${orderBy}"""
            def dorisRows = sql """SELECT * FROM ${tableName} ${orderBy}"""
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        // FT-004: PK table, deduplicate — duplicate keys merged by Paimon SDK
        sql """INSERT INTO t_pk_dedup SELECT id, name, score, ts FROM internal.${dbName}.t_source"""
        order_qt_pk_dedup """SELECT id, name, score FROM t_pk_dedup ORDER BY id"""
        assertTableEquals("t_pk_dedup", "ORDER BY id")

        // FT-005: Interleaved duplicate keys verify that SDK routing preserves the
        // input order within each bucket and the last row for each key wins.
        sql """INSERT INTO t_pk_dedup VALUES
            (100, 'key100_v1', 10.0, 1000),
            (200, 'key200_v1', 20.0, 1000),
            (100, 'key100_v2', 11.0, 2000),
            (200, 'key200_v2', 21.0, 2000),
            (100, 'key100_v3', 12.0, 3000)
        """
        order_qt_pk_interleaved """SELECT id, name, score, ts FROM t_pk_dedup
            WHERE id >= 100 ORDER BY id"""
        assertTableEquals("t_pk_dedup", "ORDER BY id")

        // FT-003: Fixed bucket table with multiple buckets
        for (int i = 0; i < 20; i++) {
            sql """INSERT INTO t_pk_bucket4 VALUES (${i}, 'row${i}')"""
        }
        order_qt_pk_bucket """SELECT id, name FROM t_pk_bucket4 ORDER BY id"""
        assertTableEquals("t_pk_bucket4", "ORDER BY id")

        // PK table with composite primary key
        sql """INSERT INTO t_pk_composite VALUES
            (1, 100, 'click', 1.0),
            (1, 200, 'view', 2.0),
            (2, 100, 'click', 3.0),
            (1, 100, 'click_updated', 99.0)
        """
        // (1,100) duplicated → 3 unique PKs: (1,100), (1,200), (2,100)
        order_qt_pk_composite """SELECT user_id, event_time, event_type, value FROM t_pk_composite ORDER BY user_id, event_time"""
        assertTableEquals("t_pk_composite", "ORDER BY user_id, event_time")

        // FT-006: A string bucket key must use Paimon's string hash and preserve
        // UTF-8 values while routing rows to fixed buckets.
        sql """INSERT INTO t_pk_string_bucket VALUES
            ('alpha', 1, 'alpha_v1'),
            ('beta', 2, '中文_payload'),
            ('emoji_😀', 3, 'emoji_payload'),
            ('alpha', 1, 'alpha_v2')
        """
        order_qt_pk_string_bucket """SELECT user_key, event_id, payload
            FROM t_pk_string_bucket ORDER BY event_id"""
        assertTableEquals("t_pk_string_bucket", "ORDER BY event_id")

        // FT-045: More than 25 MiB crosses the non-partitioned writer-scaling
        // threshold. All versions of one PK must still reach one writer.
        sql """SET parallel_pipeline_task_num = 4"""
        sql """SET enable_strict_consistency_dml = false"""
        qt_pk_writer_scaling_plan """EXPLAIN SHAPE PLAN
            INSERT INTO t_pk_writer_scaling
            SELECT 1, number, repeat('x', 4096)
            FROM numbers("number" = "10000")"""
        sql """INSERT INTO t_pk_writer_scaling
            SELECT 1, number, repeat('x', 4096)
            FROM numbers("number" = "10000")
            ORDER BY number"""
        qt_pk_writer_scaling """SELECT COUNT(*), MIN(id), MAX(id),
            MIN(LENGTH(payload)), MAX(LENGTH(payload)) FROM t_pk_writer_scaling"""
        assertTableEquals("t_pk_writer_scaling", "ORDER BY id")
        sql """SET parallel_pipeline_task_num = 0"""
        sql """SET enable_strict_consistency_dml = true"""
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
