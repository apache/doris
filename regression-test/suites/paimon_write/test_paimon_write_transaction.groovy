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

suite("test_paimon_write_transaction", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    String catalogName = "test_pw_txn_catalog"
    String dbName = "test_pw_txn_db"

    // Create Paimon tables via Spark
    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_commit;
        CREATE TABLE paimon.${dbName}.t_commit (
            id INT, name STRING
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_commit_batch;
        CREATE TABLE paimon.${dbName}.t_commit_batch (
            id INT, val DOUBLE
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_overwrite;
        CREATE TABLE paimon.${dbName}.t_overwrite (
            id INT, name STRING
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_overwrite_part;
        CREATE TABLE paimon.${dbName}.t_overwrite_part (
            id INT, name STRING, region STRING
        ) USING paimon
        PARTITIONED BY (region);

        DROP TABLE IF EXISTS paimon.${dbName}.t_overwrite_part_case;
        CREATE TABLE paimon.${dbName}.t_overwrite_part_case (
            id INT, name STRING, Region STRING
        ) USING paimon
        PARTITIONED BY (Region);

        DROP TABLE IF EXISTS paimon.${dbName}.t_static_multi;
        CREATE TABLE paimon.${dbName}.t_static_multi (
            id INT, name STRING, pt0 INT, pt1 STRING
        ) USING paimon
        PARTITIONED BY (pt0, pt1)
        TBLPROPERTIES (
            'dynamic-partition-overwrite' = 'true'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_static_default;
        CREATE TABLE paimon.${dbName}.t_static_default (
            id INT, name STRING, region STRING
        ) USING paimon
        PARTITIONED BY (region)
        TBLPROPERTIES (
            'dynamic-partition-overwrite' = 'true',
            'partition.default-name' = '__CUSTOM_DEFAULT_PARTITION__'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_dynamic_bucket;
        CREATE TABLE paimon.${dbName}.t_dynamic_bucket (
            id INT, name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-1'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_multi;
        CREATE TABLE paimon.${dbName}.t_multi (
            id INT, name STRING, score DOUBLE
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_parallel;
        CREATE TABLE paimon.${dbName}.t_parallel (
            id BIGINT, name STRING
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_spill;
        CREATE TABLE paimon.${dbName}.t_spill (
            id BIGINT, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'write-buffer-size' = '256 kb',
            'page-size' = '64 kb',
            'write-buffer-spillable' = 'true'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_failed_write;
        CREATE TABLE paimon.${dbName}.t_failed_write (
            id BIGINT, payload STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'write-buffer-size' = '256 kb',
            'page-size' = '64 kb',
            'write-buffer-spillable' = 'true'
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

        // FT-010: Basic commit — INSERT INTO, commit, read back
        sql """INSERT INTO t_commit VALUES (1, 'alice'), (2, 'bob')"""
        assertTableEquals("t_commit", "ORDER BY id")

        sql """INSERT INTO t_commit VALUES (3, 'charlie'), (4, 'diana')"""
        order_qt_txn_commit """SELECT id, name FROM t_commit ORDER BY id"""
        assertTableEquals("t_commit", "ORDER BY id")

        // FT-011: Batch INSERT — 10 rows, then self-copy via SELECT
        sql """INSERT INTO t_commit_batch VALUES
            (1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0), (5, 5.0),
            (6, 6.0), (7, 7.0), (8, 8.0), (9, 9.0), (10, 10.0)"""
        sql """INSERT INTO t_commit_batch SELECT id + 10, val + 10.0 FROM t_commit_batch"""
        order_qt_txn_batch """SELECT id, val FROM t_commit_batch ORDER BY id"""
        assertTableEquals("t_commit_batch", "ORDER BY id")

        // FT-012: Full-table overwrite must remove every row from the previous snapshot.
        sql """INSERT INTO t_overwrite VALUES (1, 'old1'), (2, 'old2'), (3, 'old3')"""
        assertTableEquals("t_overwrite", "ORDER BY id")

        sql """INSERT OVERWRITE TABLE t_overwrite VALUES (10, 'new1'), (20, 'new2')"""
        order_qt_txn_overwrite """SELECT id, name FROM t_overwrite ORDER BY id"""
        assertTableEquals("t_overwrite", "ORDER BY id")

        // FT-013: An overwrite with an empty input still commits an empty snapshot.
        sql """INSERT OVERWRITE TABLE t_overwrite SELECT 1, 'unused' WHERE 1 = 0"""
        qt_txn_empty_overwrite """SELECT COUNT(*) FROM t_overwrite"""
        assertTableEquals("t_overwrite", "ORDER BY id")

        // A direct PhysicalEmptyRelation must still publish the empty overwrite snapshot.
        sql """INSERT INTO t_overwrite VALUES (30, 'old_for_limit_zero')"""
        sql """INSERT OVERWRITE TABLE t_overwrite SELECT 1, 'unused' LIMIT 0"""
        qt_txn_empty_overwrite_limit_zero """SELECT COUNT(*) FROM t_overwrite"""
        assertTableEquals("t_overwrite", "ORDER BY id")

        // FT-015: Static partition overwrite replaces only the requested partition.
        sql """INSERT INTO t_overwrite_part VALUES
            (1, 'east_old', 'east'), (2, 'west_old', 'west')"""
        sql """INSERT OVERWRITE TABLE t_overwrite_part
            PARTITION (region = 'east') VALUES (10, 'east_new')"""
        order_qt_txn_static_partition """SELECT id, name, region FROM t_overwrite_part ORDER BY id"""
        assertTableEquals("t_overwrite_part", "ORDER BY id")

        // Static partition names are case-insensitive in Doris and canonicalized
        // to the exact Paimon schema field name before commit.
        sql """INSERT INTO t_overwrite_part_case VALUES
            (1, 'east_old', 'east'), (2, 'west_old', 'west')"""
        sql """INSERT OVERWRITE TABLE t_overwrite_part_case
            PARTITION (region = 'east') VALUES (10, 'east_new')"""
        order_qt_txn_static_partition_case """SELECT id, name, Region
            FROM t_overwrite_part_case ORDER BY id"""
        assertTableEquals("t_overwrite_part_case", "ORDER BY id")

        test {
            sql """INSERT OVERWRITE TABLE t_overwrite_part
                PARTITION (region = 'east', REGION = 'west') VALUES (20, 'ambiguous')"""
            exception "Duplicate partition column: REGION"
        }

        // A static partial spec uses static overwrite semantics even when the
        // table default is dynamic partition overwrite. It replaces every
        // matching subpartition, including those absent from the new input.
        sql """INSERT INTO t_static_multi VALUES
            (1, 'old_a', 1, 'A'), (2, 'old_b', 1, 'B'), (3, 'keep', 2, 'C')"""
        sql """INSERT OVERWRITE TABLE t_static_multi
            PARTITION (pt0 = 1) VALUES (10, 'new_a', 'A')"""
        order_qt_txn_static_partial """SELECT id, name, pt0, pt1
            FROM t_static_multi ORDER BY id"""
        assertTableEquals("t_static_multi", "ORDER BY id")

        // Empty static overwrite still removes the complete matching spec.
        sql """INSERT OVERWRITE TABLE t_static_multi
            PARTITION (pt0 = 1) SELECT 1, 'unused', 'A' LIMIT 0"""
        order_qt_txn_static_partial_empty """SELECT id, name, pt0, pt1
            FROM t_static_multi ORDER BY id"""
        assertTableEquals("t_static_multi", "ORDER BY id")

        // NULL must use the table's actual configurable default partition
        // name, while the literal string "null" and blank strings remain
        // distinct typed partition values.
        sql """INSERT INTO t_static_default VALUES
            (1, 'null_old', NULL),
            (2, 'literal_null', 'null'),
            (3, 'blank_old', ''),
            (4, 'east_old', 'east')"""
        sql """INSERT OVERWRITE TABLE t_static_default
            PARTITION (region = NULL) VALUES (10, 'null_new')"""
        order_qt_txn_static_null """SELECT id, name,
            IF(region = '', '<EMPTY>', region) AS region
            FROM t_static_default ORDER BY id"""
        assertTableEquals("t_static_default", "ORDER BY id")

        sql """INSERT OVERWRITE TABLE t_static_default
            PARTITION (region = 'east') SELECT 1, 'unused' LIMIT 0"""
        order_qt_txn_static_empty """SELECT id, name,
            IF(region = '', '<EMPTY>', region) AS region
            FROM t_static_default ORDER BY id"""
        assertTableEquals("t_static_default", "ORDER BY id")

        sql """INSERT OVERWRITE TABLE t_static_default
            PARTITION (region = '') VALUES (30, 'blank_new')"""
        order_qt_txn_static_blank """SELECT id, name,
            IF(region = '', '<EMPTY>', region) AS region
            FROM t_static_default ORDER BY id"""
        assertTableEquals("t_static_default", "ORDER BY id")

        test {
            sql """INSERT INTO t_dynamic_bucket VALUES (1, 'unsupported')"""
            exception "Paimon dynamic-bucket tables are not supported for writes"
        }

        // FT-016: Dynamic partition overwrite replaces the partitions present in the
        // input while preserving existing partitions that are not touched.
        sql """INSERT OVERWRITE TABLE t_overwrite_part VALUES (30, 'south_new', 'south')"""
        order_qt_txn_dynamic_partition """SELECT id, name, region FROM t_overwrite_part ORDER BY id"""
        assertTableEquals("t_overwrite_part", "ORDER BY id")

        test {
            sql """INSERT OVERWRITE TABLE t_overwrite_part
                PARTITION (region) VALUES (40, 'bare_partition', 'east')"""
            exception "Paimon tables do not support PARTITION name lists"
        }
        test {
            sql """INSERT OVERWRITE TABLE t_overwrite_part
                TEMPORARY PARTITION (region) VALUES (40, 'temporary_partition', 'east')"""
            exception "Paimon tables do not support temporary partitions"
        }
        order_qt_txn_unsupported_partition_syntax """
            SELECT id, name, region FROM t_overwrite_part ORDER BY id
        """
        assertTableEquals("t_overwrite_part", "ORDER BY id")

        // FT-017: Multiple pipeline tasks create multiple LocalState-scoped writers.
        // FE must aggregate every writer's commit payload into one Paimon snapshot.
        sql """SET parallel_pipeline_task_num = 4"""
        sql """INSERT INTO t_parallel
            SELECT number, concat('row_', CAST(number AS STRING))
            FROM numbers("number" = "256")"""
        qt_txn_parallel_writers """SELECT COUNT(*), MIN(id), MAX(id), SUM(id) FROM t_parallel"""
        assertTableEquals("t_parallel", "ORDER BY id")
        sql """SET parallel_pipeline_task_num = 0"""

        // FT-018: The payload is larger than the 256 KB write buffer while each
        // individual row still fits, forcing Paimon's spillable buffer path.
        sql """INSERT INTO t_spill
            SELECT number, repeat('spill_payload_', 80)
            FROM numbers("number" = "2048")"""
        qt_txn_spill """SELECT COUNT(*), MIN(id), MAX(id), SUM(id) FROM t_spill"""
        assertTableEquals("t_spill", "ORDER BY id")

        // FT-019: A row larger than the complete write buffer fails inside the
        // Paimon writer. The failed statement must not publish rows or a snapshot.
        sql """INSERT INTO t_failed_write VALUES (1, 'committed_before_failure')"""
        qt_txn_failed_write_before """SELECT id, payload FROM t_failed_write ORDER BY id"""
        qt_txn_failed_snapshot_before """SELECT COUNT(*) FROM t_failed_write\$snapshots"""
        test {
            sql """INSERT INTO t_failed_write VALUES
                (2, 'accepted_before_error'),
                (3, repeat('x', 1048576))"""
            exception "The record exceeds the maximum size of a sort buffer"
        }
        qt_txn_failed_write_after """SELECT id, payload FROM t_failed_write ORDER BY id"""
        qt_txn_failed_snapshot_after """SELECT COUNT(*) FROM t_failed_write\$snapshots"""
        assertTableEquals("t_failed_write", "ORDER BY id")

        // Multi-row VALUES — verify all 20 rows committed
        sql """
            INSERT INTO t_multi VALUES
            (1, 'a', 10.0),  (2, 'b', 20.0),  (3, 'c', 30.0),  (4, 'd', 40.0),  (5, 'e', 50.0),
            (6, 'f', 60.0),  (7, 'g', 70.0),  (8, 'h', 80.0),  (9, 'i', 90.0),  (10, 'j', 100.0),
            (11, 'k', 110.0),(12, 'l', 120.0),(13, 'm', 130.0),(14, 'n', 140.0),(15, 'o', 150.0),
            (16, 'p', 160.0),(17, 'q', 170.0),(18, 'r', 180.0),(19, 's', 190.0),(20, 't', 200.0)
        """
        order_qt_txn_multi """SELECT id, name, score FROM t_multi ORDER BY id"""
        assertTableEquals("t_multi", "ORDER BY id")
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
