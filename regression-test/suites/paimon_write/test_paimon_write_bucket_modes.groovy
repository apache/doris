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

suite("test_paimon_write_bucket_modes", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    String catalogName = "test_pw_bucket_catalog"
    String dbName = "test_pw_bucket_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_hash_fixed;
        CREATE TABLE paimon.${dbName}.t_hash_fixed (
            pt STRING, id INT, name STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'pt,id',
            'bucket' = '4',
            'bucket-key' = 'id'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_hash_dynamic;
        CREATE TABLE paimon.${dbName}.t_hash_dynamic (
            pt STRING, id INT, name STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'pt,id',
            'bucket' = '-1',
            'dynamic-bucket.target-row-num' = '2',
            'dynamic-bucket.initial-buckets' = '1',
            'dynamic-bucket.max-buckets' = '4'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_hash_dynamic_partial;
        CREATE TABLE paimon.${dbName}.t_hash_dynamic_partial (
            pt STRING, id INT, name STRING, score INT
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'pt,id',
            'bucket' = '-1',
            'dynamic-bucket.target-row-num' = '2',
            'merge-engine' = 'partial-update'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_hash_dynamic_overwrite;
        CREATE TABLE paimon.${dbName}.t_hash_dynamic_overwrite (
            id INT, name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-1',
            'dynamic-bucket.target-row-num' = '2',
            'dynamic-bucket.max-buckets' = '4'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_key_dynamic;
        CREATE TABLE paimon.${dbName}.t_key_dynamic (
            pt STRING, id INT, name STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-1',
            'dynamic-bucket.target-row-num' = '2',
            'dynamic-bucket.max-buckets' = '4'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_key_dynamic_partial;
        CREATE TABLE paimon.${dbName}.t_key_dynamic_partial (
            pt STRING, id INT, name STRING, score INT
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-1',
            'dynamic-bucket.target-row-num' = '2',
            'merge-engine' = 'partial-update'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_key_dynamic_first_row;
        CREATE TABLE paimon.${dbName}.t_key_dynamic_first_row (
            pt STRING, id INT, name STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-1',
            'dynamic-bucket.target-row-num' = '2',
            'merge-engine' = 'first-row'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_key_dynamic_aggregation;
        CREATE TABLE paimon.${dbName}.t_key_dynamic_aggregation (
            pt STRING, id INT, total BIGINT
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-1',
            'dynamic-bucket.target-row-num' = '2',
            'merge-engine' = 'aggregation',
            'fields.total.aggregate-function' = 'sum'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_key_dynamic_scale;
        CREATE TABLE paimon.${dbName}.t_key_dynamic_scale (
            pt STRING, id BIGINT, payload STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-1',
            'dynamic-bucket.target-row-num' = '128',
            'dynamic-bucket.max-buckets' = '16'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_bucket_unaware;
        CREATE TABLE paimon.${dbName}.t_bucket_unaware (
            pt STRING, id INT, name STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'bucket' = '-1'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_postpone;
        CREATE TABLE paimon.${dbName}.t_postpone (
            pt STRING, id INT, name STRING
        ) USING paimon
        PARTITIONED BY (pt)
        TBLPROPERTIES (
            'primary-key' = 'pt,id',
            'bucket' = '-2',
            'postpone.default-bucket-num' = '2'
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

        def bucketIds = { String tableName ->
            def rows = spark_paimon """
                SELECT DISTINCT bucket
                FROM paimon.${dbName}.`${tableName}\$files`
                ORDER BY bucket
            """
            return rows.collect { row -> row[0].toString().toInteger() }
        }

        def assertBucketsInRange = { String tableName, int minBucket, int maxBucket ->
            def buckets = bucketIds(tableName)
            assertFalse(buckets.isEmpty())
            assertTrue(buckets.every { bucket ->
                bucket >= minBucket && bucket <= maxBucket
            })
            return buckets
        }

        // Dynamic bucket modes must gather into one fragment instance and one JNI writer.
        def hashDynamicPlan = sql """
            EXPLAIN SHAPE PLAN
            INSERT INTO t_hash_dynamic
            SELECT 'plan_only', CAST(number AS INT), 'unused'
            FROM numbers("number" = "8")
        """
        assertTrue(hashDynamicPlan.flatten().join("\n").contains("DistributionSpecGather"))

        def keyDynamicPlan = sql """
            EXPLAIN SHAPE PLAN
            INSERT INTO t_key_dynamic
            SELECT 'plan_only', CAST(number AS INT), 'unused'
            FROM numbers("number" = "8")
        """
        assertTrue(keyDynamicPlan.flatten().join("\n").contains("DistributionSpecGather"))

        // HASH_FIXED: SDK computes the fixed bucket from bucket-key=id.
        sql """
            INSERT INTO t_hash_fixed
            SELECT concat('p', CAST(number % 2 AS STRING)),
                   CAST(number AS INT),
                   concat('fixed_', CAST(number AS STRING))
            FROM numbers("number" = "16")
        """
        qt_bucket_hash_fixed """
            SELECT COUNT(*), MIN(id), MAX(id), COUNT(DISTINCT pt)
            FROM t_hash_fixed
        """
        assertTableEquals("t_hash_fixed", "ORDER BY pt, id")
        def fixedBuckets = assertBucketsInRange("t_hash_fixed", 0, 3)
        assertTrue(fixedBuckets.size() > 1)

        // HASH_DYNAMIC: new keys expand buckets independently per partition.
        sql """INSERT INTO t_hash_dynamic VALUES
            ('p1', 1, 'v1'),
            ('p1', 2, 'v2'),
            ('p1', 3, 'v3'),
            ('p1', 4, 'v4'),
            ('p1', 5, 'v5'),
            ('p1', 6, 'v6'),
            ('p2', 1, 'p2_v1'),
            ('p2', 2, 'p2_v2')
        """
        assertTableEquals("t_hash_dynamic", "ORDER BY pt, id")
        def dynamicBucketsBeforeUpdate =
                assertBucketsInRange("t_hash_dynamic", 0, 3)
        assertTrue(dynamicBucketsBeforeUpdate.size() > 1)

        // A new Doris transaction must load the existing hash index. Updating only
        // existing keys must not allocate another bucket.
        sql """INSERT INTO t_hash_dynamic VALUES
            ('p1', 1, 'v1_updated'),
            ('p1', 4, 'v4_updated'),
            ('p2', 2, 'p2_v2_updated')
        """
        order_qt_bucket_hash_dynamic """
            SELECT pt, id, name FROM t_hash_dynamic ORDER BY pt, id
        """
        assertTableEquals("t_hash_dynamic", "ORDER BY pt, id")
        assertEquals(dynamicBucketsBeforeUpdate, bucketIds("t_hash_dynamic"))

        // Dynamic bucket and partial-update share the same normalized table row.
        sql """INSERT INTO t_hash_dynamic_partial VALUES
            ('p1', 1, 'alice', 10),
            ('p1', 2, 'bob', 20)
        """
        sql """INSERT INTO t_hash_dynamic_partial (pt, id, score) VALUES
            ('p1', 1, 15),
            ('p1', 3, 30)
        """
        order_qt_bucket_hash_dynamic_partial """
            SELECT pt, id, name, score FROM t_hash_dynamic_partial ORDER BY pt, id
        """
        assertTableEquals("t_hash_dynamic_partial", "ORDER BY pt, id")
        assertBucketsInRange("t_hash_dynamic_partial", 0, Integer.MAX_VALUE)

        // HASH_DYNAMIC overwrite uses the SDK's overwrite assigner and replaces
        // both data files and the dynamic hash index.
        sql """INSERT INTO t_hash_dynamic_overwrite VALUES
            (1, 'old_1'), (2, 'old_2'), (3, 'old_3'), (4, 'old_4')
        """
        sql """INSERT OVERWRITE TABLE t_hash_dynamic_overwrite VALUES
            (10, 'new_10'), (11, 'new_11'), (12, 'new_12')
        """
        order_qt_bucket_hash_dynamic_overwrite """
            SELECT id, name FROM t_hash_dynamic_overwrite ORDER BY id
        """
        assertTableEquals("t_hash_dynamic_overwrite", "ORDER BY id")
        def overwriteRows = sql """
            SELECT id, name FROM t_hash_dynamic_overwrite ORDER BY id
        """
        assertEquals([
                [10, "new_10"],
                [11, "new_11"],
                [12, "new_12"]
        ], overwriteRows)
        assertBucketsInRange("t_hash_dynamic_overwrite", 0, 3)

        // KEY_DYNAMIC: the second statement bootstraps the existing global index.
        // Deduplicate moves an existing primary key to its new partition.
        sql """INSERT INTO t_key_dynamic VALUES
            ('p1', 1, 'id1_old'),
            ('p2', 2, 'id2_stable'),
            ('p1', 3, 'id3_old')
        """
        sql """INSERT INTO t_key_dynamic VALUES
            ('p2', 1, 'id1_moved'),
            ('p3', 3, 'id3_moved'),
            ('p2', 4, 'id4_added')
        """
        order_qt_bucket_key_dynamic """
            SELECT pt, id, name FROM t_key_dynamic ORDER BY id
        """
        assertTableEquals("t_key_dynamic", "ORDER BY id")
        def keyDynamicRows = sql """
            SELECT pt, id, name FROM t_key_dynamic ORDER BY id
        """
        assertEquals([
                ["p2", 1, "id1_moved"],
                ["p2", 2, "id2_stable"],
                ["p3", 3, "id3_moved"],
                ["p2", 4, "id4_added"]
        ], keyDynamicRows)
        assertBucketsInRange("t_key_dynamic", 0, 3)

        // For cross-partition partial-update, the global index keeps the old
        // partition and applies the new non-null fields there.
        sql """INSERT INTO t_key_dynamic_partial VALUES
            ('p1', 10, 'old_10', 10),
            ('p2', 20, 'stable_20', 20)
        """
        sql """INSERT INTO t_key_dynamic_partial (pt, id, score) VALUES
            ('p9', 10, 15),
            ('p3', 30, 30)
        """
        order_qt_bucket_key_dynamic_partial """
            SELECT pt, id, name, score FROM t_key_dynamic_partial ORDER BY id
        """
        assertTableEquals("t_key_dynamic_partial", "ORDER BY id")
        def keyDynamicPartialRows = sql """
            SELECT pt, id, name, score FROM t_key_dynamic_partial ORDER BY id
        """
        assertEquals([
                ["p1", 10, "old_10", 15],
                ["p2", 20, "stable_20", 20],
                ["p3", 30, null, 30]
        ], keyDynamicPartialRows)

        // FIRST_ROW ignores a later value even if it arrives in another partition.
        sql """INSERT INTO t_key_dynamic_first_row VALUES
            ('p1', 1, 'first_1')
        """
        sql """INSERT INTO t_key_dynamic_first_row VALUES
            ('p2', 1, 'ignored_1'),
            ('p2', 2, 'first_2')
        """
        order_qt_bucket_key_dynamic_first_row """
            SELECT pt, id, name FROM t_key_dynamic_first_row ORDER BY id
        """
        assertTableEquals("t_key_dynamic_first_row", "ORDER BY id")
        def keyDynamicFirstRowRows = sql """
            SELECT pt, id, name FROM t_key_dynamic_first_row ORDER BY id
        """
        assertEquals([
                ["p1", 1, "first_1"],
                ["p2", 2, "first_2"]
        ], keyDynamicFirstRowRows)

        // Aggregation also stays in the original partition and combines values.
        sql """INSERT INTO t_key_dynamic_aggregation VALUES
            ('p1', 1, 10)
        """
        sql """INSERT INTO t_key_dynamic_aggregation VALUES
            ('p9', 1, 7),
            ('p2', 2, 20)
        """
        order_qt_bucket_key_dynamic_aggregation """
            SELECT pt, id, total FROM t_key_dynamic_aggregation ORDER BY id
        """
        assertTableEquals("t_key_dynamic_aggregation", "ORDER BY id")
        def keyDynamicAggregationRows = sql """
            SELECT pt, id, total FROM t_key_dynamic_aggregation ORDER BY id
        """
        assertEquals([
                ["p1", 1, 17L],
                ["p2", 2, 20L]
        ], keyDynamicAggregationRows)

        // Bootstrap a larger KEY_DYNAMIC global index across multiple partitions
        // and transactions. REFRESH CATALOG forces the next statement to reopen
        // table metadata and construct a new JNI writer before restoring the index.
        sql """
            INSERT INTO t_key_dynamic_scale
            SELECT concat('p', CAST(number % 16 AS STRING)),
                   number,
                   concat('txn1_', CAST(number AS STRING))
            FROM numbers("number" = "4096")
        """
        sql """
            INSERT INTO t_key_dynamic_scale
            SELECT concat('p', CAST((number + 3) % 16 AS STRING)),
                   number,
                   concat('txn2_', CAST(number AS STRING))
            FROM numbers("number" = "2048")
        """
        sql """REFRESH CATALOG ${catalogName}"""
        sql """SWITCH ${catalogName}"""
        sql """USE ${dbName}"""
        sql """
            INSERT INTO t_key_dynamic_scale
            SELECT concat('p', CAST((number + 5) % 16 AS STRING)),
                   number + 2048,
                   concat('txn3_', CAST(number + 2048 AS STRING))
            FROM numbers("number" = "2048")
        """
        def keyDynamicScaleSummary = sql """
            SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id), SUM(id),
                   COUNT(DISTINCT pt),
                   SUM(IF(payload LIKE 'txn2_%', 1, 0)),
                   SUM(IF(payload LIKE 'txn3_%', 1, 0))
            FROM t_key_dynamic_scale
        """
        assertEquals([[4096L, 4096L, 0L, 4095L, 8386560L, 16L, 2048L, 2048L]],
                keyDynamicScaleSummary)
        assertEquals(3L,
                (sql """SELECT COUNT(*) FROM t_key_dynamic_scale\$snapshots""")[0][0] as long)
        assertBucketsInRange("t_key_dynamic_scale", 0, 15)
        def sparkScaleSummary = spark_paimon """
            SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id), SUM(id),
                   COUNT(DISTINCT pt),
                   SUM(CASE WHEN payload LIKE 'txn2_%' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN payload LIKE 'txn3_%' THEN 1 ELSE 0 END)
            FROM paimon.${dbName}.t_key_dynamic_scale
        """
        assertSparkDorisResultEquals(sparkScaleSummary, keyDynamicScaleSummary)
        order_qt_bucket_key_dynamic_scale_samples """
            SELECT pt, id, payload
            FROM t_key_dynamic_scale
            WHERE id IN (0, 1023, 2047, 2048, 3071, 4095)
            ORDER BY id
        """

        // BUCKET_UNAWARE: append-only writers remain parallel while all files use bucket 0.
        sql """SET parallel_pipeline_task_num = 4"""
        sql """
            INSERT INTO t_bucket_unaware
            SELECT concat('p', CAST(number % 2 AS STRING)),
                   CAST(number AS INT),
                   concat('unaware_', CAST(number AS STRING))
            FROM numbers("number" = "32")
        """
        sql """SET parallel_pipeline_task_num = 0"""
        qt_bucket_unaware """
            SELECT COUNT(*), MIN(id), MAX(id), COUNT(DISTINCT pt)
            FROM t_bucket_unaware
        """
        assertTableEquals("t_bucket_unaware", "ORDER BY pt, id")
        assertEquals([0], bucketIds("t_bucket_unaware"))

        // POSTPONE_MODE commits files to bucket -2. Paimon deliberately
        // excludes those files from readers and the files system table until
        // an external compaction job assigns final buckets.
        sql """INSERT INTO t_postpone VALUES
            ('p1', 1, 'old_1'),
            ('p1', 2, 'stable_2'),
            ('p2', 3, 'stable_3')
        """
        assertEquals([], bucketIds("t_postpone"))
        assertTableEquals("t_postpone", "ORDER BY pt, id")
        def postponeSnapshots = spark_paimon """
            SELECT COUNT(*)
            FROM paimon.${dbName}.`t_postpone\$snapshots`
        """
        assertEquals(1, postponeSnapshots[0][0].toString().toInteger())

        sql """INSERT INTO t_postpone VALUES
            ('p1', 1, 'new_1'),
            ('p2', 4, 'added_4')
        """
        assertEquals([], bucketIds("t_postpone"))
        assertTableEquals("t_postpone", "ORDER BY pt, id")
        postponeSnapshots = spark_paimon """
            SELECT COUNT(*)
            FROM paimon.${dbName}.`t_postpone\$snapshots`
        """
        assertEquals(2, postponeSnapshots[0][0].toString().toInteger())
        qt_bucket_postpone """SELECT COUNT(*) FROM t_postpone"""
    } finally {
        sql """SET parallel_pipeline_task_num = 0"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
