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

suite("test_bloom_filter_named_index", "nonConcurrent") {
    def getBaseIndexSchemaProcPath = { String tableName ->
        def dbName = (sql """select database()""")[0][0]
        def catalogId = get_catalog_id("internal")
        def dbId = get_database_id("internal", dbName)
        def tableId = get_table_id("internal", dbName, tableName)
        def indexSchemaProc = """/catalogs/${catalogId}/${dbId}/${tableId}/index_schema"""
        def indexRows = sql """show proc '${indexSchemaProc}'"""
        def baseIndexId = null
        for (int i = 0; i < indexRows.size(); i++) {
            if (indexRows[i][1].equals(tableName)) {
                baseIndexId = indexRows[i][0]
                break
            }
        }
        assert baseIndexId != null
        return """${indexSchemaProc}/${baseIndexId}"""
    }

    sql """DROP TABLE IF EXISTS test_bloom_filter_named_inline"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_named_create_index"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_named_add_on_legacy"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_named_drop_mixed"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_named_rename_legacy_mixed"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_named_drop_legacy_column_mixed"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_named_rollup"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_cov_inline_invalid_type"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_cov_legacy_same_columns"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_cov_legacy_same_fpp"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_cov_no_bf_fpp_only"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_cov_add_index_invalid_type"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_cov_add_index_with_properties"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_cov_empty_legacy_columns"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_named_variant"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_named_array_invalid"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_named_map_invalid"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_named_struct_invalid"""
    sql """DROP TABLE IF EXISTS test_bloom_filter_named_duplicate_inline"""
    sql """DROP TABLE IF EXISTS test_mixed_fpp_bloom_filter_tb"""

    test {
        sql """
            CREATE TABLE test_bloom_filter_named_duplicate_inline (
                k1 INT,
                v1 STRING,
                INDEX idx_v1_a (v1) USING BLOOMFILTER,
                INDEX idx_v1_b (v1) USING BLOOMFILTER
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        exception "column: v1 cannot have multiple indexes, index type: BLOOMFILTER"
    }

    sql """
        CREATE TABLE test_bloom_filter_named_inline (
            k1 INT,
            v1 STRING,
            v2 INT,
            INDEX idx_v1 (v1) USING BLOOMFILTER,
            INDEX idx_v2 (v2) USING BLOOMFILTER
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """INSERT INTO test_bloom_filter_named_inline VALUES (1, 'alpha', 10), (2, 'beta', 20)"""

    sql """ALTER TABLE test_bloom_filter_named_inline SET ("bloom_filter_columns" = "k1")"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_bloom_filter_named_inline' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }

    order_qt_named_inline_data """SELECT * FROM test_bloom_filter_named_inline ORDER BY k1"""
    qt_desc_named_inline """DESC test_bloom_filter_named_inline"""
    qt_desc_all_named_inline """DESC test_bloom_filter_named_inline ALL"""
    qt_show_proc_named_inline_mixed """SHOW PROC '${getBaseIndexSchemaProcPath("test_bloom_filter_named_inline")}'"""
    qt_show_index_named_inline """SHOW INDEX FROM test_bloom_filter_named_inline"""

    test {
        sql """ALTER TABLE test_bloom_filter_named_inline SET ("bloom_filter_columns" = "k1,v1")"""
        exception "ALTER TABLE failed, expected to create bloom filter index on column v1, but this column is already defined by named BLOOMFILTER index"
    }

    test {
        sql """DROP INDEX k1 ON test_bloom_filter_named_inline"""
        exception "index k1 does not exist"
    }

    test {
        sql """CREATE INDEX idx_k1 ON test_bloom_filter_named_inline(k1) USING BLOOMFILTER"""
        exception "k1 should have only one ngram bloom filter index or bloom filter index"
    }

    test {
        sql """CREATE INDEX idx_v1_dup ON test_bloom_filter_named_inline(v1) USING BLOOMFILTER"""
        exception "BLOOMFILTER index for column (v1) with non-analyzed type already exists."
    }

    sql """DROP INDEX IF EXISTS idx_missing ON test_bloom_filter_named_inline"""

    test {
        sql """DROP INDEX idx_missing ON test_bloom_filter_named_inline"""
        exception "index idx_missing does not exist"
    }

    sql """ALTER TABLE test_bloom_filter_named_inline SET ("bloom_filter_columns" = "")"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_bloom_filter_named_inline' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }

    qt_desc_named_inline_after_legacy_drop """DESC test_bloom_filter_named_inline"""

    sql """
        CREATE TABLE test_bloom_filter_named_add_on_legacy (
            k1 INT,
            v1 STRING,
            v2 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "bloom_filter_columns" = "k1"
        )
    """

    sql """CREATE INDEX idx_v2 ON test_bloom_filter_named_add_on_legacy(v2) USING BLOOMFILTER"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_bloom_filter_named_add_on_legacy' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }

    qt_desc_named_add_on_legacy """DESC test_bloom_filter_named_add_on_legacy"""
    qt_show_index_named_add_on_legacy """SHOW INDEX FROM test_bloom_filter_named_add_on_legacy"""

    sql """
        CREATE TABLE test_bloom_filter_named_drop_mixed (
            k1 INT,
            v1 STRING,
            v2 INT,
            INDEX idx_v1 (v1) USING BLOOMFILTER
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """ALTER TABLE test_bloom_filter_named_drop_mixed SET ("bloom_filter_columns" = "k1")"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_bloom_filter_named_drop_mixed' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }
    qt_desc_named_drop_mixed_before """DESC test_bloom_filter_named_drop_mixed"""
    qt_show_index_named_drop_mixed_before """SHOW INDEX FROM test_bloom_filter_named_drop_mixed"""

    sql """DROP INDEX idx_v1 ON test_bloom_filter_named_drop_mixed"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_bloom_filter_named_drop_mixed' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }

    qt_desc_named_drop_mixed_after """DESC test_bloom_filter_named_drop_mixed"""
    qt_show_index_named_drop_mixed_after """SHOW INDEX FROM test_bloom_filter_named_drop_mixed"""

    sql """
        CREATE TABLE test_bloom_filter_named_rename_legacy_mixed (
            k1 INT,
            v1 STRING,
            v2 INT,
            INDEX idx_v2 (v2) USING BLOOMFILTER
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "bloom_filter_columns" = "v1"
        )
    """

    sql """ALTER TABLE test_bloom_filter_named_rename_legacy_mixed RENAME COLUMN v1 v1_new"""
    sql """SYNC"""

    qt_desc_named_rename_legacy_mixed """DESC test_bloom_filter_named_rename_legacy_mixed"""
    qt_show_index_named_rename_legacy_mixed """SHOW INDEX FROM test_bloom_filter_named_rename_legacy_mixed"""

    sql """
        CREATE TABLE test_bloom_filter_named_drop_legacy_column_mixed (
            k1 INT,
            v1 STRING,
            v2 INT,
            INDEX idx_v2 (v2) USING BLOOMFILTER
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "bloom_filter_columns" = "k1,v1"
        )
    """

    sql """ALTER TABLE test_bloom_filter_named_drop_legacy_column_mixed DROP COLUMN v1"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_bloom_filter_named_drop_legacy_column_mixed' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }

    qt_desc_named_drop_legacy_column_mixed """DESC test_bloom_filter_named_drop_legacy_column_mixed"""
    qt_show_index_named_drop_legacy_column_mixed """SHOW INDEX FROM test_bloom_filter_named_drop_legacy_column_mixed"""

    sql """
        CREATE TABLE test_bloom_filter_named_rollup (
            k1 INT,
            v2 INT,
            v1 STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, v2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        ROLLUP (
            r1(k1, v1)
        )
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """INSERT INTO test_bloom_filter_named_rollup VALUES (1, 10, 'alpha'), (2, 20, 'beta')"""

    sql """CREATE INDEX idx_v1 ON test_bloom_filter_named_rollup(v1) USING BLOOMFILTER"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_bloom_filter_named_rollup' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }

    order_qt_named_rollup_data """SELECT * FROM test_bloom_filter_named_rollup ORDER BY k1"""
    qt_show_index_named_rollup """DESC test_bloom_filter_named_rollup ALL"""

    sql """DROP INDEX idx_v1 ON test_bloom_filter_named_rollup"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_bloom_filter_named_rollup' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }

    qt_show_index_named_rollup_after_drop """DESC test_bloom_filter_named_rollup ALL"""

    sql """
        CREATE TABLE test_bloom_filter_named_create_index (
            k1 INT,
            v1 STRING,
            v2 INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """INSERT INTO test_bloom_filter_named_create_index VALUES (1, 'one', 101), (2, 'two', 202)"""

    sql """CREATE INDEX idx_v2 ON test_bloom_filter_named_create_index(v2)
            USING BLOOMFILTER PROPERTIES ("bloom_filter_fpp" = "0.02")"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_bloom_filter_named_create_index' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }

    order_qt_named_create_index_data """SELECT * FROM test_bloom_filter_named_create_index ORDER BY k1"""
    qt_desc_named_create_index """DESC test_bloom_filter_named_create_index"""
    qt_show_index_named_create_index """SHOW INDEX FROM test_bloom_filter_named_create_index"""

    try {
        GetDebugPoint().enableDebugPointForAllBEs("BloomFilterIndexWriter::create", [fpp: "0.02"])
        sql """INSERT INTO test_bloom_filter_named_create_index VALUES (3, 'three', 303), (4, 'four', 404)"""
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("BloomFilterIndexWriter::create")
    }

    test {
        sql """ALTER TABLE test_bloom_filter_named_create_index SET ("bloom_filter_fpp" = "0.01")"""
        exception "Bloom filter index has no change"
    }

    sql """DROP INDEX idx_v2 ON test_bloom_filter_named_create_index"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_bloom_filter_named_create_index' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }

    qt_desc_named_create_index_after_drop """DESC test_bloom_filter_named_create_index"""
    qt_show_index_named_create_index_after_drop """SHOW INDEX FROM test_bloom_filter_named_create_index"""

    test {
        sql """
            CREATE TABLE test_bloom_filter_cov_inline_invalid_type (
                k1 INT,
                f1 FLOAT,
                INDEX idx_f1 (f1) USING BLOOMFILTER
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        exception "FLOAT is not supported in bloom filter index. invalid column: f1"
    }

    sql """
        CREATE TABLE test_bloom_filter_cov_add_index_invalid_type (
            k1 INT,
            f1 FLOAT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    test {
        sql """ALTER TABLE test_bloom_filter_cov_add_index_invalid_type ADD INDEX idx_f1 (f1) USING BLOOMFILTER"""
        exception "FLOAT is not supported in bloom filter index. invalid column: f1"
    }

    sql """
        CREATE TABLE test_bloom_filter_cov_add_index_with_properties (
            k1 INT,
            v1 STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    test {
        sql """ALTER TABLE test_bloom_filter_cov_add_index_with_properties
                ADD INDEX idx_v1 (v1) USING BLOOMFILTER PROPERTIES ("foo" = "bar")"""
        exception "BLOOMFILTER index only supports property bloom_filter_fpp"
    }

    sql """
        CREATE TABLE test_bloom_filter_cov_legacy_same_columns (
            k1 INT,
            v1 STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "bloom_filter_columns" = "k1"
        )
    """

    test {
        sql """ALTER TABLE test_bloom_filter_cov_legacy_same_columns SET ("bloom_filter_columns" = "k1")"""
        exception "Bloom filter index has no change"
    }

    sql """
        CREATE TABLE test_bloom_filter_cov_legacy_same_fpp (
            k1 INT,
            v1 STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "bloom_filter_columns" = "k1",
            "bloom_filter_fpp" = "0.05"
        )
    """

    test {
        sql """ALTER TABLE test_bloom_filter_cov_legacy_same_fpp
                SET ("bloom_filter_columns" = "k1", "bloom_filter_fpp" = "0.05")"""
        exception "Can only set one table property(without dynamic partition && binlog) at a time"
    }

    sql """
        CREATE TABLE test_bloom_filter_cov_no_bf_fpp_only (
            k1 INT,
            v1 STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    test {
        sql """ALTER TABLE test_bloom_filter_cov_no_bf_fpp_only SET ("bloom_filter_fpp" = "0.01")"""
        exception "Bloom filter index has no change"
    }

    test {
        sql """
            CREATE TABLE test_bloom_filter_cov_empty_legacy_columns (
                k1 INT,
                v1 STRING
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "bloom_filter_columns" = ""
            )
        """
        exception "Unknown properties: {bloom_filter_columns=}"
    }

    test {
        sql """
            CREATE TABLE test_bloom_filter_named_array_invalid (
                k1 INT,
                v ARRAY<INT>,
                INDEX idx_v (v) USING BLOOMFILTER PROPERTIES ("bloom_filter_fpp" = "0.03")
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        exception "is not supported"
    }

    test {
        sql """
            CREATE TABLE test_bloom_filter_named_map_invalid (
                k1 INT,
                v MAP<INT, STRING>,
                INDEX idx_v (v) USING BLOOMFILTER PROPERTIES ("bloom_filter_fpp" = "0.03")
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        exception "is not supported"
    }

    test {
        sql """
            CREATE TABLE test_bloom_filter_named_struct_invalid (
                k1 INT,
                v STRUCT<f1:INT, f2:STRING>,
                INDEX idx_v (v) USING BLOOMFILTER PROPERTIES ("bloom_filter_fpp" = "0.03")
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        exception "is not supported"
    }

    sql """
        CREATE TABLE test_bloom_filter_named_variant (
            k1 INT,
            v VARIANT,
            INDEX idx_v (v) USING BLOOMFILTER PROPERTIES ("bloom_filter_fpp" = "0.03")
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """INSERT INTO test_bloom_filter_named_variant VALUES
            (1, '{"a": 1, "b": "alpha", "c": {"d": 10}, "e": [1, 2]}'),
            (2, '{"a": 2, "b": "beta", "c": {"d": 20}, "e": [3, 4]}')"""
    qt_show_index_named_variant """SHOW INDEX FROM test_bloom_filter_named_variant"""

    try {
        GetDebugPoint().enableDebugPointForAllBEs("BloomFilterIndexWriter::create", [fpp: "0.03"])
        sql """INSERT INTO test_bloom_filter_named_variant VALUES
                (3, '{"a": 3, "b": "gamma", "c": {"d": 30}, "e": [5, 6]}')"""
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("BloomFilterIndexWriter::create")
    }

    // Test: table-level bloom_filter_fpp and index-level bloom_filter_fpp are independent.
    // Legacy bloom_filter_columns use the table-level FPP, while named BLOOMFILTER indexes
    // use their per-index FPP from PROPERTIES. We test each source in isolation — only one
    // type of bloom filter is active during each INSERT, so the global debug point can
    // assert the correct FPP for every bloom filter writer that fires.
    def test_mixed_fpp_tb = "test_mixed_fpp_bloom_filter_tb"
    sql """CREATE TABLE IF NOT EXISTS ${test_mixed_fpp_tb} (
            `k1` int(11) NOT NULL,
            `v1` varchar(50) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "bloom_filter_columns" = "k1",
            "bloom_filter_fpp" = "0.03"
    )"""

    // 
    try {
        GetDebugPoint().enableDebugPointForAllBEs("BloomFilterIndexWriter::create", [fpp: "0.03"])
        sql """ INSERT INTO ${test_mixed_fpp_tb} VALUES (1, 'aaa'), (2, 'bbb'), (3, 'ccc') """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("BloomFilterIndexWriter::create");
    }

    // Step 2: remove legacy BF, create a named BLOOMFILTER index on v1 with its own FPP.
    // Only the named index is active → index-level FPP (0.02) is used.
    sql """ ALTER TABLE ${test_mixed_fpp_tb} SET ("bloom_filter_columns" = "") """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_mixed_fpp_bloom_filter_tb' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }
    sql """ CREATE INDEX idx_v1_bf ON ${test_mixed_fpp_tb} (v1) USING BLOOMFILTER
            PROPERTIES("bloom_filter_fpp" = "0.02") """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_mixed_fpp_bloom_filter_tb' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("BloomFilterIndexWriter::create", [fpp: "0.02"])
        sql """ INSERT INTO ${test_mixed_fpp_tb} VALUES (4, 'ddd'), (5, 'eee'), (6, 'fff') """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("BloomFilterIndexWriter::create");
    }

    // Step 3: drop named index, restore legacy BF on k1 → table-level FPP (0.03) again.
    sql """ DROP INDEX idx_v1_bf ON ${test_mixed_fpp_tb} """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_mixed_fpp_bloom_filter_tb' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }
    sql """ ALTER TABLE ${test_mixed_fpp_tb} SET ("bloom_filter_columns" = "k1") """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_mixed_fpp_bloom_filter_tb' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }
    try {
        GetDebugPoint().enableDebugPointForAllBEs("BloomFilterIndexWriter::create", [fpp: "0.05"])
        sql """ INSERT INTO ${test_mixed_fpp_tb} VALUES (7, 'ggg'), (8, 'hhh'), (9, 'iii') """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("BloomFilterIndexWriter::create");
    }

    sql """ ALTER TABLE ${test_mixed_fpp_tb} SET ("bloom_filter_fpp" = "0.03") """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_mixed_fpp_bloom_filter_tb' ORDER BY createtime DESC LIMIT 1"""
        time 120
    }
    try {
        GetDebugPoint().enableDebugPointForAllBEs("BloomFilterIndexWriter::create", [fpp: "0.03"])
        sql """ INSERT INTO ${test_mixed_fpp_tb} VALUES (10, 'jjj'), (11, 'kkk'), (9, 'lll') """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("BloomFilterIndexWriter::create");
    }
}
