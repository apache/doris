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

suite("test_column_compression") {
    def tableName = "test_column_compression_tbl"
    sql "DROP TABLE IF EXISTS ${tableName}"

    // CREATE TABLE with a per-column codec on the heavy column, table default LZ4F
    sql """
        CREATE TABLE ${tableName} (
            k INT,
            v_default VARCHAR(64),
            v_heavy VARCHAR(64) COMPRESSION 'zstd:9'
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "compression" = "lz4f")
    """

    // SHOW CREATE TABLE must round-trip the clause
    def createTableResult = sql "SHOW CREATE TABLE ${tableName}"
    def createSql = createTableResult[0][1].toString()
    assertTrue(createSql.contains("COMPRESSION \"zstd:9\""),
            "SHOW CREATE TABLE should render per-column compression, got: ${createSql}")

    // insert + read back correctness
    sql """ INSERT INTO ${tableName} VALUES
            (1, 'a', 'hello world hello world'),
            (2, 'b', 'the quick brown fox jumps'),
            (3, 'c', 'lorem ipsum dolor sit amet') """
    sql "sync"
    order_qt_select_main "SELECT k, v_default, v_heavy FROM ${tableName} ORDER BY k"

    // ADD COLUMN with a codec
    sql "ALTER TABLE ${tableName} ADD COLUMN v_added VARCHAR(64) COMPRESSION 'zstd:5'"
    // wait for schema change to finish
    def maxWait = 60
    for (int i = 0; i < maxWait; i++) {
        def jobs = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1"
        if (jobs.size() == 0 || jobs[0][9].toString() == "FINISHED") {
            break
        }
        Thread.sleep(1000)
    }
    def createSql2 = (sql "SHOW CREATE TABLE ${tableName}")[0][1].toString()
    assertTrue(createSql2.contains("`v_added`") && createSql2.contains("zstd:5"),
            "added column should carry its codec, got: ${createSql2}")

    // MODIFY COLUMN codec (metadata-only): old data still readable, new codec applies to new data
    sql "ALTER TABLE ${tableName} MODIFY COLUMN v_heavy VARCHAR(64) COMPRESSION 'zstd:12'"
    for (int i = 0; i < maxWait; i++) {
        def jobs = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1"
        if (jobs.size() == 0 || jobs[0][9].toString() == "FINISHED") {
            break
        }
        Thread.sleep(1000)
    }
    // old data still readable after a metadata-only codec change
    order_qt_select_after_modify "SELECT k, v_heavy FROM ${tableName} ORDER BY k"

    // MODIFY COLUMN must actually update the persisted per-column codec, not silently
    // no-op the change (Column.equals() historically excludes compression fields, which
    // could make the FE treat a compression-only MODIFY as a no-op and drop it).
    def createSql3 = (sql "SHOW CREATE TABLE ${tableName}")[0][1].toString()
    assertTrue(createSql3.contains("zstd:12"),
            "MODIFY COLUMN should update the per-column codec to zstd:12, got: ${createSql3}")
    assertTrue(!createSql3.contains("zstd:9"),
            "MODIFY COLUMN should have replaced the old zstd:9 codec, got: ${createSql3}")

    // invalid: level on lz4 must be rejected at DDL time
    test {
        sql """
            CREATE TABLE ${tableName}_bad (
                k INT,
                v VARCHAR(64) COMPRESSION 'lz4:5'
            ) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        exception "level"
    }

    // invalid: COMPRESSION on a complex-type column must be rejected -- the override only
    // stamps the top-level column meta, so the element data would silently ignore it.
    test {
        sql """
            CREATE TABLE ${tableName}_bad_complex (
                k INT,
                v ARRAY<INT> COMPRESSION 'zstd:9'
            ) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        exception "complex type"
    }

    sql "DROP TABLE IF EXISTS ${tableName}"

    // A compression-only MODIFY on a non-VARCHAR scalar value column must be treated as a
    // metadata-only (light) schema change, not a full shadow-column rewrite. Regression guard
    // for Column.equals() now including compression: the routing/type guards and the light
    // schema-change classification must recognize a compression-only delta.
    def scalarTable = "test_column_compression_scalar"
    sql "DROP TABLE IF EXISTS ${scalarTable}"
    sql """
        CREATE TABLE ${scalarTable} (
            k INT,
            v BIGINT COMPRESSION 'zstd:5'
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO ${scalarTable} VALUES (1, 100), (2, 200)"
    sql "sync"
    sql "ALTER TABLE ${scalarTable} MODIFY COLUMN v BIGINT COMPRESSION 'zstd:12'"
    for (int i = 0; i < maxWait; i++) {
        def jobs = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${scalarTable}' ORDER BY CreateTime DESC LIMIT 1"
        if (jobs.size() == 0 || jobs[0][9].toString() == "FINISHED") {
            break
        }
        Thread.sleep(1000)
    }
    def scalarCreateSql = (sql "SHOW CREATE TABLE ${scalarTable}")[0][1].toString()
    assertTrue(scalarCreateSql.contains("zstd:12"),
            "compression-only MODIFY on a scalar column should persist the new codec, got: ${scalarCreateSql}")
    order_qt_select_scalar "SELECT k, v FROM ${scalarTable} ORDER BY k"
    sql "DROP TABLE IF EXISTS ${scalarTable}"

    // A compression-only MODIFY on partition and distribution key columns must be allowed:
    // the codec is per-segment metadata and does not affect partitioning or hash routing.
    def keyTable = "test_column_compression_key"
    sql "DROP TABLE IF EXISTS ${keyTable}"
    sql """
        CREATE TABLE ${keyTable} (
            p INT COMPRESSION 'zstd:5',
            d INT COMPRESSION 'zstd:5',
            v INT
        )
        DUPLICATE KEY(p, d)
        PARTITION BY RANGE(p) (
            PARTITION p1 VALUES LESS THAN (100),
            PARTITION p2 VALUES LESS THAN (200)
        )
        DISTRIBUTED BY HASH(d) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // modify the range-partition column codec: must not be rejected as "modify partition column"
    sql "ALTER TABLE ${keyTable} MODIFY COLUMN p INT COMPRESSION 'zstd:12'"
    for (int i = 0; i < maxWait; i++) {
        def jobs = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${keyTable}' ORDER BY CreateTime DESC LIMIT 1"
        if (jobs.size() == 0 || jobs[0][9].toString() == "FINISHED") {
            break
        }
        Thread.sleep(1000)
    }
    // modify the hash-distribution column codec: must not be rejected as "modify distribution column"
    sql "ALTER TABLE ${keyTable} MODIFY COLUMN d INT COMPRESSION 'zstd:12'"
    for (int i = 0; i < maxWait; i++) {
        def jobs = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${keyTable}' ORDER BY CreateTime DESC LIMIT 1"
        if (jobs.size() == 0 || jobs[0][9].toString() == "FINISHED") {
            break
        }
        Thread.sleep(1000)
    }
    def keyCreateSql = (sql "SHOW CREATE TABLE ${keyTable}")[0][1].toString()
    assertTrue(!keyCreateSql.contains("zstd:5"),
            "compression-only MODIFY on partition/distribution columns should persist the new codec, got: ${keyCreateSql}")
    sql "DROP TABLE IF EXISTS ${keyTable}"
}
