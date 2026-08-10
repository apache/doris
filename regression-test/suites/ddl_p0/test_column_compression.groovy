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
    // Per-column compression codec is only supported in non-cloud mode.
    if (isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_column_compression_tbl"
    sql "DROP TABLE IF EXISTS test_column_compression_bad"
    sql "DROP TABLE IF EXISTS test_column_compression_bad_complex"
    sql "DROP TABLE IF EXISTS test_column_compression_scalar"

    // CREATE TABLE with a per-column codec on the heavy column, table default LZ4F
    sql """
        CREATE TABLE test_column_compression_tbl (
            k INT,
            v_default VARCHAR(64),
            v_heavy VARCHAR(64) COMPRESSION ZSTD(9)
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "compression" = "lz4f")
    """

    // insert + read back correctness
    sql """ INSERT INTO test_column_compression_tbl VALUES
            (1, 'a', 'hello world hello world'),
            (2, 'b', 'the quick brown fox jumps'),
            (3, 'c', 'lorem ipsum dolor sit amet') """
    sql "sync"
    order_qt_select_main "SELECT k, v_default, v_heavy FROM test_column_compression_tbl ORDER BY k"
    order_qt_select_after_modify "SELECT k, v_heavy FROM test_column_compression_tbl ORDER BY k"

    test {
        sql "ALTER TABLE test_column_compression_tbl ADD COLUMN v_added VARCHAR(64) COMPRESSION ZSTD(5)"
        exception "Per-column compression is not supported for ADD COLUMN"
    }

    test {
        sql "ALTER TABLE test_column_compression_tbl MODIFY COLUMN v_heavy VARCHAR(64) COMPRESSION ZSTD(12)"
        exception "Per-column compression is not supported for MODIFY COLUMN"
    }

    // invalid: level on lz4 must be rejected at DDL time
    test {
        sql """
            CREATE TABLE test_column_compression_bad (
                k INT,
                v VARCHAR(64) COMPRESSION LZ4(5)
            ) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        exception "level"
    }

    // invalid: COMPRESSION on a complex-type column must be rejected -- the override only
    // stamps the top-level column meta, so the element data would silently ignore it.
    test {
        sql """
            CREATE TABLE test_column_compression_bad_complex (
                k INT,
                v ARRAY<INT> COMPRESSION ZSTD(9)
            ) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        exception "complex type"
    }

    sql """
        CREATE TABLE test_column_compression_scalar (
            k INT,
            v BIGINT COMPRESSION ZSTD(5)
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO test_column_compression_scalar VALUES (1, 100), (2, 200)"
    sql "sync"
    order_qt_select_scalar "SELECT k, v FROM test_column_compression_scalar ORDER BY k"
}
