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

// A VARCHAR key byte up to 0x03 used to be stored raw in the encoded key, where it read as the
// marker of the next key column: ("a\x01", "a") encoded below ("a", "z") although it sorts after
// it, and ("a\x02", "b") encoded the same as ("a", "\x02b").
suite("test_string_key_low_bytes") {
    // Load 1 holds K = ("a\x01", "a"); load 2 holds ("a", "z") < K < ("b", "a"). By the raw key
    // bytes load 1 ends before load 2 starts, so the reader skipped merging the two loads and
    // returned K twice instead of merging it.
    sql "DROP TABLE IF EXISTS test_string_key_low_bytes_agg"
    sql """
        CREATE TABLE test_string_key_low_bytes_agg (k1 VARCHAR(10), k2 VARCHAR(10), v INT SUM)
        AGGREGATE KEY(k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql "INSERT INTO test_string_key_low_bytes_agg SELECT unhex('6101'), 'a', 1"
    sql """
        INSERT INTO test_string_key_low_bytes_agg
        SELECT 'a', 'z', 1 UNION ALL SELECT unhex('6101'), 'a', 10 UNION ALL SELECT 'b', 'a', 1
    """
    order_qt_agg "SELECT hex(k1), k2, v FROM test_string_key_low_bytes_agg"

    sql "DROP TABLE IF EXISTS test_string_key_low_bytes_mor"
    sql """
        CREATE TABLE test_string_key_low_bytes_mor (k1 VARCHAR(10), k2 VARCHAR(10), v INT)
        UNIQUE KEY(k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true",
                    "enable_unique_key_merge_on_write" = "false")
    """
    sql "INSERT INTO test_string_key_low_bytes_mor SELECT unhex('6101'), 'a', 1"
    sql """
        INSERT INTO test_string_key_low_bytes_mor
        SELECT 'a', 'z', 1 UNION ALL SELECT unhex('6101'), 'a', 10 UNION ALL SELECT 'b', 'a', 1
    """
    order_qt_mor "SELECT hex(k1), k2, v FROM test_string_key_low_bytes_mor"

    sql "DROP TABLE IF EXISTS test_string_key_low_bytes_dup"
    sql """
        CREATE TABLE test_string_key_low_bytes_dup (k1 VARCHAR(10), k2 VARCHAR(10), v INT)
        DUPLICATE KEY(k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql "INSERT INTO test_string_key_low_bytes_dup SELECT unhex('6101'), 'a', 1"
    sql """
        INSERT INTO test_string_key_low_bytes_dup
        SELECT 'a', 'z', 1 UNION ALL SELECT unhex('6101'), 'a', 10 UNION ALL SELECT 'b', 'a', 1
    """
    order_qt_dup "SELECT hex(k1), k2, v FROM test_string_key_low_bytes_dup"
    qt_dup_top "SELECT hex(k1), k2, v FROM test_string_key_low_bytes_dup ORDER BY k1, k2 LIMIT 1"

    // ("a\x02", "b") and ("a", "\x02b") are two keys. Sharing one encoded primary key, the second
    // load deleted the row of the first.
    sql "DROP TABLE IF EXISTS test_string_key_low_bytes_mow"
    sql """
        CREATE TABLE test_string_key_low_bytes_mow (k1 VARCHAR(10), k2 VARCHAR(10), v INT)
        UNIQUE KEY(k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true",
                    "enable_unique_key_merge_on_write" = "true")
    """
    sql "INSERT INTO test_string_key_low_bytes_mow SELECT unhex('6102'), 'b', 1"
    sql "INSERT INTO test_string_key_low_bytes_mow SELECT 'a', unhex('0262'), 2"
    sql "INSERT INTO test_string_key_low_bytes_mow SELECT unhex('6102'), 'b', 3"
    order_qt_mow "SELECT hex(k1), hex(k2), v FROM test_string_key_low_bytes_mow"
    order_qt_mow_key_range """
        SELECT hex(k1), hex(k2), v FROM test_string_key_low_bytes_mow WHERE k1 = unhex('6102')
    """

    // Compaction merges by the rows, and its output must keep reading the same.
    trigger_and_wait_compaction("test_string_key_low_bytes_agg", "full")
    trigger_and_wait_compaction("test_string_key_low_bytes_mor", "full")
    trigger_and_wait_compaction("test_string_key_low_bytes_dup", "full")
    trigger_and_wait_compaction("test_string_key_low_bytes_mow", "full")
    order_qt_agg_compacted "SELECT hex(k1), k2, v FROM test_string_key_low_bytes_agg"
    order_qt_mor_compacted "SELECT hex(k1), k2, v FROM test_string_key_low_bytes_mor"
    order_qt_dup_compacted "SELECT hex(k1), k2, v FROM test_string_key_low_bytes_dup"
    order_qt_mow_compacted "SELECT hex(k1), hex(k2), v FROM test_string_key_low_bytes_mow"
}
