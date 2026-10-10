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

// ORDER BY a key prefix with LIMIT on a MOR table listed in read_mor_as_dup_tables.
// The table is read as DUP, so every version of a key is a row and the TopN sees all of them.
suite("test_read_mor_as_dup_key_topn") {
    sql "DROP TABLE IF EXISTS test_read_mor_as_dup_key_topn"
    sql """
        CREATE TABLE test_read_mor_as_dup_key_topn (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v INT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"
        )
    """
    // Three loads make three overlapping rowsets; key (1, 1) has three versions.
    sql "INSERT INTO test_read_mor_as_dup_key_topn VALUES (1, 1, 10), (2, 2, 20), (5, 5, 50)"
    sql "INSERT INTO test_read_mor_as_dup_key_topn VALUES (1, 1, 11), (3, 3, 30), (6, 6, 60)"
    sql "INSERT INTO test_read_mor_as_dup_key_topn VALUES (1, 1, 12), (4, 4, 40), (7, 7, 70)"

    // Merged read: one row per key.
    qt_merged "SELECT * FROM test_read_mor_as_dup_key_topn ORDER BY k1, k2 LIMIT 3"

    sql "SET read_mor_as_dup_tables = 'test_read_mor_as_dup_key_topn'"
    // The crash needs segment LIMIT pushdown, which fuzzy sessions may turn off.
    sql "SET enable_segment_limit_pushdown = true"
    order_qt_dup_asc "SELECT * FROM test_read_mor_as_dup_key_topn ORDER BY k1, k2 LIMIT 4"
    qt_dup_desc "SELECT * FROM test_read_mor_as_dup_key_topn ORDER BY k1 DESC, k2 DESC LIMIT 3"
    order_qt_dup_prefix "SELECT * FROM test_read_mor_as_dup_key_topn ORDER BY k1 LIMIT 4"
    order_qt_dup_filter "SELECT * FROM test_read_mor_as_dup_key_topn WHERE v > 10 ORDER BY k1, k2 LIMIT 3"
    qt_dup_offset "SELECT * FROM test_read_mor_as_dup_key_topn ORDER BY k1, k2 LIMIT 2 OFFSET 3"
    // A plain LIMIT without ORDER BY still works on the table read as DUP.
    order_qt_dup_limit "SELECT count(*) FROM (SELECT * FROM test_read_mor_as_dup_key_topn LIMIT 5) t"
}
