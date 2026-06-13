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

suite("test_row_store_only", "p0, nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_row_store_only FORCE"
    sql "DROP TABLE IF EXISTS test_row_store_only_with_subset FORCE"

    test {
        sql """
            CREATE TABLE test_row_store_only_with_subset (
                k INT NOT NULL,
                v1 INT,
                v2 VARCHAR(20)
            ) UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "store_row_column" = "true",
                "row_store_only" = "true",
                "row_store_columns" = "k,v1"
            )
        """
        exception "row_store_columns must be empty when row_store_only is true"
    }

    sql """
        CREATE TABLE test_row_store_only (
            k INT NOT NULL,
            v1 INT DEFAULT "0",
            v2 VARCHAR(20) DEFAULT "x",
            v3 INT DEFAULT "7"
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true",
            "store_row_column" = "true",
            "row_store_only" = "true"
        )
    """

    sql """ INSERT INTO test_row_store_only VALUES (1, 10, 'a', 100), (2, 20, 'b', 200), (3, 30, 'c', 300) """
    sql "sync"
    order_qt_initial """ SELECT * FROM test_row_store_only ORDER BY k """
    qt_count_star """ SELECT COUNT(*) FROM test_row_store_only """

    test {
        sql """ SELECT COUNT(*) + SUM(v1) FROM test_row_store_only """
        exception "row_store_only table does not support queries with aggregate or join"
    }

    sql "SET enable_unique_key_partial_update = true"
    sql "SET enable_insert_strict = false"
    sql """ INSERT INTO test_row_store_only(k, v2) VALUES (2, 'bb'), (4, 'd') """
    sql "SET enable_unique_key_partial_update = false"
    sql "SET enable_insert_strict = true"
    sql "sync"
    order_qt_partial_update """ SELECT * FROM test_row_store_only ORDER BY k """
    qt_point_lookup """ SELECT v2, v3 FROM test_row_store_only WHERE k = 2 """

    sql """ ALTER TABLE test_row_store_only ADD COLUMN v4 INT DEFAULT "9" """
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='test_row_store_only' ORDER BY createtime DESC LIMIT 1 """
        time 2000
    }
    sql """ INSERT INTO test_row_store_only VALUES (5, 50, 'e', 500, 5000) """
    sql "sync"
    order_qt_after_schema_change """ SELECT k, v1, v2, v3, v4 FROM test_row_store_only ORDER BY k """

    trigger_and_wait_compaction("test_row_store_only", "full")
    order_qt_after_full_compaction """ SELECT k, v1, v2, v3, v4 FROM test_row_store_only ORDER BY k """

    def configRow = sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'enable_row_store_only_complex_query_block' """
    String oldBlockConfig = configRow[0][1]
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_row_store_only_complex_query_block" = "false") """
        qt_complex_query_when_block_disabled """ SELECT COUNT(*) + SUM(v1) FROM test_row_store_only """
    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_row_store_only_complex_query_block" = "${oldBlockConfig}") """
    }
}
