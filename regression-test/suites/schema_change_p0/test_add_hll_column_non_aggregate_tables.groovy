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

suite("test_add_hll_column_non_aggregate_tables") {
    sql "DROP TABLE IF EXISTS test_add_hll_column_duplicate"
    sql "DROP TABLE IF EXISTS test_add_hll_column_unique_mow"
    sql "DROP TABLE IF EXISTS test_add_hll_column_unique_mor"

    sql """
        CREATE TABLE test_add_hll_column_duplicate (
            k INT
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql "INSERT INTO test_add_hll_column_duplicate VALUES (1), (2)"

    sql """
        CREATE TABLE test_add_hll_column_unique_mow (
            k INT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql "INSERT INTO test_add_hll_column_unique_mow VALUES (1), (2)"

    sql """
        CREATE TABLE test_add_hll_column_unique_mor (
            k INT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        )
    """
    sql "INSERT INTO test_add_hll_column_unique_mor VALUES (1), (2)"

    sql "SYNC"

    sql "ALTER TABLE test_add_hll_column_duplicate ADD COLUMN hll_col HLL"
    sql "ALTER TABLE test_add_hll_column_unique_mow ADD COLUMN hll_col HLL"
    sql "ALTER TABLE test_add_hll_column_unique_mor ADD COLUMN hll_col HLL"

    order_qt_duplicate_existing_rows """
        SELECT k, hll_cardinality(hll_col)
        FROM test_add_hll_column_duplicate
    """
    order_qt_unique_mow_existing_rows """
        SELECT k, hll_cardinality(hll_col)
        FROM test_add_hll_column_unique_mow
    """
    order_qt_unique_mor_existing_rows """
        SELECT k, hll_cardinality(hll_col)
        FROM test_add_hll_column_unique_mor
    """

    sql """
        INSERT INTO test_add_hll_column_duplicate VALUES
            (1, hll_hash(10)),
            (2, hll_hash(20))
    """
    sql "INSERT INTO test_add_hll_column_unique_mow VALUES (1, hll_hash(10))"
    sql "INSERT INTO test_add_hll_column_unique_mor VALUES (1, hll_hash(10))"

    order_qt_duplicate_new_rows """
        SELECT k, hll_cardinality(hll_col)
        FROM test_add_hll_column_duplicate
    """
    order_qt_unique_mow_new_rows """
        SELECT k, hll_cardinality(hll_col)
        FROM test_add_hll_column_unique_mow
    """
    order_qt_unique_mor_new_rows """
        SELECT k, hll_cardinality(hll_col)
        FROM test_add_hll_column_unique_mor
    """
}
