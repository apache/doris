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

suite("test_time_travel_dup", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_time_travel_dup FORCE"
    sql """
        CREATE TABLE test_time_travel_dup (
            k INT,
            v INT
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """

    sql "SET show_hidden_columns = true"

    // batch 1
    sql "INSERT INTO test_time_travel_dup VALUES (1, 10), (2, 20)"
    def tso1 = sql("SELECT MAX(__DORIS_COMMIT_TSO_COL__) FROM test_time_travel_dup")[0][0] as Long
    // batch 2
    sql "INSERT INTO test_time_travel_dup VALUES (3, 30)"
    def tso2 = sql("SELECT MAX(__DORIS_COMMIT_TSO_COL__) FROM test_time_travel_dup")[0][0] as Long
    // batch 3
    sql "INSERT INTO test_time_travel_dup VALUES (4, 40)"
    def tso3 = sql("SELECT MAX(__DORIS_COMMIT_TSO_COL__) FROM test_time_travel_dup")[0][0] as Long

    sql "SET show_hidden_columns = false"

    // ---- mid points: exact restore via fetched tso variables ----
    def r1 = sql("SELECT k, v FROM test_time_travel_dup FOR VERSION AS OF ${tso1} ORDER BY k")
    assertEquals([[1, 10], [2, 20]], r1)
    def r2 = sql("SELECT k, v FROM test_time_travel_dup FOR VERSION AS OF ${tso2} ORDER BY k")
    assertEquals([[1, 10], [2, 20], [3, 30]], r2)
    def r3 = sql("SELECT k, v FROM test_time_travel_dup FOR VERSION AS OF ${tso3} ORDER BY k")
    assertEquals([[1, 10], [2, 20], [3, 30], [4, 40]], r3)

    // ---- fixed boundaries: stable .out ----
    // huge value => latest (all 4 rows)
    order_qt_future "SELECT k, v FROM test_time_travel_dup FOR VERSION AS OF 9223372036854775807"
    // zero => empty (all tso > 0)
    order_qt_zero "SELECT k, v FROM test_time_travel_dup FOR VERSION AS OF 0"

    // ---- after compaction the materialized tso keeps the result stable ----
    trigger_and_wait_compaction("test_time_travel_dup", "cumulative")
    def r1c = sql("SELECT k, v FROM test_time_travel_dup FOR VERSION AS OF ${tso1} ORDER BY k")
    assertEquals([[1, 10], [2, 20]], r1c)
    order_qt_future_after_compaction "SELECT k, v FROM test_time_travel_dup FOR VERSION AS OF 9223372036854775807"

    // ---- error path: AS OF on a table without row binlog ----
    sql "DROP TABLE IF EXISTS test_time_travel_dup_nobinlog FORCE"
    sql """
        CREATE TABLE test_time_travel_dup_nobinlog (k INT, v INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    test {
        sql "SELECT * FROM test_time_travel_dup_nobinlog FOR VERSION AS OF 100"
        exception "requires row binlog"
    }
}
