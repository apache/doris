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
            "disable_auto_compaction" = "true",
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
    def hiddenBeforeCompaction = sql("SELECT k, __DORIS_COMMIT_TSO_COL__ FROM test_time_travel_dup ORDER BY k")

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

    // Include the initial empty [0-1] rowset, whose TSO is [-1, -1]. The full-compaction
    // output inherits that lower bound but must read the materialized per-row TSOs.
    def checkRowsetCount = { int expected ->
        for (def tablet : sql_return_maparray("SHOW TABLETS FROM test_time_travel_dup")) {
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
            assertEquals(0, code)
            assertEquals(expected, parseJson(out.trim()).rowsets.size())
        }
    }
    checkRowsetCount(4)
    trigger_and_wait_compaction("test_time_travel_dup", "full")
    checkRowsetCount(1)
    sql "SET show_hidden_columns = true"
    assertEquals(hiddenBeforeCompaction,
            sql("SELECT k, __DORIS_COMMIT_TSO_COL__ FROM test_time_travel_dup ORDER BY k"))
    sql "SET show_hidden_columns = false"
    def r1c = sql("SELECT k, v FROM test_time_travel_dup FOR VERSION AS OF ${tso1} ORDER BY k")
    assertEquals(r1, r1c)
    order_qt_future_after_compaction "SELECT k, v FROM test_time_travel_dup FOR VERSION AS OF 9223372036854775807"

    // A subsequent compaction must also be able to read the range rowset carrying -1.
    sql "INSERT INTO test_time_travel_dup VALUES (5, 50)"
    sql "SET show_hidden_columns = true"
    def hiddenBeforeSecondCompaction = sql("SELECT k, __DORIS_COMMIT_TSO_COL__ FROM test_time_travel_dup ORDER BY k")
    checkRowsetCount(2)
    trigger_and_wait_compaction("test_time_travel_dup", "full")
    checkRowsetCount(1)
    assertEquals(hiddenBeforeSecondCompaction,
            sql("SELECT k, __DORIS_COMMIT_TSO_COL__ FROM test_time_travel_dup ORDER BY k"))
    sql "SET show_hidden_columns = false"
    assertEquals(r1, sql("SELECT k, v FROM test_time_travel_dup FOR VERSION AS OF ${tso1} ORDER BY k"))
    assertEquals(r3, sql("SELECT k, v FROM test_time_travel_dup FOR VERSION AS OF ${tso3} ORDER BY k"))

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
