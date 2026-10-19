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

suite("test_time_travel_mow_complex", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_time_travel_mow_complex FORCE"
    sql """
        CREATE TABLE test_time_travel_mow_complex (
            k INT,
            v INT
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql "SET show_hidden_columns = true"
    // t1: four keys exist (k1=10, k2=20, k3=30, k5=50)
    sql "INSERT INTO test_time_travel_mow_complex VALUES (1, 10), (2, 20), (3, 30), (5, 50)"
    def t1 = sql("SELECT MAX(__DORIS_COMMIT_TSO_COL__) FROM test_time_travel_mow_complex")[0][0] as Long
    sql "SET show_hidden_columns = false"

    // changes after t1, covering merge-on-read collapse corner cases for the binlog before-image:
    //   k2: updated multiple times 20 -> 200 -> 300 (must restore the earliest before value 20)
    //   k3: deleted (must restore the pre-delete value 30)
    //   k4: brand-new insert (must NOT appear at t1)
    //   k5: deleted then re-inserted 50 -> delete -> 55 (must restore the pre-delete value 50,
    //       not be collapsed into a fresh insert)
    sql "INSERT INTO test_time_travel_mow_complex VALUES (2, 200)"
    sql "INSERT INTO test_time_travel_mow_complex VALUES (2, 300)"
    sql "DELETE FROM test_time_travel_mow_complex WHERE k = 3"
    sql "INSERT INTO test_time_travel_mow_complex VALUES (4, 40)"
    sql "DELETE FROM test_time_travel_mow_complex WHERE k = 5"
    sql "INSERT INTO test_time_travel_mow_complex VALUES (5, 55)"

    // latest: k1=10, k2=300, k4=40, k5=55 (k3 deleted)
    order_qt_latest "SELECT k, v FROM test_time_travel_mow_complex ORDER BY k"

    // ---- restore t1 snapshot: every key keeps its t1-time value ----
    // k2=20 (earliest before across multiple updates), k3=30 (pre-delete),
    // k5=50 (pre-delete, survives the delete-then-reinsert collapse), no k4.
    def snap = sql("SELECT k, v FROM test_time_travel_mow_complex FOR VERSION AS OF ${t1} ORDER BY k")
    assertEquals([[1, 10], [2, 20], [3, 30], [5, 50]], snap)

    // fixed boundary: huge value => latest
    order_qt_future "SELECT k, v FROM test_time_travel_mow_complex FOR VERSION AS OF 9223372036854775807 ORDER BY k"
}
