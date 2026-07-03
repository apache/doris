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

suite("test_time_travel_mow", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_time_travel_mow FORCE"
    sql """
        CREATE TABLE test_time_travel_mow (
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
    // t1: initial state of three keys (k1, k2, k3 exist; k4 not yet)
    sql "INSERT INTO test_time_travel_mow VALUES (1, 10), (2, 20), (3, 30)"
    def t1 = sql("SELECT MAX(__DORIS_COMMIT_TSO_COL__) FROM test_time_travel_mow")[0][0] as Long
    sql "SET show_hidden_columns = false"

    // changes after t1: k2 update, k3 delete, k4 insert
    sql "INSERT INTO test_time_travel_mow VALUES (2, 200)"
    sql "DELETE FROM test_time_travel_mow WHERE k = 3"
    sql "INSERT INTO test_time_travel_mow VALUES (4, 40)"

    // latest: k1=10, k2=200, k4=40 (k3 deleted)
    order_qt_latest "SELECT k, v FROM test_time_travel_mow ORDER BY k"

    // ---- restore t1 snapshot: k1=10, k2=20(old), k3=30(old, though deleted later), no k4 ----
    def snap = sql("SELECT k, v FROM test_time_travel_mow FOR VERSION AS OF ${t1} ORDER BY k")
    assertEquals([[1, 10], [2, 20], [3, 30]], snap)

    // fixed boundary: huge value => latest
    order_qt_future "SELECT k, v FROM test_time_travel_mow FOR VERSION AS OF 9223372036854775807 ORDER BY k"

    // ---- qualified column / star coverage on mow union path ----
    def snapQualified1 = sql("""SELECT test_time_travel_mow.k, test_time_travel_mow.v
            FROM test_time_travel_mow FOR VERSION AS OF ${t1} ORDER BY test_time_travel_mow.k""")
    assertEquals([[1, 10], [2, 20], [3, 30]], snapQualified1)

    def snapQualified2 = sql("""SELECT test_time_travel_mow.k
            FROM test_time_travel_mow FOR VERSION AS OF ${t1} ORDER BY test_time_travel_mow.k""")
    assertEquals([[1], [2], [3]], snapQualified2)

    def snapStar = sql("""SELECT test_time_travel_mow.*
            FROM test_time_travel_mow FOR VERSION AS OF ${t1} ORDER BY k""")
    assertEquals([[1, 10], [2, 20], [3, 30]], snapStar)

    // ---- error path: mow but need_historical_value=false ----
    sql "DROP TABLE IF EXISTS test_time_travel_mow_nohist FORCE"
    sql """
        CREATE TABLE test_time_travel_mow_nohist (k INT, v INT)
        UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    test {
        sql "SELECT * FROM test_time_travel_mow_nohist FOR VERSION AS OF 100"
        exception "need_historical_value"
    }
}
