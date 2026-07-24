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

suite("test_row_ttl_time_travel", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS row_ttl_time_travel FORCE"
    sql """
        CREATE TABLE row_ttl_time_travel (
            k INT,
            event_time DATETIMEV2(6),
            v STRING
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_row_ttl" = "true",
            "function_column.ttl_col" = "event_time",
            "function_column.ttl" = "0"
        )
    """

    sql """
        INSERT INTO row_ttl_time_travel VALUES
            (1, now(6) - INTERVAL 1 DAY, 'expired-at-snapshot'),
            (2, now(6) + INTERVAL 1 DAY, 'live-at-snapshot')
    """
    sql "SET show_hidden_columns = true"
    def snapshotTso = sql("""
        SELECT MAX(__DORIS_COMMIT_TSO_COL__) FROM row_ttl_time_travel
    """)[0][0] as Long
    sql "SET show_hidden_columns = false"

    sql """
        INSERT INTO row_ttl_time_travel VALUES
            (1, now(6) + INTERVAL 1 DAY, 'live-now'),
            (2, now(6) - INTERVAL 1 DAY, 'expired-now')
    """
    order_qt_row_ttl_latest """
        SELECT k, v FROM row_ttl_time_travel ORDER BY k
    """
    order_qt_row_ttl_snapshot """
        SELECT k, v FROM row_ttl_time_travel
        FOR VERSION AS OF ${snapshotTso} ORDER BY k
    """
}
