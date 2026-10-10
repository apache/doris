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


// time_zone accepts Java short IDs such as JST or PST, which the backend tz database does not have. The frontend
// must hand backends the canonical zone ID; otherwise a backend computes in a fallback zone without any error.
suite("test_session_time_zone_short_ids") {
    sql "drop table if exists test_session_time_zone_short_ids"
    sql """
        create table test_session_time_zone_short_ids (k int, ts bigint)
        unique key(k) distributed by hash(k) buckets 1
        properties ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                "store_row_column" = "true")
    """
    sql "insert into test_session_time_zone_short_ids values (1, 0)"

    // from_unixtime runs on the backend, in the time zone of the query.
    for (String tz : ["Asia/Tokyo", "JST", "PST", "IST", "CTT", "EST", "CST"]) {
        sql "set time_zone = '${tz}'"
        order_qt_scan "select '${tz}', from_unixtime(ts) from test_session_time_zone_short_ids"
    }

    // A short-circuit point query sends the time zone in its own request.
    sql "set time_zone = 'JST'"
    sql "set enable_short_circuit_query = true"
    explain {
        sql "select k, from_unixtime(ts) from test_session_time_zone_short_ids where k = 1"
        contains "SHORT-CIRCUIT"
    }
    order_qt_point "select k, from_unixtime(ts) from test_session_time_zone_short_ids where k = 1"
    sql "set time_zone = default"

    // A stream load carries its own time zone.
    sql "drop table if exists test_session_time_zone_short_ids_load"
    sql """
        create table test_session_time_zone_short_ids_load (k int, ts bigint, dt datetime)
        duplicate key(k) distributed by hash(k) buckets 1
        properties ("replication_num" = "1")
    """
    streamLoad {
        table "test_session_time_zone_short_ids_load"
        set "column_separator", ","
        set "timezone", "JST"
        set "columns", "k, ts, dt = from_unixtime(ts)"
        inputText "1,0\n"
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            qt_load_status "select '${json.Status}', ${json.NumberLoadedRows}"
        }
    }
    order_qt_load "select k, ts, dt from test_session_time_zone_short_ids_load"
}
