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

// Arrow Stream Load must convert time-zone-aware Arrow timestamps into DATETIME in the load's
// time zone (the `timezone` / `time_zone` header, else the session time zone), and must keep the
// wall-clock fields of naive Arrow timestamps whatever that zone is.
//
// arrow_load_timezone.arrow holds k: int32, ts_zoned: timestamp[us, tz=UTC], ts_naive:
// timestamp[us], with the rows
//   1: 2026-10-13 16:37:00 UTC
//   2: 1969-12-31 23:59:59.123456 UTC
//   3: NULL
suite("test_arrow_stream_load_timezone", "p0") {
    sql "DROP TABLE IF EXISTS test_arrow_stream_load_timezone"
    sql """
        CREATE TABLE test_arrow_stream_load_timezone (
            case_id INT,
            k INT,
            ts_zoned DATETIME(6),
            ts_naive DATETIME(6)
        ) DUPLICATE KEY(case_id, k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    def loadArrow = { int caseId, String headerName, String headerValue ->
        streamLoad {
            table "test_arrow_stream_load_timezone"
            set 'format', 'arrow'
            set 'columns', "k,ts_zoned,ts_naive,case_id=${caseId}"
            if (headerName != null) {
                set headerName, headerValue
            }
            file 'arrow_load_timezone.arrow'
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(3, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }
    }

    loadArrow(1, 'timezone', 'UTC')
    loadArrow(2, 'timezone', 'Asia/Shanghai')
    // The alternate header spelling.
    loadArrow(3, 'time_zone', 'America/Los_Angeles')
    // No header: the session time zone, which depends on the cluster's global setting.
    loadArrow(4, null, null)

    sql "sync"
    order_qt_with_header """
        SELECT case_id, k, CAST(ts_zoned AS STRING), CAST(ts_naive AS STRING)
        FROM test_arrow_stream_load_timezone
        WHERE case_id < 4
        ORDER BY case_id, k
    """
    order_qt_without_header """
        SELECT k,
               ts_zoned <=> CONVERT_TZ(
                       CASE k WHEN 1 THEN CAST('2026-10-13 16:37:00' AS DATETIME(6))
                              WHEN 2 THEN CAST('1969-12-31 23:59:59.123456' AS DATETIME(6)) END,
                       'UTC', @@global.time_zone),
               CAST(ts_naive AS STRING)
        FROM test_arrow_stream_load_timezone
        WHERE case_id = 4
        ORDER BY k
    """
}
