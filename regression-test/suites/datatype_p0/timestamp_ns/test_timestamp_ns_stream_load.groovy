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

suite("test_timestamp_ns_stream_load") {
    sql "set time_zone = '+08:00'"
    sql "drop table if exists test_timestamp_ns_stream_load"
    sql """
        create table test_timestamp_ns_stream_load (
            id int,
            dt timestamp_ns
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """

    streamLoad {
        table "test_timestamp_ns_stream_load"
        file "test_timestamp_ns_stream_load.csv"
        set "column_separator", "|"
        set "strict_mode", "true"
        set "max_filter_ratio", "0.3"

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(9, json.NumberTotalRows)
            assertEquals(7, json.NumberLoadedRows)
            assertEquals(2, json.NumberFilteredRows)
        }
    }

    order_qt_stream_load """
        select id, dt
        from test_timestamp_ns_stream_load
        order by id
    """

}
