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

suite("test_stream_load_json", "p0") {
    def tableName = "test_stream_load_json"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
          `id` int NOT NULL,
          `v1` json NULL,
          `v2` json NOT NULL,
          `v3` int NOT NULL,
          `v4` string
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // test strict_mode success
    streamLoad {
        table "${tableName}"

        file 'test_json.csv'
        set "column_separator", ","
        set "columns", "id,v1,v2,v3=json_extract_int(v1, '\$'),v4=json_extract_string(v2, '\$.k')"

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(4, json.NumberTotalRows)
        }
        time 10000 // limit inflight 10s
    }

    sql "sync"
    qt_sql "select * from ${tableName} order by id"
}