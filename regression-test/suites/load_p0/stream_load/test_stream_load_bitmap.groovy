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

suite("test_stream_load_bitmap", "p0") {
    def tableName = "test_stream_load_bitmap"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
          `cache_key` varchar(20) NOT NULL,
          `result_cnt` int NULL,
          `result` bitmap NOT NULL 
        ) ENGINE=OLAP
        UNIQUE KEY(`cache_key`)
        DISTRIBUTED BY HASH(`cache_key`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // test strict_mode success
    streamLoad {
        table "${tableName}"

        file 'test_bitmap.csv'
        set "column_separator", ","
        set "columns", "cache_key,result_cnt,result,result=bitmap_from_base64(result)"

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
        }
        time 10000 // limit inflight 10s
    }

    sql "sync"
    qt_sql2 "select * from ${tableName}"
}