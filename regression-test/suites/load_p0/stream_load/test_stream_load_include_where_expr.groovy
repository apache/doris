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

suite("test_stream_load_include_where_expr", "p0") {
    // define a sql table
    def tableName = "tbl_test_stream_load_include_where_expr"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
                CREATE TABLE IF NOT EXISTS ${tableName}
                (
                    `a` INT COMMENT 'timestamp',
                    `b` INT   COMMENT 'a int value',
                    `c` INT COMMENT 'b int value',
                    `d` varchar(100)
                )
                DUPLICATE KEY(`a`)
                DISTRIBUTED BY HASH(a) BUCKETS AUTO
                properties(
                    "replication_num" = "1"
                );
    """

    streamLoad {
        table "${tableName}"
        set 'columns', 'a, b, c, d'
        set 'format', 'json'
        set 'where', 'd = \'doris\' or d = \'asf\' or b = 9 or b =8'

        file 'test_include_where_expr.json'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(1,json.NumberLoadedRows)
        }
    }

}