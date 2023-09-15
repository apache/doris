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

import java.util.Random;

suite("test_http_stream_csv_with_names_and_types", "p0") {

    // 1. test csv_with_names_and_types
    def tableName1 = "test_http_stream_csv_with_names_and_types"
    def db = "regression_test_load_p0_http_stream"
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName1} (
            id int,
            name VARCHAR(100),
            age int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        streamLoad {
            set 'version', '1'
            set 'sql', """
                    insert into ${db}.${tableName1} (id, name, age) select cast(id as INT) as id, name, age from http_stream(
                        "format"="csv_with_names_and_types"
                        )
                    """
            time 10000
            file 'student_with_names_and_types.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(10, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_sql1 "select id, name, age from ${tableName1} order by id"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName1}"
    }
}

