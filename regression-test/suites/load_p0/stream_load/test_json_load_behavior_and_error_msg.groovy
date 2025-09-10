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

suite("test_json_load_behavior_and_error_msg", "p0") {

    // case1 test json-load's default behavior
    try {
        sql """
            CREATE TABLE IF NOT EXISTS test_table1 (
                id INT DEFAULT '10',
                city VARCHAR(32) DEFAULT '',
                code BIGINT SUM DEFAULT '0'
            )
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """
        // re use case_sensitive_json to check
        streamLoad {
            table "test_table1"
            set 'label', "behavior_check" + UUID.randomUUID().toString()
            set 'format', 'json'
            set 'columns', 'Id, cIty, CodE'
            file 'case_sensitive_json.json'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberLoadedRows, 2)
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS test_table1")
    }

    // case2 test multi jsons in a line load error
    try {        
        sql """
            CREATE TABLE IF NOT EXISTS test_table2 (
                user_id INT,
                name VARCHAR(32),
                age INT
            ) ENGINE=OLAP
            DUPLICATE KEY(user_id)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
            """
        
        // use multi_line_json2.json to test ERROR
        streamLoad {
            table "test_table2"
            set 'label', "error_check_" + UUID.randomUUID().toString()
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'columns', 'user_id, name, age'
            file 'multi_line_json2.json'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("[DATA_QUALITY_ERROR]Multiple JSON objects in a single line"))
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS test_table2")
    }
}