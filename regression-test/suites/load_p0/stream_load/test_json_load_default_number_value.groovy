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

suite("test_json_load_default_number_value") {

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    def testTable = "test_json_load_default_number_value"

    def create_test_table = {testTablex ->
        // multi-line sql
        sql """ DROP TABLE IF EXISTS ${testTable} """
        def result = sql """
                        CREATE TABLE IF NOT EXISTS ${testTable} (
                        id INT NOT NULL DEFAULT 0,
                        country VARCHAR(32) NULL DEFAULT 'default_country',
                        city VARCHAR(32) NULL DEFAULT 'default_city',
                        code BIGINT DEFAULT 1111,
                        date_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                        flag CHAR,
                        name VARCHAR(64),
                        descript STRING)
                        DISTRIBUTED BY RANDOM BUCKETS 10
                        PROPERTIES("replication_allocation" = "tag.location.default: 1");
                        """

        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 1)
        assertTrue(result[0][0] == 0, "Create table should update 0 rows")
    }

    def load_json_data = {strip_flag, read_flag, format_flag, json_paths, file_name ->
        // load the json data
        streamLoad {
            table testTable

            // set http request header params
            set 'strip_outer_array', strip_flag
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            set 'jsonpaths', json_paths
            file file_name
        }
    }

    // case1: import simple json lack one column
    try {
        create_test_table.call(testTable)
        load_json_data.call('true', '', 'json', '', 'simple_json.json')
        sql "sync"
        qt_select1 "select id, country, city, code, flag, name, descript from ${testTable} order by id"
        qt_select2 "select count(1) from ${testTable} where date_time is not null"
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case2: import json lack one column of rows
    try {
        create_test_table.call(testTable)
        load_json_data.call('true', '', 'json', '', 'simple_json2_lack_one_column.json')
        sql "sync"
        qt_select3 "select id, country, city, code, flag, name, descript from ${testTable} order by id"
        qt_select4 "select count(1) from ${testTable} where date_time is not null"
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}