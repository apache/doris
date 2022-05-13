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

suite("test_json_load", "load") {
    // define a sql table
    def testTable = "test_json_load"
    
    def create_test_table1 = {testTablex ->
        // multi-line sql
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${testTable} (
                        id INT DEFAULT '10',
                        city VARCHAR(32) DEFAULT '',
                        code BIGINT SUM DEFAULT '0')
                        DISTRIBUTED BY HASH(id) BUCKETS 10
                        PROPERTIES("replication_num" = "1");
                        """
        
        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")
        
        // insert 1 row to check whether the table is ok
        def result2 = sql "INSERT INTO test_json_load (id, city, code) VALUES (200, 'shenzhen', 0755)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    def create_test_table2 = {testTablex ->
        // multi-line sql
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${testTable} (
                        id INT DEFAULT '10',
                        code BIGINT SUM DEFAULT '0')
                        DISTRIBUTED BY HASH(id) BUCKETS 10
                        PROPERTIES("replication_num" = "1");
                        """
        
        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")
        
        // insert 1 row to check whether the table is ok
        def result2 = sql "INSERT INTO test_json_load (id, code) VALUES (200, 0755)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }
    
    def load_json_data = {strip_flag, read_flag, format_flag, exprs, json_paths, 
                            json_root, where_expr, fuzzy_flag, file_name ->
        // load the json data
        streamLoad {
            table "test_json_load"
            
            // set http request header params
            set 'strip_outer_array', strip_flag
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            set 'columns', exprs
            set 'jsonpaths', json_paths
            set 'json_root', json_root
            set 'where', where_expr
            set 'fuzzy_parse', fuzzy_flag
            file file_name // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    // case1: import simple json
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table1.call(testTable)

        load_json_data.call('true', '', 'json', '', '', '', '', '', 'simple_json.json')

        // select the table and check whether the data is correct
        def result3 = sql "select * from test_json_load order by id"
        assertTrue(result3.size() == 11)
        assertTrue(result3[0].size() == 3)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == "beijing")
        assertTrue(result3[0][2] == 2345671)
        assertTrue(result3[9].size() == 3)
        assertTrue(result3[9][0] == 10)
        assertTrue(result3[9][1] == "hefei")
        assertTrue(result3[9][2] == 23456710)

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case2: import json and apply exprs
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table1.call(testTable)

        load_json_data.call('true', '', 'json', 'id= id * 10', '', '', '', '', 'simple_json.json')

        def result3 = sql "select * from test_json_load order by id"
        assertTrue(result3.size() == 11)
        assertTrue(result3[0].size() == 3)
        assertTrue(result3[0][0] == 10)
        assertTrue(result3[0][1] == "beijing")
        assertTrue(result3[0][2] == 2345671)
        assertTrue(result3[9].size() == 3)
        assertTrue(result3[9][0] == 100)
        assertTrue(result3[9][1] == "hefei")
        assertTrue(result3[9][2] == 23456710)

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case3: import json and apply jsonpaths
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table2.call(testTable)
        
        load_json_data.call('true', '', 'json', '', '[\"$.id\", \"$.code\"]',
                            '', '', '', 'simple_json.json')

        def result3 = sql "select * from test_json_load order by id"
        assertTrue(result3.size() == 11)
        assertTrue(result3[0].size() == 2)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == 2345671)
        assertTrue(result3[9].size() == 2)
        assertTrue(result3[9][0] == 10)
        assertTrue(result3[9][1] == 23456710)

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case4: import json with line reader
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table2.call(testTable)
        
        load_json_data.call('true', 'true', 'json', '', '[\"$.id\", \"$.code\"]',
                            '', '', '', 'multi_line_json.json')
        
        def result3 = sql "select * from test_json_load order by id"
        assertTrue(result3.size() == 11)
        assertTrue(result3[0].size() == 2)
        assertTrue(result3[0][0] == 1)
        assertTrue(result3[0][1] == 1454547)
        assertTrue(result3[9].size() == 2)
        assertTrue(result3[9][0] == 10)
        assertTrue(result3[9][1] == 2345676)

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case5: import json use exprs and jsonpaths
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table2.call(testTable)
        
        load_json_data.call('true', 'true', 'json', 'id= id * 10', '[\"$.id\", \"$.code\"]',
                            '', '', '', 'multi_line_json.json')

        def result3 = sql "select * from test_json_load order by id"
        assertTrue(result3.size() == 11)
        assertTrue(result3[0].size() == 2)
        assertTrue(result3[0][0] == 10)
        assertTrue(result3[0][1] == 1454547)
        assertTrue(result3[9].size() == 2)
        assertTrue(result3[9][0] == 100)
        assertTrue(result3[9][1] == 2345676)

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case6: import json use where
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table2.call(testTable)
        
        load_json_data.call('true', 'true', 'json', 'id= id * 10', '[\"$.id\", \"$.code\"]',
                            '', 'id > 50', '', 'multi_line_json.json')

        def result3 = sql "select * from test_json_load order by id"
        assertTrue(result3.size() == 6)
        assertTrue(result3[0].size() == 2)
        assertTrue(result3[0][0] == 60)
        assertTrue(result3[0][1] == 2345672)
        assertTrue(result3[4].size() == 2)
        assertTrue(result3[4][0] == 100)
        assertTrue(result3[4][1] == 2345676)
        assertTrue(result3[5].size() == 2)
        assertTrue(result3[5][0] == 200)
        assertTrue(result3[5][1] == 755)
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case7: import json use fuzzy_parse
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table2.call(testTable)
        
        load_json_data.call('true', 'true', 'json', 'id= id * 10', '[\"$.id\", \"$.code\"]',
                            '', 'id > 50', 'true', 'multi_line_json.json')

        def result3 = sql "select * from test_json_load order by id"
        assertTrue(result3.size() == 6)
        assertTrue(result3[0].size() == 2)
        assertTrue(result3[0][0] == 60)
        assertTrue(result3[0][1] == 2345672)
        assertTrue(result3[4].size() == 2)
        assertTrue(result3[4][0] == 100)
        assertTrue(result3[4][1] == 2345676)
        assertTrue(result3[5].size() == 2)
        assertTrue(result3[5][0] == 200)
        assertTrue(result3[5][1] == 755)
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case8: import json use json_root
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table1.call(testTable)
        
        load_json_data.call('', 'true', 'json', 'id= id * 10', '',
                            '$.item', '', 'true', 'nest_json.json')

        def result3 = sql "select * from test_json_load order by id"
        assertTrue(result3.size() == 3)
        assertTrue(result3[0].size() == 3)
        assertTrue(result3[0][0] == 10)
        assertTrue(result3[0][1] == "beijing")
        assertTrue(result3[0][2] == 2345671)
        assertTrue(result3[1].size() == 3)
        assertTrue(result3[1][0] == 20)
        assertTrue(result3[1][1] == "shanghai")
        assertTrue(result3[1][2] == 2345672)
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}