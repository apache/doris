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

suite("test_json_load", "p0") {
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
        def result2 = sql "INSERT INTO test_json_load (id, city, code) VALUES (200, 'changsha', 3456789)"
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
    
    def load_from_hdfs1 = {testTablex, label, hdfsFilePath, format, brokerName, hdfsUser, hdfsPasswd ->
        def result1= sql """
                        LOAD LABEL ${label} (
                            DATA INFILE("${hdfsFilePath}")
                            INTO TABLE ${testTablex} 
                            FORMAT as "${format}"
                            PRECEDING FILTER id > 1 and id < 10)
                        with BROKER "${brokerName}"
                        ("username"="${hdfsUser}", "password"="${hdfsPasswd}");
                        """
        
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")
    }
    
    def load_from_hdfs2 = {testTablex, label, hdfsFilePath, format, brokerName, hdfsUser, hdfsPasswd ->
        def result1= sql """
                        LOAD LABEL ${label} (
                            DATA INFILE("${hdfsFilePath}")
                            INTO TABLE ${testTablex} 
                            FORMAT as "${format}"
                            PRECEDING FILTER id < 10
                            where id > 1 and id < 5)
                        with BROKER "${brokerName}"
                        ("username"="${hdfsUser}", "password"="${hdfsPasswd}");
                        """
        
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")
    }

    def check_load_result = {checklabel, testTablex ->
        max_try_milli_secs = 10000
        while(max_try_milli_secs) {
            result = sql "show load where label = '${checklabel}'"
            if(result[0][2] == "FINISHED") {
                qt_select "select * from ${testTablex} order by id"
                break
            } else {
                sleep(1000) // wait 1 second every time
                max_try_milli_secs -= 1000
                if(max_try_milli_secs <= 0) {
                    assertEquals(1, 2)
                }
            }
        }
    }

    // case1: import simple json
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table1.call(testTable)

        load_json_data.call('true', '', 'json', '', '', '', '', '', 'simple_json.json')

        qt_select "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case2: import json and apply exprs
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table1.call(testTable)

        load_json_data.call('true', '', 'json', 'id= id * 10', '', '', '', '', 'simple_json.json')

        qt_select "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case3: import json and apply jsonpaths
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table2.call(testTable)
        
        load_json_data.call('true', '', 'json', '', '[\"$.id\", \"$.code\"]',
                            '', '', '', 'simple_json.json')

        qt_select "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
    
    // case4: import json and apply jsonpaths & exprs
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table2.call(testTable)
        
        load_json_data.call('true', '', 'json', 'code = id * 10 + 200', '[\"$.id\"]',
                            '', '', '', 'simple_json.json')

        qt_select "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case5: import json with line reader
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table2.call(testTable)
        
        load_json_data.call('true', 'true', 'json', '', '[\"$.id\", \"$.code\"]',
                            '', '', '', 'multi_line_json.json')
        
        qt_select "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case6: import json use exprs and jsonpaths
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table2.call(testTable)
        
        load_json_data.call('true', 'true', 'json', 'id= id * 10', '[\"$.id\", \"$.code\"]',
                            '', '', '', 'multi_line_json.json')

        qt_select "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case7: import json use where
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table2.call(testTable)
        
        load_json_data.call('true', 'true', 'json', 'id= id * 10', '[\"$.id\", \"$.code\"]',
                            '', 'id > 50', '', 'multi_line_json.json')

        qt_select "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case8: import json use fuzzy_parse
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table2.call(testTable)
        
        load_json_data.call('true', 'true', 'json', 'id= id * 10', '[\"$.id\", \"$.code\"]',
                            '', 'id > 50', 'true', 'multi_line_json.json')

        qt_select "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case9: import json use json_root
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table1.call(testTable)
        
        load_json_data.call('', 'true', 'json', 'id= id * 10', '',
                            '$.item', '', 'true', 'nest_json.json')

        qt_select "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
    
    // if 'enableHdfs' in regression-conf.groovy has been set to true,
    // the test will run these case as below.
    if (enableHdfs()) {
        brokerName =getBrokerName()
        hdfsUser = getHdfsUser()
        hdfsPasswd = getHdfsPasswd()
        def hdfs_file_path = uploadToHdfs "stream_load/simple_object_json.json"
        def format = "json" 

        // case10: import json use pre-filter exprs
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            
            create_test_table1.call(testTable)
            
            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs1.call(testTable, test_load_label, hdfs_file_path, format,
                                brokerName, hdfsUser, hdfsPasswd)
            
            check_load_result.call(test_load_label, testTable)
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }

        // case11: import json use pre-filter and where exprs
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            
            create_test_table1.call(testTable)
            
            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs2.call(testTable, test_load_label, hdfs_file_path, format,
                                brokerName, hdfsUser, hdfsPasswd)
            
            check_load_result.call(test_load_label, testTable)
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }
}
