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
                        DISTRIBUTED BY RANDOM BUCKETS 10
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
                        DISTRIBUTED BY RANDOM BUCKETS 10
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

    // city is NOT NULL
    def create_test_table3 = {testTablex ->
        // multi-line sql
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${testTablex} (
                        id INT DEFAULT '10',
                        city VARCHAR(32) NOT NULL,
                        code BIGINT SUM DEFAULT '0')
                        DISTRIBUTED BY RANDOM BUCKETS 10
                        PROPERTIES("replication_num" = "1");
                        """
        
        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")
        
        // insert 1 row to check whether the table is ok
        def result2 = sql "INSERT INTO ${testTablex} (id, city, code) VALUES (200, 'hangzhou', 12345)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    def test_invalid_json_array_table = { testTablex ->
        // multi-line sql
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${testTable} (
                            k1 TINYINT NOT NULL,
                            k2 SMALLINT NOT NULL,
                            k3 INT NOT NULL,
                            k4 BIGINT NOT NULL,
                            k5 DATETIME NOT NULL,
                            v1 DATE REPLACE NOT NULL,
                            v2 CHAR REPLACE NOT NULL,
                            v3 VARCHAR(4096) REPLACE NOT NULL,
                            v4 FLOAT SUM NOT NULL,
                            v5 DOUBLE SUM NOT NULL,
                            v6 DECIMAL(20,7) SUM NOT NULL
                            ) AGGREGATE KEY(k1,k2,k3,k4,k5)
                            DISTRIBUTED BY HASH(k1) BUCKETS 15
                            PROPERTIES("replication_num" = "1");
                        """

        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")
    }
    
    def load_json_data = {table_name, label, strip_flag, read_flag, format_flag, exprs, json_paths, 
                        json_root, where_expr, fuzzy_flag, file_name, ignore_failure=false,
                        expected_succ_rows = -1, load_to_single_tablet = 'true' ->
        
        // load the json data
        streamLoad {
            table "${table_name}"
            
            // set http request header params
            set 'label', label + "_" + UUID.randomUUID().toString()
            set 'strip_outer_array', strip_flag
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            set 'columns', exprs
            set 'jsonpaths', json_paths
            set 'json_root', json_root
            set 'where', where_expr
            set 'fuzzy_parse', fuzzy_flag
            set 'load_to_single_tablet', load_to_single_tablet
            file file_name // import json file
            time 10000 // limit inflight 10s
            if (expected_succ_rows >= 0) {
                set 'max_filter_ratio', '1'
            }

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
		if (ignore_failure && expected_succ_rows < 0) { return }
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                if (expected_succ_rows >= 0) {
                    assertEquals(json.NumberLoadedRows, expected_succ_rows)
                } else {
                    assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                    assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
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
                sql "sync"
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

        load_json_data.call("test_json_load", 'test_json_load_case1_2', 'true', '', 'json', '', '', '', '', '', 'simple_json.json')

        sql "sync"
        qt_select1 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case2: import json and apply exprs
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table1.call(testTable)

        load_json_data.call("test_json_load", 'test_json_load_case2_2', 'true', '', 'json', 'id= id * 10', '', '', '', '', 'simple_json.json')

        sql "sync" 
        qt_select2 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case3: import json and apply jsonpaths
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table2.call(testTable)
        
        load_json_data.call("test_json_load", 'test_json_load_case3_2', 'true', '', 'json', '', '[\"$.id\", \"$.code\"]',
                            '', '', '', 'simple_json.json')

        sql "sync"
        qt_select3 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
    
    // case4: import json and apply jsonpaths & exprs
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table2.call(testTable)
        
        load_json_data.call("test_json_load", 'test_json_load_case4_2', 'true', '', 'json', 'code = id * 10 + 200', '[\"$.id\"]',
                            '', '', '', 'simple_json.json')

        sql "sync"
        qt_select4 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case5: import json with line reader
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table2.call(testTable)
        
        load_json_data.call("test_json_load", 'test_json_load_case5_2', 'true', 'true', 'json', '', '[\"$.id\", \"$.code\"]',
                            '', '', '', 'multi_line_json.json')
        
        sql "sync"
        qt_select5 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case6: import json use exprs and jsonpaths
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table2.call(testTable)
        
        load_json_data.call("test_json_load", 'test_json_load_case6_2', 'true', 'true', 'json', 'id= id * 10', '[\"$.id\", \"$.code\"]',
                            '', '', '', 'multi_line_json.json')

        sql "sync"
        qt_select6 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case7: import json use where
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table2.call(testTable)
        
        load_json_data.call("test_json_load", 'test_json_load_case7_2', 'true', 'true', 'json', 'id= id * 10', '[\"$.id\", \"$.code\"]',
                            '', 'id > 50', '', 'multi_line_json.json')

        sql "sync"
        qt_select7 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case8: import json use fuzzy_parse
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table2.call(testTable)
        
        load_json_data.call("test_json_load", 'test_json_load_case8_2', 'true', 'true', 'json', 'id= id * 10', '[\"$.id\", \"$.code\"]',
                            '', 'id > 50', 'true', 'multi_line_json.json')

        sql "sync"
        qt_select8 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case9: import json use json_root
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table1.call(testTable)
        
        load_json_data.call("test_json_load", 'test_json_load_case9_2', '', 'true', 'json', 'id= id * 10', '',
                            '$.item', '', 'true', 'nest_json.json')

        sql "sync"
        qt_select9 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case10: invalid json
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table1.call(testTable)
        
        load_json_data.call("test_json_load", 'test_json_load_case10_2', '', 'true', 'json', 'id= id * 10', '',
                            '$.item', '', 'false', 'invalid_json.json', false, 4)

        sql "sync"
        qt_select10 "select * from ${testTable} order by id"

    } finally {
        // try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case11: test json file which is unordered and no use json_path
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table1.call(testTable)

        load_json_data.call("test_json_load", 'test_json_load_case11_2', 'true', '', 'json', '', '', '', '', '', 'simple_json2.json', false, 10)

        sql "sync"
        qt_select11 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case12: test json file which is unordered and lack one column which is nullable
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table1.call(testTable)

        load_json_data.call("test_json_load", 'test_json_load_case12_2', 'true', '', 'json', '', '', '', '', '', 'simple_json2_lack_one_column.json')

        sql "sync"
        qt_select12 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case13: test json file which is unordered and lack one column which is not nullable
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        create_test_table3.call(testTable)
        // load the json data
        streamLoad {
            table "${testTable}"
            
            // set http request header params
            set 'strip_outer_array', "true"
            set 'format', "json"
            set 'max_filter_ratio', '1'
            file "simple_json2_lack_one_column.json" // import json file
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
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows + json.NumberFilteredRows)
                assertEquals(json.NumberFilteredRows, 4)
                assertEquals(json.NumberLoadedRows, 6)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
        sql "sync"
        qt_select13 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case14: use json_path and json_root
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table1.call(testTable)
        
        load_json_data.call("test_json_load", 'test_json_load_case14_2', '', 'true', 'json', 'id= id * 10', '[\"$.id\", \"$.code\"]',
                            '$.item', '', 'true', 'nest_json.json')

        // invalid nest_json
        load_json_data.call("test_json_load", 'test_json_load_case14_3', '', 'true', 'json', 'id= id * 10', '[\"$.id\",  \"$.city\", \"$.code\"]',
                            '$.item', '', 'true', 'invalid_nest_json1.json', true) 
        load_json_data.call("test_json_load", 'test_json_load_case14_4', '', 'true', 'json', 'id= id * 10', '[\"$.id\",  \"$.city\", \"$.code\"]',
                            '$.item', '', 'true', 'invalid_nest_json2.json', false, 7) 
        load_json_data.call("test_json_load", 'test_json_load_case14_5', '', 'true', 'json', 'id= id * 10', '[\"$.id\",  \"$.city\", \"$.code\"]',
                            '$.item', '', 'true', 'invalid_nest_json3.json', true) 

        sql "sync"
        qt_select14 "select * from ${testTable} order by id, code, city"

    } finally {
        // try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case15: apply jsonpaths & exprs & json_root
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table1.call(testTable)
        
        load_json_data.call("test_json_load", 'test_json_load_case15_2', '', 'true', 'json', 'id, code, city,id= id * 10',
                            '[\"$.id\", \"$.code\", \"$.city\"]', '$.item', '', 'true', 'nest_json.json')

        sql "sync"
        qt_select15 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case16: apply jsonpaths & exprs & json_root
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table1.call(testTable)
        
        load_json_data.call("test_json_load", 'test_json_load_case16_2', 'true', '', 'json', 'id, code, city',
                            '[\"$.id\", \"$.code\", \"$.city[2]\"]', '$.item', '', 'true', 'nest_json_array.json', false, 7)

        sql "sync"
        qt_select16 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case17: invalid json
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        test_invalid_json_array_table.call(testTable)
        load_json_data.call("test_json_load", 'test_json_load_case17', 'true', '', 'json', '', '',
                '', '', '', 'invalid_json_array.json', false, 0, 'false')
        load_json_data.call("test_json_load", 'test_json_load_case17_1', 'true', '', 'json', '', '',
                '$.item', '', '', 'invalid_json_array1.json', false, 0, 'false')
        load_json_data.call("test_json_load", 'test_json_load_case17_2', 'true', '', 'json', '', '',
                '$.item', '', '', 'invalid_json_array2.json', false, 0, 'false')
        load_json_data.call("test_json_load", 'test_json_load_case17_3', 'true', '', 'json', '', '',
                '$.item', '', '', 'invalid_json_array3.json', false, 0, 'false')
        sql "sync"
        qt_select17 "select * from ${testTable}"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case18: invalid nest json
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table1.call(testTable)
        load_json_data.call("test_json_load", 'test_json_load_case16_2', 'true', '', 'json', 'id, code, city',
                            '[\"$.id\", \"$.code\", \"$.city[2]\"]', '$.item', '', 'true', 'invalid_nest_json_array.json', true) 
        load_json_data.call("test_json_load", 'test_json_load_case16_2', 'true', '', 'json', 'id, code, city',
                            '[\"$.id\", \"$.code\", \"$.city[100]\"]', '$.item', '', 'true', 'invalid_nest_json_array1.json', true) 
        load_json_data.call("test_json_load", 'test_json_load_case16_2', 'true', '', 'json', 'id, code, city',
                            '[\"$.id\", \"$.code\", \"$.city\"]', '$.item', '', 'true', 'invalid_nest_json_array2.json', true) 
        load_json_data.call("test_json_load", 'test_json_load_case16_2', 'true', '', 'json', 'id, code, city',
                            '[\"$.id\", \"$.code\", \"$.city[2]\"]', '$.item', '', 'true', 'invalid_nest_json_array3.json', true) 

        sql "sync"
        qt_select18 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case19: test case sensitive json load
     try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table1.call(testTable)
        load_json_data.call("test_json_load", 'test_json_load_case19', 'false', 'true', 'json', 'Id, cIty, CodE', '',
                '', '', '', 'case_sensitive_json.json', false, 2)
        sql "sync"
        qt_select19 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case20: import json with BOM file
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table1.call(testTable)

        load_json_data.call("test_json_load", 'test_json_load_case1_2', 'true', '', 'json', '', '', '', '', '', 'simple_json_bom.json')

        sql "sync"
        qt_select1 "select * from ${testTable} order by id"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    } 

    // case21: import json with jsonb field 
    try {
        testTable = "with_jsonb" 
        sql "DROP TABLE IF EXISTS ${testTable}"
        sql """
                CREATE TABLE ${testTable} (
                `name` varchar(29) NOT NULL COMMENT '年月',
                `age` int,
                `city` varchar(29),
                `contact` jsonb
                ) ENGINE=OLAP
                DUPLICATE KEY(`name`)
                COMMENT '流水月结明细表'
                DISTRIBUTED BY RANDOM BUCKETS auto
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
                ); 
            """
        load_json_data.call("with_jsonb", "with_jsonb_load" , 'false', '', 'json', '', '', '', '', '', 'with_jsonb.json')

        sql "sync"
        qt_select1 "select * from ${testTable}"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    } 

    // case22: nested and it's member with jsonpath
    try {
        testTable = "test_json_load"
        sql "DROP TABLE IF EXISTS ${testTable}"
        sql """CREATE TABLE IF NOT EXISTS ${testTable}
        (
         `productid` bigint NOT NULL COMMENT "productid",
         `deviceid` bigint NOT NULL COMMENT "deviceid",
         `datatimestamp` string  NULL COMMENT "datatimestamp",
         `dt` int   NULL COMMENT "dt",
         `data` string 
        )
        DUPLICATE KEY(`productid`, `deviceid`)
        DISTRIBUTED BY RANDOM BUCKETS auto
        properties(
            "replication_num" = "1"
        );
        """

        load_json_data.call("${testTable}", 'with_jsonpath', '', 'true', 'json', """productid, deviceid, data, datatimestamp, dt=from_unixtime(substr(datatimestamp,1,10),'%Y%m%d')""",
                '["$.productid","$.deviceid","$.data","$.data.datatimestamp"]', '', '', '', 'with_jsonpath.json')
        sql "sync"
        qt_select22 "select * from ${testTable}"

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

        // case18: import json use pre-filter exprs
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

        // case19: import json use pre-filter and where exprs
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
