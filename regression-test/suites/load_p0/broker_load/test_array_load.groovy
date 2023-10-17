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

suite("test_array_load", "load_p0") {
    // define a sql table
    def testTable = "tbl_test_array_load"
    def testTable01 = "tbl_test_array_load01"
    
    def create_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL COMMENT "",
              `k2` ARRAY<SMALLINT> NOT NULL COMMENT "",
              `k3` ARRAY<INT(11)> NOT NULL COMMENT "",
              `k4` ARRAY<BIGINT> NOT NULL COMMENT "",
              `k5` ARRAY<CHAR> NOT NULL COMMENT "",
              `k6` ARRAY<VARCHAR(20)> NULL COMMENT "",
              `k7` ARRAY<DATE> NOT NULL COMMENT "", 
              `k8` ARRAY<DATETIME> NOT NULL COMMENT "",
              `k9` ARRAY<FLOAT> NOT NULL COMMENT "",
              `k10` ARRAY<DOUBLE> NOT NULL COMMENT "",
              `k11` ARRAY<DECIMAL(20, 6)> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
            """
        
        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")
        
        // insert 1 row to check whether the table is ok
        def result2 = sql """ INSERT INTO ${testTable} VALUES
                        (100, [1, 2, 3], [32767, 32768, 32769], [65534, 65535, 65536], ['a', 'b', 'c'], ["hello", "world"], 
                        ['2022-07-13'], ['2022-07-13 12:30:00'], [0.33, 0.67], [3.1415926, 0.878787878], [4, 5.5, 6.67])
                        """
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    def create_test_table01 = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable01} (
              `k1` INT(11) NULL COMMENT "",
              `k2` ARRAY<SMALLINT> NULL COMMENT "",
              `k3` ARRAY<INT(11)> NULL COMMENT "",
              `k4` ARRAY<BIGINT> NULL COMMENT "",
              `k5` ARRAY<CHAR> NULL COMMENT "",
              `k6` ARRAY<VARCHAR(20)> NULL COMMENT "",
              `k7` ARRAY<DATE> NULL COMMENT "", 
              `k8` ARRAY<DATETIME> NULL COMMENT "",
              `k9` ARRAY<FLOAT> NULL COMMENT "",
              `k10` ARRAY<DOUBLE> NULL COMMENT "",
              `k11` ARRAY<DECIMAL(20, 6)> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
            """
        
        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")
        
        // insert 1 row to check whether the table is ok
        def result2 = sql """ INSERT INTO ${testTable01} VALUES
                        (100, [1, 2, 3], [32767, 32768, 32769], [65534, 65535, 65536], ['a', 'b', 'c'], ["hello", "world"], 
                        ['2022-07-13'], ['2022-07-13 12:30:00'], [0.33, 0.67], [3.1415926, 0.878787878], [4, 5.5, 6.67])
                        """
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    def load_array_data = {table_name, strip_flag, read_flag, format_flag, exprs, json_paths, 
                            json_root, where_expr, fuzzy_flag, column_sep, file_name ->
        // load the json data
        streamLoad {
            table table_name
            
            // set http request header params
            set 'strip_outer_array', strip_flag
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            set 'columns', exprs
            set 'jsonpaths', json_paths
            set 'json_root', json_root
            set 'where', where_expr
            set 'fuzzy_parse', fuzzy_flag
            set 'column_separator', column_sep
            set 'max_filter_ratio', '0.6'
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
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows + json.NumberFilteredRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }
    
    def load_from_hdfs = {testTablex, label, hdfsFilePath, format, brokerName, hdfsUser, hdfsPasswd ->
        def result1= sql """
                        LOAD LABEL ${label} (
                            DATA INFILE("${hdfsFilePath}")
                            INTO TABLE ${testTablex}
                            FORMAT as "${format}")
                        with BROKER "${brokerName}" (
                        "username"="${hdfsUser}",
                        "password"="${hdfsPasswd}")
                        PROPERTIES  (
                        "timeout"="1200",
                        "max_filter_ratio"="0.1");
                        """
        
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")
    }

    def load_from_hdfs1 = {testTablex, label, hdfsFilePath, format, brokerName, hdfsUser, hdfsPasswd ->
        def result1= sql """
                        LOAD LABEL ${label} (
                            DATA INFILE("${hdfsFilePath}")
                            INTO TABLE ${testTablex}
                            COLUMNS TERMINATED BY "/"
                            FORMAT as "${format}")
                        with BROKER "${brokerName}" (
                        "username"="${hdfsUser}",
                        "password"="${hdfsPasswd}")
                        PROPERTIES  (
                        "timeout"="1200",
                        "max_filter_ratio"="0.1");
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
                qt_select "select * from ${testTablex} order by k1"
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

    def check_data_correct = {table_name ->
        sql "sync"
        // select the table and check whether the data is correct
        qt_select "select * from ${table_name} order by k1"
        qt_select_count "select count(3), count(k6) from ${table_name}"
    }

    try {
        for ( i in 0..1 ) {
            // case1: import array data in json format and enable vectorized engine
            try {
                sql "DROP TABLE IF EXISTS ${testTable}"
                
                create_test_table.call(testTable)

                load_array_data.call(testTable, 'true', '', 'json', '', '', '', '', '', '', 'simple_array.json')
                
                check_data_correct(testTable)

            } finally {
                try_sql("DROP TABLE IF EXISTS ${testTable}")
            }

            // case3: import array data in csv format and enable vectorized engine
            try {
                sql "DROP TABLE IF EXISTS ${testTable}"
                
                create_test_table.call(testTable)

                load_array_data.call(testTable, 'true', '', 'csv', '', '', '', '', '', '/', 'simple_array.csv')
                
                check_data_correct(testTable)

            } finally {
                try_sql("DROP TABLE IF EXISTS ${testTable}")
            }

            // case5: import array data not specify the format
            try {
                sql "DROP TABLE IF EXISTS ${testTable01}"
                
                create_test_table01.call(testTable01)

                load_array_data.call(testTable01, '', '', '', '', '', '', '', '', '/', 'simple_array.data')
                
                check_data_correct(testTable01)

            } finally {
                // try_sql("DROP TABLE IF EXISTS ${testTable01}")
            }
        }
    } finally {
    }


    // if 'enableHdfs' in regression-conf.groovy has been set to true,
    // the test will run these case as below.
    if (enableHdfs()) {
        brokerName =getBrokerName()
        hdfsUser = getHdfsUser()
        hdfsPasswd = getHdfsPasswd()
        def hdfs_json_file_path = uploadToHdfs "load_p0/broker_load/simple_object_array.json"
        def hdfs_csv_file_path = uploadToHdfs "load_p0/broker_load/simple_array.csv"
        def hdfs_orc_file_path = uploadToHdfs "load_p0/broker_load/simple_array.orc"
        // orc file with native array(list) type
        def hdfs_orc_file_path2 = uploadToHdfs "load_p0/broker_load/simple_array_list_type.orc"
        def hdfs_parquet_file_path = uploadToHdfs "load_p0/broker_load/simple_array.parquet"
 
        // case5: import array data by hdfs and enable vectorized engine
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            
            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs.call(testTable, test_load_label, hdfs_json_file_path, "json",
                                brokerName, hdfsUser, hdfsPasswd)
            
            check_load_result.call(test_load_label, testTable)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
        // test unified load
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            sql """ set enable_unified_load=true; """

            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs.call(testTable, test_load_label, hdfs_json_file_path, "json",
                    brokerName, hdfsUser, hdfsPasswd)

            check_load_result.call(test_load_label, testTable)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
            sql """ set enable_unified_load=false; """
        }

        // case7: import array data by hdfs in csv format and enable vectorized
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"

            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs1.call(testTable, test_load_label, hdfs_csv_file_path, "csv",
                                brokerName, hdfsUser, hdfsPasswd)
                        
            check_load_result.call(test_load_label, testTable)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
        // test unified load
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            sql """ set enable_unified_load=true; """

            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs1.call(testTable, test_load_label, hdfs_csv_file_path, "csv",
                    brokerName, hdfsUser, hdfsPasswd)

            check_load_result.call(test_load_label, testTable)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
            sql """ set enable_unified_load=false; """
        }

        // case9: import array data by hdfs in orc format and enable vectorized
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"

            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs1.call(testTable, test_load_label, hdfs_orc_file_path, "orc",
                                brokerName, hdfsUser, hdfsPasswd)
            
            check_load_result.call(test_load_label, testTable)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
        // test unified load
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            sql """ set enable_unified_load=true; """

            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs1.call(testTable, test_load_label, hdfs_orc_file_path, "orc",
                    brokerName, hdfsUser, hdfsPasswd)

            check_load_result.call(test_load_label, testTable)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
            sql """ set enable_unified_load=false; """
        }

        // case11: import array data by hdfs in parquet format and enable vectorized
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"

            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs1.call(testTable, test_load_label, hdfs_parquet_file_path, "parquet",
                                brokerName, hdfsUser, hdfsPasswd)
            
            check_load_result.call(test_load_label, testTable)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
        // test unified load
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            sql """ set enable_unified_load=true; """

            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs1.call(testTable, test_load_label, hdfs_parquet_file_path, "parquet",
                    brokerName, hdfsUser, hdfsPasswd)

            check_load_result.call(test_load_label, testTable)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
            sql """ set enable_unified_load=false; """
        }

        // case13: import array data by hdfs in orc format(with array type) and enable vectorized
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"

            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs1.call(testTable, test_load_label, hdfs_orc_file_path2, "orc",
                                brokerName, hdfsUser, hdfsPasswd)
            
            check_load_result.call(test_load_label, testTable)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
        // test unified load
        try {
            sql "DROP TABLE IF EXISTS ${testTable}"
            sql """ set enable_unified_load=true; """

            create_test_table.call(testTable)

            def test_load_label = UUID.randomUUID().toString().replaceAll("-", "")
            load_from_hdfs1.call(testTable, test_load_label, hdfs_orc_file_path2, "orc",
                    brokerName, hdfsUser, hdfsPasswd)

            check_load_result.call(test_load_label, testTable)

        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
            sql """ set enable_unified_load=false; """
        }
    }
}
