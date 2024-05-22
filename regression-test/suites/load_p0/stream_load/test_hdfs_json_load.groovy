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

suite("test_hdfs_json_load", "p0,external,external_docker,external_docker_hive,hive") {
    // define a sql table
    def testTable = "test_hdfs_json_load"
    
    def create_test_table1 = {testTablex ->
        // multi-line sql
        def result1 = sql """
                        CREATE TABLE IF NOT EXISTS ${testTablex} (
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
        def result2 = sql "INSERT INTO ${testTablex} (id, city, code) VALUES (200, 'changsha', 3456789)"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    def load_from_hdfs1 = {new_json_reader_flag, strip_flag, fuzzy_flag, testTablex, label, fileName,
                            fsPath, hdfsUser, exprs, jsonpaths, json_root, columns_parameter, where ->
        def hdfsFilePath = "${fsPath}/user/doris/preinstalled_data/json_format_test/${fileName}"
        def result1= sql """
                        LOAD LABEL ${label} (
                            DATA INFILE("${hdfsFilePath}")
                            INTO TABLE ${testTablex} 
                            FORMAT as "json"
                            ${columns_parameter}
                            ${exprs}
                            ${where}
                            properties(
                                "json_root" = "${json_root}",
                                "jsonpaths" = "${jsonpaths}",
                                "strip_outer_array" = "${strip_flag}",
                                "fuzzy_parse" = "${fuzzy_flag}"
                                )
                            )
                        with HDFS (
                            "fs.defaultFS"="${fsPath}",
                            "hadoop.username" = "${hdfsUser}"
                            )
                        PROPERTIES (
                            "timeout"="1200",
                            "max_filter_ratio"="0"
                            );
                        """

        println "${result1}"
        
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")
    }

    def load_from_hdfs2 = {new_json_reader_flag, strip_flag, fuzzy_flag, testTablex, label, fileName,
                           fsPath, hdfsUser, exprs, jsonpaths, json_root, columns_parameter, where ->
        def hdfsFilePath = "${fsPath}/user/doris/preinstalled_data/json_format_test/${fileName}"
        def result1= sql """
                        LOAD LABEL ${label} (
                            DATA INFILE("${hdfsFilePath}")
                            INTO TABLE ${testTablex} 
                            FORMAT as "json"
                            ${columns_parameter}
                            ${exprs}
                            ${where}
                            properties(
                                "json_root" = "${json_root}",
                                "jsonpaths" = "${jsonpaths}",
                                "strip_outer_array" = "${strip_flag}",
                                "fuzzy_parse" = "${fuzzy_flag}"
                                )
                            )
                        with HDFS (
                            "hadoop.username" = "${hdfsUser}"
                            )
                        PROPERTIES (
                            "timeout"="1200"
                            );
                        """

        println "${result1}"

        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Query OK, 0 rows affected")
    }

    def check_load_result = {checklabel, testTablex ->
        def max_try_milli_secs = 30000
        while(max_try_milli_secs) {
            def result = sql "show load where label = '${checklabel}'"
            if(result[0][2] == "FINISHED") {
                log.info("LOAD FINISHED: ${checklabel}")
                break
            } else {
                sleep(1000) // wait 1 second every time
                max_try_milli_secs -= 1000
                if(max_try_milli_secs <= 0) {
                    log.info("Broker load result: ${result}".toString())
                    assertEquals(1 == 2, "load timeout: ${checklabel}")
                }
            }
        }
    }
    


    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def fsPath = "hdfs://${externalEnvIp}:${hdfs_port}"
    // It's okay to use random `hdfsUser`, but can not be empty.
    def hdfsUser = "doris"


    // case1: import simple json
    def q1 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "false", "false", testTable, test_load_label1, "simple_object_json.json",
                                fsPath, hdfsUser, '', '', '', '', '')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select1 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "false", "false", testTable, test_load_label2, "simple_object_json.json",
                                fsPath, hdfsUser, '', '', '', '', '')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select1 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    // case2: import json and apply exprs
    def q2 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "false", "false", testTable, test_load_label1, "simple_object_json.json",
                                fsPath, hdfsUser, "SET(id= id * 10)", '', '', '', '')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select2 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "false", "false", testTable, test_load_label2, "simple_object_json.json",
                                fsPath, hdfsUser, "SET(id= id * 10)", '', '', '', '')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select2 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    // case3: import json and apply jsonpaths
    def q3 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "false", "false", testTable, test_load_label1, "simple_object_json.json",
                                fsPath, hdfsUser, '', """[\\"\$.id\\", \\"\$.code\\"]""", '', '', '')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select3 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "false", "false", testTable, test_load_label2, "simple_object_json.json",
                                fsPath, hdfsUser, '', """[\\"\$.id\\", \\"\$.code\\"]""", '', '', '')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select3 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    // case4: import json and apply jsonpaths & exprs
    def q4 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "false", "false", testTable, test_load_label1, "simple_object_json.json",
                                fsPath, hdfsUser, "SET(code = id * 10 + 200)", """[\\"\$.id\\"]""", '', '', '')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select4 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "false", "false", testTable, test_load_label2, "simple_object_json.json",
                                fsPath, hdfsUser, "SET(code = id * 10 + 200)", """[\\"\$.id\\"]""", '', '', '')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select4 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    // case5: import json with line reader
    def q5 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "true", "false", testTable, test_load_label1, "multi_line_json.json",
                                fsPath, hdfsUser, '', '', '', '', '')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select5 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "true", "false", testTable, test_load_label2, "multi_line_json.json",
                                fsPath, hdfsUser, '', '', '', '', '')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select5 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    // case6: import json use exprs and jsonpaths
    def q6 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "true", "false", testTable, test_load_label1, "multi_line_json.json",
                                fsPath, hdfsUser, "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.code\\"]""", '', '', '')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select6 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "true", "false", testTable, test_load_label2, "multi_line_json.json",
                                fsPath, hdfsUser, "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.code\\"]""", '', '', '')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select6 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    // case7: import json use where
    def q7 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "true", "false", testTable, test_load_label1, "multi_line_json.json",
                                fsPath, hdfsUser, "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.code\\"]""", '', '', 'WHERE id>50')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select7 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "true", "false", testTable, test_load_label2, "multi_line_json.json",
                                fsPath, hdfsUser, "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.code\\"]""", '', '', 'WHERE id>50')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select7 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }


    // case8: import json use fuzzy_parse
    def q8 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "true", "true", testTable, test_load_label1, "multi_line_json.json",
                                fsPath, hdfsUser, "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.city\\"]""", '', '', 'WHERE id>50')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select8 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "true", "true", testTable, test_load_label2, "multi_line_json.json",
                                fsPath, hdfsUser, "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.city\\"]""", '', '', 'WHERE id>50')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select8 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    // case9: import json use json_root
    def q9 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "false", "true", testTable, test_load_label1, "nest_json.json",
                                fsPath, hdfsUser, "SET(id = id * 10)", '', '$.item', '', '')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select9 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "false", "true", testTable, test_load_label2, "nest_json.json",
                                fsPath, hdfsUser, "SET(id = id * 10)", '', '$.item', '', '')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select9 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    // case10: test json file which is unordered and no use json_path
    def q10 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "true", "false", testTable, test_load_label1, "multi_line_json_unorder.json",
                                fsPath, hdfsUser, '', '', '', '', '')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select10 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "true", "false", testTable, test_load_label2, "multi_line_json_unorder.json",
                                fsPath, hdfsUser, '', '', '', '', '')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select10 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    // case11: test json file which is unordered and lack one column which is nullable
    def q11 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "true", "false", testTable, test_load_label1, "multi_line_json_lack_column.json",
                                fsPath, hdfsUser, '', '', '', '', '')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select11 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "true", "false", testTable, test_load_label2, "multi_line_json_lack_column.json",
                                fsPath, hdfsUser, '', '', '', '', '')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select11 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }


    // case12: use json_path and json_root
    def q12 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "false", "false", testTable, test_load_label1, "nest_json.json", fsPath, hdfsUser,
                                "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.city\\"]""", '$.item', '', '')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select12 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "false", "false", testTable, test_load_label2, "nest_json.json", fsPath, hdfsUser,
                                "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.city\\"]""", '$.item', '', '')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select12 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    // case13: use json_path & json_root & where
    def q13 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "false", "false", testTable, test_load_label1, "nest_json.json", fsPath, hdfsUser,
                                "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.city\\"]""", '$.item', '', 'WHERE id>20')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select13 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "false", "false", testTable, test_load_label2, "nest_json.json", fsPath, hdfsUser,
                                "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.city\\"]""", '$.item', '', 'WHERE id>20')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select13 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    // case14: use jsonpaths & json_root & where & columns
    def q14 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
        
            create_test_table1.call(testTable)
            load_from_hdfs1.call("false", "false", "false", testTable, test_load_label1, "nest_json.json", fsPath, hdfsUser,
                                "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.code\\",\\"\$.city\\"]""", '$.item',
                                '(id, code, city)', 'WHERE id>20')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select14 "select * from ${testTable} order by id"

            // test new json reader
            def test_load_label2 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs1.call("true", "false", "false", testTable, test_load_label2, "nest_json.json", fsPath, hdfsUser,
                                "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.code\\", \\"\$.city\\"]""", '$.item',
                                '(id, code, city)', 'WHERE id>20')

            check_load_result(test_load_label2, testTable)
            sql "sync"
            qt_select14 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    // case15: verify no default FS properties
    def q15 = {
        try {
            def test_load_label1 = UUID.randomUUID().toString().replaceAll("-", "")
            sql "DROP TABLE IF EXISTS ${testTable}"
            create_test_table1.call(testTable)
            load_from_hdfs2.call("false", "false", "false", testTable, test_load_label1, "nest_json.json", fsPath, hdfsUser,
                    "SET(id = id * 10)", """[\\"\$.id\\", \\"\$.code\\",\\"\$.city\\"]""", '$.item',
                    '(id, code, city)', 'WHERE id>20')

            check_load_result(test_load_label1, testTable)
            sql "sync"
            qt_select15 "select * from ${testTable} order by id"
        } finally {
            try_sql("DROP TABLE IF EXISTS ${testTable}")
        }
    }

    
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        log.info("Begin Test q1:")
        q1()
        log.info("Begin Test q2:")
        q2()
        log.info("Begin Test q3:")
        q3()
        log.info("Begin Test q4:")
        q4()
        log.info("Begin Test q5:")
        q5()
        log.info("Begin Test q6:")
        q6()
        log.info("Begin Test q7:")
        q7()
        log.info("Begin Test q8:")
        q8()
        log.info("Begin Test q9:")
        q9()
        log.info("Begin Test q10:")
        q10()
        log.info("Begin Test q11:")
        q11()
        log.info("Begin Test q12:")
        q12()
        log.info("Begin Test q13:")
        q13()
        log.info("Begin Test q14:")
        q14()
        log.info("Begin Test q15:")
        q15()
    }
}
