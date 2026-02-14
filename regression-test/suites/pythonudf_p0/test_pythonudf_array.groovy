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

suite("test_pythonudf_array") {
    def pyPath = """${context.file.parent}/udf_scripts/pyudf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python Zip path: ${pyPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS test_pythonudf_array """
        sql """
        CREATE TABLE IF NOT EXISTS test_pythonudf_array (
            `user_id`      INT      NOT NULL COMMENT "",
            `tinyint_col`  TINYINT  NOT NULL COMMENT "",
            `string_col`   STRING   NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i ++) {
            sb.append("""
                (${i},${i}*2,'a${i}b'),
            """)
        }
        sb.append("""
                (${i},${i}*2,'a${i}b')
            """)
        sql """ INSERT INTO test_pythonudf_array VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM test_pythonudf_array t ORDER BY user_id; """

        File path = new File(pyPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${pyPath} doesn't exist! """)
        }

        sql """ DROP FUNCTION IF EXISTS python_udf_array_int_test(array<int>); """
        sql """ CREATE FUNCTION python_udf_array_int_test(array<int>) RETURNS int PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="array_int_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """
        qt_select_1 """ SELECT python_udf_array_int_test(array(user_id)) result FROM test_pythonudf_array ORDER BY result; """
        qt_select_2 """ SELECT python_udf_array_int_test(null) result ; """


        sql """ DROP FUNCTION IF EXISTS python_udf_array_return_int_test(array<int>); """
        sql """ CREATE FUNCTION python_udf_array_return_int_test(array<int>) RETURNS array<int> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="array_return_array_int_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """
        qt_select_3 """ SELECT python_udf_array_return_int_test(array(user_id)), tinyint_col as result FROM test_pythonudf_array ORDER BY result; """
        qt_select_4 """ SELECT python_udf_array_return_int_test(array(user_id,user_id)), tinyint_col as result FROM test_pythonudf_array ORDER BY result; """
        qt_select_5 """ SELECT python_udf_array_return_int_test(null) result ; """


        sql """ DROP FUNCTION IF EXISTS python_udf_array_return_string_test(array<string>); """
        sql """ CREATE FUNCTION python_udf_array_return_string_test(array<string>) RETURNS array<string> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="array_return_array_string_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """
        qt_select_6 """ SELECT python_udf_array_return_string_test(array(string_col)), tinyint_col as result FROM test_pythonudf_array ORDER BY result; """
        qt_select_7 """ SELECT python_udf_array_return_string_test(array(string_col, cast(user_id as string))), tinyint_col as result FROM test_pythonudf_array ORDER BY result; """
        qt_select_8 """ SELECT python_udf_array_return_string_test(null) result ; """

        sql """ DROP FUNCTION IF EXISTS python_udf_array_string_test(array<string>); """
        sql """ CREATE FUNCTION python_udf_array_string_test(array<string>) RETURNS string PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="array_string_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """
        qt_select_9 """ SELECT python_udf_array_string_test(array(string_col)), tinyint_col as result FROM test_pythonudf_array ORDER BY result; """
        qt_select_10 """ SELECT python_udf_array_string_test(array(string_col, cast(user_id as string))), tinyint_col as result FROM test_pythonudf_array ORDER BY result; """
        qt_select_11 """ SELECT python_udf_array_string_test(null) result ; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS python_udf_array_int_test(array<int>);")
        try_sql("DROP FUNCTION IF EXISTS python_udf_array_return_int_test(array<int>);")
        try_sql("DROP FUNCTION IF EXISTS python_udf_array_return_string_test(array<string>);")
        try_sql("DROP FUNCTION IF EXISTS python_udf_array_string_test(array<string>);")
        try_sql("DROP TABLE IF EXISTS test_pythonudf_array")
    }
}
