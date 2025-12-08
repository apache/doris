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

suite("test_pythonudf_int") {
    def pyPath = """${context.file.parent}/udf_scripts/pyudf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python Zip path: ${pyPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS test_pythonudf_int """
        sql """
        CREATE TABLE IF NOT EXISTS test_pythonudf_int (
            `user_id`      INT      NOT NULL COMMENT "",
            `tinyint_col`  TINYINT  NOT NULL COMMENT "",
            `smallint_col` SMALLINT NOT NULL COMMENT "",
            `bigint_col`   BIGINT   NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i++) {
            sb.append("""
                (${i},${i}*2,${i}*3,${i}*4),
            """)
        }
        sb.append("""
                (${i},${i}*2,${i}*3,${i}*4)
            """)
        sql """ INSERT INTO test_pythonudf_int VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM test_pythonudf_int t ORDER BY user_id; """

        File path = new File(pyPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${pyPath} doesn't exist! """)
        }

        sql """ DROP FUNCTION IF EXISTS python_udf_int_test(int) """

        sql """ CREATE FUNCTION python_udf_int_test(int) RETURNS int PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="int_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """

        qt_select """ SELECT python_udf_int_test(user_id) result FROM test_pythonudf_int ORDER BY result; """
        qt_select """ SELECT python_udf_int_test(null) result ; """


        sql """ CREATE FUNCTION python_udf_tinyint_test(tinyint) RETURNS tinyint PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="int_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """

        qt_select """ SELECT python_udf_tinyint_test(tinyint_col) result FROM test_pythonudf_int ORDER BY result; """
        qt_select """ SELECT python_udf_tinyint_test(null) result ; """


        sql """ CREATE FUNCTION python_udf_smallint_test(smallint) RETURNS smallint PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="int_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """

        qt_select """ SELECT python_udf_smallint_test(smallint_col) result FROM test_pythonudf_int ORDER BY result; """
        qt_select """ SELECT python_udf_smallint_test(null) result ; """


        sql """ CREATE FUNCTION python_udf_bigint_test(bigint) RETURNS bigint PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="int_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """

        qt_select """ SELECT python_udf_bigint_test(bigint_col) result FROM test_pythonudf_int ORDER BY result; """
        qt_select """ SELECT python_udf_bigint_test(null) result ; """

        sql """ CREATE GLOBAL FUNCTION python_udf_int_test_global(int) RETURNS int PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="int_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """

        qt_select_global_1 """ SELECT python_udf_int_test_global(user_id) result FROM test_pythonudf_int ORDER BY result; """
        qt_select_global_2 """ SELECT python_udf_int_test_global(null) result ; """
        qt_select_global_3 """ SELECT python_udf_int_test_global(3) result FROM test_pythonudf_int ORDER BY result; """
        qt_select_global_4 """ SELECT abs(python_udf_int_test_global(3)) result FROM test_pythonudf_int ORDER BY result; """

    } finally {
        try_sql("DROP GLOBAL FUNCTION IF EXISTS python_udf_int_test_global(int);")
        try_sql("DROP FUNCTION IF EXISTS python_udf_tinyint_test(tinyint);")
        try_sql("DROP FUNCTION IF EXISTS python_udf_smallint_test(smallint);")
        try_sql("DROP FUNCTION IF EXISTS python_udf_bigint_test(bigint);")
        try_sql("DROP FUNCTION IF EXISTS python_udf_int_test(int);")
        try_sql("DROP TABLE IF EXISTS test_pythonudf_int")
    }
}
