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

suite("test_pythonudf_string") {
    def tableName = "test_pythonudf_string"
    def pyPath = """${context.file.parent}/udf_scripts/pyudf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python Zip path: ${pyPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS test_pythonudf_string """
        sql """ DROP TABLE IF EXISTS test_pythonudf_string_2 """
        sql """
        CREATE TABLE IF NOT EXISTS test_pythonudf_string (
            `user_id`     INT         NOT NULL COMMENT "用户id",
            `char_col`    CHAR        NOT NULL COMMENT "",
            `varchar_col` VARCHAR(10) NOT NULL COMMENT "",
            `string_col`  STRING      NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 9; i ++) {
            sb.append("""
                (${i}, '${i}','abcdefg${i}','poiuytre${i}abcdefg'),
            """)
        }
        sb.append("""
                (${i}, '${i}','abcdefg${i}','poiuytre${i}abcdefg')
            """)
        sql """ INSERT INTO test_pythonudf_string VALUES
             ${sb.toString()}
            """
        sql """ create table test_pythonudf_string_2 like test_pythonudf_string """
        sql """ insert into test_pythonudf_string_2 select * from test_pythonudf_string; """
        qt_select_default """ SELECT * FROM test_pythonudf_string t ORDER BY user_id; """
        qt_select_default_2 """ SELECT * FROM test_pythonudf_string_2 t ORDER BY user_id; """

        File path = new File(pyPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${pyPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION python_udf_string_test(string, int, int) RETURNS string PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="string_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """

        qt_select """ SELECT python_udf_string_test(varchar_col, 2, 3) result FROM test_pythonudf_string ORDER BY result; """
        qt_select """ SELECT python_udf_string_test(string_col, 2, 3)  result FROM test_pythonudf_string ORDER BY result; """
        qt_select """ SELECT python_udf_string_test('abcdef', 2, 3), python_udf_string_test('abcdefg', 2, 3) result FROM test_pythonudf_string ORDER BY result; """

        qt_select_4 """ 
            SELECT
                COALESCE(
                    python_udf_string_test(test_pythonudf_string.varchar_col, 2, 3),
                    'not1'
                ),
                COALESCE(
                    python_udf_string_test(test_pythonudf_string.varchar_col, 2, 3),
                    'not2'
                )
            FROM
                test_pythonudf_string
                JOIN test_pythonudf_string_2 ON test_pythonudf_string.user_id = test_pythonudf_string_2.user_id order by 1,2;
        """
    } finally {
        try_sql("DROP FUNCTION IF EXISTS python_udf_string_test(string, int, int);")
        try_sql("DROP TABLE IF EXISTS test_pythonudf_string")
        try_sql("DROP TABLE IF EXISTS test_pythonudf_string_2")
    }
}
