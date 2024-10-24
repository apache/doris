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

import org.codehaus.groovy.runtime.IOGroovyMethods

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_javaudf_string") {
    def tableName = "test_javaudf_string"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ DROP TABLE IF EXISTS test_javaudf_string_2 """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
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
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        sql """ create table test_javaudf_string_2 like test_javaudf_string """
        sql """ insert into test_javaudf_string_2 select * from test_javaudf_string; """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """
        qt_select_default_2 """ SELECT * FROM test_javaudf_string_2 t ORDER BY user_id; """

        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION java_udf_string_test(string, int, int) RETURNS string PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.StringTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_string_test(varchar_col, 2, 3) result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_string_test(string_col, 2, 3)  result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_string_test('abcdef', 2, 3), java_udf_string_test('abcdefg', 2, 3) result FROM ${tableName} ORDER BY result; """

        qt_select_4 """ 
            SELECT
                COALESCE(
                    java_udf_string_test(test_javaudf_string.varchar_col, 2, 3),
                    'not1'
                ),
                COALESCE(
                    java_udf_string_test(test_javaudf_string.varchar_col, 2, 3),
                    'not2'
                )
            FROM
                test_javaudf_string
                JOIN test_javaudf_string_2 ON test_javaudf_string.user_id = test_javaudf_string_2.user_id order by 1,2;
        """
        test {
            sql """ CREATE FUNCTION java_udf_string_test(varchar, int, int) RETURNS string PROPERTIES (
                "file"="file://${jarPath}",
                "symbol"="org.apache.doris.udf.StringTest",
                "type"="JAVA_UDF"
            ); """
            exception "does not support type"
        }
        test {
            sql """ CREATE FUNCTION java_udf_string_test(string, int, int) RETURNS varchar PROPERTIES (
                "file"="file://${jarPath}",
                "symbol"="org.apache.doris.udf.StringTest",
                "type"="JAVA_UDF"
            ); """
            exception "does not support type"
        }
        sql """DROP TABLE IF EXISTS tbl1"""
        sql """create table tbl1(k1 int, k2 string) distributed by hash(k1) buckets 1 properties("replication_num" = "1");"""
        sql """ insert into tbl1 values(1, "5");"""
        Integer count = 0;
        Integer maxCount = 20;
        while (count < maxCount) {
            sql """  insert into tbl1 select * from tbl1;"""
            count++
            sleep(100);
        }
        sql """  insert into tbl1 select random()%10000 * 10000, "5" from tbl1;"""
        qt_select_5 """ select count(0) from (select k1, max(k2) as k2 from tbl1 group by k1)v where java_udf_string_test(k2, 0, 1) = "asd" """;
    } finally {
        try_sql("DROP FUNCTION IF EXISTS java_udf_string_test(string, int, int);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        try_sql("DROP TABLE IF EXISTS tbl1")
        try_sql("DROP TABLE IF EXISTS test_javaudf_string_2")
    }
}
