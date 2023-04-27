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

suite("test_javaudf_array") {
    def tableName = "test_javaudf_array"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""

    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id`      INT      NOT NULL COMMENT "",
            `tinyint_col`  TINYINT  NOT NULL COMMENT "",
            `datev2_col`   datev2 NOT NULL COMMENT "",
            `datetimev2_col` datetimev2 NOT NULL COMMENT "", 
            `string_col`   STRING   NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i ++) {
            sb.append("""
                (${i},${i}*2,'2022-01-01','2022-01-01 11:11:11','a${i}b'),
            """)
        }
        sb.append("""
                (${i},${i}*2,'2022-06-06','2022-01-01 12:12:12','a${i}b')
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """ DROP FUNCTION IF EXISTS java_udf_array_int_test(array<int>); """
        sql """ CREATE FUNCTION java_udf_array_int_test(array<int>) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.ArrayIntTest",
            "type"="JAVA_UDF"
        ); """
        qt_select_1 """ SELECT java_udf_array_int_test(array(user_id)) result FROM ${tableName} ORDER BY result; """
        qt_select_2 """ SELECT java_udf_array_int_test(null) result ; """


        sql """ DROP FUNCTION IF EXISTS java_udf_array_return_int_test(array<int>); """
        sql """ CREATE FUNCTION java_udf_array_return_int_test(array<int>) RETURNS array<int> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.ArrayReturnArrayIntTest",
            "type"="JAVA_UDF"
        ); """
        qt_select_3 """ SELECT java_udf_array_return_int_test(array(user_id)), tinyint_col as result FROM ${tableName} ORDER BY result; """
        qt_select_4 """ SELECT java_udf_array_return_int_test(array(user_id,user_id)), tinyint_col as result FROM ${tableName} ORDER BY result; """
        qt_select_5 """ SELECT java_udf_array_return_int_test(null) result ; """


        sql """ DROP FUNCTION IF EXISTS java_udf_array_return_string_test(array<string>); """
        sql """ CREATE FUNCTION java_udf_array_return_string_test(array<string>) RETURNS array<string> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.ArrayReturnArrayStringTest",
            "type"="JAVA_UDF"
        ); """
        qt_select_6 """ SELECT java_udf_array_return_string_test(array(string_col)), tinyint_col as result FROM ${tableName} ORDER BY result; """
        qt_select_7 """ SELECT java_udf_array_return_string_test(array(string_col, cast(user_id as string))), tinyint_col as result FROM ${tableName} ORDER BY result; """
        qt_select_8 """ SELECT java_udf_array_return_string_test(null) result ; """

        sql """ DROP FUNCTION IF EXISTS java_udf_array_string_test(array<string>); """
        sql """ CREATE FUNCTION java_udf_array_string_test(array<string>) RETURNS string PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.ArrayStringTest",
            "type"="JAVA_UDF"
        ); """
        qt_select_9 """ SELECT java_udf_array_string_test(array(string_col)), tinyint_col as result FROM ${tableName} ORDER BY result; """
        qt_select_10 """ SELECT java_udf_array_string_test(array(string_col, cast(user_id as string))), tinyint_col as result FROM ${tableName} ORDER BY result; """
        qt_select_11 """ SELECT java_udf_array_string_test(null) result ; """

        //ArrayDateTimeTest
        sql """ DROP FUNCTION IF EXISTS java_udf_array_datatime_test(array<datetime>); """
        sql """ CREATE FUNCTION java_udf_array_datatime_test(array<datetime>) RETURNS array<datetime> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.ArrayDateTimeTest",
            "type"="JAVA_UDF"
        ); """
        qt_select_12 """ SELECT java_udf_array_datatime_test(array(datetimev2_col)), tinyint_col as result FROM ${tableName} ORDER BY result; """

        sql """ DROP FUNCTION IF EXISTS java_udf_array_date_test(array<date>); """
        sql """ CREATE FUNCTION java_udf_array_date_test(array<date>) RETURNS array<date> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.ArrayDateTest",
            "type"="JAVA_UDF"
        ); """
        qt_select_13 """ SELECT java_udf_array_date_test(array(datev2_col)), tinyint_col as result FROM ${tableName} ORDER BY result; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS java_udf_array_int_test(array<int>);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_array_return_int_test(array<int>);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_array_return_string_test(array<string>);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_array_string_test(array<string>);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_array_datatime_test(array<datetime>);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_array_date_test(array<date>);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
