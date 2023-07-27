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

suite("nereids_test_javaudaf_mysum_array") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    // nereids doesn't support array now, we will enable the suite after supporting.
    // def tableName = "test_javaudaf_mysum_array"
    // def jarPath = """${context.file.parent}/../../javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""

    // log.info("Jar path: ${jarPath}".toString())
    // try {
    //     sql """ DROP TABLE IF EXISTS ${tableName} """
    //     sql """
    //     CREATE TABLE IF NOT EXISTS ${tableName} (
    //         `user_id`     INT         NOT NULL COMMENT "用户id",
    //         `char_col`    CHAR        NOT NULL COMMENT "",
    //         `varchar_col` VARCHAR(10) NOT NULL COMMENT "",
    //         `string_col`  STRING      NOT NULL COMMENT ""
    //         )
    //         DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
    //     """
    //     StringBuilder sb = new StringBuilder()
    //     int i = 1
    //     for (; i < 9; i ++) {
    //         sb.append("""
    //             (${i % 3}, '${i}','abcdefg${i}','poiuytre${i}abcdefg'),
    //         """)
    //     }
    //     sb.append("""
    //             (${i}, '${i}','abcdefg${i}','poiuytre${i}abcdefg')
    //         """)
    //     sql """ INSERT INTO ${tableName} VALUES
    //          ${sb.toString()}
    //         """
    //     qt_select_default """ SELECT * FROM ${tableName} t ORDER BY char_col; """

    //     File path1 = new File(jarPath)
    //     if (!path1.exists()) {
    //         throw new IllegalStateException("""${jarPath} doesn't exist! """)
    //     }

    //     sql """ DROP FUNCTION IF EXISTS udaf_my_sum_arrayint(array<int>); """
    //     sql """ CREATE AGGREGATE FUNCTION udaf_my_sum_arrayint(array<int>) RETURNS BigInt PROPERTIES (
    //         "file"="file://${jarPath}",
    //         "symbol"="org.apache.doris.udf.MySumArrayInt",
    //         "type"="JAVA_UDF"
    //     ); """

    //     qt_select1 """ SELECT udaf_my_sum_arrayint(array(user_id)) result FROM ${tableName}; """

    //     qt_select2 """ select user_id, udaf_my_sum_arrayint(array(user_id)) from ${tableName} group by user_id order by user_id; """

    //     qt_select3 """ select user_id, sum(user_id), udaf_my_sum_arrayint(array(user_id)) from ${tableName} group by user_id order by user_id; """

    //     qt_select4 """ select user_id, udaf_my_sum_arrayint(array(user_id)), sum(user_id) from ${tableName} group by user_id order by user_id; """
        
    //     qt_select5 """ SELECT udaf_my_sum_arrayint(array(user_id, user_id)) result FROM ${tableName}; """


    //     sql """ DROP FUNCTION IF EXISTS udaf_my_sum_return_arrayint(array<int>); """
    //     sql """ CREATE AGGREGATE FUNCTION udaf_my_sum_return_arrayint(array<int>) RETURNS array<int> PROPERTIES (
    //         "file"="file://${jarPath}",
    //         "symbol"="org.apache.doris.udf.MySumReturnArrayInt",
    //         "type"="JAVA_UDF"
    //     ); """

    //     qt_select6 """ SELECT udaf_my_sum_return_arrayint(array(user_id)) result FROM ${tableName}; """
    //     qt_select6_1 """ SELECT udaf_my_sum_return_arrayint(array(user_id)) result FROM ${tableName} group by user_id order by user_id; """
    //     qt_select7 """ SELECT udaf_my_sum_return_arrayint(array(user_id, user_id)) result FROM ${tableName}; """
    //     qt_select7_1 """ SELECT udaf_my_sum_return_arrayint(array(user_id, user_id)) result FROM ${tableName} group by user_id order by user_id; """


    //     sql """ DROP FUNCTION IF EXISTS udaf_my_sum_return_arraystring(array<string>); """
    //     sql """ CREATE AGGREGATE FUNCTION udaf_my_sum_return_arraystring(array<string>) RETURNS array<string> PROPERTIES (
    //         "file"="file://${jarPath}",
    //         "symbol"="org.apache.doris.udf.MyReturnArrayString",
    //         "type"="JAVA_UDF"
    //     ); """

    //     qt_select_8 """ SELECT udaf_my_sum_return_arraystring(array(string_col)) FROM ${tableName}; """
    //     qt_select_9 """ SELECT udaf_my_sum_return_arraystring(array(string_col)), user_id as result FROM ${tableName} group by result ORDER BY result;  """
    //     qt_select_10 """ SELECT udaf_my_sum_return_arraystring(array(string_col, cast(user_id as string))), user_id as result FROM ${tableName} group by result ORDER BY result; """
    //     qt_select_11 """ SELECT udaf_my_sum_return_arraystring(null) result FROM ${tableName}; """


    //     sql """ DROP FUNCTION IF EXISTS udaf_my_sum_arraystring(array<string>); """
    //     sql """ CREATE AGGREGATE FUNCTION udaf_my_sum_arraystring(array<string>) RETURNS string PROPERTIES (
    //         "file"="file://${jarPath}",
    //         "symbol"="org.apache.doris.udf.MyArrayString",
    //         "type"="JAVA_UDF"
    //     ); """

    //     qt_select_13 """ SELECT udaf_my_sum_arraystring(array(string_col)), user_id as result FROM ${tableName} group by result ORDER BY result;  """
    //     qt_select_14 """ SELECT udaf_my_sum_arraystring(array(string_col, cast(user_id as string))), user_id as result FROM ${tableName} group by result ORDER BY result; """
    //     qt_select_15 """ SELECT udaf_my_sum_arraystring(null) result FROM ${tableName}; """

    // } finally {
    //     try_sql("DROP FUNCTION IF EXISTS udaf_my_sum_arraystring(array<string>);")
    //     try_sql("DROP FUNCTION IF EXISTS udaf_my_sum_return_arraystring(array<string>);")
    //     try_sql("DROP FUNCTION IF EXISTS udaf_my_sum_arrayint(array<int>);")
    //     try_sql("DROP FUNCTION IF EXISTS udaf_my_sum_return_arrayint(array<int>);")
    //     try_sql("DROP TABLE IF EXISTS ${tableName}")
    // }
}
