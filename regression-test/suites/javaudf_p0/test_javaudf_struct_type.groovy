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

suite("test_javaudf_struct_type") {
    def tableName = "test_javaudf_struct_type"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` int,
            `name` string,
            `decimal_col` decimal(9,3),
            `s_info` STRUCT<s_id:int(11), s_name:string> NULL
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        sql """ INSERT INTO ${tableName} VALUES (1, "asd", 123.456, {1, 'sn11'}); """
        sql """ INSERT INTO ${tableName} VALUES (2, "qwe", 546.677, {2, 'sn22'}); """
        sql """ INSERT INTO ${tableName} VALUES (3, "kljkl", 8347.121, {3, 'sn333'}); """
        sql """ INSERT INTO ${tableName} VALUES (4, "nvblk", 999.123, {4, 'sn444'}); """
        sql """ INSERT INTO ${tableName} VALUES (5, "uyuy", 1.784, {5, 'sn3423'}); """
        sql """ INSERT INTO ${tableName} VALUES (6, "ghjhj", 2.784, {6, 'sne543'}); """
        sql """ INSERT INTO ${tableName} VALUES (7, "rtut", 3.784, {7, 'sn6878'}); """
        sql """ INSERT INTO ${tableName} VALUES (8, "vnvc", 4.784, {8, 'sn7989'}); """
        sql """ INSERT INTO ${tableName} VALUES (9, "asdzdf", 5.784, {9, 'sn242'}); """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION java_udf_struct_test(STRUCT<s_id:int, s_name:string>) RETURNS STRUCT<s_id:int, s_name:string> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.StructTypeTest",
            "type"="JAVA_UDF"
        ); """

        qt_select_1 """ SELECT s_info, java_udf_struct_test(s_info) result FROM ${tableName} ORDER BY user_id; """


        sql """ CREATE FUNCTION java_udf_struct_string_int_test(int,string) RETURNS STRUCT<s_id:int, s_name:string> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.StructTypeTestStringInt",
            "type"="JAVA_UDF"
        ); """

        qt_select_2 """ SELECT user_id, name, java_udf_struct_string_int_test(user_id, name) result FROM ${tableName} ORDER BY user_id; """

        sql """ CREATE FUNCTION java_udf_struct_return_int_test(STRUCT<s_id:int, s_name:string>) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.StructTypeTestReturnInt",
            "type"="JAVA_UDF"
        ); """

        qt_select_3 """ SELECT s_info, java_udf_struct_return_int_test(s_info) result FROM ${tableName} ORDER BY user_id; """


        sql """ CREATE FUNCTION java_udf_struct_decimal_test(decimal(9,3)) RETURNS STRUCT<s_id:decimal(9,3)> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.StructTypeTestDecimal",
            "type"="JAVA_UDF"
        ); """

        qt_select_4 """ SELECT decimal_col, java_udf_struct_decimal_test(decimal_col) result FROM ${tableName} ORDER BY user_id; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS java_udf_struct_test(STRUCT<s_id:int, s_name:string>);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_struct_string_int_test(int,string);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_struct_return_int_test(STRUCT<s_id:int, s_name:string>);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_struct_decimal_test(decimal(9,3));")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
