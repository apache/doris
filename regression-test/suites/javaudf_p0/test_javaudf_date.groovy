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

suite("test_javaudf_date") {
    def tableName = "test_javaudf_date"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` INT NOT NULL COMMENT "用户id",
            `date_col` date NOT NULL,
            `datetime_col` datetime NOT NULL,
            `datev2_col` datev2 NOT NULL,
            `datetimev2_col` datetimev2 NOT NULL
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 9; i ++) {
            sb.append("""
                (1, '2022-01-01', '2022-01-01 11:11:11', '2022-01-01', '2022-01-01 11:11:11'),
            """)
        }
        sb.append("""
                (1, '2022-01-01', '2022-01-01 11:11:11', '2022-01-01', '2022-01-01 11:11:11')
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION java_udf_date_test1(date, date) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTest1",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_date_test1(date_col, '2011-01-01') result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_date_test1('2011-01-01', '2011-01-01') result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_date_test1(null, '2011-01-01') result FROM ${tableName} ORDER BY result; """



        sql """ CREATE FUNCTION java_udf_date_test2(date, date) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTest2",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_date_test2(date_col, '2011-01-01') result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_date_test2('2011-01-01', '2011-01-01') result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_date_test2(null, '2011-01-01') result FROM ${tableName} ORDER BY result; """

        

        sql """ CREATE FUNCTION java_udf_date_test3(date, date) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTest3",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_date_test3(date_col, '2011-01-01') result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_date_test3('2011-01-01', '2011-01-01') result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_date_test3(null, '2011-01-01') result FROM ${tableName} ORDER BY result; """

        

        sql """ CREATE FUNCTION java_udf_datetime_test1(datetime, datetime) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTimeTest1",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_datetime_test1(datetime_col, '2011-01-01') result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_datetime_test1('2011-01-01', '2011-01-01') result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_datetime_test1(null, '2011-01-01') result FROM ${tableName} ORDER BY result; """

        

        sql """ CREATE FUNCTION java_udf_datetime_test2(datetime, datetime) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTimeTest2",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_datetime_test2(datetime_col, '2011-01-01') result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_datetime_test2('2011-01-01', '2011-01-01') result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_datetime_test2(null, '2011-01-01') result FROM ${tableName} ORDER BY result; """

       

        sql """ CREATE FUNCTION java_udf_datetime_test3(datetime, datetime) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTimeTest3",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_datetime_test3(datetime_col, '2011-01-01') result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_datetime_test3('2011-01-01', '2011-01-01') result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_datetime_test3(null, '2011-01-01') result FROM ${tableName} ORDER BY result; """

        


        sql """ CREATE FUNCTION java_udf_datev2_test1(datev2, datev2) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTest1",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_datev2_test1(datev2_col, '2011-01-01') result FROM ${tableName} ORDER BY result; """

        

        sql """ CREATE FUNCTION java_udf_datev2_test2(datev2, datev2) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTest2",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_datev2_test2(datev2_col, '2011-01-01') result FROM ${tableName} ORDER BY result; """

        

        sql """ CREATE FUNCTION java_udf_datev2_test3(datev2, datev2) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTest3",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_datev2_test3(datev2_col, '2011-01-01') result FROM ${tableName} ORDER BY result; """

        

        sql """ CREATE FUNCTION java_udf_datetimev2_test1(datetimev2, datetimev2) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTimeTest1",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_datetimev2_test1(datetimev2_col, '2011-01-01') result FROM ${tableName} ORDER BY result; """

        

        sql """ CREATE FUNCTION java_udf_datetimev2_test2(datetimev2, datetimev2) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTimeTest2",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_datetimev2_test2(datetimev2_col, '2011-01-01') result FROM ${tableName} ORDER BY result; """

       

        sql """ CREATE FUNCTION java_udf_datetimev2_test3(datetimev2, datetimev2) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DateTimeTest3",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_datetimev2_test3(datetimev2_col, '2011-01-01') result FROM ${tableName} ORDER BY result; """

        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS java_udf_date_test3(date, date);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_datetime_test1(datetime, datetime);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_datetime_test2(datetime, datetime);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_datetime_test3(datetime, datetime);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_datev2_test1(datev2, datev2);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_datev2_test2(datev2, datev2);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_datev2_test3(datev2, datev2);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_datetimev2_test1(datetimev2, datetimev2);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_datetimev2_test2(datetimev2, datetimev2);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_datetimev2_test3(datetimev2, datetimev2);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_date_test2(date, date);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_date_test1(date, date);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
