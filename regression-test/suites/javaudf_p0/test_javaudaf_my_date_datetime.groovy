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

suite("test_javaudaf_my_date_datetime") {
    def tableName = "test_javaudaf_my_date_datetime"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

    log.info("Jar path: ${jarPath}".toString())
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
        // test datev2
        sql """ CREATE AGGREGATE FUNCTION udaf_my_day_datev2(datev2) RETURNS datev2 PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MyDayDate",
            "always_nullable"="false",
            "type"="JAVA_UDF"
        ); """

        qt_select1 """ SELECT udaf_my_day_datev2(datev2_col) result FROM ${tableName}; """

        qt_select2 """ select user_id, udaf_my_day_datev2(datev2_col) from ${tableName} group by user_id order by user_id; """
        


        // test date
        sql """ CREATE AGGREGATE FUNCTION udaf_my_day_date(date) RETURNS date PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MyDayDate",
            "always_nullable"="false",
            "type"="JAVA_UDF"
        ); """

        qt_select3 """ SELECT udaf_my_day_date(date_col) result FROM ${tableName}; """

        qt_select4 """ select user_id, udaf_my_day_date(date_col) from ${tableName} group by user_id order by user_id; """
        


        // test datetimev2
        sql """ CREATE AGGREGATE FUNCTION udaf_my_hour_datetimev2(datetimev2) RETURNS datetimev2 PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MyHourDateTime",
            "always_nullable"="false",
            "type"="JAVA_UDF"
        ); """

        qt_select5 """ SELECT udaf_my_hour_datetimev2(datetimev2_col) result FROM ${tableName}; """

        qt_select6 """ select user_id, udaf_my_hour_datetimev2(datetimev2_col) from ${tableName} group by user_id order by user_id; """



        // test datetime
        sql """ CREATE AGGREGATE FUNCTION udaf_my_hour_datetime(datetime) RETURNS datetime PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MyHourDateTime",
            "always_nullable"="false",
            "type"="JAVA_UDF"
        ); """

        qt_select5 """ SELECT udaf_my_hour_datetime(datetime_col) result FROM ${tableName}; """

        qt_select6 """ select user_id, udaf_my_hour_datetime(datetime_col) from ${tableName} group by user_id order by user_id; """

        

    } finally {
        try_sql("DROP FUNCTION IF EXISTS udaf_my_hour_datetime(datetime)")
        try_sql("DROP FUNCTION IF EXISTS udaf_my_day_datev2(datev2);")
        try_sql("DROP FUNCTION IF EXISTS udaf_my_day_date(date);")
        try_sql("DROP FUNCTION IF EXISTS udaf_my_hour_datetimev2(datetimev2);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
