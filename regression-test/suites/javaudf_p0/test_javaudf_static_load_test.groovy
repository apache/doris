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

suite("test_javaudf_static_load_test") {
    // static load udf tests suite for 1 backends regression.
    if (sql("show backends").size() > 1) return


    def tableName = "test_javaudf_static_load_test"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""


    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
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
                (${i % 3}, '${i}','abcdefg${i}','poiuytre${i}abcdefg'),
            """)
        }
        sb.append("""
                (${i}, '${i}','abcdefg${i}','poiuytre${i}abcdefg')
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY char_col; """

        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION static_load_test() RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.StaticIntTest",
            "static_load"="true",
            "expiration_time"="10",
            "type"="JAVA_UDF"
        ); """

        // the result of the following queries should be the accumulation
        sql """set parallel_pipeline_task_num = 1; """
        qt_select1 """ SELECT static_load_test(); """
        qt_select2 """ SELECT static_load_test(); """
        qt_select3 """ SELECT static_load_test(); """
        qt_select4 """ SELECT static_load_test(); """
        qt_select5 """ SELECT static_load_test(); """


        sql """ CREATE AGGREGATE FUNCTION static_load_test_udaf(int) RETURNS BigInt PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.StaticIntTestUDAF",
            "always_nullable"="true",
            "static_load"="true",
            "expiration_time"="10",
            "type"="JAVA_UDF"
        ); """

        // the result of the following queries should be the accumulation
        // maybe we need drop funtion and test again, the result should be the same
        // but the regression test will copy the jar to be custom_lib, and loaded by BE when it's started
        // so it's can't be unloaded
        sql """set parallel_pipeline_task_num = 1; """
        qt_select6 """ SELECT static_load_test_udaf(0); """
        qt_select7 """ SELECT static_load_test_udaf(0); """
        qt_select8 """ SELECT static_load_test_udaf(0); """
        qt_select9 """ SELECT static_load_test_udaf(0); """
        qt_select10 """ SELECT static_load_test_udaf(0); """

        sql """ CREATE TABLES FUNCTION static_load_test_udtf() RETURNS array<int> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.StaticIntTestUDTF",
            "always_nullable"="true",
            "static_load"="true",
            "expiration_time"="10",
            "type"="JAVA_UDF"
        ); """

        sql """set parallel_pipeline_task_num = 1; """
        qt_select11 """ select k1, e1 from (select 1 k1) as t lateral view static_load_test_udtf() tmp1 as e1; """
        qt_select12 """ select k1, e1 from (select 1 k1) as t lateral view static_load_test_udtf() tmp1 as e1; """
        qt_select13 """ select k1, e1 from (select 1 k1) as t lateral view static_load_test_udtf() tmp1 as e1; """
        qt_select14 """ select k1, e1 from (select 1 k1) as t lateral view static_load_test_udtf() tmp1 as e1; """
        qt_select15 """ select k1, e1 from (select 1 k1) as t lateral view static_load_test_udtf() tmp1 as e1; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS static_load_test();")
        try_sql("DROP FUNCTION IF EXISTS static_load_test_udaf(int);")
        try_sql("DROP FUNCTION IF EXISTS static_load_test_udtf();")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}

