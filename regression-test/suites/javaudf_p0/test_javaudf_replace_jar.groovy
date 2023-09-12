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

suite("test_javaudf_replace_jar") {
    def tableName = "test_javaudf_replace_jar"
    def origin_jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    def replace_jarPath = """${context.file.parent}/jars/java-udf-replace-jar-with-dependencies.jar"""

    try {
        sql """ DROP FUNCTION IF EXISTS java_udf_bigint_test(bigint); """
        sql """ DROP FUNCTION IF EXISTS java_udf_int_test(int); """
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id`      INT      NOT NULL COMMENT "",
            `bigint_col`   BIGINT   NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i ++) {
            sb.append("""
                (${i},${i}*2),
            """)
        }
        sb.append("""
                (${i},${i}*2)
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File originPath = new File(origin_jarPath)
        if (!originPath.exists()) {
            throw new IllegalStateException("""${originPath} doesn't exist! """)
        }
        log.info("origin Jar path: ${origin_jarPath}".toString())
        File replacePath = new File(replace_jarPath)
        if (!replacePath.exists()) {
            throw new IllegalStateException("""${replacePath} doesn't exist! """)
        }
        log.info("replace Jar path: ${replace_jarPath}".toString())

        sql """ CREATE FUNCTION java_udf_int_test(int) RETURNS int PROPERTIES (
            "file"="file://${origin_jarPath}",
            "symbol"="org.apache.doris.udf.IntTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_int_test(user_id) result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_int_test(null) result ; """

        sql """ CREATE FUNCTION java_udf_bigint_test(bigint) RETURNS bigint PROPERTIES (
            "file"="file://${origin_jarPath}",
            "symbol"="org.apache.doris.udf.BigintTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_bigint_test(bigint_col) result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_bigint_test(null) result ; """


        // drop function
        sql """ DROP FUNCTION IF EXISTS java_udf_bigint_test(bigint); """
        sql """ DROP FUNCTION IF EXISTS java_udf_int_test(int); """

        sql """ CREATE FUNCTION java_udf_int_test(int) RETURNS int PROPERTIES (
            "file"="file://${replace_jarPath}",
            "symbol"="org.apache.doris.udf.IntTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_int_test(user_id) result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_int_test(null) result ; """


        sql """ CREATE FUNCTION java_udf_bigint_test(bigint) RETURNS bigint PROPERTIES (
            "file"="file://${replace_jarPath}",
            "symbol"="org.apache.doris.udf.BigintTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_bigint_test(bigint_col) result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_bigint_test(null) result ; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS java_udf_bigint_test(bigint);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_int_test(int);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
