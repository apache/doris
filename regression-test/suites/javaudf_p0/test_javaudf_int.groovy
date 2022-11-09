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

suite("test_javaudf_int") {
    def tableName = "test_javaudf_int"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""

    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id`      INT      NOT NULL COMMENT "",
            `tinyint_col`  TINYINT  NOT NULL COMMENT "",
            `smallint_col` SMALLINT NOT NULL COMMENT "",
            `bigint_col`   BIGINT   NOT NULL COMMENT "",
            `largeint_col` LARGEINT NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i ++) {
            sb.append("""
                (${i},${i}*2,${i}*3,${i}*4,${i}*5),
            """)
        }
        sb.append("""
                (${i},${i}*2,${i}*3,${i}*4,${i}*5)
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION java_udf_int_test(int) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.IntTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_int_test(user_id) result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_int_test(null) result ; """

        sql """ DROP FUNCTION java_udf_int_test(int); """

        sql """ CREATE FUNCTION java_udf_tinyint_test(tinyint) RETURNS tinyint PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.TinyintTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_tinyint_test(tinyint_col) result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_tinyint_test(null) result ; """

        sql """ DROP FUNCTION java_udf_tinyint_test(tinyint); """

        sql """ CREATE FUNCTION java_udf_smallint_test(smallint) RETURNS smallint PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.SmallintTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_smallint_test(smallint_col) result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_smallint_test(null) result ; """

        sql """ DROP FUNCTION java_udf_smallint_test(smallint); """

        sql """ CREATE FUNCTION java_udf_bigint_test(bigint) RETURNS bigint PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.BigintTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_bigint_test(bigint_col) result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_bigint_test(null) result ; """

        sql """ DROP FUNCTION java_udf_bigint_test(bigint); """

        sql """ CREATE FUNCTION java_udf_largeint_test(largeint) RETURNS largeint PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.LargeintTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_largeint_test(largeint_col) result FROM ${tableName} ORDER BY result; """
        qt_select """ SELECT java_udf_largeint_test(null) result ; """

        sql """ DROP FUNCTION java_udf_largeint_test(largeint); """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
