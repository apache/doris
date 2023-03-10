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
                (${i}, '${i}','abcdefg${i}','poiuytre${i}abcdefg'),
            """)
        }
        sb.append("""
                (${i}, '${i}','abcdefg${i}','poiuytre${i}abcdefg')
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

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

        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS java_udf_string_test(string, int, int);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
