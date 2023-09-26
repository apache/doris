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

suite("test_javaudf_no_input") {
    def tableName = "test_javaudf_no_input"
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

        sql """ CREATE FUNCTION no_input_udf() RETURNS String PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.NoInputTest",
            "always_nullable"="true",
            "type"="JAVA_UDF"
        ); """

        qt_select1 """ SELECT no_input_udf() FROM ${tableName}; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS no_input_udf();")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}

