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

suite("test_javaudtf_arrayint") {
    def tableName = "test_javaudtf_arrayint"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

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

        sql """DROP FUNCTION IF EXISTS udtf_arrayint_outer(array<int>);"""
        sql """ CREATE TABLES FUNCTION udtf_arrayint(array<int>) RETURNS array<int> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDTFArrayIntTest",
            "always_nullable"="true",
            "type"="JAVA_UDF"
        ); """

        qt_select1 """ SELECT user_id, string_col, e1 FROM ${tableName} lateral view  udtf_arrayint(array(user_id)) temp as e1 order by user_id; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS udtf_arrayint(array<int>);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
