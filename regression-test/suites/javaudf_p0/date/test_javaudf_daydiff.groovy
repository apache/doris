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

suite("test_javaudf_daydiff") {
    def tableName = "test_javaudf_daydiff"
    File path = new File("${context.file.parent}")
    def jarPath = """${path.getParent()}/jars/java-udf-case-jar-with-dependencies.jar"""

    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `col_1` varchar(10) NOT NULL,
            `col_2` varchar(10) NOT NULL
            )
            DISTRIBUTED BY HASH(col_1) PROPERTIES("replication_num" = "1");
        """

        sql """ INSERT INTO ${tableName} VALUES ("20220101", "20220102"), ("20221231", "20230101"); """

        File path1 = new File(jarPath)
        if (!path1.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION day_diff(string, string) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.date.DayDiffUDF",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT day_diff(col_1, col_2)  as a FROM ${tableName} ORDER BY a; """

        sql """ DROP FUNCTION day_diff(string, string); """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
