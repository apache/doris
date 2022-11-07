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

suite("test_javaudf_boolean") {
    def tableName = "test_javaudf_boolean"
    def jarPath   = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""

    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id`  INT     NOT NULL COMMENT "",
            `boo_1`    BOOLEAN NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """

        sql """ INSERT INTO ${tableName} (`user_id`,`boo_1`) VALUES
                (111,true),
                (112,false),
                (113,0),
                (114,1)
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION java_udf_boolean_test(BOOLEAN) RETURNS BOOLEAN PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.BooleanTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_boolean_test(1)     as result; """
        qt_select """ SELECT java_udf_boolean_test(0)     as result ; """
        qt_select """ SELECT java_udf_boolean_test(true)  as result ; """
        qt_select """ SELECT java_udf_boolean_test(false) as result ; """
        qt_select """ SELECT java_udf_boolean_test(null)  as result ; """
        qt_select """ SELECT user_id,java_udf_boolean_test(boo_1) as result FROM ${tableName} order by user_id; """
        

        sql """ DROP FUNCTION java_udf_boolean_test(BOOLEAN); """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
