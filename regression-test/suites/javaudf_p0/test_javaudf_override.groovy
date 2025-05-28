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

suite("test_javaudf_override") {
    def tableName = "test_javaudf_override"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

    log.info("Jar path: ${jarPath}".toString())
    sql "drop database if exists test_javaudf_override_db"
    sql "create database test_javaudf_override_db"
    try {
        sql """ use test_javaudf_override_db"""
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            int_col int
            )
            DISTRIBUTED BY HASH(int_col) PROPERTIES("replication_num" = "1");
        """
        sql """insert into ${tableName} values(-100),(100),(0)"""

        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        // 1. create global udf "abs", same name as builtin function "abs"

        sql """DROP GLOBAL FUNCTION IF EXISTS abs(int);"""
        sql """CREATE GLOBAL FUNCTION abs(int) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoInt",
            "type"="JAVA_UDF"
        );"""

        // default, will use builtin function first
        sql "set prefer_udf_over_builtin=false"
        qt_sql01 """select abs(-1), abs(1), abs(int_col) from ${tableName}"""
        // set prefer_udf_over_builtin, will use udf first
        sql "set prefer_udf_over_builtin=true"
        qt_sql02 """select abs(-1), abs(1), abs(int_col) from ${tableName}"""

        // 2. create database udf "abs", same name as builtin function "abs"
        def curdb = "test_javaudf_override_db"
        sql """DROP GLOBAL FUNCTION IF EXISTS abs(int);"""
        sql "use ${curdb}"
        sql """CREATE FUNCTION abs(int) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoInt",
            "type"="JAVA_UDF"
        );"""

        
        sql "set prefer_udf_over_builtin=false"
        qt_sql03 """select abs(-1), abs(1), ${curdb}.abs(-1), ${curdb}.abs(1), abs(int_col), ${curdb}.abs(int_col) from ${tableName}"""
        sql "set prefer_udf_over_builtin=true"
        qt_sql04 """select abs(-1), abs(1), ${curdb}.abs(-1), ${curdb}.abs(1), abs(int_col), ${curdb}.abs(int_col) from ${tableName}"""

    } finally {
    }
}
