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

suite("test_javaudf_custom_lib_all_types") {
    def tableName = "test_javaudf_custom_lib_all_types"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            int_col int,
            string_col string
            )
            DISTRIBUTED BY HASH(int_col) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i++) {
            sb.append("""
                (${i},${i%2}),
            """)
        }
        sb.append("""
                (${i},null)
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """

        sql """DROP FUNCTION IF EXISTS c_echo_int(int);"""
        sql """CREATE FUNCTION c_echo_int(int) RETURNS int PROPERTIES (
            "symbol"="org.apache.doris.udf.Echo\$EchoInt",
            "type"="JAVA_UDF"
        );"""

        qt_java_udf_all_types """select
            int_col,
            c_echo_int(int_col)
            from ${tableName} order by int_col;"""
    } finally {
        try_sql """DROP FUNCTION IF EXISTS c_echo_int(int);"""
        try_sql("""DROP TABLE IF EXISTS ${tableName};""")
    }
}
