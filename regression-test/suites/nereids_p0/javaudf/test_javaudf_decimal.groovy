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

suite("nereids_test_javaudf_decimal") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    def tableName = "test_javaudf_decimal"
    def jarPath = """${context.file.parent}/../../javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` INT NOT NULL COMMENT "",
            `cost_1` decimal(27,9) NOT NULL COMMENT "",
            `cost_2` decimal(27,9) COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        
        
        sql """ INSERT INTO ${tableName} (`user_id`,`cost_1`,`cost_2`) VALUES
                (111,11111.11111,222222.3333333),
                (112,1234556.11111,222222.3333333),
                (113,87654321.11111,null)
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path1 = new File(jarPath)
        if (!path1.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION java_udf_decimal_test(decimal(27,9),decimal(27,9)) RETURNS decimal(27,9) PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DecimalTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_decimal_test(cast(2.83645 as decimal(27,9)),cast(111.1111111 as decimal(27,9))) as result; """
        qt_select """ SELECT java_udf_decimal_test(2.83645,111.1111111) as result ; """
        qt_select """ SELECT java_udf_decimal_test(2.83645,null) as result ; """
        qt_select """ SELECT java_udf_decimal_test(cast(2.83645 as decimal(27,9)),null) as result ; """
        qt_select """ SELECT user_id,java_udf_decimal_test(cost_1, cost_2) as sum FROM ${tableName} order by user_id; """
        




        sql """ DROP FUNCTION if exists java_udf_decimal_string_test(decimal(27,9),String,String); """
        sql """ CREATE FUNCTION java_udf_decimal_string_test(decimal(27,9),String,String) RETURNS decimal(27,9) PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.DecimalStringTest",
            "type"="JAVA_UDF"
        ); """
        qt_select_decimal_string """ SELECT java_udf_decimal_string_test(2.83645,'asd','a') as result; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS java_udf_decimal_test(decimal(27,9),decimal(27,9));")
        try_sql("DROP FUNCTION IF EXISTS java_udf_decimal_string_test(decimal(27,9),String,String);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
