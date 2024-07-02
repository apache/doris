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

suite("nereids_test_javaudaf_mysum_float_double") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    def tableName = "test_javaudaf_mysum_float_double"
    def jarPath = """${context.file.parent}/../../javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id`     INT        NOT NULL COMMENT "用户id",
            `float_col`   float      NOT NULL COMMENT "",
            `double_col`  double     NOT NULL COMMENT "",
            `string_col`  STRING     NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 9; i ++) {
            sb.append("""
                (${i % 3}, '${i*111/10}','${i*111/100+i/1000}','poiuytre${i}abcdefg'),
            """)
        }
        sb.append("""
                (${i}, '${i*111/10}','${i*111/100+i/10}','poiuytre${i}abcdefg')
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY float_col; """

        File path1 = new File(jarPath)
        if (!path1.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """ CREATE AGGREGATE FUNCTION udaf_my_sum_double(double,double) RETURNS double PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MySumDouble",
            "always_nullable"="false",
            "type"="JAVA_UDF"
        ); """

        qt_select1 """ SELECT udaf_my_sum_double(double_col,double_col) result FROM ${tableName}; """

        qt_select2 """ select user_id, udaf_my_sum_double(double_col,double_col) from ${tableName} group by user_id order by user_id; """
        


        sql """ CREATE AGGREGATE FUNCTION udaf_my_sum_float(float) RETURNS float PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MySumFloat",
            "always_nullable"="false",
            "type"="JAVA_UDF"
        ); """

        qt_select3 """ SELECT udaf_my_sum_float(float_col) result FROM ${tableName}; """

        qt_select4 """ select user_id, udaf_my_sum_float(float_col) from ${tableName} group by user_id order by user_id; """
        

    } finally {
        try_sql("DROP FUNCTION IF EXISTS udaf_my_sum_float(float);")
        try_sql("DROP FUNCTION IF EXISTS udaf_my_sum_double(double,double);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
