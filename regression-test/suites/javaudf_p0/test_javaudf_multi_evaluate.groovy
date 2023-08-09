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

suite("test_javaudf_multi_evaluate") {
    def tableName = "test_javaudf_multi_evaluate"
    def jarPath   = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""

    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id`  INT    NOT NULL COMMENT "",
            `float_1`  FLOAT  COMMENT "",
            `int_1`  INT      COMMENT "",
            `int_2` INT       COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """


        sql """ INSERT INTO ${tableName} (`user_id`,`float_1`,`int_1`,`int_2`) VALUES
                (1,1.11,2,3),
                (2,null,3,4),
                (3,3.33,2,null);
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """ CREATE FUNCTION java_udf_multi_evaluate_test(FLOAT) RETURNS FLOAT PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MultiEvaluateTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_multi_evaluate_test(float_1) FROM ${tableName} where user_id = 1; """
        qt_select """ SELECT java_udf_multi_evaluate_test(float_1) FROM ${tableName} where user_id = 2; """

        sql """ CREATE FUNCTION java_udf_multi_evaluate_test(int) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MultiEvaluateTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_multi_evaluate_test(int_2) FROM ${tableName} where user_id = 2; """
        qt_select """ SELECT java_udf_multi_evaluate_test(int_2) FROM ${tableName} where user_id = 3; """
        
        sql """ CREATE FUNCTION java_udf_multi_evaluate_test(int,int) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MultiEvaluateTest",
            "type"="JAVA_UDF"
        ); """

        qt_select """ SELECT java_udf_multi_evaluate_test(int_1, int_2) FROM ${tableName} where user_id = 2; """
        qt_select """ SELECT java_udf_multi_evaluate_test(int_1, int_2) FROM ${tableName} where user_id = 3; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS java_udf_multi_evaluate_test(FLOAT);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_multi_evaluate_test(int);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_multi_evaluate_test(int,int);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
