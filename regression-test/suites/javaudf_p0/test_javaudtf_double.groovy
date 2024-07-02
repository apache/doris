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

suite("test_javaudtf_double") {
    def tableName = "test_javaudtf_double"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id`  INT    NOT NULL COMMENT "",
            `float_1`  FLOAT  NOT NULL COMMENT "",
            `float_2`  FLOAT           COMMENT "",
            `double_1` DOUBLE NOT NULL COMMENT "",
            `double_2` DOUBLE          COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        
        
        sql """ INSERT INTO ${tableName} (`user_id`,`float_1`,`float_2`,double_1,double_2) VALUES
                (111,11111.11111,222222.3333333,12345678.34455677,1111111.999999999999),
                (112,1234556.11111,222222.3333333,222222222.3333333333333,4444444444444.555555555555),
                (113,87654321.11111,null,6666666666.6666666666,null)
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """DROP FUNCTION IF EXISTS udtf_double_outer(double);"""
        sql """ CREATE TABLES FUNCTION udtf_double(double) RETURNS array<double> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDTFDoubleTest",
            "always_nullable"="true",
            "type"="JAVA_UDF"
        ); """

        qt_select1 """ SELECT user_id, double_1, e1 FROM ${tableName} lateral view  udtf_double(double_1) temp as e1 order by user_id; """
        qt_select2 """ SELECT user_id, double_2, e1 FROM ${tableName} lateral view  udtf_double(double_2) temp as e1 order by user_id; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS udtf_double(double);")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
