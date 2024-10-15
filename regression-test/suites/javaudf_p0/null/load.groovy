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

suite('test_javaudf_null_load', 'p0,restart_fe') {
    // In order to cover the scenario that udf can be used normally after fe restart.
    // We devided the origin case test_javaudf_null.groovy into two parts, load and query, this case is the first part.
    // run load and query -> restart fe -> run query again,
    // by this way, we can cover the scenario.
    def tableName = 'test_javaudf_null'
    def jarPath = """${context.file.parent}/../jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

    log.info("Jar path: ${jarPath}".toString())
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """DROP FUNCTION IF EXISTS java_udf_null_test(int);"""
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
        `user_id`     INT         NOT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
    """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 9; i ++) {
        sb.append("""
            (${i}),
        """)
    }
    sb.append("""
            (${i})
        """)
    sql """ INSERT INTO ${tableName} VALUES
            ${sb.toString()}
        """

    File path = new File(jarPath)
    if (!path.exists()) {
        throw new IllegalStateException("""${jarPath} doesn't exist! """)
    }

    sql """ CREATE FUNCTION java_udf_null_test(int) RETURNS int PROPERTIES (
        "file"="file://${jarPath}",
        "symbol"="org.apache.doris.udf.NullTest",
        "type"="JAVA_UDF"
    ); """
}
