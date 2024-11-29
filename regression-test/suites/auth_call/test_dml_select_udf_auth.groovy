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

suite("test_dml_select_udf_auth","p0,auth_call") {

    String user = 'test_dml_select_udf_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_dml_select_udf_auth_db'
    String tableName = 'test_dml_select_udf_auth_tb'
    String udfName = 'test_dml_select_udf_auth_udf'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""

    def jarPath = """${context.file.parent}/../javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)
    log.info("Jar path: ${jarPath}".toString())

    sql """ CREATE FUNCTION ${dbName}.${udfName}(string) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.collect.MurmurHash3UDF",
            "type"="JAVA_UDF"
        ); """

    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `col_1` varchar(10) NOT NULL
            )
            DISTRIBUTED BY HASH(col_1) PROPERTIES("replication_num" = "1");
        """
    sql """ INSERT INTO ${dbName}.${tableName} VALUES ("abc"), ("123"), ("123"); """

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """ SELECT ${dbName}.${udfName}(col_1) as a FROM ${dbName}.${tableName} ORDER BY a; """
            exception "Can not found function"
        }
    }
    sql """grant select_priv on ${dbName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """ SELECT ${dbName}.${udfName}(col_1) as a FROM ${dbName}.${tableName} ORDER BY a; """
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
