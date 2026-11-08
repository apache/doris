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

// A Java UDF runs only in Doris, so a filter that calls one must not reach MySQL, even with function pushdown
// on: MySQL has no such function, or one of the same name that computes something else.
suite("test_jdbc_mysql_udf_filter", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String mysql_port = context.config.otherConfigs.get("mysql_57_port")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

    def jarPath = """${context.file.parent}/../../javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)
    scp_udf_file_to_all_fe(jarPath)

    // IntTest returns its argument plus one. It is registered under a name MySQL does not have and under
    // abs, which MySQL and Doris both have; the database-qualified call reaches the UDF.
    sql """drop database if exists test_jdbc_mysql_udf_filter_db"""
    sql """create database test_jdbc_mysql_udf_filter_db"""
    sql """use test_jdbc_mysql_udf_filter_db"""
    sql """CREATE FUNCTION udf_add_one(int) RETURNS int PROPERTIES (
        "file"="file://${jarPath}",
        "symbol"="org.apache.doris.udf.IntTest",
        "always_nullable"="true",
        "type"="JAVA_UDF"
    )"""
    sql """CREATE FUNCTION abs(int) RETURNS int PROPERTIES (
        "file"="file://${jarPath}",
        "symbol"="org.apache.doris.udf.IntTest",
        "always_nullable"="true",
        "type"="JAVA_UDF"
    )"""

    sql """drop catalog if exists test_jdbc_mysql_udf_filter_catalog"""
    sql """create catalog test_jdbc_mysql_udf_filter_catalog properties(
        "type"="jdbc",
        "user"="root",
        "password"="123456",
        "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
    )"""
    sql """set enable_ext_func_pred_pushdown = true"""

    // ex_tb0 holds the ids 111 to 115.
    for (String udf : ["test_jdbc_mysql_udf_filter_db.udf_add_one", "test_jdbc_mysql_udf_filter_db.abs"]) {
        explain {
            sql("select id from test_jdbc_mysql_udf_filter_catalog.doris_test.ex_tb0 where ${udf}(id) = 113")
            check { String plan ->
                String remote = plan.readLines().find { it.contains("QUERY:") }
                assertTrue(remote != null, plan)
                assertFalse(remote.contains("WHERE"), remote)
            }
        }
    }
    order_qt_udf_unique_name """select id from test_jdbc_mysql_udf_filter_catalog.doris_test.ex_tb0
        where test_jdbc_mysql_udf_filter_db.udf_add_one(id) = 113"""
    order_qt_udf_builtin_name """select id from test_jdbc_mysql_udf_filter_catalog.doris_test.ex_tb0
        where test_jdbc_mysql_udf_filter_db.abs(id) = 113"""

    // The builtin abs is still pushed.
    explain {
        sql("select id from test_jdbc_mysql_udf_filter_catalog.doris_test.ex_tb0 where abs(id) = 113")
        check { String plan ->
            String remote = plan.readLines().find { it.contains("QUERY:") }
            assertTrue(remote != null && remote.contains("WHERE") && remote.contains("abs("), plan)
        }
    }
    order_qt_builtin """select id from test_jdbc_mysql_udf_filter_catalog.doris_test.ex_tb0 where abs(id) = 113"""
}
