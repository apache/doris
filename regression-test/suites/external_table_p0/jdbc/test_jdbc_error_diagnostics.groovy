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

// Remote codes stay separate from Doris errCode across FE and BE JDBC failures.
suite("test_jdbc_error_diagnostics", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableJdbcTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("mysql_57_port")
    String driverUrl = context.config.otherConfigs.get("jdbcDiagnosticsDriverUrl")
    if (driverUrl == null) {
        driverUrl = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    }
    String adminUser = context.config.otherConfigs.get("jdbcDiagnosticsUser") ?: "root"
    String adminPassword = context.config.otherConfigs.get("jdbcDiagnosticsPassword") ?: "123456"
    String jdbcUrl = "jdbc:mysql://${host}:${port}/doris_test?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=1000&socketTimeout=5000"

    ["jdbc_diag_admin", "jdbc_diag_reader", "jdbc_diag_bad_auth", "jdbc_diag_bad_connection"].each { name ->
        sql "drop catalog if exists ${name}"
    }
    sql """create catalog jdbc_diag_admin properties (
        "type"="jdbc", "user"="${adminUser}", "password"="${adminPassword}",
        "jdbc_url"="${jdbcUrl}", "driver_url"="${driverUrl}",
        "driver_class"="com.mysql.cj.jdbc.Driver"
    )"""

    // The remote fixture belongs to this suite; keep it after execution for debugging.
    sql """call execute_stmt("jdbc_diag_admin", "DROP TABLE IF EXISTS doris_test.jdbc_diag_table")"""
    sql """call execute_stmt("jdbc_diag_admin", "CREATE TABLE doris_test.jdbc_diag_table (id INT PRIMARY KEY)")"""
    sql """call execute_stmt("jdbc_diag_admin", "INSERT INTO doris_test.jdbc_diag_table VALUES (1)")"""
    sql """call execute_stmt("jdbc_diag_admin", "DROP USER IF EXISTS 'doris_jdbc_diag_reader'@'%'")"""
    sql """call execute_stmt("jdbc_diag_admin", "CREATE USER 'doris_jdbc_diag_reader'@'%' IDENTIFIED BY 'jdbc-diag-reader-password'")"""
    sql """call execute_stmt("jdbc_diag_admin", "GRANT SELECT ON doris_test.jdbc_diag_table TO 'doris_jdbc_diag_reader'@'%'")"""
    sql """create catalog jdbc_diag_reader properties (
        "type"="jdbc", "user"="doris_jdbc_diag_reader", "password"="jdbc-diag-reader-password",
        "jdbc_url"="${jdbcUrl}", "driver_url"="${driverUrl}",
        "driver_class"="com.mysql.cj.jdbc.Driver"
    )"""

    order_qt_read "select * from jdbc_diag_reader.doris_test.jdbc_diag_table"

    // FE JDBC statement execution: a remote syntax error.
    test {
        sql """call execute_stmt("jdbc_diag_admin", "SELCT id FROM doris_test.jdbc_diag_table")"""
        exception "remote_sqlstate=42000, remote_vendor_error_code=1064"
    }
    // BE JDBC writer: metadata is readable, but INSERT is denied remotely.
    test {
        sql "insert into jdbc_diag_reader.doris_test.jdbc_diag_table values (2)"
        exception "remote_sqlstate=42000, remote_vendor_error_code=1142"
    }
    // BE batch execution: a remote constraint error, often carried by nextException.
    test {
        sql "insert into jdbc_diag_admin.doris_test.jdbc_diag_table values (1)"
        exception "remote_sqlstate=23000, remote_vendor_error_code=1062"
    }
    // CREATE connection validation: rejected credentials.
    test {
        sql """create catalog jdbc_diag_bad_auth properties (
            "type"="jdbc", "user"="${adminUser}", "password"="definitely-not-the-password",
            "jdbc_url"="${jdbcUrl}", "driver_url"="${driverUrl}",
            "driver_class"="com.mysql.cj.jdbc.Driver"
        )"""
        exception "remote_sqlstate=28000, remote_vendor_error_code=1045"
    }
    // A loopback port reserved for the intentionally unreachable JDBC target.
    String closedPort = context.config.otherConfigs.get("jdbcDiagnosticsClosedPort") ?: "3399"
    test {
        sql """create catalog jdbc_diag_bad_connection properties (
            "type"="jdbc", "user"="${adminUser}", "password"="${adminPassword}",
            "jdbc_url"="jdbc:mysql://127.0.0.1:${closedPort}/doris_test?connectTimeout=1000",
            "driver_url"="${driverUrl}", "driver_class"="com.mysql.cj.jdbc.Driver",
            "connection_pool_min_size"="0", "connection_pool_max_wait_time"="1000"
        )"""
        exception "remote_sqlstate=08S01, remote_vendor_error_code=0"
    }
    order_qt_unchanged "select * from jdbc_diag_admin.doris_test.jdbc_diag_table"
}
