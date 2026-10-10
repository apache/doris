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

suite("test_mysql_query_tvf_sql_mode", "p0,external") {
    if (!context.config.otherConfigs.get("enableJdbcTest")?.toString()?.equalsIgnoreCase("true")) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("mysql_57_port")
    String bucket = getS3BucketName()
    String s3_endpoint = getS3Endpoint()
    String driver = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    sql "SET time_zone = '+00:00'"
    for (boolean noBackslash : [false, true]) {
        String catalog = "mysql_uuid_query_mode_${noBackslash}"
        String mode = noBackslash ? "NO_BACKSLASH_ESCAPES" : ""
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            "type"="jdbc", "user"="root", "password"="123456",
            "jdbc_url"="jdbc:mysql://${host}:${port}/doris_test?useSSL=false&sessionVariables=sql_mode='${mode}'",
            "driver_url"="${driver}", "driver_class"="com.mysql.cj.jdbc.Driver")"""
        def remoteExecute = { String query ->
            sql("CALL EXECUTE_STMT('${catalog}', '" + query.replace("'", "''") + "')")
        }
        remoteExecute("DROP TABLE IF EXISTS doris_test.uuid_query_mode")
        remoteExecute("CREATE TABLE doris_test.uuid_query_mode (ts TIMESTAMP, balance INT, label VARCHAR(20))")
        remoteExecute("INSERT INTO doris_test.uuid_query_mode VALUES " +
                "('2024-01-02 03:04:05', 5, CONCAT('a', CHAR(92)))")
        def runQuery = { String tag, String remote ->
            String escaped = remote.replace("\\", "\\\\").replace('"', '\\"')
            "qt_${tag}_${noBackslash}" """SELECT * FROM query("catalog"="${catalog}", "query"="${escaped}")"""
        }
        // Both queries project a TIMESTAMP so the UTC-preserving wrapper is exercised.
        runQuery("arithmetic", "SELECT ts, balance--1 AS n FROM doris_test.uuid_query_mode;")
        String literal = noBackslash ? "'a\\'" : "'a\\\\'"
        runQuery("backslash", "SELECT ts, balance FROM doris_test.uuid_query_mode WHERE label=${literal}; -- tail")
    }
}
