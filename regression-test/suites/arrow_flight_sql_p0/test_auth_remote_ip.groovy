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

import java.sql.Connection
import java.sql.DriverManager

suite("test_auth_remote_ip", "arrow_flight_sql") {
    String user = "flight_auth_remote_ip_user"
    String password = "flight_auth_remote_ip_pwd"
    String wrongPassword = "wrong_flight_auth_remote_ip_pwd"
    List<String> remoteIpHosts = [
            "127.%",
            "10.%",
            "172.%",
            "192.168.%",
            "::1",
            "0:0:0:0:0:0:0:1"
    ]
    List<String> allHosts = ["0.0.0.0"] + remoteIpHosts

    try {
        allHosts.each { host ->
            jdbc_sql """DROP USER IF EXISTS '${user}'@'${host}'"""
        }

        jdbc_sql """CREATE USER '${user}'@'0.0.0.0' IDENTIFIED BY '${wrongPassword}'"""
        String validComputeGroup = null
        if (isCloudMode()) {
            def computeGroups = sql """SHOW COMPUTE GROUPS"""
            assertTrue(!computeGroups.isEmpty())
            validComputeGroup = computeGroups[0][0]
        }
        remoteIpHosts.each { host ->
            jdbc_sql """CREATE USER '${user}'@'${host}' IDENTIFIED BY '${password}'"""
            jdbc_sql """GRANT SELECT_PRIV ON *.* TO '${user}'@'${host}'"""
            if (validComputeGroup != null) {
                jdbc_sql """GRANT USAGE_PRIV ON COMPUTE GROUP '${validComputeGroup}' TO '${user}'@'${host}'"""
            }
        }

        Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")
        String arrowFlightSqlHost = context.config.otherConfigs.get("extArrowFlightSqlHost")
        String arrowFlightSqlPort = context.config.otherConfigs.get("extArrowFlightSqlPort")
        String arrowFlightSqlUrl = "jdbc:arrow-flight-sql://${arrowFlightSqlHost}:${arrowFlightSqlPort}" +
                "/?useServerPrepStmts=false&useSSL=false&useEncryption=false"

        Connection conn = DriverManager.getConnection(arrowFlightSqlUrl, user, password)
        try {
            List<List<Object>> result = sql_impl(conn, "SELECT 1")
            assertEquals(1, result.size())
            assertEquals(1, (result[0][0] as Number).intValue())
        } finally {
            conn.close()
        }
    } finally {
        allHosts.each { host ->
            try {
                jdbc_sql """DROP USER IF EXISTS '${user}'@'${host}'"""
            } catch (Throwable t) {
                logger.warn("Failed to drop test user '${user}'@'${host}'", t)
            }
        }
    }
}
