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

// A MySQL 9 client library announces caching_sha2_password and cannot switch to
// mysql_native_password; the FE then takes it through the plugin's full authentication. The
// framework's Connector/J is made to announce the plugin the same way, and to fetch the FE's RSA
// public key like a client without TLS does; with the FE config set to "all" it is served like a
// MySQL 9 client, with "none" it is switched to mysql_native_password as before.
suite("test_caching_sha2_password", "p0,auth,nonConcurrent") {
    def user = "test_caching_sha2_password_user"
    def password = "Sha2_pass_123"
    def url = context.config.jdbcUrl + (context.config.jdbcUrl.contains("?") ? "&" : "?") +
            "useSSL=false&defaultAuthenticationPlugin=caching_sha2_password&allowPublicKeyRetrieval=true"
    def oldMode = sql("ADMIN SHOW FRONTEND CONFIG LIKE 'mysql_caching_sha2_password_clients'")[0][1]

    sql "DROP USER IF EXISTS '${user}'"
    sql "CREATE USER '${user}' IDENTIFIED BY '${password}'"
    // connect() switches to the suite's database
    sql "GRANT SELECT_PRIV ON *.*.* TO '${user}'"
    try {
        sql "ADMIN SET FRONTEND CONFIG ('mysql_caching_sha2_password_clients' = 'all')"

        connect(user, password, url) {
            def rows = sql "SELECT CURRENT_USER()"
            assertEquals("'${user}'@'%'".toString(), rows[0][0])
        }

        def denied = false
        try {
            connect(user, "wrong_password", url) {
                sql "SELECT 1"
            }
        } catch (Exception e) {
            denied = e.getMessage().contains("Access denied")
        }
        assertTrue(denied)

        sql "ADMIN SET FRONTEND CONFIG ('mysql_caching_sha2_password_clients' = 'none')"
        connect(user, password, url) {
            def rows = sql "SELECT CURRENT_USER()"
            assertEquals("'${user}'@'%'".toString(), rows[0][0])
        }
    } finally {
        sql "ADMIN SET FRONTEND CONFIG ('mysql_caching_sha2_password_clients' = '${oldMode}')"
    }
}
