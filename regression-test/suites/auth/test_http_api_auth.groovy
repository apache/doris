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

suite("test_http_api_auth", "p0,docker") {

    def feHost = context.config.feHttpAddress
    def beHost = context.config.beHttpAddress

    // ========== Setup ==========
    sql """CREATE USER IF NOT EXISTS 'test_user'@'%' IDENTIFIED BY 'test_password'"""
    sql """GRANT SELECT_PRIV ON *.* TO 'test_user'@'%'"""

    sql """CREATE USER IF NOT EXISTS 'admin_user'@'%' IDENTIFIED BY 'admin_password'"""
    sql """GRANT ADMIN_PRIV ON *.* TO 'admin_user'@'%'"""

    sql """CREATE DATABASE IF NOT EXISTS test_http_auth_db"""
    sql """CREATE TABLE IF NOT EXISTS test_http_auth_db.test_table (
        id INT,
        name VARCHAR(100)
    ) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1")"""

    sql """GRANT LOAD_PRIV ON test_http_auth_db.* TO 'test_user'@'%'"""

    // ========== Test Scenario 1: enable_all_http_auth = false ==========
    sql """ADMIN SET FRONTEND CONFIG ("enable_all_http_auth" = "false")"""

    // FE Health - no auth needed
    def result = httpTest {
        endpoint feHost
        uri "/api/health"
        op "get"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // FE Metrics - no auth needed
    httpTest {
        endpoint feHost
        uri "/metrics"
        op "get"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // FE Admin API (set_config) - no auth needed
    httpTest {
        endpoint feHost
        uri "/api/_set_config?query_timeout=60"
        op "post"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // BE Health - no auth needed
    httpTest {
        endpoint beHost
        uri "/api/health"
        op "get"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // BE Metrics - no auth needed
    httpTest {
        endpoint beHost
        uri "/metrics"
        op "get"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // ========== Test Scenario 2: enable_all_http_auth = true - Public APIs ==========
    sql """ADMIN SET FRONTEND CONFIG ("enable_all_http_auth" = "true")"""

    // FE Health - no auth returns 401
    httpTest {
        endpoint feHost
        uri "/api/health"
        op "get"
        check { code, body ->
            assertEquals(401, code)
        }
    }

    // FE Health - normal user can access
    httpTest {
        endpoint feHost
        uri "/api/health"
        op "get"
        user "test_user"
        password "test_password"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // FE Health - admin user can access
    httpTest {
        endpoint feHost
        uri "/api/health"
        op "get"
        user "admin_user"
        password "admin_password"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // FE Metrics - similar tests
    httpTest {
        endpoint feHost
        uri "/metrics"
        op "get"
        check { code, body ->
            assertEquals(401, code)
        }
    }

    httpTest {
        endpoint feHost
        uri "/metrics"
        op "get"
        user "test_user"
        password "test_password"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // BE Health - similar tests
    httpTest {
        endpoint beHost
        uri "/api/health"
        op "get"
        check { code, body ->
            assertEquals(401, code)
        }
    }

    httpTest {
        endpoint beHost
        uri "/api/health"
        op "get"
        user "test_user"
        password "test_password"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // ========== Test Scenario 3: Stream Load ==========

    // Stream Load - no auth returns 401
    streamLoad {
        table "test_table"
        db "test_http_auth_db"
        file "test_data.csv"
        check { code, msg ->
            assertEquals(401, code)
        }
    }

    // Stream Load - user with LOAD privilege succeeds
    streamLoad {
        table "test_table"
        db "test_http_auth_db"
        file "test_data.csv"
        user "test_user"
        password "test_password"
        check { code, msg ->
            assertEquals(200, code)
        }
    }

    // Stream Load - admin user succeeds
    streamLoad {
        table "test_table"
        db "test_http_auth_db"
        file "test_data.csv"
        user "admin_user"
        password "admin_password"
        check { code, msg ->
            assertEquals(200, code)
        }
    }

    // Stream Load - user without LOAD privilege fails
    sql """CREATE USER IF NOT EXISTS 'no_load_user'@'%' IDENTIFIED BY 'password'"""
    streamLoad {
        table "test_table"
        db "test_http_auth_db"
        file "test_data.csv"
        user "no_load_user"
        password "password"
        check { code, msg ->
            assertEquals(403, code)  // Forbidden
        }
    }

    // ========== Test Scenario 4: Admin APIs ==========

    // FE Set Config - no auth returns 401
    httpTest {
        endpoint feHost
        uri "/api/_set_config?query_timeout=60"
        op "post"
        check { code, body ->
            assertEquals(401, code)
        }
    }

    // FE Set Config - normal user returns 403
    httpTest {
        endpoint feHost
        uri "/api/_set_config?query_timeout=60"
        op "post"
        user "test_user"
        password "test_password"
        check { code, body ->
            assertEquals(403, code)  // Forbidden
        }
    }

    // FE Set Config - admin user succeeds
    httpTest {
        endpoint feHost
        uri "/api/_set_config?query_timeout=60"
        op "post"
        user "admin_user"
        password "admin_password"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // BE Update Config - similar tests
    httpTest {
        endpoint beHost
        uri "/api/update_config?query_timeout=60"
        op "post"
        check { code, body ->
            assertEquals(401, code)
        }
    }

    httpTest {
        endpoint beHost
        uri "/api/update_config?query_timeout=60"
        op "post"
        user "test_user"
        password "test_password"
        check { code, body ->
            assertEquals(403, code)
        }
    }

    httpTest {
        endpoint beHost
        uri "/api/update_config?query_timeout=60"
        op "post"
        user "admin_user"
        password "admin_password"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // ========== Test Scenario 5: Additional Admin APIs ==========

    // Test FE Backends API - requires admin privilege
    httpTest {
        endpoint feHost
        uri "/api/backends"
        op "get"
        check { code, body ->
            assertEquals(401, code)
        }
    }

    httpTest {
        endpoint feHost
        uri "/api/backends"
        op "get"
        user "test_user"
        password "test_password"
        check { code, body ->
            assertEquals(403, code)
        }
    }

    httpTest {
        endpoint feHost
        uri "/api/backends"
        op "get"
        user "admin_user"
        password "admin_password"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // Test FE Profile API - requires admin privilege
    httpTest {
        endpoint feHost
        uri "/api/profile"
        op "get"
        check { code, body ->
            assertEquals(401, code)
        }
    }

    httpTest {
        endpoint feHost
        uri "/api/profile"
        op "get"
        user "test_user"
        password "test_password"
        check { code, body ->
            assertEquals(403, code)
        }
    }

    httpTest {
        endpoint feHost
        uri "/api/profile"
        op "get"
        user "admin_user"
        password "admin_password"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // Test FE Query Stats API - requires admin privilege
    httpTest {
        endpoint feHost
        uri "/api/query_stats"
        op "get"
        check { code, body ->
            assertEquals(401, code)
        }
    }

    httpTest {
        endpoint feHost
        uri "/api/query_stats"
        op "get"
        user "admin_user"
        password "admin_password"
        check { code, body ->
            assertEquals(200, code)
        }
    }

    // ========== Test Scenario 6: Special Cases ==========

    // Test that metrics API works with any valid user (not admin required)
    httpTest {
        endpoint feHost
        uri "/metrics"
        op "get"
        user "test_user"
        password "test_password"
        check { code, body ->
            assertEquals(200, code)
            assertTrue(body.contains("doris_"))  // Verify metrics content
        }
    }

    httpTest {
        endpoint beHost
        uri "/metrics"
        op "get"
        user "test_user"
        password "test_password"
        check { code, body ->
            assertEquals(200, code)
            assertTrue(body.contains("doris_"))
        }
    }

    // Test that health API works with any valid user (not admin required)
    httpTest {
        endpoint feHost
        uri "/api/health"
        op "get"
        user "test_user"
        password "test_password"
        check { code, body ->
            assertEquals(200, code)
            assertTrue(body.contains("OK") || body.contains("ok"))
        }
    }

    httpTest {
        endpoint beHost
        uri "/api/health"
        op "get"
        user "test_user"
        password "test_password"
        check { code, body ->
            assertEquals(200, code)
            assertTrue(body.contains("OK") || body.contains("ok"))
        }
    }

    // ========== Test Scenario 7: Wrong Credentials ==========

    // Test wrong password returns 401
    httpTest {
        endpoint feHost
        uri "/api/health"
        op "get"
        user "test_user"
        password "wrong_password"
        check { code, body ->
            assertEquals(401, code)
        }
    }

    // Test non-existent user returns 401
    httpTest {
        endpoint feHost
        uri "/api/health"
        op "get"
        user "non_existent_user"
        password "password"
        check { code, body ->
            assertEquals(401, code)
        }
    }

    // ========== Cleanup ==========
    sql """DROP TABLE IF EXISTS test_http_auth_db.test_table"""
    sql """DROP DATABASE IF EXISTS test_http_auth_db"""
    sql """DROP USER IF EXISTS 'test_user'@'%'"""
    sql """DROP USER IF EXISTS 'admin_user'@'%'"""
    sql """DROP USER IF EXISTS 'no_load_user'@'%'"""

    // Restore default config
    sql """ADMIN SET FRONTEND CONFIG ("enable_all_http_auth" = "false")"""
}
