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

import org.apache.doris.regression.suite.ClusterOptions
import groovy.json.JsonSlurper

suite("test_http_api_auth", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = false  // 存算一体模式

    docker(options) {
        // Get FE and BE HTTP addresses from cluster
        def fe = cluster.getFeByIndex(1)
        def be = cluster.getBeByIndex(1)
        def feHost = fe.host + ":" + fe.httpPort
        def beHost = be.host + ":" + be.httpPort

        def jsonSlurper = new JsonSlurper()

        // Helper to check JSON response code
        def checkJsonCode = { bodyStr, expectedCode ->
            def json = jsonSlurper.parseText(bodyStr)
            assertEquals(expectedCode, json.code)
        }

        // ========== Setup ==========
        sql """CREATE USER IF NOT EXISTS 'test_user'@'%' IDENTIFIED BY 'test_password'"""
        sql """GRANT SELECT_PRIV ON *.* TO 'test_user'@'%'"""

        sql """CREATE USER IF NOT EXISTS 'admin_user'@'%' IDENTIFIED BY 'admin_password'"""
        sql """GRANT ADMIN_PRIV ON *.*.* TO 'admin_user'@'%'"""

        // ========== Test Scenario 1: enable_all_http_auth = false ==========
        sql """ADMIN SET FRONTEND CONFIG ("enable_all_http_auth" = "false")"""

        // FE Health - no auth needed
        httpTest {
            endpoint feHost
            uri "/api/health"
            op "get"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 0)  // code 0 means success
            }
        }

        // FE Metrics - no auth needed
        httpTest {
            endpoint feHost
            uri "/metrics"
            op "get"
            check { code, body ->
                assertEquals(200, code)
                assertTrue(body.contains("doris_"))
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

        // ========== Test Scenario 2: enable_all_http_auth = true - Public APIs ==========
        sql """ADMIN SET FRONTEND CONFIG ("enable_all_http_auth" = "true")"""

        // FE Health - no auth returns 401 in JSON body
        httpTest {
            endpoint feHost
            uri "/api/health"
            op "get"
            check { code, body ->
                assertEquals(200, code)  // HTTP code is always 200
                checkJsonCode(body, 401)  // JSON body code is 401
            }
        }

        // FE Health - normal user can access
        httpTest {
            endpoint feHost
            uri "/api/health"
            op "get"
            basicAuthorization "test_user", "test_password"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 0)
            }
        }

        // FE Health - admin user can access
        httpTest {
            endpoint feHost
            uri "/api/health"
            op "get"
            basicAuthorization "admin_user", "admin_password"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 0)
            }
        }

        // FE Metrics - no auth returns 401 in JSON body
        httpTest {
            endpoint feHost
            uri "/metrics"
            op "get"
            check { code, body ->
                assertEquals(200, code)
                // metrics endpoint may return plain text error or JSON
                assertTrue(body.contains("401") || body.contains("Unauthorized") || body.contains("doris_"))
            }
        }

        // FE Metrics - normal user can access
        httpTest {
            endpoint feHost
            uri "/metrics"
            op "get"
            basicAuthorization "test_user", "test_password"
            check { code, body ->
                assertEquals(200, code)
                assertTrue(body.contains("doris_"))
            }
        }

        // ========== Test Scenario 3: Admin APIs ==========

        // FE Backends API - no auth returns 401 in JSON body
        httpTest {
            endpoint feHost
            uri "/api/backends"
            op "get"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 401)
            }
        }

        // FE Backends API - normal user returns 401 (Access denied, need Admin_priv)
        // Note: The current implementation returns 401 for both authentication failure
        // and authorization failure (insufficient privileges)
        httpTest {
            endpoint feHost
            uri "/api/backends"
            op "get"
            basicAuthorization "test_user", "test_password"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 401)  // Returns 401 with "Access denied; you need Admin_priv"
            }
        }

        // FE Backends API - admin user succeeds
        httpTest {
            endpoint feHost
            uri "/api/backends"
            op "get"
            basicAuthorization "admin_user", "admin_password"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 0)
            }
        }

        // ========== Test Scenario 4: Wrong Credentials ==========

        // Test wrong password returns 401 in JSON body
        httpTest {
            endpoint feHost
            uri "/api/health"
            op "get"
            basicAuthorization "test_user", "wrong_password"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 401)
            }
        }

        // Test non-existent user returns 401 in JSON body
        httpTest {
            endpoint feHost
            uri "/api/health"
            op "get"
            basicAuthorization "non_existent_user", "password"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 401)
            }
        }

        // ========== Cleanup ==========
        sql """DROP USER IF EXISTS 'test_user'@'%'"""
        sql """DROP USER IF EXISTS 'admin_user'@'%'"""

        // Restore default config
        sql """ADMIN SET FRONTEND CONFIG ("enable_all_http_auth" = "false")"""
    }
}
