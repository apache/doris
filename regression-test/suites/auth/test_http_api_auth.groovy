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
    def jsonSlurper = new JsonSlurper()

    // Helper to check JSON response code
    def checkJsonCode = { bodyStr, expectedCode ->
        def json = jsonSlurper.parseText(bodyStr)
        assertEquals(expectedCode, json.code)
    }

    // ========== Test Scenario 1: enable_all_http_auth = false ==========
    // enable_all_http_auth is not a mutable config, so the disabled path can only be exercised
    // by starting a cluster with it turned off in fe.conf.
    def authOffOptions = new ClusterOptions()
    authOffOptions.cloudMode = false  // 存算一体模式
    authOffOptions.feConfigs += ['enable_all_http_auth=false']

    docker(authOffOptions) {
        def fe = cluster.getFeByIndex(1)
        def be = cluster.getBeByIndex(1)
        def feHost = fe.host + ":" + fe.httpPort
        def beHost = be.host + ":" + be.httpPort

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

        // FE Metrics - no auth needed while the flag is off; the default-on block below gets 401
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

        // The health assertions above hold no matter how the flag is set, so on their own they
        // would still pass if this cluster's startup override were ignored or misspelled. The
        // metrics one and this one depend on the flag: /api/show_runtime_info gates its whole
        // auth block on enable_all_http_auth, so with the flag off an anonymous caller gets the
        // payload, and the default-on block below proves the same request is rejected when it is on.
        httpTest {
            endpoint feHost
            uri "/api/show_runtime_info"
            op "get"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 0)
                assertTrue("${body}".contains("thread_cnt"))
            }
        }
    }

    // ========== enable_all_http_auth = true (the default) ==========
    def options = new ClusterOptions()
    options.cloudMode = false  // 存算一体模式
    // BE ships with the flag off; turn it on here so the BE side of "health is public, the rest
    // needs credentials" is exercised too.
    options.beConfigs.add("enable_all_http_auth=true")

    docker(options) {
        // Get FE and BE HTTP addresses from cluster
        def fe = cluster.getFeByIndex(1)
        def be = cluster.getBeByIndex(1)
        def feHost = fe.host + ":" + fe.httpPort
        def beHost = be.host + ":" + be.httpPort

        // ========== Setup ==========
        sql """CREATE USER IF NOT EXISTS 'test_user'@'%' IDENTIFIED BY 'test_password'"""
        sql """GRANT SELECT_PRIV ON *.* TO 'test_user'@'%'"""

        sql """CREATE USER IF NOT EXISTS 'admin_user'@'%' IDENTIFIED BY 'admin_password'"""
        sql """GRANT ADMIN_PRIV ON *.*.* TO 'admin_user'@'%'"""

        // ========== Test Scenario 2: Health is public, metrics need credentials ==========
        // /api/health is the one endpoint the flag leaves open, on FE and on BE, because probes and
        // load balancers poll it without credentials. /metrics is gated like everything else.

        // FE Health - still accessible without auth (public endpoint)
        httpTest {
            endpoint feHost
            uri "/api/health"
            op "get"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 0)  // Health is always public
            }
        }

        // FE Health - also works with auth
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

        // FE Metrics - an anonymous scrape is rejected with a real HTTP 401, not with the HTTP 200
        // JSON envelope the other REST endpoints use, because a scraper reads the status code.
        httpTest {
            endpoint feHost
            uri "/metrics"
            op "get"
            check { code, body ->
                assertEquals(401, code)
                assertFalse(body.contains("doris_"))
            }
        }

        // FE Metrics - a wrong password is rejected the same way
        httpTest {
            endpoint feHost
            uri "/metrics"
            op "get"
            basicAuthorization "test_user", "wrong_password"
            check { code, body ->
                assertEquals(401, code)
                assertFalse(body.contains("doris_"))
            }
        }

        // FE Metrics - any valid account may read it: test_user holds SELECT_PRIV only
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

        // BE Health - still answers anonymous probes with the BE flag on
        httpTest {
            endpoint beHost
            uri "/api/health"
            op "get"
            check { code, body ->
                assertEquals(200, code)
                assertEquals("OK", jsonSlurper.parseText(body).status)
            }
        }

        // BE Metrics - unlike health, needs credentials once the BE flag is on
        httpTest {
            endpoint beHost
            uri "/metrics"
            op "get"
            check { code, body ->
                assertEquals(401, code)
            }
        }

        httpTest {
            endpoint beHost
            uri "/metrics"
            op "get"
            basicAuthorization "test_user", "test_password"
            check { code, body ->
                assertEquals(200, code)
                assertTrue(body.contains("doris_be_"))
            }
        }

        // ========== Test Scenario 3: The flag actually took effect ==========

        // Counterpart of the auth-off block's request: with the flag on, the same anonymous
        // request to /api/show_runtime_info must be rejected.
        httpTest {
            endpoint feHost
            uri "/api/show_runtime_info"
            op "get"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 401)
            }
        }

        // A valid but non-admin account is authenticated and then refused by the ADMIN check.
        httpTest {
            endpoint feHost
            uri "/api/show_runtime_info"
            op "get"
            basicAuthorization "test_user", "test_password"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 401)  // "Access denied; you need ... Admin_priv"
            }
        }

        // An admin account gets the payload.
        httpTest {
            endpoint feHost
            uri "/api/show_runtime_info"
            op "get"
            basicAuthorization "admin_user", "admin_password"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 0)
                assertTrue("${body}".contains("thread_cnt"))
            }
        }

        // ========== Test Scenario 3b: Password-only APIs ==========

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

        // BackendsAction only calls executeCheckPassword -- it deliberately has no privilege
        // check, because the Flink/Spark connectors call it with an ordinary load account to
        // discover backends before a stream load. So a valid non-admin user succeeds here. This
        // endpoint authenticates; it does not authorize.
        httpTest {
            endpoint feHost
            uri "/api/backends"
            op "get"
            basicAuthorization "test_user", "test_password"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 0)
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

        // ========== Test Scenario 4: Wrong Credentials on Admin APIs ==========

        // Test wrong password returns 401 on admin API
        httpTest {
            endpoint feHost
            uri "/api/backends"
            op "get"
            basicAuthorization "admin_user", "wrong_password"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 401)
            }
        }

        // Test non-existent user returns 401 on admin API
        httpTest {
            endpoint feHost
            uri "/api/backends"
            op "get"
            basicAuthorization "non_existent_user", "password"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 401)
            }
        }

        // Health endpoint works even with wrong credentials (public endpoint)
        httpTest {
            endpoint feHost
            uri "/api/health"
            op "get"
            basicAuthorization "test_user", "wrong_password"
            check { code, body ->
                assertEquals(200, code)
                checkJsonCode(body, 0)  // Health is always public
            }
        }

        // ========== Cleanup ==========
        sql """DROP USER IF EXISTS 'test_user'@'%'"""
        sql """DROP USER IF EXISTS 'admin_user'@'%'"""
    }
}
