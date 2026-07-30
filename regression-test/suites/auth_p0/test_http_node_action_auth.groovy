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

import org.junit.Assert;

// Verify the node management endpoints (add/drop fe/be/broker) require
// authentication and ADMIN privilege. Without the check, any caller could
// add or drop cluster nodes via these REST APIs.
//
// NOTE on cluster safety: the bogus node addresses below use the RFC 5737
// TEST-NET-1 range (192.0.2.0/24), which can never match a real FE/BE in any
// cluster. The negative (non-admin) cases use ADD, but the ADMIN check runs
// before the node operation, so the add is never executed. The positive
// (admin) cases use DROP, which on a non-existent node returns a harmless
// "does not exist" error -- it never mutates real cluster state.
suite("test_http_node_action_auth", "p0,auth,nonConcurrent") {
    String suiteName = "test_http_node_action_auth"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'
    String bogusFe = "192.0.2.111:12345"
    String bogusBe = "192.0.2.112:12345"
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

    try {
        sql """ ADMIN SET ALL FRONTENDS CONFIG ("enable_all_http_auth" = "true"); """

        def operateFe = { user_name, password, action, check_func ->
            httpTest {
                basicAuthorization "${user_name}", "${password}"
                endpoint "${context.config.feHttpAddress}"
                uri "/rest/v2/manager/node/${action}/fe"
                op "post"
                body """{"role": "OBSERVER", "hostPort": "${bogusFe}"}"""
                check check_func
            }
        }

        def operateBe = { user_name, password, action, check_func ->
            httpTest {
                basicAuthorization "${user_name}", "${password}"
                endpoint "${context.config.feHttpAddress}"
                uri "/rest/v2/manager/node/${action}/be"
                op "post"
                body """{"hostPorts": ["${bogusBe}"]}"""
                check check_func
            }
        }

        // A non-admin user must be rejected by the ADMIN privilege check.
        // The node operation is never reached, so nothing is mutated.
        operateFe.call(user, pwd, "ADD") {
            respCode, body ->
                log.info("add fe (non-admin) body:${body}")
                assertTrue("${body}".contains("Unauthorized"))
        }

        operateBe.call(user, pwd, "ADD") {
            respCode, body ->
                log.info("add be (non-admin) body:${body}")
                assertTrue("${body}".contains("Unauthorized"))
        }

        sql """grant 'admin' to ${user}"""

        // After granting ADMIN, the request passes the auth check. We use DROP
        // on a bogus (TEST-NET) node so the call reaches the operation but only
        // gets a "does not exist" error -- it must no longer be rejected with an
        // authorization error, and must not touch any real node.
        operateFe.call(user, pwd, "DROP") {
            respCode, body ->
                log.info("drop fe (admin) body:${body}")
                assertFalse("${body}".contains("Unauthorized"))
        }

        operateBe.call(user, pwd, "DROP") {
            respCode, body ->
                log.info("drop be (admin) body:${body}")
                assertFalse("${body}".contains("Unauthorized"))
        }

        // The query qerror endpoint must require authentication. Without
        // credentials it must not return the stats payload.
        httpTest {
            endpoint "${context.config.feHttpAddress}"
            uri "/rest/v2/manager/query/qerror/no_such_query_id"
            op "get"
            check {
                respCode, body ->
                    log.info("qerror (no auth) respCode:${respCode} body:${body}")
                    assertTrue(respCode == 401 || "${body}".contains("Unauthorized")
                            || "${body}".contains("Authentication"))
            }
        }
    } finally {
        sql """ ADMIN SET ALL FRONTENDS CONFIG ("enable_all_http_auth" = "false"); """
        try_sql("DROP USER ${user}")
    }
}
