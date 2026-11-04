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

// The /rest/v1 endpoints behind the Web UI require ADMIN_OR_NODE. HTTP Basic requests are checked for it
// just as session-cookie requests are, in every deployment mode. /rest/v1/login itself only authenticates,
// so the UI can tell an account without the privilege apart from a failed sign-in.
suite("test_http_rest_v1_auth", "p0,auth") {
    String user = "test_http_rest_v1_auth_user"
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

    def uris = [
            "/rest/v1/system?path=/",
            "/rest/v1/config/fe",
            "/rest/v1/session",
            "/rest/v1/query_profile",
            "/rest/v1/log",
            "/rest/v1/ha"
    ]

    def getRestV1 = { uriPath, checkFunc ->
        httpTest {
            basicAuthorization "${user}", "${pwd}"
            endpoint "${context.config.feHttpAddress}"
            uri uriPath
            op "get"
            check checkFunc
        }
    }

    def login = { checkFunc ->
        httpTest {
            basicAuthorization "${user}", "${pwd}"
            endpoint "${context.config.feHttpAddress}"
            uri "/rest/v1/login"
            op "post"
            body "{}"
            check checkFunc
        }
    }

    uris.each { uriPath ->
        getRestV1.call(uriPath) {
            respCode, body ->
                log.info("${uriPath} (no privilege) body:${body}")
                assertEquals(200, respCode)
                assertEquals(401, parseJson(body).code)
        }
    }

    login.call {
        respCode, body ->
            log.info("login (no privilege) body:${body}")
            assertEquals(200, respCode)
            assertEquals(200, parseJson(body).code)
    }

    sql """GRANT 'admin' TO '${user}'"""

    uris.each { uriPath ->
        getRestV1.call(uriPath) {
            respCode, body ->
                log.info("${uriPath} (admin) body:${body}")
                assertEquals(200, respCode)
                assertEquals(0, parseJson(body).code)
        }
    }

    login.call {
        respCode, body ->
            log.info("login (admin) body:${body}")
            assertEquals(200, respCode)
            assertEquals(200, parseJson(body).code)
    }
}
