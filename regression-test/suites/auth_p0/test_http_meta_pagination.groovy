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

suite("test_http_meta_pagination", "p0,auth,nonConcurrent") {
    def getMeta = { uriPath, checkFunc ->
        httpTest {
            basicAuthorization "${context.config.jdbcUser}", "${context.config.jdbcPassword}"
            endpoint "${context.config.feHttpAddress}"
            uri uriPath
            op "get"
            check checkFunc
        }
    }

    def assertEmptyPage = { uriPath ->
        getMeta.call(uriPath) {
            respCode, body ->
                assertEquals(200, respCode)
                def json = parseJson(body)
                assertEquals(0, json.code)
                assertEquals([], json.data)
        }
    }

    def assertBadRequest = { uriPath, message ->
        getMeta.call(uriPath) {
            respCode, body ->
                assertEquals(200, respCode)
                def json = parseJson(body)
                assertEquals(403, json.code)
                assertEquals(message, json.data)
        }
    }

    [
            "/api/meta/namespaces/default_cluster/databases",
            "/rest/v2/api/meta/namespaces"
    ].each { uriPath ->
        assertEmptyPage.call("${uriPath}?limit=1&offset=999999999")
        assertEmptyPage.call("${uriPath}?limit=2147483647&offset=2147483647")

        getMeta.call("${uriPath}?limit=2147483648") {
            respCode, body ->
                assertEquals(200, respCode)
                assertEquals(0, parseJson(body).code)
        }

        assertBadRequest.call("${uriPath}?limit=9223372036854775808",
                "Param limit should be a non-negative integer")
        assertBadRequest.call("${uriPath}?limit=1&offset=invalid",
                "Param offset should be a non-negative integer")
    }
}
