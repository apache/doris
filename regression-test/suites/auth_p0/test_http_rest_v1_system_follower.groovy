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

suite("test_http_rest_v1_system_follower", "docker") {
    def options = new ClusterOptions()
    options.setFeNum(2)
    options.setBeNum(1)
    docker(options) {
        def follower = cluster.getAllFrontends(true).find { !it.isMaster }
        assertNotNull(follower)

        // Wait for the follower to replay the journal before authenticating over HTTP.
        String followerJdbcUrl = "jdbc:mysql://${follower.host}:${follower.queryPort}/"
        connect(context.config.jdbcUser, context.config.jdbcPassword, followerJdbcUrl) {
            sql "SYNC"
        }

        httpTest {
            basicAuthorization "${context.config.jdbcUser}", "${context.config.jdbcPassword}"
            endpoint "${follower.host}:${follower.httpPort}"
            uri "/rest/v1/system?path=/"
            op "get"
            check { respCode, body ->
                assertEquals(200, respCode)
                assertEquals(0, parseJson(body).code)
            }
        }
    }
}
