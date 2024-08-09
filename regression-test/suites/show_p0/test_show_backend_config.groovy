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

suite("test_show_backend_config") {
    
    def userName = "test_show_backends_config_user"
    def passwd = "12345"

    sql """drop user if exists ${userName}"""
    sql """create user ${userName} identified by '${passwd}'"""
    sql """grant ADMIN_PRIV on *.*.* to ${userName}"""

    def checkResult = {results, beId, bePort -> 
        for (def row in results) {
            if (row.BackendId == beId && row.Key == "be_port") {
                assertEquals(bePort, row.Value);
                break;
            }
        }
    }

    connect(user = userName, password = passwd, url = context.config.jdbcUrl) {
        def backends = sql_return_maparray """ show backends """
        def beId = backends[0].BackendId
        def bePort = backends[0].BePort

        def result1 = sql_return_maparray """show backend config"""
        checkResult result1, beId, bePort

        // test with pattern
        def result2 = sql_return_maparray """show backend config like 'be_port' """
        checkResult result2, beId, bePort

        // test from beId
        def result3 = sql_return_maparray """show backend config from ${beId} """
        checkResult result3, beId, bePort

        // test from beId with pattern
        def result4 = sql_return_maparray """show backend config like 'be_port' from ${beId}"""
        checkResult result4, beId, bePort
    }
}

