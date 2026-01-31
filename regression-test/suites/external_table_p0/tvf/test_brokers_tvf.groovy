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

// This suit test the `brokers` tvf
suite("test_brokers_tvf","p0") {
    def brokerClusterName = "brokers_tvf_test_cluster"
    try {
        def address1 = "172.18.0.1"
        def address2 = "172.18.0.2"
        def address3 = "172.18.0.3"
        def port = 18310
        def columns = new ArrayList()
        def columnMeta = sql """describe function brokers();"""
        def size = columnMeta.size()
        for (i in 0..<size) {
            columns.add(columnMeta[i][0])
        }
        def columnsExpr = String.join(",", columns)
        log.info("columns expr: ${columnsExpr}")
        def brokers = sql """select ${columnsExpr} from brokers()"""
        assertTrue(brokers.size() == 0)
        brokers = sql """show broker;"""
        assertTrue(brokers.size() == 0)
        def brokerNodes = "\"${address1}:${port}\",\"${address2}:${port}\",\"${address3}:${port}\""
        log.info("broker nodes to add: ${brokerNodes}")
        sql """alter system add broker ${brokerClusterName} ${brokerNodes};"""
        brokers = sql """select ${columnsExpr} from brokers()"""
        assertTrue(brokers.size() == 3)
        def explainResult = sql """explain verbose select * from brokers();"""
        // table name is BrokersTableValuedFunction certainly
        log.info("explain result: ${explainResult}")
        def explainStr = explainResult.collect { row -> row.join("") }.join("\n")
        explainStr.contains("BrokersTableValuedFunction")
        for (i in 0..<size) {
            def colName = columns[i]
            log.info("current column name: ${colName}")
            assertTrue(explainStr.contains("${colName}"))
        }
        def tvfSelectResult = sql """select * from brokers() where name='${brokerClusterName}';"""
        def showBrokerResult = sql """show broker;"""
        def showBrokerProcResult = sql """show proc '/brokers';"""
        log.info("tvfSelectResult0: ${tvfSelectResult}")
        log.info("showBrokerResult0: ${showBrokerResult}")
        log.info("showBrokerProcResult0: ${showBrokerProcResult}")
        assertTrue(tvfSelectResult.size() == 3)
        assertTrue(showBrokerResult.size() == 3)
        assertTrue(showBrokerProcResult.size() == 3)
        for (i in 0..<tvfSelectResult.size()) {
            for (j in 0..<columns.size() - 3) {
                // check Name,Host,Port,Alive columns values are the same
                assertTrue(showBrokerResult[i][j] == tvfSelectResult[i][j])
                assertTrue(showBrokerProcResult[i][j] == tvfSelectResult[i][j])
            }
        }
        tvfSelectResult = sql """select * from brokers() where host='${address1}';"""
        log.info("tvfSelectResult1: ${tvfSelectResult}")
        assertTrue(tvfSelectResult.size() == 1)
        assertTrue(tvfSelectResult[0][1] == "${address1}")
        tvfSelectResult = sql """select * from brokers() where host in('${address2}', '${address3}') order by host;"""
        log.info("tvfSelectResult2: ${tvfSelectResult}")
        assertTrue(tvfSelectResult.size() == 2)
        assertTrue(tvfSelectResult[0][1] == "${address2}")
        assertTrue(tvfSelectResult[1][1] == "${address3}")
    } finally {
        sql """ALTER SYSTEM DROP ALL BROKER ${brokerClusterName};"""
    }
}
