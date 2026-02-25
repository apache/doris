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

suite('test_unfinished_txn_2pc', 'multi_cluster,docker') {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.feConfigs += [
            'cloud_cluster_check_interval_second=1',
    ]

    docker(options) {
        def clusterInfo = sql_return_maparray """show clusters"""
        def currentCluster = clusterInfo.find { it.is_current == 'TRUE' }
        assertNotNull(currentCluster)
        sql """use @${currentCluster.cluster}"""

        def dbName = context.config.getDbNameByFile(context.file)
        def tableName = 'test_unfinished_txn_2pc_tbl'
        Long txnId = null

        def doStreamLoad2pcOperation = { long id, String operation ->
            def feHttpAddress = context.getFeHttpAddress()
            def command = "curl -sS -X PUT --location-trusted -u root: " +
                    " -H txn_id:${id}" +
                    " -H txn_operation:${operation}" +
                    " http://${feHttpAddress.hostString}:${feHttpAddress.port}/api/${dbName}/${tableName}/_stream_load_2pc"
            logger.info("execute stream load 2pc operation: {}", command)

            def process = command.execute()
            def code = process.waitFor()
            def out = process.text
            logger.info("stream load 2pc {} result: {}", operation, out)
            assertEquals(0, code)

            def resultText = out == null ? "" : out.trim()
            assertTrue(resultText.startsWith("{"), "stream load 2pc ${operation} non-json response: ${resultText}")
            def json = parseJson(resultText)
            def status = ((json.status != null ? json.status : json.Status) as String).toLowerCase()
            assertEquals('success', status)
        }

        try {
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    k1 INT,
                    k2 INT
                )
                DUPLICATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """

            String content = "1,10\n2,20\n3,30\n"
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'two_phase_commit', 'true'
                inputStream new ByteArrayInputStream(content.getBytes())
                time 10000

                check { loadResult, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    logger.info("stream load result: {}", loadResult)
                    def json = parseJson(loadResult)
                    def status = ((json.status != null ? json.status : json.Status) as String).toLowerCase()
                    assertEquals('success', status)
                    txnId = Long.valueOf(json.TxnId.toString())
                    assertTrue(txnId != null && txnId > 0)
                }
            }

            def rowCount = sql "select count(*) from ${tableName}"
            assertEquals(0, rowCount[0][0] as int)

            def finalStatuses = ['VISIBLE', 'ABORTED'] as Set
            for (int i = 0; i < 20; i++) {
                def txns = sql_return_maparray "show transaction from ${dbName} where id = ${txnId}"
                assertEquals(1, txns.size())
                def txnStatus = txns[0].TransactionStatus as String
                logger.info("txn {} status after {}s: {}", txnId, i, txnStatus)
                assertTrue(!finalStatuses.contains(txnStatus))
                sleep(1000)
            }
        } finally {
            if (txnId != null) {
                try {
                    doStreamLoad2pcOperation(txnId, 'abort')
                    awaitUntil(30) {
                        def txns = sql_return_maparray "show transaction from ${dbName} where id = ${txnId}"
                        txns.size() == 1 && (txns[0].TransactionStatus as String) == 'ABORTED'
                    }
                } catch (Exception e) {
                    logger.info("abort unfinished txn {} failed in cleanup: {}", txnId, e.getMessage())
                }
            }
            sql "drop table if exists ${tableName}"
        }
    }
}
