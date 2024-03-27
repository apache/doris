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
import org.apache.http.NoHttpResponseException

suite('test_coordidator_be_restart') {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.enableDebugPoints()

    docker(options) {
        def tableName = 'tbl_test_coordidator_be_restart'
        setFeConfig('abort_txn_after_lost_heartbeat_time_second', 3600)
        GetDebugPoint().enableDebugPointForAllBEs('StreamLoadExecutor.commit_txn.block')
        def dbId = getDbId()

        def txns = sql_return_maparray "show proc '/transactions/${dbId}/running'"
        assertEquals(0, txns.size())
        txns = sql_return_maparray "show proc '/transactions/${dbId}/finished'"
        assertEquals(0, txns.size())

        def coordinatorBe = cluster.getAllBackends().get(0)
        def coordinatorBeHost = coordinatorBe.host

        def future = thread {
            try {
                runStreamLoadExample(tableName, coordinatorBe.host + ':' + coordinatorBe.httpPort)
            } catch (NoHttpResponseException t) {
            // be down  will raise NoHttpResponseException
            }
        }

        sleep(5000)
        txns = sql_return_maparray "show proc '/transactions/${dbId}/running'"
        assertEquals(1, txns.size())
        assertEquals('PREPARE', txns.get(0).TransactionStatus)
        def txnId = txns.get(0).TransactionId

        txns = sql_return_maparray "show proc '/transactions/${dbId}/finished'"
        assertEquals(0, txns.size())

        // coordinatorBe shutdown not abort txn because abort_txn_after_lost_heartbeat_time_second = 3600
        cluster.stopBackends(coordinatorBe.index)
        def isDead = false
        for (def i = 0; i < 10; i++) {
            def be = sql_return_maparray('show backends').find { it.Host == coordinatorBeHost }
            if (!be.Alive.toBoolean()) {
                isDead = true
                break
            }
            sleep 1000
        }
        assertTrue(isDead)
        sleep 5000
        txns = sql_return_maparray "show proc '/transactions/${dbId}/running'"
        assertEquals(1, txns.size())
        assertEquals(txnId, txns.get(0).TransactionId)
        assertEquals('PREPARE', txns.get(0).TransactionStatus)

        // coordinatorBe restart, abort txn on it
        cluster.startBackends(coordinatorBe.index)
        def isAlive = false
        for (def i = 0; i < 20; i++) {
            def be = sql_return_maparray('show backends').find { it.Host == coordinatorBeHost }
            if (be.Alive.toBoolean()) {
                isAlive = true
                break
            }
            sleep 1000
        }
        assertTrue(isAlive)
        sleep 5000
        txns = sql_return_maparray "show proc '/transactions/${dbId}/running'"
        assertEquals(0, txns.size())
        txns = sql_return_maparray "show proc '/transactions/${dbId}/finished'"
        assertEquals(1, txns.size())
        assertEquals(txnId, txns.get(0).TransactionId)
        assertEquals('ABORTED', txns.get(0).TransactionStatus)

        try {
            future.get()
        } catch (Throwable t) {
        }
    }
}
