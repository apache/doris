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
import org.apache.doris.regression.util.NodeType

// Test that GenericPool.reopen() uses a short connect timeout instead of
// the inherited large RPC timeout when reconnecting stale connections.
//
// Two debug points work together:
// 1. "GenericPool.borrowObject.break_connection" - closes the underlying socket
//    after borrowObject, simulating a TCP half-open (stale) connection. The next
//    RPC will fail with TTransportException, triggering reopen().
// 2. "GenericPool.reopen.simulate_stale" - reads TSocket's connectTimeout_ via
//    reflection and sleeps that duration, simulating a blocked TCP connect.
//
//   With fix: connectTimeout_ = thrift_rpc_connect_timeout_ms (5s) → sleeps 5s
//   Without fix: connectTimeout_ = query_timeout * 1.2 (36s) → sleeps 36s
suite('test_forward_reopen_timeout', 'docker') {
    def options = new ClusterOptions()
    options.setFeNum(2)
    options.setBeNum(1)
    options.enableDebugPoints()
    options.connectToFollower = true
    options.feConfigs += [
        'thrift_rpc_connect_timeout_ms=5000',
    ]

    docker(options) {
        // Step 1: Basic DDL forwarding works
        sql "DROP DATABASE IF EXISTS test_reopen_timeout"
        sql "CREATE DATABASE test_reopen_timeout"
        sql "USE test_reopen_timeout"
        sql """
            CREATE TABLE tbl1 (k1 INT)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO tbl1 VALUES (1)"
        def result = sql "SELECT * FROM tbl1"
        assertEquals(1, result.size())
        logger.info("Step 1 passed: basic DDL forwarding works")

        // Step 2: Inject both debug points on follower FE
        def followerFe = cluster.getAllFrontends().find { !it.isMaster }
        assertNotNull(followerFe)
        logger.info("Injecting debug points on follower FE (index=${followerFe.index})")
        // This breaks the connection after borrow → forces TTransportException → triggers reopen
        followerFe.enableDebugPoint('GenericPool.borrowObject.break_connection', null)
        // This simulates stale connect in reopen by sleeping for connectTimeout_ duration
        followerFe.enableDebugPoint('GenericPool.reopen.simulate_stale', null)

        // Step 3: Set query timeout so borrowObject sets connectTimeout_ to 30*1.2=36s
        sql "SET query_timeout = 30"

        // Step 4: DDL from follower triggers the full chain:
        // borrowObject → break_connection closes socket → forward() fails →
        // TTransportException → reopen() → simulate_stale reads connectTimeout_ and sleeps.
        //
        // With fix: setTimeout(5000) called before close → connectTimeout_=5s → sleeps 5s
        // Without fix: connectTimeout_=36s (from borrowObject) → sleeps 36s → exceeds 15s
        def startTime = System.currentTimeMillis()
        def ddlError = null
        try {
            sql "USE test_reopen_timeout"
            sql """
                CREATE TABLE tbl_blocked (k1 INT)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
        } catch (Exception e) {
            ddlError = e.getMessage()
        }
        def elapsed = System.currentTimeMillis() - startTime
        logger.info("Step 4: DDL attempt took ${elapsed}ms, error=${ddlError}")

        // Key assertion: should complete within 15s.
        // With fix (connectTimeout=5s): debug point sleeps ~5s → PASS
        // Without fix (connectTimeout=36s): debug point sleeps ~36s → FAIL
        assertTrue(elapsed < 15000,
                "DDL forwarding took ${elapsed}ms, expected < 15s. " +
                "Without the fix, connectTimeout_ would be 36s instead of 5s.")

        // Step 5: Remove debug points, verify recovery
        followerFe.disableDebugPoint('GenericPool.borrowObject.break_connection')
        followerFe.disableDebugPoint('GenericPool.reopen.simulate_stale')
        sleep(2000)

        sql "USE test_reopen_timeout"
        sql """
            CREATE TABLE IF NOT EXISTS tbl_recovered (k1 INT)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO tbl_recovered VALUES (42)"
        result = sql "SELECT * FROM tbl_recovered"
        assertEquals(1, result.size())
        logger.info("Step 5 passed: DDL works after debug points removed")

        sql "DROP DATABASE IF EXISTS test_reopen_timeout"
    }
}
