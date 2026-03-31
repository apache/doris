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

// Test that DDL forwarding from follower to master recovers quickly
// after master FE restart, instead of hanging for 18 minutes due to
// stale connections in GenericPool.
suite('test_forward_reopen_timeout', 'docker') {
    def options = new ClusterOptions()
    options.setFeNum(2)
    options.setBeNum(1)
    options.connectToFollower = true
    // Set a short connect timeout for faster test execution
    options.feConfigs += [
        'thrift_rpc_connect_timeout_ms=5000',
    ]

    docker(options) {
        // Step 1: Verify basic DDL forwarding works (follower -> master)
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

        // Step 2: Restart master FE to invalidate pooled connections
        def masterFe = cluster.getMasterFe()
        logger.info("Restarting master FE (index=${masterFe.index}) to create stale connections")
        cluster.restartFrontends(masterFe.index)

        // Wait for master to come back
        boolean masterReady = false
        for (int i = 0; i < 60; i++) {
            try {
                def alive = sql "SHOW FRONTENDS"
                if (alive.size() >= 2) {
                    masterReady = true
                    break
                }
            } catch (Exception e) {
                // ignore
            }
            sleep(1000)
        }
        assertTrue(masterReady, "Master FE did not come back within 60s")
        context.reconnectFe()
        logger.info("Step 2 passed: master FE restarted")

        // Step 3: Execute DDL from follower — this triggers reopen() on stale connection
        // Before the fix, this would hang for 18 minutes (1080s).
        // After the fix, reopen fails within thrift_rpc_connect_timeout_ms (5s),
        // then clearPool + retry with new connection succeeds quickly.
        def startTime = System.currentTimeMillis()
        sql "USE test_reopen_timeout"
        sql """
            CREATE TABLE tbl2 (k1 INT)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        def elapsed = System.currentTimeMillis() - startTime
        logger.info("Step 3: DDL after master restart took ${elapsed}ms")

        // Should complete well within 60s (generous margin).
        // Without the fix, this would take 1080s+.
        assertTrue(elapsed < 60000, "DDL forwarding took ${elapsed}ms, expected < 60s")

        // Step 4: Verify the DDL actually worked
        sql "INSERT INTO tbl2 VALUES (2)"
        result = sql "SELECT * FROM tbl2"
        assertEquals(1, result.size())
        assertEquals(2, result[0][0])
        logger.info("Step 4 passed: DDL result verified")

        sql "DROP DATABASE IF EXISTS test_reopen_timeout"
    }
}
