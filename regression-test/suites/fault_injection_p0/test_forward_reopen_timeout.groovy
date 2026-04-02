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

// Test that GenericPool.reopen() fails fast when connecting to an unreachable
// address, instead of blocking for the full RPC timeout (18 minutes).
//
// Uses debug point "GenericPool.reopen.unreachable" to redirect the TSocket to
// a black-hole IP (192.0.2.1). Then:
//   With fix: connectTimeout_ = thrift_rpc_connect_timeout_ms (5s) → fails in ~5s
//   Without fix: connectTimeout_ = inherited from borrowObject (large) → blocks for minutes
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

        // Step 2: Inject debug point on FOLLOWER FE only — redirect reopen() to black-hole
        def followerFe = cluster.getAllFrontends().find { !it.isMaster }
        assertNotNull(followerFe)
        logger.info("Injecting debug point on follower FE (index=${followerFe.index})")
        followerFe.enableDebugPoint('GenericPool.reopen.unreachable', null)

        // Step 3: Set short query timeout so the test doesn't wait too long on failure
        sql "SET query_timeout = 30"

        // Step 4: Force the pooled connection to go stale by closing it from server side.
        // Restart master to invalidate all pooled connections on the follower.
        def masterFe = cluster.getMasterFe()
        logger.info("Restarting master FE (index=${masterFe.index}) to create stale connections")
        cluster.restartFrontends(masterFe.index)
        sleep(5000)
        // Reconnect so SHOW FRONTENDS works
        context.reconnectFe()

        // Wait for master to be ready
        for (int i = 0; i < 30; i++) {
            try {
                def alive = sql "SHOW FRONTENDS"
                if (alive.size() >= 2) break
            } catch (Exception e) { /* ignore */ }
            sleep(1000)
        }
        logger.info("Master FE restarted")

        // Step 5: DDL from follower → forward to master → stale connection detected →
        // reopen() triggered → debug point redirects to 192.0.2.1 → open() blocks on connect.
        //
        // With fix: connectTimeout_ = 5s → fails in ~5s → clearPool → retry with new connection
        // Without fix: connectTimeout_ = query_timeout*1.2 = 36s → blocks 36s (or 1080s with defaults)
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
        logger.info("Step 5: DDL attempt took ${elapsed}ms, error=${ddlError}")

        // Key assertion: should complete within 15s.
        // With fix (connectTimeout=5s): ~5s for reopen timeout
        // Without fix (connectTimeout=36s from query_timeout=30*1.2): would exceed 15s
        assertTrue(elapsed < 15000,
                "DDL forwarding took ${elapsed}ms, expected < 15s. " +
                "Without the fix, reopen() would block for the full connect timeout.")

        // Step 6: Remove debug point, verify recovery
        followerFe.disableDebugPoint('GenericPool.reopen.unreachable')
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
        logger.info("Step 6 passed: DDL works after debug point removed")

        sql "DROP DATABASE IF EXISTS test_reopen_timeout"
    }
}
