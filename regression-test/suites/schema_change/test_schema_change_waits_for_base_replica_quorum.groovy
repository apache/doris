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

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

suite('test_schema_change_waits_for_ready_base_replica_quorum', 'docker') {
    def options = new ClusterOptions()
    options.beNum = 3
    options.enableDebugPoints()
    options.feConfigs.add('disable_tablet_scheduler=true')
    options.beConfigs.add('report_tablet_interval_seconds=1')
    docker(options) {
        def stoppedBe1 = cluster.getBeByIndex(1)
        def stoppedBe2 = cluster.getBeByIndex(2)
        def normalBe = cluster.getBeByIndex(3)

        sql ''' DROP TABLE IF EXISTS test_schema_change_waits_for_base_replica_quorum '''
        sql '''
            CREATE TABLE test_schema_change_waits_for_base_replica_quorum (
                k1 INT,
                k2 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "3",
                "disable_auto_compaction" = "true",
                "light_schema_change" = "false"
            )
        '''

        sql ''' INSERT INTO test_schema_change_waits_for_base_replica_quorum VALUES (1, 10) '''

        def transactionReady = new CountDownLatch(1)
        def commitTransaction = new CountDownLatch(1)
        def transactionErrors = Collections.synchronizedList(new ArrayList<String>())
        def jdbcUrl = context.threadLocalConn.get().conn.getMetaData().getURL()
        def transactionThread = new Thread(() -> {
            try (Connection connection = DriverManager.getConnection(
                    jdbcUrl, context.config.jdbcUser, context.config.jdbcPassword);
                    Statement statement = connection.createStatement()) {
                statement.execute('BEGIN')
                statement.execute('''
                    INSERT INTO test_schema_change_waits_for_base_replica_quorum VALUES (2, 20)
                ''')
                transactionReady.countDown()
                if (!commitTransaction.await(1, TimeUnit.MINUTES)) {
                    throw new IllegalStateException('Timed out waiting to commit the transaction')
                }
                statement.execute('COMMIT')
            } catch (Throwable t) {
                transactionErrors.add(t.getMessage())
                transactionReady.countDown()
            }
        })
        transactionThread.start()
        assertTrue(transactionReady.await(1, TimeUnit.MINUTES))
        assertEquals(0, transactionErrors.size())

        sql '''
            ALTER TABLE test_schema_change_waits_for_base_replica_quorum
            ADD COLUMN k3 INT DEFAULT "0"
        '''

        def getAlterJob = {
            def jobs = sql_return_maparray('''
                SHOW ALTER TABLE COLUMN
                WHERE TableName = 'test_schema_change_waits_for_base_replica_quorum'
                ORDER BY CreateTime DESC LIMIT 1
            ''')
            assertEquals(1, jobs.size())
            return jobs[0]
        }

        def waitingTxnObserved = false
        for (int i = 0; i < 15; i++) {
            def state = getAlterJob().State
            if (state == 'WAITING_TXN') {
                waitingTxnObserved = true
                break
            }
            assertNotEquals('CANCELLED', state)
            sleep(1000)
        }
        assertTrue(waitingTxnObserved)

        cluster.injectDebugPoints(NodeType.FE, ['FE.STOP_ALTER_JOB_RUN': null])
        commitTransaction.countDown()
        transactionThread.join(30000)
        assertFalse(transactionThread.isAlive())
        assertEquals(0, transactionErrors.size())

        def getReplicaVersion = { be ->
            def replica = sql_return_maparray(
                    ''' SHOW TABLETS FROM test_schema_change_waits_for_base_replica_quorum ''')
                    .find { (it.BackendId as long) == be.backendId }
            assertNotNull(replica)
            return replica.Version as long
        }

        def allReplicasCaughtUp = false
        for (int i = 0; i < 30; i++) {
            if ([stoppedBe1, stoppedBe2, normalBe].every { getReplicaVersion(it) == 3L }) {
                allReplicasCaughtUp = true
                break
            }
            sleep(1000)
        }
        assertTrue(allReplicasCaughtUp)

        cluster.stopBackends(stoppedBe1.index, stoppedBe2.index)
        cluster.clearFrontendDebugPoints()

        sleep(5000)
        assertEquals('WAITING_TXN', getAlterJob().State)

        cluster.startBackends(stoppedBe1.index)
        def restoredReplicaReady = false
        for (int i = 0; i < 30; i++) {
            if (getReplicaVersion(stoppedBe1) == 3L) {
                restoredReplicaReady = true
                break
            }
            sleep(1000)
        }
        assertTrue(restoredReplicaReady)

        def finished = false
        for (int i = 0; i < 60; i++) {
            def state = getAlterJob().State
            if (state == 'FINISHED') {
                finished = true
                break
            }
            assertNotEquals('CANCELLED', state)
            sleep(1000)
        }
        assertTrue(finished)

        order_qt_final_rows '''
            SELECT k1, k2 FROM test_schema_change_waits_for_base_replica_quorum ORDER BY k1
        '''
    }
}
