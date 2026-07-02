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

suite('test_schema_change_waits_for_base_replica_catch_up_quorum', 'docker') {
    def options = new ClusterOptions()
    options.beNum = 3
    options.enableDebugPoints()
    options.feConfigs.add('disable_tablet_scheduler=true')
    docker(options) {
        def blockedBe1 = cluster.getBeByIndex(1)
        def blockedBe2 = cluster.getBeByIndex(2)
        def normalBe = cluster.getBeByIndex(3)
        def debugToken = 'schema_change_wait_base_replica'

        onFinish {
            blockedBe1.clearDebugPoints()
            blockedBe2.clearDebugPoints()
        }

        sql ''' DROP TABLE IF EXISTS test_schema_change_waits_for_lagging_base_replica '''
        sql '''
            CREATE TABLE test_schema_change_waits_for_lagging_base_replica (
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

        sql ''' INSERT INTO test_schema_change_waits_for_lagging_base_replica VALUES (1, 10) '''

        [blockedBe1, blockedBe2].each { be ->
            be.enableDebugPoint('EnginePublishVersionTask::execute.enable_spin_wait', ['token': debugToken])
            be.enableDebugPoint('EnginePublishVersionTask::execute.block', ['pass_token': 'keep_blocked'])
        }

        sql ''' SET GLOBAL insert_visible_timeout_ms = 2000 '''
        sql ''' INSERT INTO test_schema_change_waits_for_lagging_base_replica VALUES (2, 20) '''

        def visibleRows = sql '''
            SELECT k1, k2 FROM test_schema_change_waits_for_lagging_base_replica ORDER BY k1
        '''
        assertEquals([[1, 10], [2, 20]], visibleRows)

        def tabletInfo = sql_return_maparray(
                ''' SHOW TABLETS FROM test_schema_change_waits_for_lagging_base_replica ''')
                .find { (it.BackendId as long) == blockedBe1.backendId }
        assertNotNull(tabletInfo)
        def tabletId = tabletInfo.TabletId

        def (code, out, err) = be_show_tablet_status(normalBe.host, normalBe.httpPort, tabletId)
        assertEquals(0, code)
        assertTrue(out.contains('[3-3]'))

        (code, out, err) = be_show_tablet_status(blockedBe1.host, blockedBe1.httpPort, tabletId)
        assertEquals(0, code)
        assertTrue(out.contains('[2-2]'))
        assertFalse(out.contains('[3-3]'))

        (code, out, err) = be_show_tablet_status(blockedBe2.host, blockedBe2.httpPort, tabletId)
        assertEquals(0, code)
        assertTrue(out.contains('[2-2]'))
        assertFalse(out.contains('[3-3]'))

        sql '''
            ALTER TABLE test_schema_change_waits_for_lagging_base_replica
            ORDER BY (k2, k1)
        '''

        def waitingTxnObserved = false
        for (int i = 0; i < 15; i++) {
            def jobs = sql_return_maparray("""
                SHOW ALTER TABLE COLUMN
                WHERE TableName = 'test_schema_change_waits_for_lagging_base_replica'
                ORDER BY CreateTime DESC LIMIT 1
            """)
            assertEquals(1, jobs.size())
            def state = jobs[0].State
            if (state == 'WAITING_TXN') {
                waitingTxnObserved = true
                break
            }
            assertNotEquals('CANCELLED', state)
            sleep(1000)
        }
        assertTrue(waitingTxnObserved)

        sleep(5000)
        def waitingJobs = sql_return_maparray("""
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_schema_change_waits_for_lagging_base_replica'
            ORDER BY CreateTime DESC LIMIT 1
        """)
        assertEquals('WAITING_TXN', waitingJobs[0].State)

        blockedBe1.clearDebugPoints()

        def publishCaughtUp = false
        for (int i = 0; i < 30; i++) {
            (code, out, err) = be_show_tablet_status(blockedBe1.host, blockedBe1.httpPort, tabletId)
            assertEquals(0, code)
            if (out.contains('[3-3]')) {
                publishCaughtUp = true
                break
            }
            sleep(1000)
        }
        assertTrue(publishCaughtUp)

        def leftWaitingTxn = false
        for (int i = 0; i < 30; i++) {
            def jobs = sql_return_maparray("""
                SHOW ALTER TABLE COLUMN
                WHERE TableName = 'test_schema_change_waits_for_lagging_base_replica'
                ORDER BY CreateTime DESC LIMIT 1
            """)
            assertEquals(1, jobs.size())
            def state = jobs[0].State
            if (state != 'WAITING_TXN') {
                leftWaitingTxn = true
                assertNotEquals('CANCELLED', state)
                break
            }
            sleep(1000)
        }
        assertTrue(leftWaitingTxn)

        def finished = false
        for (int i = 0; i < 60; i++) {
            def jobs = sql_return_maparray("""
                SHOW ALTER TABLE COLUMN
                WHERE TableName = 'test_schema_change_waits_for_lagging_base_replica'
                ORDER BY CreateTime DESC LIMIT 1
            """)
            assertEquals(1, jobs.size())
            def state = jobs[0].State
            if (state == 'FINISHED') {
                finished = true
                break
            }
            assertNotEquals('CANCELLED', state)
            sleep(1000)
        }
        assertTrue(finished)

        (code, out, err) = be_show_tablet_status(blockedBe2.host, blockedBe2.httpPort, tabletId)
        assertEquals(0, code)
        assertTrue(out.contains('[2-2]'))
        assertFalse(out.contains('[3-3]'))

        def result = sql '''
            SELECT k1, k2 FROM test_schema_change_waits_for_lagging_base_replica ORDER BY k1
        '''
        assertEquals([[1, 10], [2, 20]], result)
    }
}
