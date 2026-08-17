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
import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite('test_cross_az_succ_quorum', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += ['disable_tablet_scheduler=true']
    options.beConfigs += ['quorum_success_min_wait_seconds=1']
    options.enableDebugPoints()
    options.cloudMode = false

    docker(options) {
        def backends = sql_return_maparray('SHOW BACKENDS')
        assertEquals(3, backends.size())
        sql """ALTER SYSTEM MODIFY BACKEND '${backends[0].BackendId}' SET ('tag.location' = 'az1')"""
        sql """ALTER SYSTEM MODIFY BACKEND '${backends[1].BackendId}' SET ('tag.location' = 'az1')"""
        sql """ALTER SYSTEM MODIFY BACKEND '${backends[2].BackendId}' SET ('tag.location' = 'az2')"""

        // tag.location doubles as the compute group name, and WorkloadGroupChecker only creates the
        // `normal` workload group for a newly seen compute group every workload_group_check_interval_ms
        // (2s by default). Loading before that fails with "Can not find workload group normal in
        // compute group az1", so wait for it rather than turning workload groups off -- production
        // runs with them on.
        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
            def normals = sql_return_maparray('SHOW WORKLOAD GROUPS')
                    .findAll { it.Name == 'normal' }
                    .collect { it.compute_group } as Set
            normals.containsAll(['az1', 'az2'])
        })

        sql 'DROP TABLE IF EXISTS cross_az_quorum_table'
        sql '''
            CREATE TABLE cross_az_quorum_table (k INT)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ('replication_allocation' = 'tag.location.az1: 2, tag.location.az2: 1')
        '''

        def injectName = 'TxnManager.prepare_txn.random_failed'
        GetDebugPoint().enableDebugPoint(backends[0].Host, backends[0].HttpPort as int,
                NodeType.BE, injectName, [percent: 1.0])

        // The default empty config preserves the normal two-of-three quorum behavior.
        sql 'INSERT INTO cross_az_quorum_table VALUES (1)'

        setFeConfig('cross_az_succ_quorum', 'az1:2,az2:1')
        test {
            sql 'INSERT INTO cross_az_quorum_table VALUES (2)'
            exception 'cross AZ success quorum failed for az1'
        }

        setFeConfig('cross_az_succ_quorum', '')
        sql 'INSERT INTO cross_az_quorum_table VALUES (3)'
        GetDebugPoint().disableDebugPoint(backends[0].Host, backends[0].HttpPort as int,
                NodeType.BE, injectName)

        // Losing all successful replicas in one AZ still leaves a normal two-of-three quorum.
        sql 'DROP TABLE IF EXISTS cross_az_quorum_az2_table'
        sql '''
            CREATE TABLE cross_az_quorum_az2_table (k INT)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ('replication_allocation' = 'tag.location.az1: 2, tag.location.az2: 1')
        '''
        GetDebugPoint().enableDebugPoint(backends[2].Host, backends[2].HttpPort as int,
                NodeType.BE, injectName, [percent: 1.0])
        sql 'INSERT INTO cross_az_quorum_az2_table VALUES (1)'
        setFeConfig('cross_az_succ_quorum', 'az1:2,az2:1')
        test {
            sql 'INSERT INTO cross_az_quorum_az2_table VALUES (2)'
            exception 'cross AZ success quorum failed for az2'
        }
        setFeConfig('cross_az_succ_quorum', '')
        GetDebugPoint().disableDebugPoint(backends[2].Host, backends[2].HttpPort as int,
                NodeType.BE, injectName)

        // A slow remote AZ must remain in the first close-wait stage instead of being dropped
        // after the two local replicas reach the ordinary quorum.
        // Use a fresh table: the tables above already committed a version while their az2 replica
        // was failing, so that replica carries a version gap and, with the tablet scheduler
        // disabled, is never repaired -- it could never count as a success again.
        sql 'DROP TABLE IF EXISTS cross_az_quorum_slow_table'
        sql '''
            CREATE TABLE cross_az_quorum_slow_table (k INT)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ('replication_allocation' = 'tag.location.az1: 2, tag.location.az2: 1')
        '''
        setFeConfig('cross_az_succ_quorum', 'az1:2,az2:1')
        GetDebugPoint().enableDebugPoint(backends[2].Host, backends[2].HttpPort as int,
                NodeType.BE, 'TxnManager.prepare_txn.wait', [duration: 3000])
        sql 'INSERT INTO cross_az_quorum_slow_table VALUES (1)'
        GetDebugPoint().disableDebugPoint(backends[2].Host, backends[2].HttpPort as int,
                NodeType.BE, 'TxnManager.prepare_txn.wait')
        setFeConfig('cross_az_succ_quorum', '')

        sql 'DROP TABLE IF EXISTS cross_az_quorum_clamp_table'
        sql '''
            CREATE TABLE cross_az_quorum_clamp_table (k INT)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ('replication_allocation' = 'tag.location.az1: 1')
        '''
        setFeConfig('cross_az_succ_quorum', 'az1:2')
        sql 'INSERT INTO cross_az_quorum_clamp_table VALUES (1)'

        // Invalid entries are ignored and must never break the commit path.
        setFeConfig('cross_az_succ_quorum', 'invalid,az1:not-a-number')
        sql 'INSERT INTO cross_az_quorum_clamp_table VALUES (2)'
        setFeConfig('cross_az_succ_quorum', '')
    }
}
