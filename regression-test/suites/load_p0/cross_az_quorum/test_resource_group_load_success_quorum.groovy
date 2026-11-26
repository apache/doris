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

// No coverage for a slow remote AZ here: BE ends its first close-wait stage at the ordinary
// load_required_replica_num, which knows nothing about resource_group_load_success_quorum, so a
// replica that is merely slow can still be dropped and fail the commit. Waiting per resource group
// is the BE-side follow-up, deliberately not done yet.
suite('test_resource_group_load_success_quorum', 'docker') {
    def options = new ClusterOptions()
    // BEs learn about a workload group only on the next topic publish, 30s apart by default.
    options.feConfigs += ['disable_tablet_scheduler=true', 'publish_topic_info_interval_ms=1000']
    options.enableDebugPoints()
    options.cloudMode = false

    docker(options) {
        def backends = sql_return_maparray('SHOW BACKENDS')
        assertEquals(3, backends.size())
        sql """ALTER SYSTEM MODIFY BACKEND '${backends[0].BackendId}' SET ('tag.location' = 'az1')"""
        sql """ALTER SYSTEM MODIFY BACKEND '${backends[1].BackendId}' SET ('tag.location' = 'az1')"""
        sql """ALTER SYSTEM MODIFY BACKEND '${backends[2].BackendId}' SET ('tag.location' = 'az2')"""

        sql 'DROP TABLE IF EXISTS cross_az_quorum_table'
        sql '''
            CREATE TABLE cross_az_quorum_table (k INT)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ('replication_allocation' = 'tag.location.az1: 2, tag.location.az2: 1')
        '''

        // tag.location doubles as the compute group name: FE has to create the `normal` workload
        // group for a newly seen compute group and then publish it to the BEs. Until both happen a
        // load fails with "Can not find workload group normal in compute group az1" (FE) or "not
        // even find normal wg in BE". Probe with a real load -- SHOW WORKLOAD GROUPS only proves
        // the FE half. Turning workload groups off is not an option, production runs with them on.
        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
            try {
                sql 'INSERT INTO cross_az_quorum_table VALUES (0)'
                true
            } catch (Exception e) {
                false
            }
        })

        def injectName = 'TxnManager.prepare_txn.random_failed'
        GetDebugPoint().enableDebugPoint(backends[0].Host, backends[0].HttpPort as int,
                NodeType.BE, injectName, [percent: 1.0])

        // The default empty config preserves the normal two-of-three quorum behavior.
        sql 'INSERT INTO cross_az_quorum_table VALUES (1)'

        setFeConfig('resource_group_load_success_quorum', 'az1:2,az2:1')
        test {
            sql 'INSERT INTO cross_az_quorum_table VALUES (2)'
            exception 'resource group success quorum failed for az1'
        }

        setFeConfig('resource_group_load_success_quorum', '')
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
        setFeConfig('resource_group_load_success_quorum', 'az1:2,az2:1')
        test {
            sql 'INSERT INTO cross_az_quorum_az2_table VALUES (2)'
            exception 'resource group success quorum failed for az2'
        }
        setFeConfig('resource_group_load_success_quorum', '')
        GetDebugPoint().disableDebugPoint(backends[2].Host, backends[2].HttpPort as int,
                NodeType.BE, injectName)

        sql 'DROP TABLE IF EXISTS cross_az_quorum_clamp_table'
        sql '''
            CREATE TABLE cross_az_quorum_clamp_table (k INT)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ('replication_allocation' = 'tag.location.az1: 1')
        '''
        setFeConfig('resource_group_load_success_quorum', 'az1:2')
        sql 'INSERT INTO cross_az_quorum_clamp_table VALUES (1)'

        // A resource group the table does not place any replica in requires nothing: the config is
        // global, tables living in a single AZ must keep loading.
        setFeConfig('resource_group_load_success_quorum', 'az1:1,az2:1')
        sql 'INSERT INTO cross_az_quorum_clamp_table VALUES (2)'

        // Invalid entries are ignored and must never break the commit path.
        setFeConfig('resource_group_load_success_quorum', 'invalid,az1:not-a-number')
        sql 'INSERT INTO cross_az_quorum_clamp_table VALUES (3)'
        setFeConfig('resource_group_load_success_quorum', '')
    }
}
