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

// min_load_replica_num lowers how many successful replicas a commit needs, which makes it easier
// for the surviving replicas to all sit in one AZ. resource_group_load_success_quorum is the guard for exactly
// that combination: the two conditions stack, the AZ requirement never relaxes the replica count.
suite('test_resource_group_load_success_quorum_min_load_replica', 'docker') {
    def options = new ClusterOptions()
    // 5 backends so the table can hold 5 replicas: the default quorum is then 3 and lowering it
    // to 2 actually changes the outcome. With 3 az1 and 2 az2 backends every backend holds exactly
    // one replica of the single tablet, which keeps the fault injection deterministic.
    options.beNum = 5
    // BEs learn about a workload group only on the next topic publish, 30s apart by default.
    options.feConfigs += ['disable_tablet_scheduler=true', 'publish_topic_info_interval_ms=1000']
    options.enableDebugPoints()
    options.cloudMode = false

    docker(options) {
        def backends = sql_return_maparray('SHOW BACKENDS')
        assertEquals(5, backends.size())
        def az1Backends = backends[0..2]
        def az2Backends = backends[3..4]
        az1Backends.each {
            sql """ALTER SYSTEM MODIFY BACKEND '${it.BackendId}' SET ('tag.location' = 'az1')"""
        }
        az2Backends.each {
            sql """ALTER SYSTEM MODIFY BACKEND '${it.BackendId}' SET ('tag.location' = 'az2')"""
        }

        // tag.location doubles as the compute group name: FE has to create the `normal` workload
        // group for the new compute groups and then publish it to the BEs. Probe with a real load,
        // SHOW WORKLOAD GROUPS only proves the FE half.
        sql 'DROP TABLE IF EXISTS cross_az_min_load_probe'
        sql '''
            CREATE TABLE cross_az_min_load_probe (k INT)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ('replication_allocation' = 'tag.location.az1: 3, tag.location.az2: 2')
        '''
        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
            try {
                sql 'INSERT INTO cross_az_min_load_probe VALUES (0)'
                true
            } catch (Exception e) {
                false
            }
        })

        def injectName = 'TxnManager.prepare_txn.random_failed'
        def enableInject = { List bes ->
            bes.each {
                GetDebugPoint().enableDebugPoint(it.Host, it.HttpPort as int, NodeType.BE,
                        injectName, [percent: 1.0])
            }
        }
        def disableInject = { List bes ->
            bes.each {
                GetDebugPoint().disableDebugPoint(it.Host, it.HttpPort as int, NodeType.BE, injectName)
            }
        }

        // Fail both az2 replicas and one az1 replica: 2 successful replicas left, both in az1.
        // Below the default quorum of 3, but enough for the lowered min_load_replica_num of 2.
        def singleAzFailures = az2Backends + [az1Backends[0]]
        // Fail two az1 replicas and one az2 replica: 2 successful replicas left, one per AZ.
        def crossAzFailures = [az1Backends[0], az1Backends[1], az2Backends[0]]

        def createTable = { String name ->
            sql "DROP TABLE IF EXISTS ${name}"
            sql """
                CREATE TABLE ${name} (k INT)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ('replication_allocation' = 'tag.location.az1: 3, tag.location.az2: 2')
            """
            sql """ALTER TABLE ${name} SET ("min_load_replica_num" = "2")"""
        }

        // Without resource_group_load_success_quorum the lowered quorum accepts a commit whose successful
        // replicas all live in az1 -- the silent durability risk this feature exists to remove.
        enableInject(singleAzFailures)
        createTable('cross_az_min_load_baseline')
        sql 'INSERT INTO cross_az_min_load_baseline VALUES (1)'

        // Same load, same lowered quorum, but now the AZ coverage requirement rejects it.
        createTable('cross_az_min_load_guarded')
        setFeConfig('resource_group_load_success_quorum', 'az1:1,az2:1')
        test {
            sql 'INSERT INTO cross_az_min_load_guarded VALUES (1)'
            exception 'resource group success quorum failed for az2'
        }
        disableInject(singleAzFailures)

        // Still only 2 successful replicas and the same lowered quorum, but this time they are
        // spread over both AZs, so the commit is accepted: the AZ requirement constrains where the
        // successful replicas sit, it does not simply reject every degraded load.
        enableInject(crossAzFailures)
        createTable('cross_az_min_load_spread')
        sql 'INSERT INTO cross_az_min_load_spread VALUES (1)'
        disableInject(crossAzFailures)

        setFeConfig('resource_group_load_success_quorum', '')
    }
}
