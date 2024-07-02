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

suite('test_clone_missing_version') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'disable_tablet_scheduler=true',
        'tablet_checker_interval_ms=500',
        'schedule_batch_size=1000',
        'schedule_slot_num_per_hdd_path=1000',
    ]
    options.beConfigs += [
        'disable_auto_compaction=true',
        'report_tablet_interval_seconds=1',
    ]

    options.enableDebugPoints()
    options.cloudMode = false
    docker(options) {
        def injectName = 'TxnManager.prepare_txn.random_failed'
        def be = sql_return_maparray('show backends').get(0)
        def addInject = { ->
            GetDebugPoint().enableDebugPoint(be.Host, be.HttpPort as int, NodeType.BE, injectName, [percent:1.0])
        }
        def deleteInject = { ->
            GetDebugPoint().disableDebugPoint(be.Host, be.HttpPort as int, NodeType.BE, injectName)
        }

        sql 'CREATE TABLE t (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1'

        sql 'INSERT INTO t VALUES(2)'

        addInject()

        sql 'INSERT INTO t VALUES(3)'
        sql 'INSERT INTO t VALUES(4)'
        sql 'INSERT INTO t VALUES(5)'

        deleteInject()

        sql 'INSERT INTO t VALUES(6)'

        addInject()

        sql 'INSERT INTO t VALUES(7)'
        sql 'INSERT INTO t VALUES(8)'
        sql 'INSERT INTO t VALUES(9)'

        deleteInject()

        sql 'INSERT INTO t VALUES(10)'

        sleep 5000

        def replica = sql_return_maparray('show tablets from t').find { it.BackendId.toLong().equals(be.BackendId.toLong()) }
        assertNotNull(replica)
        assertEquals(4, replica.VersionCount.toInteger())
        assertEquals(2, replica.Version.toInteger())
        assertEquals(9, replica.LstFailedVersion.toInteger())

        setFeConfig('disable_tablet_scheduler', false)

        sleep 10000

        replica = sql_return_maparray('show tablets from t').find { it.BackendId.toLong().equals(be.BackendId.toLong()) }
        assertNotNull(replica)
        assertEquals(10, replica.VersionCount.toInteger())
        assertEquals(10, replica.Version.toInteger())
        assertEquals(-1, replica.LstFailedVersion.toInteger())
    }
}
