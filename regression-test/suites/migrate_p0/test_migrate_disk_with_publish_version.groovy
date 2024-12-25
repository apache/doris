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

suite('test_migrate_disk_with_publish_version', 'docker') {
    if (isCloudMode()) {
        return
    }
    def checkTabletOnHDD = { isOnHdd ->
        sleep 5000

        def targetPathHash = [] as Set
        sql_return_maparray("SHOW PROC '/backends'").each {
            def paths = sql_return_maparray("SHOW PROC '/backends/${it.BackendId}'")
            for (def path : paths) {
                if (path.RootPath.endsWith(isOnHdd ? 'HDD' : 'SSD')) {
                    targetPathHash.add(path.PathHash)
                }
            }
        }

        def tablets = sql_return_maparray 'SHOW TABLETS FROM tbl'
        tablets.each {
            assertTrue(it.PathHash in targetPathHash, "tablet path hash ${it.PathHash} not in ${targetPathHash}")
        }
    }

    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.beConfigs += [
        'report_random_wait=false',
        'report_tablet_interval_seconds=1',
        'report_disk_state_interval_seconds=1'
    ]
    options.beDisks = ['HDD=1', 'SSD=1' ]
    docker(options) {
        cluster.checkBeIsAlive(1, true)
        cluster.checkBeIsAlive(2, true)
        cluster.checkBeIsAlive(3, true)
        sleep 2000

        sql 'SET GLOBAL insert_visible_timeout_ms = 2000'
        sql "ADMIN SET FRONTEND CONFIG ('agent_task_resend_wait_time_ms' = '1000')"

        sql 'CREATE TABLE tbl (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1'
        sql 'INSERT INTO tbl VALUES (1, 10)'

        checkTabletOnHDD true

        // add debug point, txn will block
        cluster.injectDebugPoints(NodeType.FE, ['PublishVersionDaemon.stop_publish':null])
        cluster.injectDebugPoints(NodeType.BE, ['EnginePublishVersionTask.handle.block_add_rowsets':null])
        sql 'INSERT INTO tbl VALUES (2, 20)'

        sql "ALTER TABLE tbl MODIFY PARTITION(*) SET ( 'storage_medium' = 'ssd' )"
        // tablet has unfinished txn, it couldn't migrate among disks
        checkTabletOnHDD true

        order_qt_select_1 'SELECT * FROM tbl'

        cluster.clearFrontendDebugPoints()
        // tablet finished all txns, but publish thread hold the migrate lock, migrate will failed
        checkTabletOnHDD true
        order_qt_select_2 'SELECT * FROM tbl'

        cluster.clearBackendDebugPoints()
        // tablet finished all txns, and publish thread not hold migrate lock, migrate should succ
        checkTabletOnHDD false
        order_qt_select_3 'SELECT * FROM tbl'
    }
}
