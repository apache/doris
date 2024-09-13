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

// This case can reproduce an clone issue which may produce duplicate keys in mow table
// the bug would be triggered in the following condition:
// 1. replica 0 miss version
// 2. replica 0 try to do full clone from other replicas
// 3. the full clone failed and the delete bitmap is overrided incorrectly
// 4. replica 0 try to do incremental clone again and this time the clone succeed
// 5. incremental clone can't fix the delete bitmap overrided by previous failed full clone
// 6. duplicate key occurred
//
// the bug is fixed in #37001

suite('test_full_clone_exception', 'docker') {
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
        'enable_java_support=false',
    ]

    options.enableDebugPoints()
    options.cloudMode = false
    docker(options) {
        def txnFailureInject = 'TxnManager.prepare_txn.random_failed'
        def fullCloneInject = 'SnapshotManager.create_snapshot_files.allow_inc_clone'
        def reviseTabletMetaInject='Tablet.revise_tablet_meta_fail'
        def be1 = sql_return_maparray('show backends').get(0)
        def be2 = sql_return_maparray('show backends').get(1)
        def be3 = sql_return_maparray('show backends').get(2)

        def addInjectToBE = { beIdx, injectName, param ->
            def be = sql_return_maparray('show backends').get(beIdx)
            GetDebugPoint().enableDebugPoint(be.Host, be.HttpPort as int, NodeType.BE, injectName, param)
        }

        def deleteInjectOnBE = { beIdx, injectName ->
            def be = sql_return_maparray('show backends').get(beIdx)
            GetDebugPoint().disableDebugPoint(be.Host, be.HttpPort as int, NodeType.BE, injectName)
        }

        sql """
                CREATE TABLE IF NOT EXISTS t (
                    k int,
                    v int
                )
                UNIQUE KEY(k)
                CLUSTER BY(v)
                DISTRIBUTED BY HASH(k) BUCKETS 1 properties(
                    "enable_unique_key_merge_on_write" = "true"
                );
            """

        sql 'INSERT INTO t VALUES(1,1),(2,2),(3,3),(4,4)'
        sql 'INSERT INTO t VALUES(1,10),(2,20),(3,30),(4,40)'

        // inject txn failure, make replica 0 miss version
        addInjectToBE(0, txnFailureInject, [percent:1.0])

        sql 'INSERT INTO t VALUES(2,200),(4,400),(5,500),(6,600)'
        sql 'INSERT INTO t VALUES(7,7)'
        sql 'INSERT INTO t VALUES(8,8)'
        sql 'INSERT INTO t VALUES(9,9)'

        deleteInjectOnBE(0, txnFailureInject)

        sql 'INSERT INTO t VALUES(10,10)'

        sleep 5000

        // check replica 0 miss version
        def replica1 = sql_return_maparray('show tablets from t').find { it.BackendId.toLong().equals(be1.BackendId.toLong()) }
        assertNotNull(replica1)
        assertEquals(3, replica1.VersionCount.toInteger())
        assertEquals(3, replica1.Version.toInteger())
        assertEquals(8, replica1.LstFailedVersion.toInteger())

        def tabletId = replica1.TabletId
        // inject failure on replica 0, which can't clone succeed
        addInjectToBE(0, reviseTabletMetaInject, [tablet_id:tabletId])
        // inject on replica 1, force replica 0 full clone from them
        addInjectToBE(1, fullCloneInject, [tablet_id:tabletId, is_full_clone:true])
        addInjectToBE(2, fullCloneInject, [tablet_id:tabletId, is_full_clone:true])

        // start clone
        setFeConfig('disable_tablet_scheduler', false)

        sleep 10000

        // now, there's lots of full clone failures, remove all debug points, make
        // replica 0 can do normal incremental clone from other replicas
        deleteInjectOnBE(0, reviseTabletMetaInject)
        deleteInjectOnBE(1, fullCloneInject)
        deleteInjectOnBE(2, fullCloneInject)

        sleep 10000

        // make sure the clone succeed
        replica1 = sql_return_maparray('show tablets from t').find { it.BackendId.toLong().equals(be1.BackendId.toLong()) }
        assertNotNull(replica1)
        assertEquals(8, replica1.VersionCount.toInteger())
        assertEquals(8, replica1.Version.toInteger())
        assertEquals(-1, replica1.LstFailedVersion.toInteger())

        // three replica's content should be consistent
        sql 'set use_fix_replica=0'
        qt_sql 'select * from t order by k'

        sql 'set use_fix_replica=1'
        qt_sql 'select * from t order by k'

        sql 'set use_fix_replica=2'
        qt_sql 'select * from t order by k'
    }
}
