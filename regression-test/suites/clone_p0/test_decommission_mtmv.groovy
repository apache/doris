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

suite('test_decommission_mtmv') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'disable_balance=true',
        'tablet_checker_interval_ms=100',
        'tablet_schedule_interval_ms=100',
        'schedule_batch_size=1000',
        'schedule_slot_num_per_hdd_path=1000',
        'storage_high_watermark_usage_percent=99',
        'storage_flood_stage_usage_percent=99',
    ]

    docker(options) {
        sql '''CREATE TABLE t1 (k1 INT, k2 INT, v INT)
               DISTRIBUTED BY HASH(k2) BUCKETS 1
            '''

        sql '''CREATE TABLE t2 (k1 INT, k2 INT, v INT)
               DISTRIBUTED BY HASH(k2) BUCKETS 1
            '''

        sql '''CREATE MATERIALIZED VIEW mv1 BUILD DEFERRED REFRESH AUTO ON MANUAL
               DISTRIBUTED BY RANDOM BUCKETS 1
               AS
               SELECT t1.k1, t1.v as v1, t2.v as v2
               FROM t1 INNER JOIN t2
               ON t1.k1 = t2.k1
            '''

        sql '''CREATE MATERIALIZED VIEW mv2 BUILD DEFERRED REFRESH AUTO ON MANUAL
               DISTRIBUTED BY HASH(k2) BUCKETS 6
               PROPERTIES ( 'colocate_with' = 'foo' )
               AS
               SELECT t1.k2 as k2, t1.v as v1, t2.v as v2
               FROM t1 INNER JOIN t2
               ON t1.k2 = t2.k2
            '''

        sql 'INSERT INTO t1 VALUES (1, 0, 100), (0, 2, 1000)'
        sql 'INSERT INTO t2 VALUES (1, 1, 200), (2, 2, 2000)'

        sql 'REFRESH MATERIALIZED VIEW mv1 AUTO'
        sql 'REFRESH MATERIALIZED VIEW mv2 AUTO'

        def dbName = context.config.getDbNameByFile(context.file)
        waitingMTMVTaskFinished(getJobName(dbName, 'mv1'))
        waitingMTMVTaskFinished(getJobName(dbName, 'mv2'))

        order_qt_select1 'SELECT * FROM mv1'
        order_qt_select2 'SELECT * FROM mv2'

        def newBeIndex = cluster.addBackend(1)[0]
        sleep(10 * 1000)

        def decommissionBeIdx = 1
        def decommissionBe = cluster.getBeByIndex(decommissionBeIdx)
        cluster.decommissionBackends(decommissionBeIdx)

        def backends = sql_return_maparray  'show backends'
        assertEquals(3, backends.size())
        for (def be : backends) {
            assertNotEquals(be.BackendId as long, decommissionBe.backendId)
        }

        cluster.stopBackends(2, 3)
        sleep(10 * 1000)
        cluster.checkBeIsAlive(2, false)
        cluster.checkBeIsAlive(3, false)
        cluster.checkBeIsAlive(4, true)

        order_qt_select3 'SELECT * FROM mv1'
        order_qt_select4 'SELECT * FROM mv2'
    }
}
