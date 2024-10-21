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

suite('test_publish_slow_not_wait') {
    def options = new ClusterOptions()
    options.beNum = 3
    options.feConfigs.add('disable_tablet_scheduler=true')
    options.feConfigs.add('publish_version_queued_limit_number=2')
    options.beConfigs.add('report_task_interval_seconds=1')
    options.enableDebugPoints()
    docker(options) {
        def tbl = 'test_publish_slow_not_wait_tbl'

        sql """ DROP TABLE IF EXISTS ${tbl} """

        sql """
         CREATE TABLE ${tbl} (
           `k1` int(11) NULL,
           `k2` int(11) NULL
         )
         DUPLICATE KEY(`k1`, `k2`)
         COMMENT 'OLAP'
         DISTRIBUTED BY HASH(`k1`) BUCKETS 1
         PROPERTIES (
           "replication_num"="3"
         );
         """
        def be1 = cluster.getBeByIndex(1)
        // be1 dp publish duration 10mins
        be1.enableDebugPoint('EnginePublishVersionTask.finish.wait', ['duration': 10*60*1000])

        sql 'SET GLOBAL insert_visible_timeout_ms = 10000'
        def begin = System.currentTimeMillis();
        def result
        for (def i = 1; i <= 2; i++) {
            result = sql """INSERT INTO ${tbl} (k1, k2) VALUES (${i}, ${i}*10)"""
        }
        def cost = System.currentTimeMillis() - begin;
        log.info("insert1 time cost : {}, result: {}", cost, result)
        // be1's replica publish slow, so wait, and txn's status: COMMITTED
        assertTrue(cost > 2 * 10000 && cost < 100000)
        qt_sql """select * from ${tbl}"""

        begin = System.currentTimeMillis();
        result = sql """INSERT INTO ${tbl} (k1, k2) VALUES (3, 30)""" 
        cost = System.currentTimeMillis() - begin;
        log.info("insert2 time cost: {}", cost)
        assertTrue(cost < 10000)

        qt_sql """select * from ${tbl}"""

        be1.clearDebugPoints()
    }
}
