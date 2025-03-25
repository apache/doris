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
import org.junit.Assert

suite('test_drop_clone_tablet_path_race', 'docker') {
    if (isCloudMode()) {
        return
    }
    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.feConfigs += [
        'tablet_checker_interval_ms=100',
        'schedule_slot_num_per_hdd_path=1000',
        'storage_high_watermark_usage_percent=99',
        'storage_flood_stage_usage_percent=99',
    ]
    options.beNum = 3
    docker(options) {
        def table = "t1"
        def checkFunc = {size -> 
            boolean succ = false
            for (int i = 0; i < 120; i++) {
                def result = sql_return_maparray """SHOW TABLETS FROM ${table}"""
                if (result.size() == size) {
                    def version = result[0].Version
                    def state = result[0].State
                    succ = result.every { it.Version.equals(version) && it.State.equals(state) }
                    if (succ) {
                        break
                    }
                }
                sleep(1000)
            }
            Assert.assertTrue(succ)
        }

        sql """DROP TABLE IF EXISTS ${table}"""
        sql """
            CREATE TABLE `${table}` (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `score` int(11) SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`id`, `name`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
                    'replication_num' = '3'
            );
        """

        try {
            // 10h
            GetDebugPoint().enableDebugPointForAllBEs("TabletManager.start_trash_sweep.sleep")
            for(int i= 0; i < 100; ++i) {
                sql """INSERT INTO ${table} values (${i}, "${i}str", ${i} * 100)"""
            }

            sql """ALTER TABLE ${table} MODIFY PARTITION(${table}) SET ("replication_num" = "2")"""

            checkFunc(20)

            sql """ALTER TABLE ${table} MODIFY PARTITION(${table}) SET ("replication_num" = "3")"""
            checkFunc(30)
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("TabletManager.start_trash_sweep.sleep")
        }
    }
}
