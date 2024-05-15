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

suite('test_drop_clone_tablet_path_race') {
    if (isCloudMode()) {
        return
    }
    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.beNum = 3
    
    docker(options) {
        def table = "t1"
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
            GetDebugPoint().enableDebugPointForAllBEs("TabletManager.start_trash_sweep.sleep", [duration:36000000])
            for(int i= 0; i < 100; ++i) {
                sql """INSERT INTO ${table} values (${i}, "${i}str", ${i} * 100)"""
            }

            sql """ALTER TABLE ${table} MODIFY PARTITION(${table}) SET ("replication_num" = "2")"""

            sleep(60 * 1000)
            boolean succ = false
            for (int i = 0; i < 60; i++) {
                def result = sql """SHOW TABLETS FROM ${table}"""
                if (result.size() == 20) {
                    succ = true
                    break
                }
                sleep(1000)
            }
            Assert.assertTrue(succ)

            sql """ALTER TABLE ${table} MODIFY PARTITION(${table}) SET ("replication_num" = "3")"""

            sleep(60 * 1000)
            succ = false
            for (int i = 0; i < 60; i++) {
                def result = sql """SHOW TABLETS FROM ${table}"""
                if (result.size() == 30) {
                    succ = true
                    break
                }
                sleep(1000)
            }
            Assert.assertTrue(succ)
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("TabletManager.start_trash_sweep.sleep")
        }
    }
}
