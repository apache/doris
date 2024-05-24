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

suite("test_clean_trash", "p0") {
    if (isCloudMode()) {
        return
    }
    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.feConfigs += [
        'tablet_checker_interval_ms=100',
        'schedule_slot_num_per_hdd_path=1000',
        'storage_high_watermark_usage_percent=99',
        'storage_flood_stage_usage_percent=99'
    ]
    options.beConfigs += [
        'max_garbage_sweep_interval=2',
        'min_garbage_sweep_interval=1',
        'report_disk_state_interval_seconds=1'
    ]
    options.beNum = 3
    docker(options) {
        def checkFunc = { boolean trashZero ->
            def succ = false
            for (int i=0; i < 300; ++i) {
                def bes = sql_return_maparray """show backends"""
                succ = bes.every {
                    if (trashZero) {
                        return "0.000".equals(it.TrashUsedCapacity.trim())
                    } else {
                        return !"0.000".equals((it.TrashUsedCapacity).trim())
                    }
                }
                if (succ) {
                    break;
                }
                sleep(1000)
            }
            Assert.assertTrue(succ) 
        }
        def table = "test_clean_trash_table"
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
        for(int i= 0; i < 10; ++i) {
            sql """INSERT INTO ${table} values (${i}, "${i}str", ${i} * 100)"""
        }

        sql """drop table ${table} force"""
        checkFunc(false)
        sql """admin clean trash"""
        checkFunc(true)
    }
}