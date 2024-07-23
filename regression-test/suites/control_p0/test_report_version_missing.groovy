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

suite('test_report_version_missing', "nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    def tableName = "test_set_replica_status_table_in_docker"
    try {
    setFeConfig('disable_tablet_scheduler', true)
    Thread.sleep(2000)

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """
    List<String> values = []
    for (int i = 1; i <= 10; ++i) {
        values.add("(${i}, ${i})")
    }
    sql """INSERT INTO ${tableName} VALUES ${values.join(",")}"""

    def result = sql_return_maparray """show tablets from ${tableName}"""
    assertNotNull(result)
    def tabletId = null
    for (def res : result) {
        tabletId = res.TabletId
        break
    }

        GetDebugPoint().enableDebugPointForAllBEs("Tablet.build_tablet_report_info.version_miss", [tablet_id:"${tabletId}",version_miss:true])
        boolean succ = false

        for (int i = 0; i < 3; ++i) {
            result = sql_return_maparray """show tablets from ${tableName}"""
            logger.info("show tablets from ${result}, has after ${i} * 60 s")
            assertNotNull(result)
            // LstFailedVersion > 0, version missing
            for (def res : result) {
                if (res.TabletId.toLong() == tabletId.toLong() && res.LstFailedVersion.toLong() > 0) {
                    succ = true
                    break
                }
            }
            if (succ) {
                break
            }
            Thread.sleep(60000)
        }
        assertTrue(succ)
        
    } finally {
        setFeConfig('disable_tablet_scheduler', false)
        GetDebugPoint().disableDebugPointForAllBEs("Tablet.build_tablet_report_info.version_miss")
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
