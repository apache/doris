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

suite("test_row_binlog_tablet_schedule_locality", "docker,nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def collectPathHashByMedium = { medium ->
        def pathHashes = [] as Set
        sql_return_maparray("SHOW PROC '/backends'").each {
            def paths = sql_return_maparray("SHOW PROC '/backends/${it.BackendId}'")
            paths.each { path ->
                if (path.RootPath.endsWith(medium)) {
                    pathHashes.add(path.PathHash)
                }
            }
        }
        return pathHashes
    }

    def assertVisibleTabletsOnMedium = { tableName, medium ->
        def targetPathHashes = collectPathHashByMedium(medium)
        assertFalse(targetPathHashes.isEmpty(), "no ${medium} path hash found")

        def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
        assertTrue(tablets.size() >= 3, "replicated table should expose base tablet replicas")
        tablets.each {
            assertTrue(it.PathHash in targetPathHashes,
                    "tablet path hash ${it.PathHash} should be on ${medium}: ${targetPathHashes}")
        }
    }

    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.feConfigs += [
        "tablet_checker_interval_ms=1000",
        "tablet_schedule_interval_ms=1000",
        "agent_task_resend_wait_time_ms=1000"
    ]
    options.beConfigs += [
        "report_random_wait=false",
        "report_tablet_interval_seconds=1",
        "report_disk_state_interval_seconds=1"
    ]
    options.beDisks = ["HDD=1", "SSD=1"]

    docker(options) {
        cluster.checkBeIsAlive(1, true)
        cluster.checkBeIsAlive(2, true)
        cluster.checkBeIsAlive(3, true)
        sleep 2000

        sql "DROP TABLE IF EXISTS test_row_binlog_tablet_schedule_locality FORCE"
        sql """
            CREATE TABLE test_row_binlog_tablet_schedule_locality (
                k1 INT,
                k2 INT,
                v1 INT,
                v2 STRING
            )
            UNIQUE KEY(k1, k2)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "3",
                "storage_medium" = "HDD",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """

        sql """
            INSERT INTO test_row_binlog_tablet_schedule_locality VALUES
                (1, 1, 10, '10'),
                (2, 2, 20, '20'),
                (3, 3, 30, '30')
        """
        assertVisibleTabletsOnMedium("test_row_binlog_tablet_schedule_locality", "HDD")

        sql "SET enable_unique_key_partial_update = true"
        sql "INSERT INTO test_row_binlog_tablet_schedule_locality(k1, k2, v2) VALUES (2, 2, '200')"
        sql "SET enable_unique_key_partial_update = false"
        sql "DELETE FROM test_row_binlog_tablet_schedule_locality WHERE k1 = 1 AND k2 = 1"

        def binlogRows = sql_return_maparray """
            SELECT COUNT(*) AS binlog_count
            FROM binlog("table" = "test_row_binlog_tablet_schedule_locality")
        """
        assertTrue((binlogRows[0].values().first() as int) >= 5,
                "row binlog should remain readable on a replicated multi-disk cluster")

        assertVisibleTabletsOnMedium("test_row_binlog_tablet_schedule_locality", "HDD")
    }
}
