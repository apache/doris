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

suite("auto_partition_recycle_count_cache", "nonConcurrent") {
    def oldCheckInterval = getFeConfig("dynamic_partition_check_interval_seconds")

    try {
        sql "SET enable_nereids_planner=true"
        sql "SET enable_fallback_to_original_planner=false"
        sql "DROP TABLE IF EXISTS auto_partition_recycle_count_cache FORCE"
        sql """
            CREATE TABLE auto_partition_recycle_count_cache (
                k0 DATETIME(6) NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(k0)
            AUTO PARTITION BY RANGE(date_trunc(k0, 'day')) ()
            DISTRIBUTED BY HASH(k0) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "partition.retention_count" = "3"
            )
        """

        sql """
            INSERT INTO auto_partition_recycle_count_cache VALUES
            ('2020-01-01 00:00:00'),
            ('2020-01-02 00:00:00'),
            ('2020-01-03 00:00:00'),
            ('2020-01-04 00:00:00'),
            ('2020-01-05 00:00:00'),
            ('2020-01-06 00:00:00')
        """
        sql "sync"

        def partitionCount = sql "SHOW PARTITIONS FROM auto_partition_recycle_count_cache"
        assertEquals(6, partitionCount.size())

        sql "SELECT count(*) FROM auto_partition_recycle_count_cache"
        def cacheReady = false
        for (int i = 0; i < 30; i++) {
            def explainResult = sql "EXPLAIN SELECT count(*) FROM auto_partition_recycle_count_cache"
            if (explainResult.toString().contains("constant exprs")) {
                cacheReady = true
                break
            }
            sleep(1000)
        }
        if (!cacheReady) {
            if (isCloudMode()) {
                logger.info("SimpleAggCacheMgr did not warm up in cloud mode, skip")
                return
            }
            assertTrue(false, "SimpleAggCacheMgr cache did not warm up within 30 seconds")
        }

        def countBeforeRecycle = sql "SELECT count(*) FROM auto_partition_recycle_count_cache"
        assertEquals(6L, countBeforeRecycle[0][0] as long)

        setFeConfig("dynamic_partition_check_interval_seconds", 1)
        def recycled = false
        for (int i = 0; i < 30; i++) {
            partitionCount = sql "SHOW PARTITIONS FROM auto_partition_recycle_count_cache"
            if (partitionCount.size() == 3) {
                recycled = true
                break
            }
            sleep(1000)
        }
        assertTrue(recycled, "auto partition retention did not recycle partitions within 30 seconds")

        def countAfterRecycle = sql "SELECT count(*) FROM auto_partition_recycle_count_cache"
        assertEquals(3L, countAfterRecycle[0][0] as long)
    } finally {
        setFeConfig("dynamic_partition_check_interval_seconds", oldCheckInterval)
        sql "DROP TABLE IF EXISTS auto_partition_recycle_count_cache FORCE"
    }
}
