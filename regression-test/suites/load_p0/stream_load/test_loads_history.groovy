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

// nonConcurrent because it toggles BE config enable_stream_load_record.
suite("test_loads_history", "p0,nonConcurrent") {
    def tableName = "test_loads_history"

    // Persist Stream Load records to BE RocksDB (source for loads / loads_history), and make sure
    // the history syncer is enabled on the master FE.
    set_be_param("enable_stream_load_record", "true")
    sql "ADMIN SET FRONTEND CONFIG ('enable_loads_history' = 'true')"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` bigint(20) NULL,
                `k2` bigint(20) NULL,
                `v1` tinyint(4) SUM NULL,
                `v2` tinyint(4) REPLACE NULL,
                `v3` tinyint(4) REPLACE_IF_NOT_NULL NULL,
                `v4` smallint(6) REPLACE_IF_NOT_NULL NULL,
                `v5` int(11) REPLACE_IF_NOT_NULL NULL,
                `v6` bigint(20) REPLACE_IF_NOT_NULL NULL,
                `v7` largeint(40) REPLACE_IF_NOT_NULL NULL,
                `v8` datetime REPLACE_IF_NOT_NULL NULL,
                `v9` date REPLACE_IF_NOT_NULL NULL,
                `v10` char(10) REPLACE_IF_NOT_NULL NULL,
                `v11` varchar(6) REPLACE_IF_NOT_NULL NULL,
                `v12` decimal(27, 9) REPLACE_IF_NOT_NULL NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`k1`, `k2`)
            COMMENT 'OLAP'
            PARTITION BY RANGE(`k1`)
            (PARTITION partition_a VALUES [("-9223372036854775808"), ("100000")),
            PARTITION partition_b VALUES [("100000"), ("1000000000")),
            PARTITION partition_c VALUES [("1000000000"), ("10000000000")),
            PARTITION partition_d VALUES [("10000000000"), (MAXVALUE)))
            DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        def label = "test_loads_history_" + UUID.randomUUID().toString().replace("-", "")

        streamLoad {
            table "${tableName}"

            set 'label', label
            set 'column_separator', '\t'
            set 'columns', 'k1, k2, v2, v10, v11'
            set 'partitions', 'partition_a, partition_b, partition_c, partition_d'
            set 'strict_mode', 'true'

            file 'test_strict_mode.csv'
            time 10000 // limit inflight 10s
        }

        // The record appears in information_schema.loads on demand (BE RocksDB read).
        def loadsRows = []
        def count = 0
        while (true) {
            sleep(1000)
            loadsRows = sql """
                SELECT LABEL, STATE, TYPE, PROGRESS, TASK_INFO, JOB_DETAILS
                FROM information_schema.loads
                WHERE LABEL = '${label}' AND TYPE = 'STREAM_LOAD'
            """
            log.info("information_schema.loads for ${label}: ${loadsRows}")
            if (loadsRows.size() > 0) {
                break
            }
            if (count > 60) {
                assertTrue(false, "information_schema.loads should contain stream load label ${label}")
            }
            count++
        }

        // The history syncer runs on the master FE every loads_history_sync_interval_second
        // (default 60s). Wait until the same label is persisted into information_schema.loads_history.
        def historyRows = []
        count = 0
        while (true) {
            sleep(2000)
            historyRows = sql """
                SELECT LABEL, STATE, TYPE, PROGRESS, TASK_INFO, JOB_DETAILS
                FROM information_schema.loads_history
                WHERE LABEL = '${label}' AND TYPE = 'STREAM_LOAD'
            """
            log.info("information_schema.loads_history for ${label}: ${historyRows}")
            if (historyRows.size() > 0) {
                break
            }
            if (count > 120) {
                assertTrue(false, "information_schema.loads_history should contain stream load label ${label}")
            }
            count++
        }

        def historyRow = historyRows[0]
        // Field semantics align with loads (step-1 Stream Load mapping rules).
        assertEquals(label, historyRow[0].toString())          // LABEL
        assertEquals("STREAM_LOAD", historyRow[2].toString())  // TYPE
        assertEquals("100%", historyRow[3].toString())         // PROGRESS
        assertTrue(historyRow[4].toString().contains("Db"))    // TASK_INFO json
        assertTrue(historyRow[4].toString().contains("Table"))
        assertTrue(historyRow[4].toString().contains(tableName))
        assertTrue(historyRow[5].toString().contains("TotalRows")) // JOB_DETAILS json
        assertTrue(historyRow[5].toString().contains("LoadBytes"))

        // The original loads record is NOT removed by history write.
        def loadsStillThere = sql """
            SELECT LABEL FROM information_schema.loads
            WHERE LABEL = '${label}' AND TYPE = 'STREAM_LOAD'
        """
        assertTrue(loadsStillThere.size() > 0, "loads record must survive history sync")

        // Idempotency: wait for at least one more sync cycle, loads_history must not duplicate the
        // logical row (UNIQUE KEY upsert).
        sleep(70000)
        def dedupRows = sql """
            SELECT COUNT(*) FROM information_schema.loads_history
            WHERE LABEL = '${label}' AND TYPE = 'STREAM_LOAD'
        """
        log.info("loads_history dedup count for ${label}: ${dedupRows}")
        assertEquals(1L, (dedupRows[0][0]) as long)

        // UNION ALL of the two aligned views executes directly (column alignment check).
        def unionRows = sql """
            SELECT LABEL, STATE, TYPE FROM information_schema.loads WHERE LABEL = '${label}'
            UNION ALL
            SELECT LABEL, STATE, TYPE FROM information_schema.loads_history WHERE LABEL = '${label}'
        """
        log.info("UNION ALL loads + loads_history for ${label}: ${unionRows}")
        assertTrue(unionRows.size() >= 1)
    } finally {
        set_be_param("enable_stream_load_record", "false")
    }
}
