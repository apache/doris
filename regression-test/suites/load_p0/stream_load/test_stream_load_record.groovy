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

// nonConcurrent because it toggles the BE config enable_stream_load_record.
suite("test_stream_load_record", "p0,nonConcurrent") {
    def tableName = "test_stream_load_record"

    // Enable BE RocksDB stream-load-record so the completed record is persisted and can be read
    // on demand by information_schema.loads.
    set_be_param("enable_stream_load_record", "true")
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

        def label = "test_stream_load_record_" + UUID.randomUUID().toString().replace("-", "")

        // test strict_mode success
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

        // Query information_schema.loads directly. This reads BE RocksDB on demand and does NOT
        // wait for SHOW STREAM LOAD / the FE periodic cache. The only latency is the BE persisting
        // the record to RocksDB right after the load finishes, so a short bounded retry is enough.
        def loadRows = []
        def count = 0
        while (true) {
            sleep(1000)
            loadRows = sql """
                SELECT
                    LABEL,
                    STATE,
                    PROGRESS,
                    TYPE,
                    TASK_INFO,
                    ERROR_MSG,
                    URL,
                    JOB_DETAILS,
                    USER,
                    COMMENT,
                    FIRST_ERROR_MSG
                FROM information_schema.loads
                WHERE LABEL = '${label}' AND TYPE = 'STREAM_LOAD'
            """
            log.info("information_schema.loads stream load result for ${label}: ${loadRows}")
            if (loadRows.size() > 0) {
                break
            }
            if (count > 60) {
                assertTrue(false, "information_schema.loads should contain stream load label ${label}")
            }
            count++
        }

        // Selected column order above:
        // 0 LABEL, 1 STATE, 2 PROGRESS, 3 TYPE, 4 TASK_INFO, 5 ERROR_MSG, 6 URL,
        // 7 JOB_DETAILS, 8 USER, 9 COMMENT, 10 FIRST_ERROR_MSG
        def row = loadRows[0]

        // STATE is now unified with LoadManager vocabulary: "Success" → "FINISHED"
        assertEquals(label, row[0].toString())            // LABEL
        assertEquals("FINISHED", row[1].toString())       // STATE (unified: Success→FINISHED)
        assertEquals("100%", row[2].toString())           // PROGRESS
        assertEquals("STREAM_LOAD", row[3].toString())    // TYPE
        assertTrue(row[8].toString().length() > 0)        // USER is populated
        // ERROR_MSG / URL / COMMENT / FIRST_ERROR_MSG are present (non-null) even when empty.
        assertNotNull(row[5])                             // ERROR_MSG
        assertNotNull(row[6])                             // URL
        assertNotNull(row[9])                             // COMMENT
        assertNotNull(row[10])                            // FIRST_ERROR_MSG

        // TASK_INFO JSON carries Db / Table / ClientIp.
        def taskInfo = row[4].toString()
        assertTrue(taskInfo.contains("Db"))
        assertTrue(taskInfo.contains("Table"))
        assertTrue(taskInfo.contains("ClientIp"))
        assertTrue(taskInfo.contains(tableName))

        // JOB_DETAILS JSON carries row/byte/timing counters.
        def jobDetails = row[7].toString()
        assertTrue(jobDetails.contains("TotalRows"))
        assertTrue(jobDetails.contains("LoadedRows"))
        assertTrue(jobDetails.contains("FilteredRows"))
        assertTrue(jobDetails.contains("UnselectedRows"))
        assertTrue(jobDetails.contains("LoadBytes"))
        assertTrue(jobDetails.contains("BeginTxnTimeMs"))
        assertTrue(jobDetails.contains("StreamLoadPutTimeMs"))
    } finally {
        set_be_param("enable_stream_load_record", "false")
    }
}
