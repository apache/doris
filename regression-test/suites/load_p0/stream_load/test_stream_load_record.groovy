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

suite("test_stream_load_record", "p0") {
    def tableName = "test_stream_load_record"

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

    // Wait until SHOW STREAM LOAD sees the exact label. FE pulls stream load records
    // from BE asynchronously (fetch_stream_load_record_interval_second, default 120s),
    // so we retry rather than assume immediate visibility.
    def showStreamLoadRows = []
    def count = 0
    while (true) {
        sleep(1000)
        showStreamLoadRows = sql "SHOW STREAM LOAD WHERE LABEL = '${label}'"
        log.info("SHOW STREAM LOAD result for ${label}: ${showStreamLoadRows}")
        if (showStreamLoadRows.size() > 0) {
            break
        }
        if (count > 150) {
            assertTrue(false, "SHOW STREAM LOAD should contain label ${label}")
        }
        count++
    }

    // information_schema.load_jobs reads from the same FE StreamLoadRecordMgr cache
    // as SHOW STREAM LOAD, so once SHOW STREAM LOAD has the row the load_jobs row
    // should be available on the next query. Keep a bounded retry to absorb any
    // catalog scan latency.
    def loadJobRows = []
    count = 0
    while (true) {
        sleep(1000)
        loadJobRows = sql """
            SELECT
                LABEL,
                STATE,
                PROGRESS,
                TYPE,
                TASK_INFO,
                ERROR_MSG,
                LOAD_START_TIME,
                LOAD_FINISH_TIME,
                URL,
                JOB_DETAILS,
                USER,
                COMMENT,
                FIRST_ERROR_MSG
            FROM information_schema.load_jobs
            WHERE LABEL = '${label}' AND TYPE = 'STREAM_LOAD'
        """
        log.info("information_schema.load_jobs stream load result for ${label}: ${loadJobRows}")
        if (loadJobRows.size() > 0) {
            break
        }
        if (count > 150) {
            assertTrue(false, "information_schema.load_jobs should contain stream load label ${label}")
        }
        count++
    }

    // SHOW STREAM LOAD column order (see ShowLoadCommand.STREAM_LOAD_TITLE_NAMES):
    // 0 Label, 1 Db, 2 Table, 3 ClientIp, 4 Status, 5 Message, 6 Url, 7 TotalRows,
    // 8 LoadedRows, 9 FilteredRows, 10 UnselectedRows, 11 LoadBytes, 12 StartTime,
    // 13 FinishTime, 14 User, 15 Comment, 16 FirstErrorMsg
    def showRow = showStreamLoadRows[0]
    def loadJobRow = loadJobRows[0]

    assertEquals(showRow[0], loadJobRow[0])   // Label
    assertEquals(showRow[4], loadJobRow[1])   // Status -> State
    assertEquals("100%", loadJobRow[2])
    assertEquals("STREAM_LOAD", loadJobRow[3])
    assertTrue(loadJobRow[4].toString().contains("Db"))
    assertTrue(loadJobRow[4].toString().contains("Table"))
    assertTrue(loadJobRow[4].toString().contains("ClientIp"))
    assertTrue(loadJobRow[4].toString().contains(showRow[1].toString()))
    assertTrue(loadJobRow[4].toString().contains(showRow[2].toString()))
    assertTrue(loadJobRow[4].toString().contains(showRow[3].toString()))
    assertEquals(showRow[5], loadJobRow[5])   // Message -> ErrorMsg
    assertEquals(showRow[12], loadJobRow[6])  // StartTime -> LoadStartTime
    assertEquals(showRow[13], loadJobRow[7])  // FinishTime -> LoadFinishTime
    assertEquals(showRow[6], loadJobRow[8])   // Url
    assertTrue(loadJobRow[9].toString().contains("TotalRows"))
    assertTrue(loadJobRow[9].toString().contains("LoadedRows"))
    assertTrue(loadJobRow[9].toString().contains("FilteredRows"))
    assertTrue(loadJobRow[9].toString().contains("UnselectedRows"))
    assertTrue(loadJobRow[9].toString().contains("LoadBytes"))
    assertTrue(loadJobRow[9].toString().contains("BeginTxnTimeMs"))
    assertTrue(loadJobRow[9].toString().contains("StreamLoadPutTimeMs"))
    assertEquals(showRow[14], loadJobRow[10]) // User
    assertEquals(showRow[15], loadJobRow[11]) // Comment
    assertEquals(showRow[16], loadJobRow[12]) // FirstErrorMsg
}
