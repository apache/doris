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

suite("test_row_binlog_publish_conflict", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def dbName = context.config.getDbNameByFile(context.file)

    sql "DROP TABLE IF EXISTS test_mow_publish_conflict_with_binlog FORCE"

    sql """
        CREATE TABLE test_mow_publish_conflict_with_binlog (
            k1 INT,
            k2 INT,
            k3 INT,
            v1 INT,
            v2 STRING
        )
        UNIQUE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """ALTER TABLE test_mow_publish_conflict_with_binlog
             ENABLE FEATURE \"SEQUENCE_LOAD\"
             WITH PROPERTIES (\"function_column.sequence_type\" = \"int\")"""

    sql """
        INSERT INTO test_mow_publish_conflict_with_binlog(k1, k2, k3, v1, v2, __DORIS_SEQUENCE_COL__)
        VALUES
            (1, 1, 1, 10, '10', 1),
            (2, 2, 2, 20, '20', 1),
            (3, 3, 3, 30, '30', 1)
    """
    sql "sync"

    def do_streamload_2pc_commit = { txnId ->
        def command = "curl -X PUT --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}" +
                " -H txn_id:${txnId}" +
                " -H txn_operation:commit" +
                " http://${context.config.feHttpAddress}/api/${dbName}/test_mow_publish_conflict_with_binlog/_stream_load_2pc"
        log.info("http_stream execute 2pc: ${command}")
        def process = command.execute()
        def code = process.waitFor()
        def out = process.text
        def json2pc = parseJson(out)
        log.info("http_stream 2pc result: ${out}".toString())
        assertEquals(code, 0)
        assertEquals("success", json2pc.status.toLowerCase())
    }

    def wait_for_status = { txnId, expectedStatus, waitSecond ->
        String st = "PREPARE"
        while (!st.equalsIgnoreCase(expectedStatus) && !st.equalsIgnoreCase("ABORTED") && waitSecond > 0) {
            Thread.sleep(1000)
            waitSecond -= 1
            def result = sql_return_maparray "show transaction from ${dbName} where id = ${txnId}"
            assertNotNull(result)
            st = result[0].TransactionStatus
        }
        log.info("Stream load with txn ${txnId} is ${st}")
        assertEquals(st, expectedStatus)
    }

    def block_publish = {
        GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
        GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.block")
    }

    def unblock_publish = {
        GetDebugPoint().disableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
        GetDebugPoint().disableDebugPointForAllBEs("EnginePublishVersionTask::execute.block")
    }

    String txnId1
    streamLoad {
        table "test_mow_publish_conflict_with_binlog"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'strict_mode', 'false'
        set 'two_phase_commit', 'true'
        set 'partial_columns', 'true'
        set 'columns', 'k1,k2,k3,v2,seq'
        set 'function_column.sequence_col', 'seq'

        String content = "2,2,2,200,2\n3,3,3,300,2\n"
        inputStream new ByteArrayInputStream(content.getBytes())
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            txnId1 = json.TxnId
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }

    String txnId2
    streamLoad {
        table "test_mow_publish_conflict_with_binlog"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'strict_mode', 'false'
        set 'two_phase_commit', 'true'
        set 'partial_columns', 'true'
        set 'columns', 'k1,k2,k3,v1,seq'
        set 'function_column.sequence_col', 'seq'

        String content = "2,2,2,2200,3\n"
        inputStream new ByteArrayInputStream(content.getBytes())
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            txnId2 = json.TxnId
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(1, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }

    try {
        GetDebugPoint().clearDebugPointsForAllBEs()
        block_publish()

        // Commit two prepared partial-update transactions while publish is blocked.
        do_streamload_2pc_commit(txnId1)
        do_streamload_2pc_commit(txnId2)

        wait_for_status(txnId1, "COMMITTED", 20)
        wait_for_status(txnId2, "COMMITTED", 20)

        unblock_publish()

        wait_for_status(txnId1, "VISIBLE", 60)
        wait_for_status(txnId2, "VISIBLE", 60)

        qt_row_binlog_publish_conflict """
            SELECT __DORIS_BINLOG_LSN__ DIV 18446744073709551616 AS version,
                   __DORIS_BINLOG_OP__ AS op,
                   k1,
                   k2,
                   k3,
                   v1,
                   v2,
                   __BEFORE__v1__,
                   __BEFORE__v2__
            FROM binlog("table" = "test_mow_publish_conflict_with_binlog")
            WHERE k1 IN (1, 2, 3)
            ORDER BY __DORIS_BINLOG_LSN__
        """

        sql "SET skip_delete_bitmap = true"
        qt_row_binlog_publish_conflict_skip_bitmap """
            SELECT __DORIS_BINLOG_LSN__ DIV 18446744073709551616 AS version,
                   __DORIS_BINLOG_OP__ AS op,
                   k1,
                   k2,
                   k3,
                   v1,
                   v2,
                   __BEFORE__v1__,
                   __BEFORE__v2__
            FROM binlog("table" = "test_mow_publish_conflict_with_binlog")
            WHERE k1 IN (1, 2, 3)
            ORDER BY __DORIS_BINLOG_LSN__
        """

        // Reset session variable before testing upsert path.
        sql "SET skip_delete_bitmap = false"

        // Upsert (full columns) goes through a different write/publish path than partial-update.
        String txnId3
        streamLoad {
            table "test_mow_publish_conflict_with_binlog"
            set 'column_separator', ','
            set 'format', 'csv'
            set 'strict_mode', 'false'
            set 'two_phase_commit', 'true'
            // full columns
            set 'columns', 'k1,k2,k3,v1,v2,seq'
            set 'function_column.sequence_col', 'seq'

            String content = "2,2,2,2000,2000,4\n3,3,3,3000,3000,4\n"
            inputStream new ByteArrayInputStream(content.getBytes())
            time 60000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                txnId3 = json.TxnId
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(2, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        String txnId4
        streamLoad {
            table "test_mow_publish_conflict_with_binlog"
            set 'column_separator', ','
            set 'format', 'csv'
            set 'strict_mode', 'false'
            set 'two_phase_commit', 'true'
            // full columns
            set 'columns', 'k1,k2,k3,v1,v2,seq'
            set 'function_column.sequence_col', 'seq'

            String content = "2,2,2,2222,2222,5\n"
            inputStream new ByteArrayInputStream(content.getBytes())
            time 60000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                txnId4 = json.TxnId
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(1, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        block_publish()

        // Commit two prepared upsert transactions while publish is blocked.
        do_streamload_2pc_commit(txnId3)
        do_streamload_2pc_commit(txnId4)

        wait_for_status(txnId3, "COMMITTED", 20)
        wait_for_status(txnId4, "COMMITTED", 20)

        unblock_publish()

        wait_for_status(txnId3, "VISIBLE", 60)
        wait_for_status(txnId4, "VISIBLE", 60)

        qt_row_binlog_publish_conflict_upsert """
            SELECT __DORIS_BINLOG_LSN__ DIV 18446744073709551616 AS version,
                   __DORIS_BINLOG_OP__ AS op,
                   k1,
                   k2,
                   k3,
                   v1,
                   v2,
                   __BEFORE__v1__,
                   __BEFORE__v2__
            FROM binlog("table" = "test_mow_publish_conflict_with_binlog")
            WHERE k1 IN (2, 3)
            ORDER BY __DORIS_BINLOG_LSN__
        """

        sql "SET skip_delete_bitmap = true"
        qt_row_binlog_publish_conflict_upsert_skip_bitmap """
            SELECT __DORIS_BINLOG_LSN__ DIV 18446744073709551616 AS version,
                   __DORIS_BINLOG_OP__ AS op,
                   k1,
                   k2,
                   k3,
                   v1,
                   v2,
                   __BEFORE__v1__,
                   __BEFORE__v2__
            FROM binlog("table" = "test_mow_publish_conflict_with_binlog")
            WHERE k1 IN (2, 3)
            ORDER BY __DORIS_BINLOG_LSN__
        """
    } finally {
        sql "SET skip_delete_bitmap = false"
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
