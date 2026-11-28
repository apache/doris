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

// Reproducer for #70873:
// BaseTablet::lookup_row_key used to return KEY_ALREADY_EXISTS against a
// bitmap-deleted lower-seq row without continuing to an older visible row.
// A later flexible partial update then inherited that deleted sequence and
// left two visible versions of the same Unique Key; compaction dropped the
// column values that had been aligned onto the low-seq lineage.

suite("test_flexible_partial_update_deleted_seq_probe") {
    if (isCloudMode()) {
        logger.info("skip test_flexible_partial_update_deleted_seq_probe in cloud mode")
        return
    }

    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "test_flexible_partial_update_deleted_seq_probe"
    sql """ DROP TABLE IF EXISTS ${tableName} force;"""
    sql """ CREATE TABLE ${tableName} (
        `id` int(11) NULL,
        `color` varchar(32) NULL,
        `note` varchar(32) NULL,
        `seq` BIGINT NULL
        ) UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "disable_auto_compaction" = "true",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "function_column.sequence_col" = "seq",
        "store_row_column" = "false"); """

    def do_streamload_2pc_commit = { txnId ->
        def url = "http://${context.config.feHttpAddress}/api/${dbName}/${tableName}/_stream_load_2pc"
        def command = ["curl", "-X", "PUT", "--location-trusted",
                       "-u", "${context.config.feHttpUser}:${context.config.feHttpPassword ?: ''}",
                       "-H", "txn_id:${txnId}",
                       "-H", "txn_operation:commit",
                       url]
        if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
            command[-1] = url.replace("http://", "https://")
            command += ["--cert", context.config.otherConfigs.get("trustCert"),
                        "--cacert", context.config.otherConfigs.get("trustCACert"),
                        "--key", context.config.otherConfigs.get("trustCAKey")]
        }
        def process = command.collect { it.toString() }.execute()
        def outBuf = new StringBuilder()
        def errBuf = new StringBuilder()
        process.waitForProcessOutput(outBuf, errBuf)
        assertEquals(0, process.exitValue(), "2PC commit failed: ${errBuf}".toString())
        def json2pc = parseJson(outBuf.toString())
        assertEquals("success", json2pc.status.toLowerCase())
    }

    def wait_for_publish = { txnId, waitSecond ->
        String st = "PREPARE"
        while (!st.equalsIgnoreCase("VISIBLE") && !st.equalsIgnoreCase("ABORTED") && waitSecond > 0) {
            Thread.sleep(1000)
            waitSecond -= 1
            def result = sql_return_maparray "show transaction from ${dbName} where id = ${txnId}"
            assertNotNull(result)
            st = result[0].TransactionStatus
        }
        assertEquals("VISIBLE", st)
    }

    def flexible_prepare = { payload, txnHolder ->
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'two_phase_commit', 'true'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            inputStream new ByteArrayInputStream(payload.getBytes())
            time 60000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                txnHolder.txnId = json.TxnId
                assertEquals("success", json.Status.toLowerCase())
            }
        }
    }

    // Prepare A/B/C while the key is still absent so their flush-time probes see no prior row.
    def txnA = [:], txnB = [:], txnC = [:]
    flexible_prepare('{"id":1,"seq":20}', txnA)
    flexible_prepare('{"id":1,"color":"blue"}', txnB)
    flexible_prepare('{"id":1,"note":"sample"}', txnC)

    do_streamload_2pc_commit(txnA.txnId)
    wait_for_publish(txnA.txnId, 60)

    // D carries a lower explicit sequence. After publish it is bitmap-deleted while A stays visible.
    def txnD = [:]
    flexible_prepare('{"id":1,"seq":10}', txnD)
    do_streamload_2pc_commit(txnD.txnId)
    wait_for_publish(txnD.txnId, 60)

    def after_d = sql """select id, color, note, seq from ${tableName} where id = 1 order by id, seq"""
    assertEquals([[1, null, null, 20]], after_d)

    // B/C publish probes must skip deleted D (KEY_ALREADY_EXISTS + delete bitmap) and align to A.
    do_streamload_2pc_commit(txnB.txnId)
    wait_for_publish(txnB.txnId, 60)
    do_streamload_2pc_commit(txnC.txnId)
    wait_for_publish(txnC.txnId, 60)

    def after_c = sql """select id, color, note, seq from ${tableName} where id = 1 order by id, seq"""
    assertEquals([[1, "blue", "sample", 20]], after_c)

    trigger_and_wait_compaction(tableName, "cumulative")
    def after_compaction = sql """select id, color, note, seq from ${tableName} where id = 1 order by id, seq"""
    assertEquals([[1, "blue", "sample", 20]], after_compaction)
}
