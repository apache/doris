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

import org.junit.Assert
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_auto_inc_replica_consistency") {
    if (isCloudMode()) {
        logger.info("skip test_auto_inc_replica_consistency in cloud mode")
        return
    }

    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "test_auto_inc_replica_consistency"
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE;"""
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL,
        `id` BIGINT NOT NULL AUTO_INCREMENT(20)
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "store_row_column" = "false"); """

    def beNodes = sql_return_maparray("show backends;")
    if (beNodes.size() > 1) {
        logger.info("skip to run the case when there are more than 1 BE.")
        return
    }

    def do_streamload_2pc_commit = { txnId ->
        def command = "curl -X PUT --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}" +
                " -H txn_id:${txnId}" +
                " -H txn_operation:commit" +
                " http://${context.config.feHttpAddress}/api/${dbName}/${tableName}/_stream_load_2pc"
        log.info("http_stream execute 2pc: ${command}")

        def process = command.execute()
        def code = process.waitFor()
        def out = process.text
        def json2pc = parseJson(out)
        log.info("http_stream 2pc result: ${out}".toString())
        assertEquals(code, 0)
        assertEquals("success", json2pc.status.toLowerCase())
    }

    def wait_for_publish = {txnId, waitSecond ->
        String st = "PREPARE"
        while (!st.equalsIgnoreCase("VISIBLE") && !st.equalsIgnoreCase("ABORTED") && waitSecond > 0) {
            Thread.sleep(1000)
            waitSecond -= 1
            def result = sql_return_maparray "show transaction from ${dbName} where id = ${txnId}"
            assertNotNull(result)
            st = result[0].TransactionStatus
        }
        log.info("Stream load with txn ${txnId} is ${st}")
        assertEquals(st, "VISIBLE")
    }


    def txnId1, txnId2
    String load1 = """{"k":1,"v1":100}"""
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'two_phase_commit', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        inputStream new ByteArrayInputStream(load1.getBytes())
        time 60000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            txnId1 = json.TxnId
            assert "success" == json.Status.toLowerCase()
        }
    }

    String load2 = """{"k":1,"v2":200}"""
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'two_phase_commit', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        inputStream new ByteArrayInputStream(load2.getBytes())
        time 60000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            txnId2 = json.TxnId
            assert "success" == json.Status.toLowerCase()
        }
    }

    do_streamload_2pc_commit(txnId1)
    wait_for_publish(txnId1, 10)
    do_streamload_2pc_commit(txnId2)
    wait_for_publish(txnId2, 10)

    sql "insert into ${tableName}(k,v1,v2) values(2,2,2);"

    qt_sql "select k,v1,v2,id from ${tableName} order by k;"
    sql "set skip_delete_bitmap=true;"
    sql "sync;"
    qt_sql "select k,v1,v2,id,__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__;"
}
