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

suite("test_p_seq_publish_read_from_old") {
    if (isCloudMode()) {
        logger.info("skip test_p_seq_publish_read_from_old in cloud mode")
        return
    }
    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "test_p_seq_publish_read_from_old"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL,
        `v3` BIGINT NOT NULL DEFAULT "9876",
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "function_column.sequence_col" = "v1",
        "store_row_column" = "false"); """
    sql """insert into ${tableName} values(1,100,1,1,1,1),(2,100,2,2,2,2),(3,100,3,3,3,3),(4,100,4,4,4,4);"""
    qt_sql "select k,v1,v2,v3,v4,v5 from ${tableName} order by k;"

    def inspectRows = { sqlStr ->
        sql "set skip_delete_sign=true;"
        sql "set skip_delete_bitmap=true;"
        sql "sync"
        qt_inspect sqlStr
        sql "set skip_delete_sign=false;"
        sql "set skip_delete_bitmap=false;"
        sql "sync"
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
    def txnId1, txnId2, txnId3

    String load1 = """1,100,1
2,200,1
3,50,1
"""
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'k,v1,__DORIS_DELETE_SIGN__'
        set 'strict_mode', "false"
        set 'two_phase_commit', 'true'
        set 'unique_key_update_mode', 'UPDATE_FIXED_COLUMNS'
        inputStream new ByteArrayInputStream(load1.getBytes())
        time 40000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }

            def json = parseJson(result)
            txnId1 = json.TxnId
            assertEquals("success", json.Status.toLowerCase())
        }
    }

    String load2 = """4,1"""
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'k,__DORIS_DELETE_SIGN__'
        set 'two_phase_commit', 'true'
        set 'unique_key_update_mode', 'UPDATE_FIXED_COLUMNS'
        inputStream new ByteArrayInputStream(load2.getBytes())
        time 40000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }

            def json = parseJson(result)
            txnId2 = json.TxnId
            assertEquals("success", json.Status.toLowerCase())
        }
    }


    String load3 = """1,987,77777
2,987,77777
3,987,77777
4,987,77777
"""
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'k,v2,v3'
        set 'two_phase_commit', 'true'
        set 'unique_key_update_mode', 'UPDATE_FIXED_COLUMNS'
        inputStream new ByteArrayInputStream(load3.getBytes())
        time 40000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }

            def json = parseJson(result)
            txnId3 = json.TxnId
            assertEquals("success", json.Status.toLowerCase())
        }
    }

    do_streamload_2pc_commit(txnId1)
    wait_for_publish(txnId1, 60)
    do_streamload_2pc_commit(txnId2)
    wait_for_publish(txnId2, 60)
    do_streamload_2pc_commit(txnId3)
    wait_for_publish(txnId2, 60)

    sql "sync;"
    qt_sql "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__ from ${tableName} order by k;"
    inspectRows "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__,__DORIS_DELETE_SIGN__ from ${tableName} order by k,__DORIS_VERSION_COL__,__DORIS_SEQUENCE_COL__;"
}
