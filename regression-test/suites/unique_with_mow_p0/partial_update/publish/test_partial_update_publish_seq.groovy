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

suite("test_partial_update_publish_seq") {
    if (isCloudMode()) {
        logger.info("skip test_partial_update_publish_seq in cloud mode")
        return
    }
    def inspect_rows = { sqlStr ->
        sql "set skip_delete_sign=true;"
        sql "set skip_delete_bitmap=true;"
        sql "sync"
        qt_inspect sqlStr
        sql "set skip_delete_sign=false;"
        sql "set skip_delete_bitmap=false;"
        sql "sync"
    }

    def dbName = context.config.getDbNameByFile(context.file)
    def table1 = "test_partial_update_publish_seq_map"
    sql "DROP TABLE IF EXISTS ${table1} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table1} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int,
            `c3` int,
            `c4` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_mow_light_delete" = "false",
            "disable_auto_compaction" = "true",
            "function_column.sequence_col" = "c1",
            "replication_num" = "1"); """

    sql "insert into ${table1} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3);"
    sql "sync;"
    qt_seq_map_0 "select * from ${table1} order by k1;"

    def load_data = { String cols, String data ->
        def txnId
        streamLoad {
            table "${table1}"
            set 'format', 'csv'
            set 'column_separator', ','
            set 'strict_mode', 'false'
            set 'columns', cols
            set 'two_phase_commit', 'true'
            set 'partial_columns', 'true'
            inputStream new ByteArrayInputStream(data.getBytes())
            time 60000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                txnId = json.TxnId
                assertEquals("success", json.Status.toLowerCase())
            } 
        }
        return txnId
    }

    def do_streamload_2pc_commit = { txnId ->
        def command = "curl -X PUT --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}" +
                " -H txn_id:${txnId}" +
                " -H txn_operation:commit" +
                " http://${context.config.feHttpAddress}/api/${dbName}/${table1}/_stream_load_2pc"
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

    // with seq map val, >/=/< conflicting seq val
    def txn1 = load_data("k1,c1,c2", "1,10,99\n2,10,99\n3,10,99\n")
    def txn2 = load_data("k1,c1,c3", "1,20,88\n2,10,88\n3,5,88\n")
    do_streamload_2pc_commit(txn1)
    wait_for_publish(txn1, 10)
    do_streamload_2pc_commit(txn2)
    wait_for_publish(txn2, 10)
    
    qt_seq_map_1 "select * from ${table1} order by k1;"
    inspect_rows "select *,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__ from ${table1} order by k1,__DORIS_VERSION_COL__;"

    // without seq map val, the filled seq val >/=/< conflicting seq val
    def txn3 = load_data("k1,c1,c2", "1,9,77\n2,10,77\n3,50,77\n")
    def txn4 = load_data("k1,c4", "1,33\n2,33\n3,33\n")
    do_streamload_2pc_commit(txn3)
    wait_for_publish(txn3, 10)
    do_streamload_2pc_commit(txn4)
    wait_for_publish(txn4, 10)
    qt_seq_map_2 "select * from ${table1} order by k1;"
    inspect_rows "select *,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__ from ${table1} order by k1,__DORIS_VERSION_COL__;"

    // with delete sign and seq col val, >/=/< conflicting seq val
    def txn5 = load_data("k1,c1,c2", "1,80,66\n2,100,66\n3,120,66\n")
    def txn6 = load_data("k1,c1,__DORIS_DELETE_SIGN__", "1,100,1\n2,100,1\n3,100,1\n")
    do_streamload_2pc_commit(txn5)
    wait_for_publish(txn5, 10)
    do_streamload_2pc_commit(txn6)
    wait_for_publish(txn6, 10)

    qt_seq_map_3 "select * from ${table1} order by k1;"
    inspect_rows "select *,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__ from ${table1} order by k1,__DORIS_VERSION_COL__;"


    sql "truncate table ${table1};"
    sql "insert into ${table1} values(1,10,1,1,1),(2,10,2,2,2),(3,10,3,3,3);"
    sql "sync;"
    // with delete sign and without seq col val, >/=/< conflicting seq val

    def txn7 = load_data("k1,c1,c2", "1,20,55\n2,100,55\n3,120,55\n")
    def txn8 = load_data("k1,c4,__DORIS_DELETE_SIGN__", "1,100,1\n2,100,1\n3,100,1\n")
    do_streamload_2pc_commit(txn7)
    wait_for_publish(txn7, 10)
    do_streamload_2pc_commit(txn8)
    wait_for_publish(txn8, 10)

    qt_seq_map_4 "select * from ${table1} order by k1;"
    inspect_rows "select *,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__ from ${table1} order by k1,__DORIS_VERSION_COL__;"
}
