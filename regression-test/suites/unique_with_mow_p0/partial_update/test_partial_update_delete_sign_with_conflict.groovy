
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

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpPut
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.RedirectStrategy
import org.apache.http.protocol.HttpContext
import org.apache.http.HttpRequest
import org.apache.http.impl.client.LaxRedirectStrategy
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.util.EntityUtils

suite("test_partial_update_delete_sign_with_conflict") {
    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "test_partial_update_delete_sign_with_conflict"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int NOT NULL,
            `c1` int default 100,
            `c2` int,
            `c3` int,
            `c4` varchar(100) default 'foo'
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${tableName} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5);"
    sql "sync;"
    qt_sql "select * from ${tableName} order by k1,c1,c2,c3,c4;"

    // NOTE: use streamload 2pc to construct the conflict of publish
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

    // concurrent load 1
    String txnId1
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'k1,c1,label_c2'
        set 'merge_type', 'MERGE'
        set 'delete', 'label_c2=1'
        set 'strict_mode', 'false'
        set 'two_phase_commit', 'true'
        file 'partial_update_parallel_with_delete_sign.csv'
        time 10000 // limit inflight 10s
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            txnId1 = json.TxnId
            assertEquals("success", json.Status.toLowerCase())
        }
    }

    String txnId2
    // concurrent load 2
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'k1,c2'
        set 'strict_mode', "false"
        set 'two_phase_commit', 'true'
        file 'partial_update_parallel3.csv'
        time 10000 // limit inflight 10s
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            txnId2 = json.TxnId
            assertEquals("success", json.Status.toLowerCase())
        }
    }
    sql "sync;"

    // complete load 1 first
    do_streamload_2pc_commit(txnId1)
    wait_for_publish(txnId1, 10)

    sql "sync;"
    qt_sql "select * from ${tableName} order by k1,c1,c2,c3,c4;"

    // publish will retry until success
    // FE retry may take logger time, wait for 20 secs
    do_streamload_2pc_commit(txnId2)
    wait_for_publish(txnId2, 20)

    sql "sync;"
    qt_sql "select * from ${tableName} order by k1,c1,c2,c3,c4;"
}
