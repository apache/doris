
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

suite("test_partial_update_publish_conflict_with_error", "nonConcurrent") {
    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "test_partial_update_publish_conflict_with_error"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        k1 varchar(20) not null,
        v1 varchar(20),
        v2 varchar(20),
        v3 varchar(20),
        v4 varchar(20),
        v5 varchar(20),
        v6 bigint not null auto_increment(100)
        )
        UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "light_schema_change" = "true",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true")"""

    // base data
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', "k1"

        file 'concurrency_update1.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync;"
    qt_sql """ select k1, v1, v2, v3, v4, v5 from ${tableName} order by k1;"""
    qt_sql """ select v6, count(*) from ${tableName} group by v6 having count(*) > 1;"""

    // NOTE: use streamload 2pc to construct the conflict of publish
    def do_streamload_2pc_commit = { txnId ->
        def command = "curl -X PUT --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}" +
                " -H txn_id:${txnId}" +
                " -H txn_operation:commit" +
                " http://${context.config.feHttpAddress}/api/${dbName}/${tableName}/_stream_load_2pc"
        log.info("http_stream execute 2pc: ${command}")

        def process = command.execute()
        code = process.waitFor()
        out = process.text
        json2pc = parseJson(out)
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

    GetDebugPoint().clearDebugPointsForAllBEs()
    def dbug_point = 'Tablet.update_delete_bitmap.partial_update_write_rowset_fail'

    // concurrent load 1
    String txnId1
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'k1,tmp,v1=substr(tmp,1,10)'
        set 'strict_mode', "false"
        set 'two_phase_commit', 'true'
        file 'concurrency_update2.csv'
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
        set 'columns', 'k1,v2'
        set 'strict_mode', "false"
        set 'two_phase_commit', 'true'
        file 'concurrency_update3.csv'
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

    // inject failure on publish
    try {
        GetDebugPoint().enableDebugPointForAllBEs(dbug_point, [percent : 1.0])
        do_streamload_2pc_commit(txnId2)
        sleep(3000)
    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(dbug_point)
    }
    // publish will retry until success
    // FE retry may take logger time, wait for 20 secs
    wait_for_publish(txnId2, 20)

    sql "sync;"
    qt_sql """ select k1, v1, v2, v3, v4, v5 from ${tableName} order by k1;"""
    qt_sql """ select v6, count(*) from ${tableName} group by v6 having count(*) > 1;"""
}
