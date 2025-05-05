
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
import org.apache.doris.regression.suite.ClusterOptions

suite("test_partial_update_conflict_be_restart", 'docker') {
    def dbName = context.config.getDbNameByFile(context.file)

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = false
    docker(options) {
        def table1 = "test_partial_update_conflict_be_restart"
        sql "DROP TABLE IF EXISTS ${table1};"
        sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                `k1` int NOT NULL,
                `c1` int,
                `c2` int,
                `c3` int,
                `c4` int
                )UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "disable_auto_compaction" = "true",
                "replication_num" = "1"); """

        sql "insert into ${table1} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3);"
        order_qt_sql "select * from ${table1};"

        def do_streamload_2pc_commit = { txnId ->
            def feNode = sql_return_maparray("show frontends;").get(0)
            def command = "curl -X PUT --location-trusted -u root:" +
                    " -H txn_id:${txnId}" +
                    " -H txn_operation:commit" +
                    " http://${feNode.Host}:${feNode.HttpPort}/api/${dbName}/${table1}/_stream_load_2pc"
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

        String txnId1
        streamLoad {
            table "${table1}"
            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'k1,c1,c2'
            set 'strict_mode', "false"
            set 'two_phase_commit', 'true'
            file 'data1.csv'
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
        sql "sync;"
        order_qt_sql "select * from ${table1};"

        // another partial update that conflicts with the previous load and publishes successfully
        sql "set enable_unique_key_partial_update=true;"
        sql "sync;"
        sql "insert into ${table1}(k1,c3,c4) values(1, 99, 99),(2,88,88),(3,77,77);"
        sql "set enable_unique_key_partial_update=false;"
        sql "sync;"
        order_qt_sql "select * from ${table1};"

        // restart backend
        cluster.restartBackends()
        Thread.sleep(5000)

        // wait for be restart
        boolean ok = false
        int cnt = 0
        for (; cnt < 10; cnt++) {
            def be = sql_return_maparray("show backends").get(0)
            if (be.Alive.toBoolean()) {
                ok = true
                break;
            }
            logger.info("wait for BE restart...")
            Thread.sleep(1000)
        }
        if (!ok) {
            logger.info("BE failed to restart")
            assertTrue(false)
        }

        Thread.sleep(5000)

        do_streamload_2pc_commit(txnId1)
        wait_for_publish(txnId1, 10)


        sql "sync;"
        order_qt_sql "select * from ${table1};"
        sql "DROP TABLE IF EXISTS ${table1};"
    }
}
