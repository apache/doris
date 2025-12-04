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

import org.apache.doris.regression.suite.ClusterOptions
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

suite("test_local_multi_segments_re_calc_in_publish", "docker") {

    def dbName = context.config.getDbNameByFile(context.file)

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = false
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    options.beConfigs += [
        'doris_scanner_row_bytes=1' // to cause multi segments
    ]
    options.enableDebugPoints()

    docker(options) {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()

        def fe = cluster.getFeByIndex(1)

        def table1 = "test_local_multi_segments_re_calc_in_publish"
        sql "DROP TABLE IF EXISTS ${table1} FORCE;"
        sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                `k1` int NOT NULL,
                `c1` int,
                `c2` int
                )UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "replication_num" = "1"); """

        sql "insert into ${table1} values(99999,99999,99999);"
        sql "insert into ${table1} values(88888,88888,88888);"
        sql "insert into ${table1} values(77777,77777,77777);"
        sql "sync;"
        qt_sql "select * from ${table1} order by k1;"

        def do_streamload_2pc_commit = { txnId ->
            def command = "curl -X PUT --location-trusted -u root:" +
                    " -H txn_id:${txnId}" +
                    " -H txn_operation:commit" +
                    " http://${fe.host}:${fe.httpPort}/api/${dbName}/${table1}/_stream_load_2pc"
            log.info("http_stream execute 2pc: ${command}")

            def process = command.execute()
            def code = process.waitFor()
            def out = process.text
            def json2pc = parseJson(out)
            log.info("http_stream 2pc result: ${out}".toString())
            assert code == 0
            assert "success" == json2pc.status.toLowerCase()
        }

        def beNodes = sql_return_maparray("show backends;")
        def tabletStat = sql_return_maparray("show tablets from ${table1};").get(0)
        def tabletBackendId = tabletStat.BackendId
        def tabletId = tabletStat.TabletId
        def be1
        for (def be : beNodes) {
            if (be.BackendId == tabletBackendId) {
                be1 = be
            }
        }
        logger.info("tablet ${tabletId} on backend ${be1.Host} with backendId=${be1.BackendId}");
        logger.info("backends: ${cluster.getBackends()}")
        int beIndex = 1
        for (def backend : cluster.getBackends()) {
            if (backend.host == be1.Host) {
                beIndex = backend.index
                break
            }
        }
        assert cluster.getBeByIndex(beIndex).backendId as String == tabletBackendId

        try {
            // batch_size is 4164 in csv_reader.cpp
            // _batch_size is 8192 in vtablet_writer.cpp
            // to cause multi segments
            GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")


            String txnId
            // load data that will have multi segments and there are duplicate keys between segments
            String content = ""
            (1..4096).each {
                content += "${it},${it},${it}\n"
            }
            content += content
            streamLoad {
                table "${table1}"
                set 'column_separator', ','
                inputStream new ByteArrayInputStream(content.getBytes())
                set 'two_phase_commit', 'true'
                time 30000

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(result)
                    logger.info(result)
                    txnId = json.TxnId
                    assert "success" == json.Status.toLowerCase()
                }
            }

            // restart be, so that load will re-calculate delete bitmaps in publish phase
            Thread.sleep(1000)
            cluster.stopBackends(1)
            Thread.sleep(1000)
            cluster.startBackends(beIndex)

            Thread.sleep(1000)
            do_streamload_2pc_commit(txnId)
            awaitUntil(30) {
                def result = sql_return_maparray "show transaction from ${dbName} where id = ${txnId}"
                result[0].TransactionStatus as String == "VISIBLE"
            }

            qt_sql "select count() from ${table1};"
            qt_dup_key_count "select count() from (select k1,count() as cnt from ${table1} group by k1 having cnt > 1) A;"

        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
