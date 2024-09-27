
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

suite("test_f_2pc_schema_change", "p0") {
    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    if (isCloudMode()) {
        return
    }

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")
        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"

            def tableName = "test_f_2pc_schema_change"
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE ${tableName} (
                k1 varchar(10) not null,
                v1 varchar(10),
                v2 varchar(10),
                v3 varchar(10),
                v4 varchar(10),
                v5 varchar(10))
                UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 4
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "true",
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "enable_unique_key_skip_bitmap_column" = "true",
                    "store_row_column" = "${use_row_store}");"""

            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'strict_mode', 'false'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                file 'concurrency_update3.json'
                time 10000
            }
            sql "sync;"
            qt_sql """ select * from ${tableName} order by k1;"""

            def doSchemaChange = { cmd ->
                sql cmd
                waitForSchemaChangeDone {
                    sql """SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1"""
                    time 20000
                }
            }

            InetSocketAddress address = context.config.feHttpInetSocketAddress

            def do_streamload_2pc = { txn_id, txn_operation, name ->
                HttpClients.createDefault().withCloseable { client ->
                    RequestBuilder requestBuilder = RequestBuilder.put("http://${address.hostString}:${address.port}/api/${db}/${name}/_stream_load_2pc")
                    String encoding = Base64.getEncoder()
                        .encodeToString((user + ":" + (password == null ? "" : password)).getBytes("UTF-8"))
                    requestBuilder.setHeader("Authorization", "Basic ${encoding}")
                    requestBuilder.setHeader("Expect", "100-Continue")
                    requestBuilder.setHeader("txn_id", "${txn_id}")
                    requestBuilder.setHeader("txn_operation", "${txn_operation}")

                    String backendStreamLoadUri = null
                    client.execute(requestBuilder.build()).withCloseable { resp ->
                        resp.withCloseable {
                            String body = EntityUtils.toString(resp.getEntity())
                            def respCode = resp.getStatusLine().getStatusCode()
                            // should redirect to backend
                            if (respCode != 307) {
                                throw new IllegalStateException("Expect frontend stream load response code is 307, " +
                                        "but meet ${respCode}\nbody: ${body}")
                            }
                            backendStreamLoadUri = resp.getFirstHeader("location").getValue()
                        }
                    }

                    requestBuilder.setUri(backendStreamLoadUri)
                    try{
                        client.execute(requestBuilder.build()).withCloseable { resp ->
                            resp.withCloseable {
                                String body = EntityUtils.toString(resp.getEntity())
                                def respCode = resp.getStatusLine().getStatusCode()
                                if (respCode != 200) {
                                    throw new IllegalStateException("Expect backend stream load response code is 200, " +
                                            "but meet ${respCode}\nbody: ${body}")
                                }
                            }
                        }
                    } catch (Throwable t) {
                        log.info("StreamLoad Exception: ", t)
                    }
                }
            }

            String txnId
            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'strict_mode', 'false'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                set 'two_phase_commit', 'true'
                file 'concurrency_update2.json'
                time 10000 
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    txnId = json.TxnId
                    assertEquals("success", json.Status.toLowerCase())
                }
            }
            sql "sync;"
            doSchemaChange """ alter table ${tableName} modify column v2 varchar(20);"""
            doSchemaChange """ alter table ${tableName} drop column v3;"""
            doSchemaChange """ alter table ${tableName} add column v6 varchar(50);"""
            doSchemaChange """ alter table ${tableName} rename column v4 renamed_v4;"""
            qt_sql "desc ${tableName} all;"
            sql "sync;"

            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'strict_mode', 'false'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                file 'concurrency_update1.json'
                time 10000 
            }
            sql "sync;"
            qt_sql """ select * from ${tableName} order by k1;"""

            do_streamload_2pc(txnId, "commit", tableName)
            sql "sync;"
            qt_sql """ select * from ${tableName} order by k1;"""

            sql "drop table if exists ${tableName};"
        }
    }
}
