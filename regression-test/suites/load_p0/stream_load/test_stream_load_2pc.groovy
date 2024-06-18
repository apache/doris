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

import org.apache.http.client.methods.RequestBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import java.text.SimpleDateFormat

suite("test_stream_load_2pc", "p0") {
    def tableName = "test_2pc_table"
    InetSocketAddress address = context.config.feHttpInetSocketAddress
    String user = context.config.feHttpUser
    String password = context.config.feHttpPassword
    String db = context.config.getDbNameByFile(context.file)

    def do_streamload_2pc_commit_by_label = { label ->
        def command = "curl -X PUT --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}" +
                " -H label:${label}" +
                " -H txn_operation:commit" +
                " http://${context.config.feHttpAddress}/api/${db}/${tableName}/_stream_load_2pc"
        log.info("http_stream execute 2pc: ${command}")

        def process = command.execute()
        code = process.waitFor()
        out = process.text
        log.info("http_stream 2pc result: ${out}".toString())
        def json2pc = parseJson(out)
        return json2pc
    }

    def do_streamload_2pc_commit_by_txn_id = { txnId ->
        def command = "curl -X PUT --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}" +
                " -H txn_id:${txnId}" +
                " -H txn_operation:commit" +
                " http://${context.config.feHttpAddress}/api/${db}/${tableName}/_stream_load_2pc"
        log.info("http_stream execute 2pc: ${command}")

        def process = command.execute()
        code = process.waitFor()
        out = process.text
        log.info("http_stream 2pc result: ${out}".toString())
        def json2pc = parseJson(out)
        return json2pc
    }

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` bigint(20) NULL DEFAULT "1",
                `k2` bigint(20) NULL ,
                `v1` tinyint(4) NULL,
                `v2` tinyint(4) NULL,
                `v3` tinyint(4) NULL,
                `v4` DATETIME NULL
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        def label = UUID.randomUUID().toString().replaceAll("-", "")
        def txnId;
        streamLoad {
            table "${tableName}"

            set 'label', "${label}"
            set 'column_separator', '|'
            set 'columns', 'k1, k2, v1, v2, v3'
            set 'two_phase_commit', 'true'

            file 'test_two_phase_commit.csv'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(2, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
                assertEquals(0, json.NumberUnselectedRows)
                txnId = json.TxnId
            }
        }

        streamLoad {
            table "${tableName}"

            set 'label', "${label}"
            set 'column_separator', '|'
            set 'columns', 'k1, k2, v1, v2, v3'
            set 'two_phase_commit', 'true'

            file 'test_two_phase_commit.csv'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("label already exists", json.Status.toLowerCase())
                assertTrue(json.Message.contains("has already been used, relate to txn"))
            }
        }

        def json2pc = do_streamload_2pc_commit_by_label.call(label)
        assertEquals("success", json2pc.status.toLowerCase())

        def count = 0
        while (true) {
            res = sql "select count(*) from ${tableName}"
            if (res[0][0] > 0) {
                break
            }
            if (count >= 60) {
                log.error("stream load commit can not visible for long time")
                assertEquals(2, res[0][0])
                break
            }
            sleep(1000)
            count++
        }

         qt_sql_2pc_commit "select * from ${tableName} order by k1"

         json2pc = do_streamload_2pc_commit_by_txn_id.call(txnId)
         assertTrue(json2pc.msg.contains("is already visible, not pre-committed"))
    } finally {
        sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    }

}

