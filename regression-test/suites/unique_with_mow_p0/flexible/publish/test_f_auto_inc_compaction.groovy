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

suite("test_f_auto_inc_compaction") {
    if (isCloudMode()) {
        logger.info("skip test_f_auto_inc_compaction in cloud mode")
        return
    }

    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "test_f_auto_inc_compaction"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL,
        `id` BIGINT NOT NULL AUTO_INCREMENT
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "disable_auto_compaction" = "true",
        "enable_mow_light_delete" = "false",
        "store_row_column" = "false"); """

    sql """insert into ${tableName}(k,v1,v2) values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6);"""
    qt_sql "select k,v1,v2,id from ${tableName} order by k;"
    sql "delete from ${tableName} where k=1;"
    sql "delete from ${tableName} where k=3;"
    sql "delete from ${tableName} where k=6;"
    qt_sql "select k,v1,v2 from ${tableName} order by k;"
    qt_check_auto_inc_dup "select count(*) from ${tableName} group by id having count(*) > 1;"

    def beNodes = sql_return_maparray("show backends;")
    def tabletStat = sql_return_maparray("show tablets from ${tableName};").get(0)
    def tabletBackendId = tabletStat.BackendId
    def tabletId = tabletStat.TabletId
    def tabletBackend;
    for (def be : beNodes) {
        if (be.BackendId == tabletBackendId) {
            tabletBackend = be
            break;
        }
    }
    logger.info("tablet ${tabletId} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");

    def check_rs_metas = { expected_rs_meta_size, check_func -> 
        if (isCloudMode()) {
            return
        }

        def metaUrl = sql_return_maparray("show tablets from ${tableName};").get(0).MetaUrl
        def (code, out, err) = curl("GET", metaUrl)
        Assert.assertEquals(code, 0)
        def jsonMeta = parseJson(out.trim())

        logger.info("jsonMeta.rs_metas.size(): ${jsonMeta.rs_metas.size()}, expected_rs_meta_size: ${expected_rs_meta_size}")
        Assert.assertEquals(jsonMeta.rs_metas.size(), expected_rs_meta_size)
        for (def meta : jsonMeta.rs_metas) {
            int startVersion = meta.start_version
            int endVersion = meta.end_version
            int numSegments = meta.num_segments
            int numRows = meta.num_rows
            String overlapPb = meta.segments_overlap_pb
            logger.info("[${startVersion}-${endVersion}] ${overlapPb} ${meta.num_segments} ${numRows} ${meta.rowset_id_v2}")
            check_func(startVersion, endVersion, numSegments, numRows, overlapPb)
        }
    }

    check_rs_metas(5, {int startVersion, int endVersion, int numSegments, int numRows, String overlapPb ->
        if (startVersion == 0) {
            // [0-1]
            Assert.assertEquals(endVersion, 1)
            Assert.assertEquals(numSegments, 0)
        } else {
            // [2-2], [3-3], [4-4], [5-5]
            Assert.assertEquals(startVersion, endVersion)
            Assert.assertEquals(numSegments, 1)
        }
    })

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
    String load1 = """99,99,99,99"""
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'two_phase_commit', 'true'
        inputStream new ByteArrayInputStream(load1.getBytes())
        time 60000 // limit inflight 60s
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


    String load2 = 
            """{"k":3,"v1":100}
            {"k":6,"v2":600}"""
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


    // most have a new load after load2's max version sees in flush phase. Otherwise load2 will skip to calc delete bitmap
    // of rowsets which is produced by compaction and whose end version is lower than the max version
    // the load1 sees in flush phase, see https://github.com/apache/doris/pull/38487 for details

    // let load1 publish
    do_streamload_2pc_commit(txnId1)
    wait_for_publish(txnId1, 10)
    Thread.sleep(1000)


    trigger_and_wait_compaction(tableName, "full")

    check_rs_metas(1, {int startVersion, int endVersion, int numSegments, int numRows, String overlapPb ->
        // check the rowset produced by full compaction
        // [0-6]
        Assert.assertEquals(startVersion, 0)
        Assert.assertEquals(endVersion, 6)
        Assert.assertEquals(numRows, 4)
        Assert.assertEquals(overlapPb, "NONOVERLAPPING")
    })

    qt_after_compaction "select k,v1,v2 from ${tableName} order by k;"
    qt_check_auto_inc_dup "select count(*) from ${tableName} group by id having count(*) > 1;"

    do_streamload_2pc_commit(txnId2)
    wait_for_publish(txnId2, 10)

    qt_sql "select k,v1,v2 from ${tableName} order by k;"
    qt_check_auto_inc_dup "select count(*) from ${tableName} group by id having count(*) > 1;"
    qt_check_auto_inc_val "select count(*) from ${tableName} where id=0;"
}
