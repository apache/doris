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
import org.junit.Assert
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_partial_update_conflict_skip_compaction", "nonConcurrent") {

    def table1 = "test_partial_update_conflict_skip_compaction"
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
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${table1} values(1,1,1,1,1);"
    sql "insert into ${table1} values(2,2,2,2,2);"
    sql "insert into ${table1} values(3,3,3,3,3);"
    sql "sync;"
    order_qt_sql "select * from ${table1};"

    def beNodes = sql_return_maparray("show backends;")
    def tabletStat = sql_return_maparray("show tablets from ${table1};").get(0)
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
        def metaUrl = sql_return_maparray("show tablets from ${table1};").get(0).MetaUrl
        def (code, out, err) = curl("GET", metaUrl)
        Assert.assertEquals(code, 0)
        def jsonMeta = parseJson(out.trim())

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

    check_rs_metas(4, {int startVersion, int endVersion, int numSegments, int numRows, String overlapPb ->
        if (startVersion == 0) {
            // [0-1]
            Assert.assertEquals(endVersion, 1)
            Assert.assertEquals(numSegments, 0)
        } else {
            // [2-2], [3-3], [4-4]
            Assert.assertEquals(startVersion, endVersion)
            Assert.assertEquals(numSegments, 1)
        }
    })

    try {
        GetDebugPoint().clearDebugPointsForAllBEs()

        // block the partial update before publish phase
        GetDebugPoint().enableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.enable_spin_wait", [tablet_id: "${tabletId}"])
        GetDebugPoint().enableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.block")

        // the first partial update load
        def t1 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "sync;"
            sql "insert into ${table1}(k1,c1,c2) values(1,999,999),(2,888,888),(3,777,777);"
        }

        Thread.sleep(200)

        // the second partial update load that has conflict with the first one
        def t2 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "sync;"
            sql "insert into ${table1}(k1,c3,c4) values(1,666,666),(3,555,555);"
        }

        // trigger full compaction on tablet
        logger.info("trigger compaction on another BE ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}")
        def (code, out, err) = be_run_full_compaction(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        Assert.assertEquals(code, 0)
        def compactJson = parseJson(out.trim())
        Assert.assertEquals("success", compactJson.status.toLowerCase())

        // wait for full compaction to complete
        Awaitility.await().atMost(3, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(
            {
                (code, out, err) = be_get_compaction_status(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                Assert.assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                Assert.assertEquals("success", compactionStatus.status.toLowerCase())
                return !compactionStatus.run_status
            }
        )

        check_rs_metas(1, {int startVersion, int endVersion, int numSegments, int numRows, String overlapPb ->
            // check the rowset produced by full compaction
            // [0-4]
            Assert.assertEquals(startVersion, 0)
            Assert.assertEquals(endVersion, 4)
            Assert.assertEquals(numRows, 3)
            Assert.assertEquals(overlapPb, "NONOVERLAPPING")
        })

        GetDebugPoint().disableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.enable_spin_wait")
        GetDebugPoint().disableDebugPointForAllBEs("BaseTablet::update_delete_bitmap.block")

        t1.join()
        t2.join()

        order_qt_sql "select * from ${table1};"

        check_rs_metas(3, {int startVersion, int endVersion, int numSegments, int numRows, String overlapPb ->
            if (startVersion == 5) {
                // the first partial update load
                // it should skip the alignment process of rowsets produced by full compaction and
                // should not generate new segment in publish phase
                Assert.assertEquals(endVersion, 5)
                Assert.assertEquals(numSegments, 1)
                Assert.assertEquals(numRows, 2)
            } else if (startVersion == 6) {
                // the first partial update load
                // it should skip the alignment process of rowsets produced by full compaction and
                // should generate new segment in publish phase for conflicting rows with the first partial update load
                Assert.assertEquals(endVersion, 6)
                Assert.assertEquals(numSegments, 2)
            }
        })
        
    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    // sql "DROP TABLE IF EXISTS ${table1};"
}
