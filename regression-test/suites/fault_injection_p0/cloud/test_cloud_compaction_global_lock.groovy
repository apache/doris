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

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.http.NoHttpResponseException
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

suite('test_cloud_compaction_global_lock', 'docker') {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableDebugPoints()
    options.beConfigs += [ "enable_java_support=false" ]
    options.beConfigs += [ "cumulative_compaction_min_deltas=2" ]
    options.beConfigs += [ "cumulative_compaction_max_deltas=3" ]
    options.beNum = 3
    docker(options) {

        def cumuInjectName = 'CloudStorageEngine._submit_cumulative_compaction_task.wait_in_line'
        def injectBe = null
        def cumuNormalName = 'CloudStorageEngine._submit_cumulative_compaction_task.sleep'
        def normalBe = null
        def backends = sql_return_maparray('show backends')

        injectBe = backends[0]
        assertNotNull(injectBe)
        normalBe = backends[1]
        assertNotNull(normalBe)

        def test_cumu_compaction_global_lock = {
            def tableName = "test_cumu_compaction_global_lock"
            sql """ DROP TABLE IF EXISTS ${tableName} force"""
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    `k` int ,
                    `v` int ,
                ) engine=olap
                DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k)
                BUCKETS 1
                properties(
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true")
                """
            sql """ INSERT INTO ${tableName} VALUES (0,0)"""
            sql """ INSERT INTO ${tableName} VALUES (1,0)"""
            sql """ INSERT INTO ${tableName} VALUES (2,0)"""
            sql """ INSERT INTO ${tableName} VALUES (3,0)"""
            sql """ INSERT INTO ${tableName} VALUES (4,0)"""

            def array = sql_return_maparray("SHOW TABLETS FROM test_cumu_compaction_global_lock")
            def originTabletId = array[0].TabletId
            def noramlOriginTabletId = array[0].TabletId

            sql """ select * from ${tableName} order by k"""

            Thread.sleep(5000)

            // inject be cu compaction
            logger.info("run inject be cumu compaction:" + originTabletId)
            def (code, out, err) = be_run_cumulative_compaction(injectBe.Host, injectBe.HttpPort, originTabletId)
            logger.info("Run inject be cumu compaction: code=" + code + ", out=" + out + ", err=" + err)

            Thread.sleep(1000)

            // normal be cu compaction
            logger.info("run normal be cumu compaction:" + noramlOriginTabletId)
            (code, out, err) = be_run_cumulative_compaction(normalBe.Host, normalBe.HttpPort, noramlOriginTabletId)
            logger.info("Run normal be cumu compaction: code=" + code + ", out=" + out + ", err=" + err)

            Thread.sleep(1000)

            // check rowsets
            logger.info("run inject be cumu show:" + originTabletId)
            (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, originTabletId)
            logger.info("Run inject be cumu show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-2]"))
            assertTrue(out.contains("[3-3]"))
            assertTrue(out.contains("[4-4]"))
            assertTrue(out.contains("[5-5]"))
            assertTrue(out.contains("[6-6]"))

            Thread.sleep(10000)

            // check rowsets
            logger.info("run normal be cumu show:" + originTabletId)
            (code, out, err) = be_show_tablet_status(normalBe.Host, normalBe.HttpPort, noramlOriginTabletId)
            logger.info("Run normal be cumu show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-4]"))
            assertTrue(out.contains("[5-5]"))
            assertTrue(out.contains("[6-6]"))

            // check rowsets
            logger.info("run inject be cumu show:" + originTabletId)
            (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, originTabletId)
            logger.info("Run inject be cumu show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-2]"))
            assertTrue(out.contains("[3-3]"))
            assertTrue(out.contains("[4-4]"))
            assertTrue(out.contains("[5-5]"))
            assertTrue(out.contains("[6-6]"))

        }

        try {
            DebugPoint.enableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, cumuInjectName)
            DebugPoint.enableDebugPoint(normalBe.Host, normalBe.HttpPort.toInteger(), NodeType.BE, cumuNormalName)

            test_cumu_compaction_global_lock()

        } finally {
            if (injectBe != null) {
                DebugPoint.disableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, cumuInjectName)
                DebugPoint.disableDebugPoint(normalBe.Host, normalBe.HttpPort.toInteger(), NodeType.BE, cumuNormalName)
            }
        }
    }
}
