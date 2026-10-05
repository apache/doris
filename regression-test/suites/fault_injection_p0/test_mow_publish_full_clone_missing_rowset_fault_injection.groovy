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

suite('test_mow_publish_full_clone_missing_rowset_fault_injection', 'docker') {

    def set_be_param = { paramName, paramValue, beIp, bePort ->
        def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
        assertTrue(out.contains("OK"))
    }

    def options = new ClusterOptions()
    options.cloudMode = false
    options.enableDebugPoints()
    options.feConfigs += [ "disable_tablet_scheduler=true" ]
    options.beConfigs += [ "enable_auto_clone_on_compaction_missing_version=true" ]
    options.beConfigs += [ "tablet_rowset_stale_sweep_time_sec=0" ]
    options.beConfigs += [ "tablet_rowset_stale_sweep_by_size=true" ]
    options.beConfigs += [ "tablet_rowset_stale_sweep_threshold_size=0" ]
    options.beNum = 3
    docker(options) {

        def injectBe = null
        def normalBe1 = null
        def normalBe2 = null
        def backends = sql_return_maparray('show backends')

        injectBe = backends[0]
        assertNotNull(injectBe)
        normalBe1 = backends[1]
        assertNotNull(normalBe1)
        normalBe2 = backends[2]
        assertNotNull(normalBe2)

        try {
            def tableName = "test_mow_publish_full_clone_missing_rowset_fault_injection"
            sql """ DROP TABLE IF EXISTS ${tableName} force"""
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    `k` int ,
                    `v` int ,
                ) engine=olap
                UNIQUE KEY(k)
                DISTRIBUTED BY HASH(k)
                BUCKETS 1
                properties(
                    "replication_num" = "3",
                    "disable_auto_compaction" = "true")
                """
            sql """ INSERT INTO ${tableName} VALUES (1,0)"""
            DebugPoint.enableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, "EnginePublishVersionTask.finish.random", [percent:"1.0"])
            sql """ INSERT INTO ${tableName} VALUES (2,0)"""
            DebugPoint.disableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, "EnginePublishVersionTask.finish.random")
            sql """ INSERT INTO ${tableName} VALUES (3,0)"""
            sql """ INSERT INTO ${tableName} VALUES (4,0)"""
            sql """ INSERT INTO ${tableName} VALUES (5,0)"""
            sql """ INSERT INTO ${tableName} VALUES (6,0)"""
            sql """ INSERT INTO ${tableName} VALUES (7,0)"""

            def array = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
            def tabletId = array[0].TabletId

            // normal BEs compaction
            logger.info("normal BE run cumu compaction:" + tabletId)
            def (code, out, err) = be_run_cumulative_compaction(normalBe1.Host, normalBe1.HttpPort, tabletId)
            logger.info("normal BE1 Run cumu compaction: code=" + code + ", out=" + out + ", err=" + err)
            (code, out, err) = be_run_cumulative_compaction(normalBe2.Host, normalBe2.HttpPort, tabletId)
            logger.info("normal BE2 Run cumu compaction: code=" + code + ", out=" + out + ", err=" + err)

            logger.info("normal BE show:" + tabletId)
            (code, out, err) = be_show_tablet_status(normalBe1.Host, normalBe1.HttpPort, tabletId)
            logger.info("normal BE1 show: code=" + code + ", out=" + out + ", err=" + err)
            (code, out, err) = be_show_tablet_status(normalBe2.Host, normalBe2.HttpPort, tabletId)
            logger.info("normal BE2 show: code=" + code + ", out=" + out + ", err=" + err)

            sleep(10000)

            // 1st inject be check rowsets
            logger.info("1st inject be show:" + tabletId)
            (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, tabletId)
            logger.info("1st inject be show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-2]"))
            assertFalse(out.contains("[3-3]"))
            assertFalse(out.contains("[4-4]"))
            assertFalse(out.contains("[5-5]"))
            assertFalse(out.contains("[6-6]"))
            assertFalse(out.contains("[7-7]"))

            set_be_param("enable_auto_clone_on_mow_publish_missing_version", "true", injectBe.Host, injectBe.HttpPort);
            Thread.sleep(10000)
            // submit clone task
            sql """ INSERT INTO ${tableName} VALUES (8,0)"""

            sleep(30000)

            // 2nd inject be check rowsets
            logger.info("2nd inject be show:" + tabletId)
            (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, tabletId)
            logger.info("2nd inject be show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-8]"))
            assertTrue(out.contains("[9-9]"))

            // inject be compaction
            logger.info("run cumu compaction:" + tabletId)
            (code, out, err) = be_run_cumulative_compaction(injectBe.Host, injectBe.HttpPort, tabletId)
            logger.info("Run cumu compaction: code=" + code + ", out=" + out + ", err=" + err)

            logger.info("3rd inject be show:" + tabletId)
            (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, tabletId)
            logger.info("3rd inject be show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-8]"))
        } finally {
            if (injectBe != null) {
                DebugPoint.disableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, "EnginePublishVersionTask.finish.random")
            }
        }
    }
}