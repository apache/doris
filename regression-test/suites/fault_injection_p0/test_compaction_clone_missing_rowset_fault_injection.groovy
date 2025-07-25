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

suite('test_compaction_clone_missing_rowset_fault_injection', 'docker') {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.enableDebugPoints()
    options.feConfigs += [ "disable_tablet_scheduler=true" ]
    options.beConfigs += [ "enable_auto_clone_on_compaction_missing_version=true" ]
    options.beNum = 3
    docker(options) {

        def injectBe = null
        def normalBe = null
        def backends = sql_return_maparray('show backends')

        injectBe = backends[0]
        assertNotNull(injectBe)
        normalBe = backends[1]
        assertNotNull(normalBe)

        try {
            def tableName = "test_compaction_clone_missing_rowset"
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
                    "replication_num" = "3",
                    "disable_auto_compaction" = "true")
                """
            sql """ INSERT INTO ${tableName} VALUES (1,0)"""
            DebugPoint.enableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, "EnginePublishVersionTask.finish.random", [percent:"1.0"])
            sql """ INSERT INTO ${tableName} VALUES (2,0)"""
            sql """ INSERT INTO ${tableName} VALUES (3,0)"""
            sql """ INSERT INTO ${tableName} VALUES (4,0)"""
            DebugPoint.disableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, "EnginePublishVersionTask.finish.random")
            sql """ INSERT INTO ${tableName} VALUES (5,0)"""

            def array = sql_return_maparray("SHOW TABLETS FROM ${tableName}")
            def tabletId = array[0].TabletId

            // 1st check rowsets
            logger.info("1st show:" + tabletId)
            def (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, tabletId)
            logger.info("1st show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-2]"))
            // missing rowset [3-5]
            assertTrue(out.contains("[3-5]"))
            assertTrue(out.contains("[6-6]"))

            logger.info("1st run cumu compaction:" + tabletId)
            (code, out, err) = be_run_cumulative_compaction(injectBe.Host, injectBe.HttpPort, tabletId)
            logger.info("1st Run cumu compaction: code=" + code + ", out=" + out + ", err=" + err)

            sleep(10000)

            // 2nd check rowsets
            logger.info("2nd show:" + tabletId)
            (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, tabletId)
            logger.info("2nd show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-2]"))
            assertTrue(out.contains("[3-3]"))
            assertTrue(out.contains("[4-4]"))
            assertTrue(out.contains("[5-5]"))
            assertTrue(out.contains("[6-6]"))

            logger.info("2nd cumu compaction:" + tabletId)
            (code, out, err) = be_run_cumulative_compaction(injectBe.Host, injectBe.HttpPort, tabletId)
            logger.info("2nd cumu compaction: code=" + code + ", out=" + out + ", err=" + err)

            // check rowsets
            logger.info("3rd show:" + tabletId)
            (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, tabletId)
            logger.info("3rd show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-2]"))
            assertTrue(out.contains("[3-6]"))

        } finally {
            if (injectBe != null) {
                DebugPoint.disableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, "EnginePublishVersionTask.finish.random")
            }
        }
    }
}
