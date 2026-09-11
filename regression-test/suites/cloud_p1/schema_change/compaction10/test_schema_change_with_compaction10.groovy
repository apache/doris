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

suite('test_schema_change_with_compaction10', 'docker') {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableDebugPoints()
    options.beConfigs += [ "enable_java_support=false" ]
    options.beConfigs += [ "enable_new_tablet_do_compaction=true" ]
    options.beConfigs += [ "disable_auto_compaction=true" ]
    options.beNum = 1
    docker(options) {
        def getJobState = { tableName ->
            def jobStateResult = sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
            logger.info("Get job state: " + jobStateResult)
            return jobStateResult[0][9]
        }

        def s3BucketName = getS3BucketName()
        def s3WithProperties = """WITH S3 (
            |"AWS_ACCESS_KEY" = "${getS3AK()}",
            |"AWS_SECRET_KEY" = "${getS3SK()}",
            |"AWS_ENDPOINT" = "${getS3Endpoint()}",
            |"AWS_REGION" = "${getS3Region()}",
            |"provider" = "${getS3Provider()}")
            |PROPERTIES(
            |"exec_mem_limit" = "8589934592",
            |"load_parallelism" = "3")""".stripMargin()

        // set fe configuration
        sql "ADMIN SET FRONTEND CONFIG ('max_bytes_per_broker_scanner' = '161061273600')"
        sql new File("""${context.file.parent}/../ddl/date_delete.sql""").text
        def load_date_once =  { String table ->
            def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
            def loadLabel = table + "_" + uniqueID
            // load data from cos
            def loadSql = new File("""${context.file.parent}/../ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
            loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
            sql loadSql

            // check load state
            while (true) {
                def stateResult = sql "show load where Label = '${loadLabel}'"
                logger.info("stateResult: " + stateResult)
                def loadState = stateResult[stateResult.size() - 1][2].toString()
                if ("CANCELLED".equalsIgnoreCase(loadState)) {
                    throw new IllegalStateException("load ${loadLabel} failed.")
                } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                    break
                }
                sleep(5000)
            }
        }

        sql new File("""${context.file.parent}/../ddl/date_unique_create.sql""").text
        def injectName = 'CloudSchemaChangeJob.process_alter_tablet.sleep'
        def injectBe = null
        def backends = sql_return_maparray('show backends')
        def array = sql_return_maparray("SHOW TABLETS FROM date")
        def injectBeId = array[0].BackendId
        def originTabletId = array[0].TabletId
        injectBe = backends.stream().filter(be -> be.BackendId == injectBeId).findFirst().orElse(null)
        assertNotNull(injectBe)

        def load_delete_compaction = {
            load_date_once("date");
            sql "delete from date where d_datekey < 19900000"
            sql "select count(*) from date"
            // cu compaction
            trigger_and_wait_compaction("date", "cumulative")
        }

        def triggerAndWaitCumulativeCompaction = { tabletId, latestVersionRange, expectedVersionRange ->
            awaitUntil(60, 1) {
                def (showCode, showOut, showErr) =
                        be_show_tablet_status(injectBe.Host, injectBe.HttpPort, tabletId)
                assertEquals(0, showCode, "Failed to show tablet status: ${showErr}")
                def tabletStatus = parseJson(showOut.trim())
                assertTrue(tabletStatus.rowsets instanceof List)
                return tabletStatus.rowsets.any { it.contains(latestVersionRange) }
            }

            logger.info("run compaction:" + tabletId)
            def (triggerCode, triggerOut, triggerErr) =
                    be_run_cumulative_compaction(injectBe.Host, injectBe.HttpPort, tabletId)
            logger.info("Run compaction: code=" + triggerCode + ", out=" + triggerOut + ", err=" + triggerErr)
            assertEquals(0, triggerCode, "Failed to trigger cumulative compaction: ${triggerErr}")
            def triggerResult = parseJson(triggerOut.trim())
            assertEquals("success", triggerResult.status.toString().toLowerCase(),
                    "Unexpected cumulative compaction response: ${triggerOut}")

            def tabletRowsets = []
            awaitUntil(60, 1) {
                def (showCode, showOut, showErr) =
                        be_show_tablet_status(injectBe.Host, injectBe.HttpPort, tabletId)
                assertEquals(0, showCode, "Failed to show tablet status: ${showErr}")
                def tabletStatus = parseJson(showOut.trim())
                assertTrue(tabletStatus.rowsets instanceof List)
                tabletRowsets = tabletStatus.rowsets
                return tabletRowsets.any { it.contains(expectedVersionRange) }
            }
            return tabletRowsets
        }

        def restartBackendAndRearmDebugPoint = {
            cluster.stopBackends()
            def rearmFuture = thread {
                long deadline = System.currentTimeMillis() + 120000L
                Exception lastError = null
                while (System.currentTimeMillis() < deadline) {
                    try {
                        DebugPoint.enableDebugPoint(injectBe.Host, injectBe.HttpPort as int,
                                NodeType.BE, injectName)
                        return
                    } catch (Exception e) {
                        lastError = e
                        sleep(50)
                    }
                }
                throw new IllegalStateException("Failed to re-enable ${injectName} after BE restart", lastError)
            }
            cluster.startBackends()
            rearmFuture.get()
        }

        def newTabletId = null
        try {
            load_delete_compaction()
            load_delete_compaction()
            load_delete_compaction()

            load_date_once("date");

            sleep(1000)
            GetDebugPoint().enableDebugPointForAllBEs(injectName)
            sql "ALTER TABLE date MODIFY COLUMN d_holidayfl bigint(11)"
            sleep(5000)
            array = sql_return_maparray("SHOW TABLETS FROM date")

            // NOTREADY tablets keep the latest 10 versions unmerged. Create enough
            // double-write rowsets for older versions to remain eligible for compaction.
            for (int i = 0; i < 16; i++) {
                load_date_once("date");
            }

            restartBackendAndRearmDebugPoint()
            sleep(30000)
            assertEquals("RUNNING", getJobState("date"),
                    "Schema change finished before the debug point was re-enabled")

            // base compaction
            trigger_and_wait_compaction("date", "base")
            newTabletId = array[1].TabletId
            logger.info("run compaction:" + newTabletId)
            def (code, out, err) = be_run_base_compaction(injectBe.Host, injectBe.HttpPort, newTabletId)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("invalid tablet state."))

            triggerAndWaitCumulativeCompaction(originTabletId, "[24-24]", "[9-24]")
            def notReadyTabletRowsets =
                    triggerAndWaitCumulativeCompaction(newTabletId, "[24-24]", "[9-14]")
            assertEquals("RUNNING", getJobState("date"))
            for (int version = 15; version <= 24; version++) {
                assertTrue(notReadyTabletRowsets.any { it.contains("[${version}-${version}]") })
            }
        } finally {
            if (injectBe != null) {
                GetDebugPoint().disableDebugPointForAllBEs(injectName)
            }
            int max_try_time = 3000
            def result = null
            while (max_try_time--){
                result = getJobState("date")
                if (result == "FINISHED" || result == "CANCELLED") {
                    sleep(3000)
                    break
                } else {
                    sleep(100)
                    if (max_try_time < 1){
                        assertEquals(1,2)
                    }
                }
            }
            assertEquals(result, "FINISHED");
            def count = sql """ select count(*) from date; """
            assertEquals(count[0][0], 2556);
            // check rowsets
            logger.info("run show:" + originTabletId)
            def (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, originTabletId)
            logger.info("Run show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-7]"))
            assertTrue(out.contains("[8-8]"))
            assertTrue(out.contains("[9-24]"))

            logger.info("run show:" + newTabletId)
            (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, newTabletId)
            logger.info("Run show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-2]"))
            assertTrue(out.contains("[7-7]"))
            assertTrue(out.contains("[8-8]"))
            assertTrue(out.contains("[9-14]"))

            // base compaction
            trigger_and_wait_compaction("date", "base")
            logger.info("run show:" + newTabletId)
            (code, out, err) = be_show_tablet_status(injectBe.Host, injectBe.HttpPort, newTabletId)
            logger.info("Run show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-7]"))
            assertTrue(out.contains("[8-8]"))
            assertTrue(out.contains("[9-14]"))

            for (int i = 0; i < 3; i++) {
                load_date_once("date");
            }

            sql """ select count(*) from date """

            def finalTabletRowsets =
                    triggerAndWaitCumulativeCompaction(newTabletId, "[27-27]", "[8-27]")
            assertTrue(finalTabletRowsets.any { it.contains("[0-1]") })
            assertTrue(finalTabletRowsets.any { it.contains("[2-7]") })
            assertTrue(finalTabletRowsets.any { it.contains("[8-27]") })
        }
    }
}
