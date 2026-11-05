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

package org.apache.doris.regression.suite

import groovy.lang.Binding
import groovy.lang.GroovyShell
import groovy.lang.GroovySystem
import groovy.json.JsonSlurper
import org.apache.doris.regression.Config
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory

import java.nio.file.Path

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertNotNull
import static org.junit.jupiter.api.Assertions.assertThrows
import static org.junit.jupiter.api.Assertions.assertTrue

class CompactionPluginCompatibilityTest {
    @TempDir
    Path tempDir

    @AfterEach
    void removePluginMethods() {
        GroovySystem.metaClassRegistry.removeMetaClass(Suite)
    }

    @Test
    void tabletStatusAndCompactionStatusSupportDefaultAndBoundedProbes() {
        File plugin = [new File("../plugins/plugin_compaction.groovy"),
                       new File("regression-test/plugins/plugin_compaction.groovy")]
                .find { it.isFile() }
        assertNotNull(plugin, "plugin_compaction.groovy must be available to this test")

        def logger = LoggerFactory.getLogger(CompactionPluginCompatibilityTest)
        new GroovyShell(getClass().classLoader, new Binding(logger: logger)).evaluate(plugin)

        def requests = []
        Suite.metaClass.curl = { String method, String url, String body, Integer timeoutSec,
                                 String user, String password, Integer maxRetries ->
            requests.add([method, url, body, timeoutSec, user, password, maxRetries])
            return [0, "{}", ""]
        }

        File suiteDir = tempDir.resolve("suites/compaction").toFile()
        assertTrue(suiteDir.mkdirs())
        File suiteFile = new File(suiteDir, "compaction_plugin_compatibility.groovy")
        assertTrue(suiteFile.createNewFile())
        Config config = new Config()
        config.suitePath = tempDir.resolve("suites").toString()
        config.dataPath = tempDir.resolve("data").toString()
        config.realDataPath = tempDir.resolve("real-data").toString()
        config.defaultDb = "regression_test"
        config.feHttpUser = "http_user"
        config.feHttpPassword = "http_password"
        ScriptContext scriptContext = new ScriptContext(
                suiteFile, null, null, config, Collections.emptyList(), { true })
        SuiteCluster cluster = new SuiteCluster("compaction_plugin_compatibility", config)
        SuiteContext context = new SuiteContext(
                suiteFile, "compaction_plugin_compatibility", "p0", scriptContext,
                cluster, null, null, config)
        Suite suite = new Suite("compaction_plugin_compatibility", "p0", context, cluster)
        assertEquals([0, "{}", ""], suite.be_show_tablet_status("127.0.0.1", "8040", "42"))
        assertEquals([0, "{}", ""], suite.be_show_tablet_status("127.0.0.1", "8040", "42", 5, 1))
        assertEquals([0, "{}", ""], suite.be_get_compaction_status("127.0.0.1", "8040", "42"))
        assertEquals([0, "{}", ""], suite.be_get_compaction_status("127.0.0.1", "8040", "42", 5, 1))

        assertEquals([
                ["GET", "http://127.0.0.1:8040/api/compaction/show?tablet_id=42",
                 null, 10, "http_user", "http_password", 10],
                ["GET", "http://127.0.0.1:8040/api/compaction/show?tablet_id=42",
                 null, 5, "http_user", "http_password", 1],
                ["GET", "http://127.0.0.1:8040/api/compaction/run_status?tablet_id=42",
                 null, 10, "http_user", "http_password", 10],
                ["GET", "http://127.0.0.1:8040/api/compaction/run_status?tablet_id=42",
                 null, 5, "http_user", "http_password", 1]
        ], requests)

        def defaultCalls = []
        Suite.metaClass.curl = { String method, String url, String body, Integer timeoutSec,
                                 String user, String password, Integer maxRetries ->
            defaultCalls.add([method, url, body, timeoutSec, user, password, maxRetries])
            return [0, "{}", ""]
        }
        assertEquals([0, "{}", ""], suite.be_get_overall_compaction_status("127.0.0.1", "8040"))
        assertEquals([0, "{}", ""], suite.be_run_base_compaction("127.0.0.1", "8040", "42"))
        assertEquals([0, "{}", ""], suite.be_run_cumulative_compaction("127.0.0.1", "8040", "42"))
        assertEquals([0, "{}", ""], suite.be_run_full_compaction("127.0.0.1", "8040", "42"))
        assertEquals([0, "{}", ""], suite.be_run_full_compaction_by_table_id("127.0.0.1", "8040", "7"))
        assertEquals([
                ["GET", "http://127.0.0.1:8040/api/compaction/run_status",
                 null, 10, "http_user", "http_password", 10],
                ["POST", "http://127.0.0.1:8040/api/compaction/run?tablet_id=42&compact_type=base",
                 null, 10, "http_user", "http_password", 10],
                ["POST", "http://127.0.0.1:8040/api/compaction/run?tablet_id=42&compact_type=cumulative",
                 null, 10, "http_user", "http_password", 10],
                ["POST", "http://127.0.0.1:8040/api/compaction/run?tablet_id=42&compact_type=full",
                 null, 10, "http_user", "http_password", 10],
                ["POST", "http://127.0.0.1:8040/api/compaction/run?table_id=7&compact_type=full",
                 null, 10, "http_user", "http_password", 10]
        ], defaultCalls)

        // This reproduces the plugin-internal three-argument call that failed in TeamCity 212706.
        Suite.metaClass.getBackendIpHttpPort = { Map hosts, Map ports ->
            hosts["1"] = "127.0.0.1"
            ports["1"] = "8040"
        }
        Suite.metaClass.sql_return_maparray = { String ignored -> [[BackendId: "1", TabletId: "42"]] }
        boolean timeSeries = false
        Suite.metaClass.sql = { String ignored -> [[null, '"disable_auto_compaction" = "true"'
                + (timeSeries ? ', "compaction_policy" = "time_series"' : '')]] }
        Suite.metaClass.curl = { String method, String url, String body, Integer timeoutSec,
                                 String user, String password, Integer maxRetries ->
            throw new IllegalStateException("tablet probe reached")
        }
        IllegalStateException probe = assertThrows(IllegalStateException) {
            suite.trigger_and_wait_compaction("t", "full")
        }
        assertEquals("tablet probe reached", probe.message)

        Suite.metaClass.parseJson = { String value -> new JsonSlurper().parseText(value) }
        int showCount = 0
        boolean failCompaction = false
        String failureStatus = "[E-1] injected failure"
        String compactionType = "full"
        boolean skipSuccessTimestamp = false
        int publicationAfter = 2
        def inputRowsets = ["[0-1] 0 DATA NONOVERLAPPING input1 0 B level=0",
                           "[2-3] 1 DATA NONOVERLAPPING input2 1 KB level=0"]
        def outputRowsets = ["[0-3] 1 DATA NONOVERLAPPING output 1 KB level=1"]
        Suite.metaClass.curl = { String method, String url, String body = null, Integer timeoutSec = 10,
                                 String user = "", String password = "", Integer maxRetries = 10 ->
            if (url.contains("/api/compaction/show?")) {
                showCount++
                return [0, groovy.json.JsonOutput.toJson([
                        "last ${compactionType} success time": !failCompaction && !skipSuccessTimestamp && showCount >= publicationAfter ? "new" : "old",
                        "last ${compactionType} failure time": failCompaction && showCount > 1 ? "new" : "old",
                        "last ${compactionType} status": failCompaction && showCount > 1 ? failureStatus : "[OK]",
                        'rowsets': !failCompaction && showCount >= publicationAfter ? outputRowsets : inputRowsets
                ]), ""]
            }
            if (url.contains("/api/compaction/run_status?")) {
                return [0, '{"status":"success","run_status":false}', ""]
            }
            return [0, '{"status":"success"}', ""]
        }
        suite.trigger_and_wait_compaction("t", "full")
        assertTrue(showCount >= 2)

        // A completed HTTP request and readable old rowsets do not prove a successful compaction.
        showCount = 0
        failCompaction = true
        AssertionError failure = assertThrows(AssertionError) {
            suite.trigger_and_wait_compaction("t", "full", 5)
        }
        assertTrue(failure.message.contains("injected failure"), failure.message)

        timeSeries = true
        showCount = 0
        failure = assertThrows(AssertionError) {
            suite.trigger_and_wait_compaction("t", "full", 5)
        }
        assertTrue(failure.message.contains("injected failure"), failure.message)
        failCompaction = false
        skipSuccessTimestamp = true
        publicationAfter = 3
        showCount = 0
        suite.trigger_and_wait_compaction("t", "full", 5)
        assertEquals(3, showCount)
        timeSeries = false

        // Expected fault injection and no-work results can also arrive asynchronously.
        failCompaction = true
        failureStatus = "[E-6010] injected index compaction failure"
        showCount = 0
        suite.trigger_and_wait_compaction("t", "full", 5, ["e-6010"] as String[])
        assertEquals(2, showCount)
        showCount = 0
        assertThrows(AssertionError) {
            suite.trigger_and_wait_compaction("t", "full", 5, ["e-238"] as String[])
        }
        [["full", "[E-2008] no suitable version"],
         ["base", "[E-808] no suitable version"],
         ["cumulative", "[E-2000] no suitable version"],
         ["cumulative", "[E-2010] no suitable version"]].each { type, status ->
            compactionType = type
            failureStatus = status
            showCount = 0
            suite.trigger_and_wait_compaction("t", type, 5)
            assertEquals(2, showCount)
        }

        // An expected result on one replica must not hide a real failure on the next.
        Suite.metaClass.sql_return_maparray = { String ignored ->
            [[BackendId: "1", TabletId: "42"], [BackendId: "1", TabletId: "43"]]
        }
        def showCounts = ["42": 0, "43": 0]
        Suite.metaClass.curl = { String method, String url, String body = null, Integer timeoutSec = 10,
                                 String user = "", String password = "", Integer maxRetries = 10 ->
            if (url.contains("/api/compaction/show?")) {
                String id = url.split("tablet_id=")[1]
                boolean completed = ++showCounts[id] > 1
                return [0, groovy.json.JsonOutput.toJson([
                        'last full success time': "old",
                        'last full failure time': completed ? "new" : "old",
                        'last full status': completed
                                ? (id == "42" ? "[E-2008] no suitable version" : "[E-238] too many segments")
                                : "[OK]",
                        'rowsets': inputRowsets
                ]), ""]
            }
            return [0, '{"status":"success","run_status":false}', ""]
        }
        failure = assertThrows(AssertionError) {
            suite.trigger_and_wait_compaction("t", "full", 5)
        }
        assertTrue(failure.message.contains("too many segments"), failure.message)
        assertEquals(["42": 2, "43": 2], showCounts)
        Suite.metaClass.sql_return_maparray = { String ignored -> [[BackendId: "1", TabletId: "42"]] }

        // E-2010 hands a delete version to base compaction; an idle first poll is not success.
        showCount = 0
        Suite.metaClass.curl = { String method, String url, String body = null, Integer timeoutSec = 10,
                                 String user = "", String password = "", Integer maxRetries = 10 ->
            if (url.contains("/api/compaction/show?")) {
                showCount++
                return [0, groovy.json.JsonOutput.toJson([
                        'cumulative point': showCount == 1 ? 2 : 5,
                        'last cumulative success time': "old",
                        'last cumulative failure time': showCount == 1 ? "old" : "new",
                        'last cumulative status': showCount == 1 ? "[OK]" : "[E-2010] delete version",
                        'last base success time': showCount < 3 ? "old" : "new",
                        'last base status': "[OK]"
                ]), ""]
            }
            if (url.contains("/api/compaction/run_status?")) {
                return [0, '{"status":"success","run_status":false}', ""]
            }
            return [0, '{"status":"success"}', ""]
        }
        suite.trigger_and_wait_compaction("t", "cumulative", 5)
        assertEquals(3, showCount)
    }
}
