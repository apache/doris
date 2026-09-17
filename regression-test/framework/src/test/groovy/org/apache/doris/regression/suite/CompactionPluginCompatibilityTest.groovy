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
                ["GET", "http://127.0.0.1:8040/api/compaction/show?tablet_id=42", null, 10, "", "", 10],
                ["GET", "http://127.0.0.1:8040/api/compaction/show?tablet_id=42", null, 5, "", "", 1],
                ["GET", "http://127.0.0.1:8040/api/compaction/run_status?tablet_id=42", null, 10, "", "", 10],
                ["GET", "http://127.0.0.1:8040/api/compaction/run_status?tablet_id=42", null, 5, "", "", 1]
        ], requests)

        def defaultCalls = []
        Suite.metaClass.curl = { String method, String url ->
            defaultCalls.add([method, url])
            return [0, "{}", ""]
        }
        assertEquals([0, "{}", ""], suite.be_get_overall_compaction_status("127.0.0.1", "8040"))
        assertEquals([0, "{}", ""], suite.be_run_base_compaction("127.0.0.1", "8040", "42"))
        assertEquals([0, "{}", ""], suite.be_run_cumulative_compaction("127.0.0.1", "8040", "42"))
        assertEquals([0, "{}", ""], suite.be_run_full_compaction("127.0.0.1", "8040", "42"))
        assertEquals([0, "{}", ""], suite.be_run_full_compaction_by_table_id("127.0.0.1", "8040", "7"))
        assertEquals([
                ["GET", "http://127.0.0.1:8040/api/compaction/run_status"],
                ["POST", "http://127.0.0.1:8040/api/compaction/run?tablet_id=42&compact_type=base"],
                ["POST", "http://127.0.0.1:8040/api/compaction/run?tablet_id=42&compact_type=cumulative"],
                ["POST", "http://127.0.0.1:8040/api/compaction/run?tablet_id=42&compact_type=full"],
                ["POST", "http://127.0.0.1:8040/api/compaction/run?table_id=7&compact_type=full"]
        ], defaultCalls)

        // This reproduces the plugin-internal three-argument call that failed in TeamCity 212706.
        Suite.metaClass.getBackendIpHttpPort = { Map hosts, Map ports ->
            hosts["1"] = "127.0.0.1"
            ports["1"] = "8040"
        }
        Suite.metaClass.sql_return_maparray = { String ignored -> [[BackendId: "1", TabletId: "42"]] }
        Suite.metaClass.sql = { String ignored -> [[null, '"disable_auto_compaction" = "true"']] }
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
        Suite.metaClass.curl = { String method, String url, String body = null, Integer timeoutSec = 10,
                                 String user = "", String password = "", Integer maxRetries = 10 ->
            if (url.contains("/api/compaction/show?")) {
                showCount++
                return [0, '{"last full success time":"' + (showCount == 1 ? "old" : "new")
                        + '","last full failure time":"old"}', ""]
            }
            if (url.contains("/api/compaction/run_status?")) {
                return [0, '{"status":"success","run_status":false}', ""]
            }
            return [0, '{"status":"success"}', ""]
        }
        suite.trigger_and_wait_compaction("t", "full")
        assertTrue(showCount >= 2)
    }
}
