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

import groovy.json.JsonOutput
import org.apache.doris.regression.Config
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

import static org.junit.jupiter.api.Assertions.*

class SnapshotRecyclerPluginTest {
    private Suite loadPlugin(Closure request) {
        def plugin = [new File("../plugins/snapshot_recycler_plugin.groovy"),
                      new File("regression-test/plugins/snapshot_recycler_plugin.groovy")].find { it.isFile() }
        assertNotNull(plugin)
        new GroovyShell(getClass().classLoader,
                new Binding(logger: LoggerFactory.getLogger(getClass()))).evaluate(plugin)
        Suite.metaClass.httpTest = { Closure action ->
            def httpCall = [method: 'POST']
            def http = new Expando(
                    endpoint: { String value -> httpCall.endpoint = value },
                    uri: { String value -> httpCall.uri = value },
                    op: { String value -> httpCall.method = value.toUpperCase() },
                    body: { String value -> httpCall.body = value },
                    check: { Closure value -> httpCall.check = value })
            action.delegate = http
            action.resolveStrategy = Closure.DELEGATE_FIRST
            action.call(http)
            def (code, response) = request(httpCall.method,
                    "http://${httpCall.endpoint}${httpCall.uri}", httpCall.body)
            httpCall.check.call(code, response)
        }
        Suite.metaClass.sleep = { long millis -> }
        def config = new Config()
        config.suitePath = new File('regression-test/suites').absolutePath
        config.dataPath = new File('regression-test/data').absolutePath
        config.realDataPath = new File('regression-test/real-data').absolutePath
        config.defaultDb = 'regression_test'
        def file = new File(config.suitePath, 'snapshot_recycler_test.groovy')
        def scriptContext = new ScriptContext(file, null, null, config, [], { true })
        def cluster = new SuiteCluster('snapshot_recycler_test', config)
        def context = new SuiteContext(file, 'snapshot_recycler_test', 'p0', scriptContext,
                cluster, null, null, config)
        return new Suite('snapshot_recycler_test', 'p0', context, cluster)
    }

    @AfterEach
    void cleanup() {
        GroovySystem.metaClassRegistry.removeMetaClass(Suite)
        GroovySystem.metaClassRegistry.removeMetaClass(SuiteCluster)
    }

    private static String job(String status, long finish, long success) {
        JsonOutput.toJson([status: status, last_ctime_ms: "1",
                          last_finish_time_ms: "${finish}", last_success_time_ms: "${success}"])
    }

    @Test
    void waitsForIdleAndRetriesTheWholeRoundUntilBothJobsSucceed() {
        def triggers = []
        def polls = [recycle: 0, check: 0]
        def runs = [recycle: 0, check: 0]
        def suite = loadPlugin { method, url, body ->
            String kind = url.contains('/recycle_') ? 'recycle' : 'check'
            if (url.contains('_job_info?')) {
                polls[kind]++
                // Both tasks are initially busy. An old successful round is insufficient.
                if (polls[kind] == 1) return [200, job('BUSY', 10, 10), '']
                long finish = 10 + runs[kind]
                // First recycle fails, second check fails, third round succeeds.
                long success = ((kind == 'recycle' && runs[kind] == 1)
                        || (kind == 'check' && runs[kind] == 2)) ? finish - 1 : finish
                return [200, job('IDLE', finish, success), '']
            }
            assertTrue(polls[kind] >= 2, 'must wait for IDLE before triggering')
            assertEquals(kind == 'recycle' ? 'POST' : 'GET', method)
            if (kind == 'recycle') assertEquals('{"instance_ids":["test_instance"]}', body)
            runs[kind]++
            triggers.add(kind)
            return [200, 'OK', '']
        }
        suite.recycleAndCheckSnapshot('127.0.0.1:5000', 'test_instance', 3, 3)
        assertEquals(['recycle', 'check', 'recycle', 'check', 'recycle', 'check'], triggers)
    }

    @Test
    void busyJobTimesOutWithoutTriggering() {
        int triggers = 0
        def suite = loadPlugin { method, url, body ->
            if (!url.contains('_job_info?')) triggers++
            [200, job('BUSY', 10, 10), '']
        }
        def error = assertThrows(AssertionError) {
            suite.recycleAndCheckSnapshot('127.0.0.1:5000', 'busy_instance', 2, 2)
        }
        assertTrue(error.message.contains('busy_instance'))
        assertEquals(0, triggers)
    }

    @Test
    void staleSuccessDoesNotCountAsCompletion() {
        int triggers = 0
        def suite = loadPlugin { method, url, body ->
            if (url.contains('_job_info?')) return [200, job('IDLE', 10, 10), '']
            triggers++
            [200, 'OK', '']
        }
        assertThrows(AssertionError) {
            suite.recycleAndCheckSnapshot('127.0.0.1:5000', 'stale_instance', 2, 2)
        }
        assertEquals(1, triggers, 'must not retrigger while awaiting completion')
    }

    @Test
    void failsAfterTheRoundLimit() {
        def runs = [recycle: 0, check: 0]
        def suite = loadPlugin { method, url, body ->
            String kind = url.contains('/recycle_') ? 'recycle' : 'check'
            if (url.contains('_job_info?')) return [200, job('IDLE', 10 + runs[kind], 0), '']
            runs[kind]++
            [200, 'OK', '']
        }
        def error = assertThrows(AssertionError) {
            suite.recycleAndCheckSnapshot('127.0.0.1:5000', 'failed_instance', 2, 2)
        }
        assertTrue(error.message.contains('2 rounds'))
        assertEquals([recycle: 2, check: 2], runs)
    }

    @Test
    void rejectsFailedTriggerResponse() {
        def suite = loadPlugin { method, url, body ->
            url.contains('_job_info?') ? [200, job('IDLE', 0, 0), ''] : [200, 'checker not enabled', '']
        }
        assertThrows(AssertionError) {
            suite.recycleAndCheckSnapshot('127.0.0.1:5000', 'test_instance', 2, 2)
        }
    }

    @Test
    void expiryAndDeletionWaitsOnlyTriggerIdleJobsOutsideTheInterval() {
        def info = [status: 'BUSY', last_ctime_ms: '0']
        int triggers = 0
        def suite = loadPlugin { method, url, body ->
            if (url.contains('_job_info?')) return [200, JsonOutput.toJson(info), '']
            triggers++
            assertEquals('POST', method)
            assertEquals('{"instance_ids":["test_instance"]}', body)
            return [200, 'OK', '']
        }
        assertFalse(suite.triggerSnapshotRecycleWhenIdle('127.0.0.1:5000', 'test_instance'))
        info = [status: 'IDLE', last_ctime_ms: System.currentTimeMillis().toString()]
        assertFalse(suite.triggerSnapshotRecycleWhenIdle('127.0.0.1:5000', 'test_instance'))
        assertEquals(0, triggers)
        info = [status: 'IDLE', last_ctime_ms: '0']
        assertTrue(suite.triggerSnapshotRecycleWhenIdle('127.0.0.1:5000', 'test_instance'))
        assertEquals(1, triggers)
    }

    private Suite dataSuite(List afterRows, boolean dropTable = false) {
        def suite = loadPlugin { method, url, body -> throw new AssertionError('unexpected HTTP call') }
        boolean recycled = false
        suite.cluster.metaClass.getAllRecyclers = { boolean alive ->
            [new Recycler(host: '127.0.0.1', httpPort: 5000, alive: true)]
        }
        Suite.metaClass.connectWithDockerCluster = { SuiteCluster cluster, Closure action -> action() }
        Suite.metaClass.recycleAndCheckSnapshot = { String address, String id ->
            assertEquals('test_instance', id)
            recycled = true
        }
        Suite.metaClass.sql = { String query ->
            if (query.contains('information_schema.tables')) {
                assertTrue(query.contains("ENGINE = 'Doris'"))
                return recycled && dropTable ? [] : [['test_db', 'test_table']]
            }
            assertTrue(query.contains('enable_sql_cache=false'))
            assertTrue(query.contains('enable_query_cache=false'))
            assertTrue(query.contains('enable_file_cache=false'))
            assertTrue(query.contains('enable_page_cache=false'))
            assertTrue(query.contains('`test_db`.`test_table`'))
            return recycled ? afterRows : [[1, 'a'], [1, 'a'], [2, 'b']]
        }
        return suite
    }

    @Test
    void dataComparisonIgnoresOrderButPreservesDuplicates() {
        def suite = dataSuite([[2, 'b'], [1, 'a'], [1, 'a']])
        suite.checkSnapshotRecycle(suite.cluster, [test_instance: suite.cluster])
    }

    @Test
    void dataComparisonRejectsMissingExtraAndChangedRows() {
        for (rows in [[[1, 'a'], [2, 'b']],
                      [[1, 'a'], [1, 'a'], [1, 'a'], [2, 'b']],
                      [[1, 'a'], [1, 'a'], [2, 'changed']]]) {
            try {
                def suite = dataSuite(rows)
                def error = assertThrows(AssertionError) {
                    suite.checkSnapshotRecycle(suite.cluster, [test_instance: suite.cluster])
                }
                assertTrue(error.message.contains('Data changed after recycling'))
            } finally {
                cleanup()
            }
        }
    }

    @Test
    void dataComparisonRejectsMissingTable() {
        def suite = dataSuite([], true)
        assertThrows(AssertionError) {
            suite.checkSnapshotRecycle(suite.cluster, [test_instance: suite.cluster])
        }
    }

}
