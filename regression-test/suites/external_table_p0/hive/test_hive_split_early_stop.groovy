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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

// Optional million-file acceptance fixture: 10,000 HMS partitions, 100 one-row files per partition.
// The S3 fixture exposes /__control (phase, empty, delay_ms) and /__metrics (state.active, counts[phase].LIST/GET).
// Empty logical listings or delayed real listings keep planning inside the first-split wait.
// Count requests at the fixture, not in SQL profiles.
suite("test_hive_split_early_stop", "p0,external") {
    String fixtureUrl = context.config.otherConfigs.get("hiveSplitEarlyStopControlUrl")
    if (!fixtureUrl) {
        return
    }
    logger.info("SPLIT_RUN_START " + System.currentTimeMillis())
    String catalog = context.config.otherConfigs.get("hiveSplitEarlyStopCatalog")
    String table = context.config.otherConfigs.get("hiveSplitEarlyStopTable")
    String producerParallelism = context.config.otherConfigs.get("hiveSplitEarlyStopProducerParallelism")
    int producerLimit = producerParallelism == null ? 0 : producerParallelism.toInteger()
    // A capped scan can wait on fewer than ten LIST calls before its first batch is available.
    int planningCancelListings = producerLimit > 0 ? Math.min(10, producerLimit) : 10
    boolean physicalFixture = context.config.otherConfigs.get("hiveSplitEarlyStopPhysicalFixture")
            ?.toString()?.toBoolean() ?: false
    def metrics = { -> new JsonSlurper().parse(new URL(fixtureUrl + "/__metrics")) }
    def control = { Map settings ->
        def conn = new URL(fixtureUrl + "/__control").openConnection()
        conn.requestMethod = "POST"
        conn.doOutput = true
        conn.setRequestProperty("Content-Type", "application/json")
        conn.outputStream.withCloseable { it.write(JsonOutput.toJson(settings).getBytes("UTF-8")) }
        conn.inputStream.withCloseable { it.text }
        conn.disconnect()
    }
    def listCount = { String phase -> (metrics().counts[phase]?.LIST ?: 0) as long }
    def assertStopped = { String phase, long stoppedAt ->
        // Calls already inside an SDK may finish during the grace period. Enforce the 10-second
        // deadline, then observe one more second to detect tasks that start requesting files again.
        long untilDeadline = stoppedAt + 10000 - System.currentTimeMillis()
        if (untilDeadline > 0) { Thread.sleep(untilDeadline) }
        def atDeadline = metrics()
        assertEquals(0, atDeadline.state.active as int, "outstanding file calls: " + phase)
        long requests = (atDeadline.counts[phase]?.LIST ?: 0) as long
        Thread.sleep(1000)
        assertEquals(requests, listCount(phase), "new file requests after the 10-second deadline: " + phase)
        assertEquals(0, metrics().state.active as int, "outstanding file calls: " + phase)
        logger.info("SPLIT_STOP " + JsonOutput.toJson([phase: phase, stop_ms: stoppedAt,
                observed_ms: System.currentTimeMillis(), requests: requests]))
    }
    def settings = ["query_timeout", "first_split_wait_timeout_ms"]
    if (producerParallelism != null) {
        settings.add("split_producer_parallelism")
    }
    def saved = settings.collectEntries { name -> [(name): sql("show variables like '${name}'")[0][1]] }
    try {
        if (producerParallelism != null) {
            sql "set split_producer_parallelism=${producerParallelism}"
        }
        sql "set query_timeout=120"
        sql "set first_split_wait_timeout_ms=30000"
        control([phase: "limit", empty: false, delay_ms: 50])
        order_qt_limit "select id from ${table} limit 10"
        assertStopped("limit", System.currentTimeMillis())

        for (boolean waitingForFirst : [false, true]) {
            String phase = waitingForFirst ? "planning_cancel" : "cancel"
            sql "refresh catalog ${catalog}"
            control([phase: phase, empty: waitingForFirst && !physicalFixture,
                    delay_ms: waitingForFirst && physicalFixture ? 5000 : 50])
            long connectionId = (sql("select connection_id()")[0][0] as Number).longValue()
            def cancelledAt = new AtomicReference<Long>()
            def killer = extraThread("split-query-canceller") {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
                def started = { ->
                    def counts = metrics().counts[phase]
                    waitingForFirst ? (counts?.LIST ?: 0) >= planningCancelListings : (counts?.GET ?: 0) > 0
                }
                while (!started() && System.nanoTime() < deadline) { Thread.sleep(25) }
                if (!started()) { throw new IllegalStateException("No scan activity: " + phase) }
                if (waitingForFirst) {
                    assertEquals(0, (metrics().counts[phase]?.GET ?: 0) as int,
                            "The first Split was already consumed before planning cancellation")
                }
                cancelledAt.set(System.currentTimeMillis())
                sql "kill query ${connectionId}"
            }
            try {
                test {
                    sql "select sum(id) from ${table}"
                    exception "cancel"
                }
            } finally {
                killer.get(35, TimeUnit.SECONDS)
            }
            assertTrue(System.currentTimeMillis() - cancelledAt.get() < 10000, phase + " did not return in 10s")
            assertStopped(phase, cancelledAt.get())
        }

        for (boolean waitingForFirst : [false, true]) {
            String phase = waitingForFirst ? "planning_timeout" : "timeout"
            sql "refresh catalog ${catalog}"
            sql "set query_timeout=3"
            control([phase: phase, empty: waitingForFirst && !physicalFixture,
                    delay_ms: waitingForFirst && physicalFixture ? 5000 : 50])
            long begin = System.currentTimeMillis()
            test {
                sql "select sum(id) from ${table}"
                exception "timeout"
            }
            assertTrue(System.currentTimeMillis() - begin < 13000, phase + " did not return after timeout")
            if (!waitingForFirst) {
                assertTrue((metrics().counts[phase]?.GET ?: 0) > 0, "SUM never started reading files")
            }
            assertStopped(phase, begin + 3000)
        }

        sql "set query_timeout=120"
        for (long timeout : [1000L, 2500L]) {
            String phase = "first_split_timeout_${timeout}"
            sql "refresh catalog ${catalog}"
            sql "set first_split_wait_timeout_ms=${timeout}"
            control([phase: phase, empty: !physicalFixture, delay_ms: physicalFixture ? timeout + 1000 : 100])
            long begin = System.currentTimeMillis()
            test {
                sql "select sum(id) from ${table}"
                exception "first split after ${timeout} ms: partitions_processed="
            }
            long elapsed = System.currentTimeMillis() - begin
            assertTrue(elapsed >= timeout && elapsed < timeout + 5000, "first-split timeout elapsed: " + elapsed)
            logger.info("FIRST_SPLIT_TIMEOUT configured_ms=${timeout} elapsed_ms=${elapsed}")
            assertStopped(phase, System.currentTimeMillis())
        }
        control([phase: "full_small_scan", empty: false, delay_ms: 0])
        sql "set first_split_wait_timeout_ms=30000"
        order_qt_full_small_scan "select count(*), sum(id) from ${table} where p=0"
    } finally {
        control([empty: false, delay_ms: 0])
        saved.each { name, value -> sql "set ${name}=${value}" }
    }
}
