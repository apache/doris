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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

suite("test_hive_split_cancel_cache_isolation", "p0,external") {
    String fixtureUrl = context.config.otherConfigs.get("hiveSplitEarlyStopControlUrl")
    if (!fixtureUrl) {
        return
    }
    String catalog = context.config.otherConfigs.get("hiveSplitEarlyStopCatalog")
    String table = context.config.otherConfigs.get("hiveSplitEarlyStopTable")
    String ddl = sql("show create catalog ${catalog}")[0][1]
    assertTrue((ddl =~ /"meta.cache.hive.file.enable"\s*=\s*"true"/).find(),
            "Prepare this catalog with --file-cache")
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
    String phase = "concurrent_cache_isolation"
    sql "refresh catalog ${catalog}"
    control([phase: phase, empty: false, delay_ms: 0, hold_listings: true])
    def ready = new CountDownLatch(2)
    def begin = new CountDownLatch(1)
    def cancelledId = new AtomicLong(-1)
    def survivorId = new AtomicLong(-1)
    def prepareQuery = { AtomicLong id ->
        // A single selected partition must still use the cancellable Split producer path.
        sql "set num_partitions_in_batch_mode=1"
        sql "set query_timeout=60"
        id.set((sql("select connection_id()")[0][0] as Number).longValue())
        ready.countDown()
        assertTrue(begin.await(30, TimeUnit.SECONDS), "Queries were not released")
    }
    def cancelled = extraThread("cancelled-split-reader") {
        prepareQuery(cancelledId)
        test {
            sql "select sum(id) from ${table} where p=0"
            exception "cancel"
        }
    }
    def survivor = extraThread("surviving-split-reader") {
        prepareQuery(survivorId)
        order_qt_survivor "select count(*), sum(id) from ${table} where p=0"
    }
    try {
        assertTrue(ready.await(30, TimeUnit.SECONDS), "Query connections were not ready")
        begin.countDown()
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
        while ((metrics().counts[phase]?.LIST ?: 0) < 2 && System.nanoTime() < deadline) {
            Thread.sleep(25)
        }
        // Two independent request-owned loads must be in flight against the same directory.
        assertTrue((metrics().counts[phase]?.LIST ?: 0) >= 2, "The scans did not start independent loads")
        assertTrue(metrics().state.active >= 2, "The directory loads did not overlap")
        def processes = sql_return_maparray "show full processlist"
        String cancelledQuery = processes.find { it["Id"].toString() == cancelledId.get().toString() }["QueryId"]
        String survivorQuery = processes.find { it["Id"].toString() == survivorId.get().toString() }["QueryId"]
        long stoppedAt = System.currentTimeMillis()
        sql "kill query ${cancelledId.get()}"
        // Release the two SDK calls immediately after cancellation. Each loader then observes
        // its own scan token, so one cancellation must not fail the healthy load or its cache entry.
        control([hold_listings: false])
        cancelled.get(10, TimeUnit.SECONDS)
        survivor.get(30, TimeUnit.SECONDS)
        long survivorReturnedAt = System.currentTimeMillis()
        long requests = metrics().counts[phase].LIST as long
        order_qt_cached_followup "select count(*), sum(id) from ${table} where p=0"
        assertEquals(requests, metrics().counts[phase].LIST as long, "The completed listing was not reused")
        long remaining = stoppedAt + 11000 - System.currentTimeMillis()
        if (remaining > 0) { Thread.sleep(remaining) }
        assertEquals(0, metrics().state.active as int, "File requests did not finish")
        logger.info("SPLIT_CONCURRENT " + JsonOutput.toJson([phase: phase, stop_ms: stoppedAt,
                observed_ms: System.currentTimeMillis(), cancelled_query_id: cancelledQuery,
                survivor_query_id: survivorQuery, survivor_return_ms: survivorReturnedAt,
                directory_requests: requests]))
    } finally {
        begin.countDown()
        control([hold_listings: false, empty: false, delay_ms: 0])
        // On failure, stop any SQL still using the fixture before resetting its controls.
        try {
            [[cancelled, cancelledId], [survivor, survivorId]].each { entry ->
                if (!entry[0].isDone() && entry[1].get() >= 0) {
                    sql "kill query ${entry[1].get()}"
                }
            }
            cancelled.get(35, TimeUnit.SECONDS)
            survivor.get(35, TimeUnit.SECONDS)
        } finally {
            control([empty: false, delay_ms: 0])
        }
    }
}
