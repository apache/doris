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
import java.util.concurrent.atomic.AtomicLong

// Optional comparative benchmark. Both versions use the same real objects and a zero-delay proxy.
suite("test_hive_split_early_stop_performance", "external") {
    def benchmarkSuite = delegate
    String fixtureUrl = context.config.otherConfigs.get("hiveSplitPerfControlUrl")
    if (!fixtureUrl) {
        return
    }
    String run = context.config.otherConfigs.get("hiveSplitPerfRun")
    String catalog = context.config.otherConfigs.get("hiveSplitEarlyStopCatalog")
    String table = context.config.otherConfigs.get("hiveSplitEarlyStopTable")
    String cacheTable = context.config.otherConfigs.get("hiveSplitPerfCacheTable")
    String syncTable = context.config.otherConfigs.get("hiveSplitPerfSyncTable") ?: table
    boolean planningOnly = context.config.otherConfigs.get("hiveSplitPerfPlanningCancelOnly")
            ?.toString()?.toBoolean() ?: false
    boolean steadyOnly = context.config.otherConfigs.get("hiveSplitPerfSteadyLimitOnly")
            ?.toString()?.toBoolean() ?: false
    boolean millionFileScan = context.config.otherConfigs.get("hiveSplitPerfMillionFileScan")
            ?.toString()?.toBoolean() ?: false
    int repetitions = context.config.otherConfigs.get("hiveSplitPerfRepetitions")?.toString()?.toInteger() ?: 3
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
    def measure = { String name, int repetition, long observationMillis, Closure action ->
        String phase = "${run}_${name}_${repetition}"
        control([phase: phase, empty: false, delay_ms: 0, hold_listings: false])
        long startedAt = System.currentTimeMillis()
        long begin = System.nanoTime()
        Long signal = action.call()
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin)
        long returnedAt = System.currentTimeMillis()
        Thread.sleep(observationMillis)
        def snapshot = metrics()
        assertEquals(0, snapshot.state.delay_ms as int, "Artificial LIST delay is enabled")
        assertFalse(snapshot.state.empty, "Synthetic empty directories are enabled")
        long observedAt = System.currentTimeMillis()
        logger.info("SPLIT_PERF " + JsonOutput.toJson([run: run, scenario: name, repetition: repetition,
                phase: phase, begin_ms: startedAt, return_ms: returnedAt, signal_ms: signal ?: returnedAt,
                elapsed_ms: elapsed, observed_ms: observedAt, counts: snapshot.counts[phase],
                active_at_observation: snapshot.state.active]))
        // Do not attribute a previous scan's late requests to the next phase. Retained idle queues
        // are left untouched so their memory and interference remain part of the measured behavior.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60)
        long stableSince = System.nanoTime()
        long previous = snapshot.counts[phase].LIST as long
        while (System.nanoTime() - stableSince < TimeUnit.SECONDS.toNanos(2)) {
            Thread.sleep(100)
            def current = metrics()
            long requests = current.counts[phase].LIST as long
            if (current.state.active != 0 || requests != previous) {
                stableSince = System.nanoTime()
            }
            previous = requests
            assertTrue(System.nanoTime() < deadline, "Previous scan did not become idle: " + phase)
        }
    }
    def cancelAfterActivity = { String phase, boolean beforeReads ->
        long connectionId = (sql("select connection_id()")[0][0] as Number).longValue()
        def signalledAt = new AtomicLong()
        def killer = extraThread("split-perf-canceller") {
            def started = { ->
                def counts = metrics().counts[phase]
                beforeReads ? (counts?.LIST ?: 0) >= 8 : (counts?.GET ?: 0) > 0
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
            while (!started() && System.nanoTime() < deadline) { Thread.sleep(10) }
            assertTrue(started(), "SUM did not reach the selected scan phase")
            if (beforeReads) {
                assertEquals(0, (metrics().counts[phase]?.GET ?: 0) as int, "BE already read a file")
            }
            signalledAt.set(System.currentTimeMillis())
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
        return signalledAt.get()
    }
    def saved = ["query_timeout", "num_partitions_in_batch_mode", "enable_file_cache", "enable_sql_cache",
            "enable_external_table_batch_mode"].collectEntries {
        name -> [(name): sql("show variables like '${name}'")[0][1]]
    }
    try {
        sql "set query_timeout=120"
        sql "set enable_file_cache=false"
        sql "set enable_sql_cache=false"
        sql "set enable_external_table_batch_mode=true"
        sql "set num_partitions_in_batch_mode=1"
        control([phase: "${run}_warmup", empty: false, delay_ms: 0, hold_listings: false])
        order_qt_warmup "select count(*), sum(id) from ${table} where p=0"
        if (steadyOnly) {
            for (int warmup = 0; warmup < 3; warmup++) {
                sql "select id from ${table} limit 10"
            }
        }
        for (int repetition = 0; repetition < repetitions; repetition++) {
            if (steadyOnly) {
                measure("limit10_steady", repetition, 10000L) {
                    benchmarkSuite."order_qt_limit_warm_${repetition}"("select id from ${table} limit 10")
                    return null
                }
                continue
            }
            sql "refresh catalog ${catalog}"
            if (planningOnly) {
                measure("cancel_before_read", repetition, 10000L) {
                    cancelAfterActivity("${run}_cancel_before_read_${repetition}".toString(), true)
                }
                continue
            }
            measure("limit10_cold_metadata", repetition, 10000L) {
                benchmarkSuite."order_qt_limit_cold_${repetition}"("select id from ${table} limit 10")
                return null
            }
            measure("limit10_warm_metadata", repetition, 10000L) {
                benchmarkSuite."order_qt_limit_warm_${repetition}"("select id from ${table} limit 10")
                return null
            }
            measure("cancel_sum", repetition, 10000L) {
                cancelAfterActivity("${run}_cancel_sum_${repetition}".toString(), false)
            }
            sql "set query_timeout=3"
            measure("timeout_sum", repetition, 10000L) {
                test {
                    sql "select sum(id) from ${table}"
                    // The BE Split fetch path reports "Timeout"; the FE deadline reports "timeout".
                    exception "imeout"
                }
                return null
            }
            sql "set query_timeout=120"
            measure("full_sum_10000_batch", repetition, 1000L) {
                benchmarkSuite."order_qt_full_batch_${repetition}"("select count(*), sum(id) from ${table} where p<100")
                return null
            }
            sql "set num_partitions_in_batch_mode=1024"
            // The partition threshold alone can still select file-count streaming.
            sql "set enable_external_table_batch_mode=false"
            measure("full_sum_10000_sync", repetition, 1000L) {
                benchmarkSuite."order_qt_full_sync_${repetition}"("select count(*), sum(id) from ${syncTable} where p<100")
                return null
            }
            sql "set enable_external_table_batch_mode=true"
            sql "set num_partitions_in_batch_mode=1"
            // Warm the completed listing before timing the cache-hit path.
            benchmarkSuite."order_qt_cache_warmup_${repetition}"("select count(*), sum(id) from ${cacheTable} where p<100")
            measure("full_sum_10000_cached", repetition, 1000L) {
                benchmarkSuite."order_qt_full_cached_${repetition}"("select count(*), sum(id) from ${cacheTable} where p<100")
                return null
            }
        }
        if (millionFileScan) {
            sql "set query_timeout=3600"
            measure("full_sum_1000000_batch", 0, 1000L) {
                order_qt_million_files "select count(*), sum(id) from ${table}"
                return null
            }
        }
    } finally {
        control([empty: false, delay_ms: 0, hold_listings: false])
        saved.each { name, value -> sql "set ${name}=${value}" }
    }
}
