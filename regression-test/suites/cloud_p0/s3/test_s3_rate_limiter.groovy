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

import groovy.json.JsonSlurper

import java.util.concurrent.CountDownLatch
import java.util.regex.Pattern

suite("test_s3_rate_limiter", "p0,nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    long bytesPerSecond = 5L * 1024 * 1024
    int smallRowsetCount = 6
    int smallRowsPerRowset = 1000
    int largeRows = 200000

    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    def backendIdToBrpcPort = [:]
    getBackendIpHttpAndBrpcPort(backendIdToIp, backendIdToHttpPort, backendIdToBrpcPort)

    List<String> metricNames = [
            "s3_get_rate_limit_sleep_ns",
            "s3_get_rate_limit_sleep_count",
            "s3_put_rate_limit_sleep_ns",
            "s3_put_rate_limit_sleep_count",
            "s3_get_bytes_rate_limit_sleep_ns",
            "s3_get_bytes_rate_limit_sleep_count",
            "s3_put_bytes_rate_limit_sleep_ns",
            "s3_put_bytes_rate_limit_sleep_count",
            "s3_get_rate_limit_rejected_count",
            "s3_put_rate_limit_rejected_count",
            "s3_get_bytes_rate_limit_rejected_count",
            "s3_put_bytes_rate_limit_rejected_count"
    ]

    def getMetricTotal = { String metricName ->
        long total = 0
        backendIdToIp.each { id, ip ->
            String brpcPort = backendIdToBrpcPort[id]
            def (code, out, err) = curl("GET", "http://${ip}:${brpcPort}/vars/${metricName}")
            assertEquals(0, code, "failed to read ${metricName} from BE ${id}: ${err}")
            def matcher = out =~ Pattern.compile(
                    '(?m)^' + Pattern.quote(metricName) + '\\s*:\\s*(\\d+)\\s*$')
            assertTrue(matcher.find(), "missing ${metricName} from BE ${id}: ${out}")
            total += matcher.group(1).toLong()
        }
        return total
    }

    def snapshotMetrics = {
        metricNames.collectEntries { name -> [(name): getMetricTotal(name)] }
    }

    def assertMetricChanges = { Map<String, Long> before, Map<String, Long> after,
                                Collection<String> expectedGrowth ->
        metricNames.each { name ->
            if (expectedGrowth.contains(name)) {
                assertTrue(after[name] > before[name],
                        "${name} should grow: before=${before[name]}, after=${after[name]}")
            } else {
                assertEquals(before[name], after[name],
                        "${name} should not grow while its limiter is disabled")
            }
        }
    }

    def assertBeConfig = { String key, Object expected ->
        Map<String, String> actualByBackend = get_be_param(key)
        actualByBackend.each { id, actual ->
            assertEquals(expected.toString(), actual, "unexpected ${key} on BE ${id}")
        }
    }

    def applyLimiterPhase = { long getQps, long putQps, long getBytes, long putBytes ->
        Map<String, Object> phase = [
                "s3_get_qps_per_core": getQps,
                "s3_put_qps_per_core": putQps,
                "s3_get_bytes_per_second_per_core": getBytes,
                "s3_put_bytes_per_second_per_core": putBytes
        ]
        phase.each { key, value -> set_be_param(key, value) }
        set_be_param("enable_s3_rate_limiter", true)
        phase.each { key, value -> assertBeConfig(key, value) }
        assertBeConfig("enable_s3_rate_limiter", true)

        // The daemon refreshes S3RateLimiterManager every 10 seconds. The config
        // assertions plus the following phase-specific bvar delta verify that the
        // dynamically rebuilt bucket, rather than only the config value, took effect.
        sleep(12000)
    }

    def applyLegacyQpsPhase = { long getQps, long putQps ->
        Map<String, Object> phase = [
                "s3_get_qps_per_core": getQps > 0 ? -1 : 0,
                "s3_put_qps_per_core": putQps > 0 ? -1 : 0,
                "s3_get_token_per_second": getQps > 0 ? getQps : 1,
                "s3_put_token_per_second": putQps > 0 ? putQps : 1,
                "s3_get_bucket_tokens": 1,
                "s3_put_bucket_tokens": 1,
                "s3_get_token_limit": 0,
                "s3_put_token_limit": 0,
                "s3_get_bytes_per_second_per_core": 0,
                "s3_put_bytes_per_second_per_core": 0
        ]
        phase.each { key, value -> set_be_param(key, value) }
        set_be_param("enable_s3_rate_limiter", true)
        phase.each { key, value -> assertBeConfig(key, value) }
        assertBeConfig("enable_s3_rate_limiter", true)
        sleep(12000)
    }

    def applyLegacyCountLimitPhase = { long getLimit, long putLimit ->
        Map<String, Object> phase = [
                "s3_get_qps_per_core": getLimit > 0 ? -1 : 0,
                "s3_put_qps_per_core": putLimit > 0 ? -1 : 0,
                "s3_get_token_per_second": 1000000000000000000L,
                "s3_put_token_per_second": 1000000000000000000L,
                "s3_get_bucket_tokens": 1000000000000000000L,
                "s3_put_bucket_tokens": 1000000000000000000L,
                "s3_get_token_limit": getLimit,
                "s3_put_token_limit": putLimit,
                "s3_get_bytes_per_second_per_core": 0,
                "s3_put_bytes_per_second_per_core": 0
        ]
        phase.each { key, value -> set_be_param(key, value) }
        set_be_param("enable_s3_rate_limiter", true)
        phase.each { key, value -> assertBeConfig(key, value) }
        assertBeConfig("enable_s3_rate_limiter", true)
        sleep(12000)
    }

    def clearFileCacheOnAllBackends = {
        backendIdToIp.each { id, ip ->
            String httpPort = backendIdToHttpPort[id]
            String response = new URL(
                    "http://${ip}:${httpPort}/api/file_cache?op=clear&sync=true").text
            def json = new JsonSlurper().parseText(response)
            assertEquals("OK", json.status, "failed to clear file cache on BE ${id}: ${response}")
        }
        sleep(5000)
    }

    String largePayload = "concat(" +
            "md5(cast(number as string))," +
            "md5(cast(number + 200000 as string))," +
            "md5(cast(number + 400000 as string))," +
            "md5(cast(number + 600000 as string)))"

    def runConcurrentSmallInserts = { int insertCount ->
        CountDownLatch ready = new CountDownLatch(insertCount)
        CountDownLatch start = new CountDownLatch(1)
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<Throwable>())
        List<Thread> writers = (0..<insertCount).collect { index ->
            Thread.start {
                ready.countDown()
                start.await()
                try {
                    connect(context.config.jdbcUser, context.config.jdbcPassword,
                            context.config.jdbcUrl) {
                        sql """
                            INSERT INTO ${context.dbName}.test_s3_rate_limiter_put
                            SELECT ${index * 1000} + number, repeat(md5(cast(number as string)), 4)
                            FROM numbers("number" = "100")
                        """
                    }
                } catch (Throwable t) {
                    errors.add(t)
                }
            }
        }
        ready.await()
        start.countDown()
        writers.each { it.join() }
        assertTrue(errors.isEmpty(), "concurrent internal PUT operations failed: ${errors}")
    }

    Map<String, Object> temporaryConfig = [
            "enable_s3_rate_limiter": false,
            "enable_packed_file": false,
            // File cache clearing is not enough for repeated scans: decoded column
            // pages may still be served from the storage page cache without S3 IO.
            "disable_storage_page_cache": true,
            // Keep PUT phases directionally pure: the optional post-upload HeadObject
            // would otherwise add GET traffic to an internal write.
            "enable_s3_object_check_after_upload": false,
            "s3_rate_limiter_cpu_cores": 1,
            "s3_get_qps_per_core": 0,
            "s3_put_qps_per_core": 0,
            "s3_get_qps_max": 0,
            "s3_put_qps_max": 0,
            // Keep the legacy QPS settings low. CPU-aware phases must ignore them
            // unless qps_per_core is explicitly switched back to -1.
            "s3_get_token_per_second": 1,
            "s3_put_token_per_second": 1,
            "s3_get_bucket_tokens": 1,
            "s3_put_bucket_tokens": 1,
            "s3_get_token_limit": 0,
            "s3_put_token_limit": 0,
            "s3_get_bytes_per_second_per_core": 0,
            "s3_put_bytes_per_second_per_core": 0,
            "s3_get_bytes_per_second_max": 0,
            "s3_put_bytes_per_second_max": 0,
            "s3_rate_limiter_log_interval": 1
    ]

    try {
        setBeConfigTemporary(temporaryConfig) {
            sql "SET disable_file_cache = true"
            sql "DROP TABLE IF EXISTS test_s3_rate_limiter_get"
            sql "DROP TABLE IF EXISTS test_s3_rate_limiter_put"
            sql """
                CREATE TABLE test_s3_rate_limiter_get (
                    id BIGINT,
                    payload STRING
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"
                )
            """
            sql """
                CREATE TABLE test_s3_rate_limiter_put (
                    id BIGINT,
                    payload STRING
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"
                )
            """

            // Build multiple small rowsets and one incompressible large rowset while
            // limiting is disabled. One bucket keeps all segment IO on one tablet/BE.
            for (int batch = 0; batch < smallRowsetCount; batch++) {
                sql """
                    INSERT INTO test_s3_rate_limiter_get
                    SELECT ${batch * smallRowsPerRowset} + number,
                           repeat(md5(cast(number + ${batch * smallRowsPerRowset} as string)), 4)
                    FROM numbers("number" = "${smallRowsPerRowset}")
                """
            }
            sql """
                INSERT INTO test_s3_rate_limiter_get
                SELECT 1000000 + number, ${largePayload}
                FROM numbers("number" = "${largeRows}")
            """

            // GET QPS: a cache-free scan reads all rowset segments from the internal vault.
            applyLimiterPhase(1, 0, 0, 0)
            clearFileCacheOnAllBackends()
            def before = snapshotMetrics()
            qt_get_qps_result """
                SELECT count(*), sum(length(payload))
                FROM test_s3_rate_limiter_get
                WHERE id < 1000000
            """
            def after = snapshotMetrics()
            assertMetricChanges(before, after, [
                    "s3_get_rate_limit_sleep_ns",
                    "s3_get_rate_limit_sleep_count"
            ])

            // GET bytes: 5 MiB/s is no lower than the default single S3 IO upper bound.
            applyLimiterPhase(0, 0, bytesPerSecond, 0)
            clearFileCacheOnAllBackends()
            before = snapshotMetrics()
            qt_get_bytes_result """
                SELECT count(*), sum(length(payload))
                FROM test_s3_rate_limiter_get
                WHERE id >= 1000000
            """
            after = snapshotMetrics()
            assertMetricChanges(before, after, [
                    "s3_get_bytes_rate_limit_sleep_ns",
                    "s3_get_bytes_rate_limit_sleep_count"
            ])

            // Disable CPU-aware GET bytes and switch qps_per_core to -1. The low
            // legacy QPS config that was ignored above must now throttle the same
            // internal-vault read. Only the original QPS metrics may grow.
            applyLegacyQpsPhase(1, 0)
            clearFileCacheOnAllBackends()
            before = snapshotMetrics()
            sql """
                SELECT count(*), sum(length(payload))
                FROM test_s3_rate_limiter_get
                WHERE id < 1000000
            """
            after = snapshotMetrics()
            assertMetricChanges(before, after, [
                    "s3_get_rate_limit_sleep_ns",
                    "s3_get_rate_limit_sleep_count"
            ])

            // PUT QPS: concurrent small inserts create independent small segment files
            // for the same tablet, guaranteeing multiple requests against one BE bucket.
            applyLimiterPhase(0, 1, 0, 0)
            before = snapshotMetrics()
            runConcurrentSmallInserts(4)
            after = snapshotMetrics()
            assertMetricChanges(before, after, [
                    "s3_put_rate_limit_sleep_ns",
                    "s3_put_rate_limit_sleep_count"
            ])

            // PUT bytes: the large, poorly compressible segment produces multiple
            // s3_write_buffer_size upload parts to the internal vault.
            applyLimiterPhase(0, 0, 0, bytesPerSecond)
            before = snapshotMetrics()
            sql """
                INSERT INTO test_s3_rate_limiter_put
                SELECT 1000000 + number, ${largePayload}
                FROM numbers("number" = "${largeRows}")
            """
            after = snapshotMetrics()
            assertMetricChanges(before, after, [
                    "s3_put_bytes_rate_limit_sleep_ns",
                    "s3_put_bytes_rate_limit_sleep_count"
            ])

            // Disable CPU-aware PUT bytes and switch qps_per_core to -1. Concurrent
            // internal-vault writes must now update only the original PUT QPS metrics.
            applyLegacyQpsPhase(0, 1)
            before = snapshotMetrics()
            runConcurrentSmallInserts(4)
            after = snapshotMetrics()
            assertMetricChanges(before, after, [
                    "s3_put_rate_limit_sleep_ns",
                    "s3_put_rate_limit_sleep_count"
            ])

            // Mixed internal IO: scan uncached vault data while another large segment
            // is uploaded. GET and PUT QPS metrics must grow independently.
            applyLimiterPhase(1, 1, 0, 0)
            clearFileCacheOnAllBackends()
            List<Throwable> mixedErrors = Collections.synchronizedList(new ArrayList<Throwable>())
            CountDownLatch mixedStart = new CountDownLatch(1)
            before = snapshotMetrics()
            def reader = Thread.start {
                mixedStart.await()
                try {
                    connect(context.config.jdbcUser, context.config.jdbcPassword,
                            context.config.jdbcUrl) {
                        sql "SET disable_file_cache = true"
                        sql """
                            SELECT count(*), sum(length(payload))
                            FROM ${context.dbName}.test_s3_rate_limiter_get
                        """
                    }
                } catch (Throwable t) {
                    mixedErrors.add(t)
                }
            }
            def writer = Thread.start {
                mixedStart.await()
                try {
                    connect(context.config.jdbcUser, context.config.jdbcPassword,
                            context.config.jdbcUrl) {
                        sql """
                            INSERT INTO ${context.dbName}.test_s3_rate_limiter_put
                            SELECT 2000000 + number, ${largePayload}
                            FROM numbers("number" = "${largeRows}")
                        """
                    }
                } catch (Throwable t) {
                    mixedErrors.add(t)
                }
            }
            mixedStart.countDown()
            reader.join()
            writer.join()
            assertTrue(mixedErrors.isEmpty(), "mixed internal S3 operations failed: ${mixedErrors}")
            after = snapshotMetrics()
            assertMetricChanges(before, after, [
                    "s3_get_rate_limit_sleep_ns",
                    "s3_get_rate_limit_sleep_count",
                    "s3_put_rate_limit_sleep_ns",
                    "s3_put_rate_limit_sleep_count"
            ])

            // Legacy GET token_limit: a cache-free scan must be rejected by the
            // internal GET bucket. Append a fresh rowset with limiting disabled so
            // no process-local cache can make this phase vacuous.
            applyLegacyCountLimitPhase(1, 0)
            set_be_param("enable_s3_rate_limiter", false)
            sql """
                INSERT INTO test_s3_rate_limiter_get
                SELECT 3000000 + number, repeat(md5(cast(number + 3000000 as string)), 4)
                FROM numbers("number" = "1000")
            """
            set_be_param("enable_s3_rate_limiter", true)
            clearFileCacheOnAllBackends()
            before = snapshotMetrics()
            test {
                sql """
                    SELECT count(*), sum(length(payload))
                    FROM test_s3_rate_limiter_get
                """
                exception "s3 get request exceeds QPS limit, rejected by BE rate limiter"
            }
            after = snapshotMetrics()
            assertMetricChanges(before, after, ["s3_get_rate_limit_rejected_count"])

            // Legacy PUT token_limit: the first one-object segment consumes the token;
            // the second is rejected with an independently attributable error/bvar.
            applyLegacyCountLimitPhase(0, 1)
            sql "INSERT INTO test_s3_rate_limiter_put VALUES (9000000, 'first token')"
            before = snapshotMetrics()
            test {
                sql "INSERT INTO test_s3_rate_limiter_put VALUES (9000001, 'rejected token')"
                exception "s3 put request exceeds QPS limit, rejected by BE rate limiter"
            }
            after = snapshotMetrics()
            assertMetricChanges(before, after, ["s3_put_rate_limit_rejected_count"])
        }
    } finally {
        // Let the daemon publish the restored bucket parameters before another suite
        // enables object-storage limiting.
        sleep(12000)
    }
}
