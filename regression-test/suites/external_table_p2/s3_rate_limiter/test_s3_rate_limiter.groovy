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

import java.util.regex.Pattern

suite("test_s3_rate_limiter", "p2,external,nonConcurrent") {
    // In cloud mode, external S3 TVF/Outfile clients intentionally bypass this limiter;
    // only internal storage-vault clients are wrapped. The SQL paths below exercise the
    // non-cloud compatibility contract where every object-storage client is wrapped.
    if (isCloudMode()) {
        return
    }

    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = getS3BucketName()
    String provider = getS3Provider()
    String runId = UUID.randomUUID().toString()
    String rootPath = "${bucket}/regression/s3_rate_limiter/${runId}"
    long bytesPerSecond = 4L * 1024 * 1024

    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    def backendIdToBrpcPort = [:]
    getBackendIpHttpAndBrpcPort(backendIdToIp, backendIdToHttpPort, backendIdToBrpcPort)

    List<String> metricNames = [
            "s3_get_rate_limit_sleep_count",
            "s3_put_rate_limit_sleep_count",
            "s3_get_bytes_rate_limit_sleep_count",
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
            def (code, out, err) =
                    curl("GET", "http://${ip}:${brpcPort}/vars/${metricName}")
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
            assertEquals(expected.toString(), actual,
                    "unexpected ${key} on BE ${id}")
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

        // S3RateLimiterManager is refreshed by the daemon every 10 seconds. Waiting
        // for one complete interval makes the following bvar delta prove that the
        // dynamically updated limiter, rather than only the config value, took effect.
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

    def outfileSql = { String path, String querySuffix ->
        return """
            SELECT id, payload FROM ${context.dbName}.test_s3_rate_limiter
            ${querySuffix}
            INTO OUTFILE "s3://${path}"
            FORMAT AS csv
            PROPERTIES (
                "column_separator" = "|",
                "s3.endpoint" = "${endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key" = "${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${provider}"
            )
        """
    }

    def tvfSql = { String path ->
        return """
            SELECT count(*) FROM S3(
                "uri" = "s3://${path}*",
                "s3.endpoint" = "${endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key" = "${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${provider}",
                "format" = "csv",
                "column_separator" = "|",
                "csv_schema" = "id:bigint;payload:string"
            )
        """
    }

    Map<String, Object> temporaryConfig = [
            "enable_s3_rate_limiter": false,
            "s3_rate_limiter_cpu_cores": 1,
            "s3_get_qps_per_core": 0,
            "s3_put_qps_per_core": 0,
            "s3_get_qps_max": 0,
            "s3_put_qps_max": 0,
            "s3_get_token_per_second": 1000000000000000000L,
            "s3_put_token_per_second": 1000000000000000000L,
            "s3_get_bucket_tokens": 1000000000000000000L,
            "s3_put_bucket_tokens": 1000000000000000000L,
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
            sql "SET enable_parallel_outfile = true"
            sql "SET parallel_pipeline_task_num = 2"
            sql "DROP TABLE IF EXISTS test_s3_rate_limiter"
            sql """
                CREATE TABLE test_s3_rate_limiter (
                    id BIGINT,
                    payload VARCHAR(256)
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 2
                PROPERTIES("replication_num" = "1")
            """
            sql """
                INSERT INTO test_s3_rate_limiter
                SELECT number, repeat('x', 128)
                FROM numbers("number" = "100000")
            """

            // Prepare two explicit small objects for GET QPS and one large object for
            // GET bytes. Do this before enabling any limiter so setup is not observed.
            String getQpsSourcePath = "${rootPath}/get_qps_source_"
            sql outfileSql("${getQpsSourcePath}first_", "WHERE id < 10")
            sql outfileSql("${getQpsSourcePath}second_", "WHERE id < 10")
            String getQpsReadSql = tvfSql(getQpsSourcePath)
            String largeSourcePath = "${rootPath}/large_source_"
            sql outfileSql(largeSourcePath, "")
            String largeSourceReadSql = tvfSql(largeSourcePath)

            // GET QPS: bytes and all PUT limiters are disabled.
            applyLimiterPhase(1, 0, 0, 0)
            def before = snapshotMetrics()
            qt_get_qps_result getQpsReadSql
            def after = snapshotMetrics()
            assertMetricChanges(before, after, ["s3_get_rate_limit_sleep_count"])

            // GET bytes: QPS and all PUT limiters are disabled.
            applyLimiterPhase(0, 0, bytesPerSecond, 0)
            before = snapshotMetrics()
            qt_get_bytes_result largeSourceReadSql
            after = snapshotMetrics()
            assertMetricChanges(before, after, ["s3_get_bytes_rate_limit_sleep_count"])

            // PUT QPS: bytes and all GET limiters are disabled.
            applyLimiterPhase(0, 1, 0, 0)
            String putQpsPath = "${rootPath}/put_qps_"
            before = snapshotMetrics()
            sql outfileSql("${putQpsPath}first_", "WHERE id < 10")
            sql outfileSql("${putQpsPath}second_", "WHERE id < 10")
            after = snapshotMetrics()
            assertMetricChanges(before, after, ["s3_put_rate_limit_sleep_count"])
            qt_put_qps_result tvfSql(putQpsPath)

            // PUT bytes: QPS and all GET limiters are disabled.
            applyLimiterPhase(0, 0, 0, bytesPerSecond)
            String putBytesPath = "${rootPath}/put_bytes_"
            before = snapshotMetrics()
            sql outfileSql(putBytesPath, "")
            after = snapshotMetrics()
            assertMetricChanges(before, after, ["s3_put_bytes_rate_limit_sleep_count"])
            qt_put_bytes_result tvfSql(putBytesPath)

            // Mixed: run a real S3 read and write concurrently. The GET and PUT QPS
            // counters must both grow while the disabled bytes counters stay unchanged,
            // proving that the directional request limiters are independent.
            set_be_param("enable_s3_rate_limiter", false)
            String mixedSourcePath = "${rootPath}/mixed_source_"
            sql outfileSql(mixedSourcePath, "")
            String mixedSourceReadSql = tvfSql(mixedSourcePath)
            applyLimiterPhase(1, 1, 0, 0)
            String mixedPath = "${rootPath}/mixed_output_"
            List<Throwable> errors = Collections.synchronizedList(new ArrayList<Throwable>())
            before = snapshotMetrics()
            def reader = Thread.start {
                try {
                    connect(context.config.jdbcUser, context.config.jdbcPassword,
                            context.config.jdbcUrl) {
                        sql mixedSourceReadSql
                    }
                } catch (Throwable t) {
                    errors.add(t)
                }
            }
            def writer = Thread.start {
                try {
                    connect(context.config.jdbcUser, context.config.jdbcPassword,
                            context.config.jdbcUrl) {
                        sql "SET enable_parallel_outfile = true"
                        sql "SET parallel_pipeline_task_num = 2"
                        sql outfileSql(mixedPath, "")
                    }
                } catch (Throwable t) {
                    errors.add(t)
                }
            }
            reader.join()
            writer.join()
            assertTrue(errors.isEmpty(), "mixed S3 operations failed: ${errors}")
            after = snapshotMetrics()
            assertMetricChanges(before, after, [
                    "s3_get_rate_limit_sleep_count",
                    "s3_put_rate_limit_sleep_count"
            ])
            qt_mixed_result tvfSql(mixedPath)

            // Legacy GET token_limit: force an admission rejection and verify both the
            // user-visible direction/reason and the dedicated rejection bvar.
            applyLegacyCountLimitPhase(1, 0)
            before = snapshotMetrics()
            test {
                sql getQpsReadSql
                exception "s3 get request exceeds QPS limit"
                exception "rejected by BE rate limiter"
            }
            after = snapshotMetrics()
            assertMetricChanges(before, after, ["s3_get_rate_limit_rejected_count"])

            // Legacy PUT token_limit has an independent counter and error message.
            applyLegacyCountLimitPhase(0, 1)
            before = snapshotMetrics()
            test {
                sql outfileSql("${rootPath}/put_rejected_", "WHERE id < 10")
                exception "s3 put request exceeds QPS limit"
                exception "rejected by BE rate limiter"
            }
            after = snapshotMetrics()
            assertMetricChanges(before, after, ["s3_put_rate_limit_rejected_count"])
        }
    } finally {
        // setBeConfigTemporary restores the values first. Allow the daemon to publish
        // the restored bucket parameters before another suite can use object storage.
        sleep(12000)
    }
}
