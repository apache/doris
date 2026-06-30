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
import org.apache.doris.regression.util.Http

import java.util.regex.Pattern

suite("test_topn_lazy_mat_phase2_no_write_file_cache", "docker") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 1
    options.msNum = 1
    options.cloudMode = true
    options.beConfigs += [
        "enable_file_cache=true",
        "disable_storage_page_cache=true",
        "enable_java_support=false",
        "enable_evict_file_cache_in_advance=false",
        "file_cache_enter_disk_resource_limit_mode_percent=99",
        "file_cache_each_block_size=4096",
        "file_cache_path=[{\"path\":\"/opt/apache-doris/be/storage/file_cache\","
                + "\"total_size\":104857600,\"query_limit\":104857600}]"
    ]

    docker(options) {
        def clusters = sql "SHOW CLUSTERS"
        assert !clusters.isEmpty()
        def computeGroup = clusters[0][0]
        sql "use @${computeGroup}"

        def backends = sql "SHOW BACKENDS"
        assert backends.size() == 1
        def beHost = backends[0][1]
        def beHttpPort = backends[0][4]
        def clearFileCache = {
            def result = Http.GET("http://${beHost}:${beHttpPort}/api/file_cache?op=clear&sync=true", true)
            logger.info("clear file cache result: ${result}")
        }

        sql "set enable_profile = true"
        sql "set profile_level = 2"
        sql "set enable_sql_cache = false"
        sql "set enable_query_cache = false"
        sql "set enable_page_cache = false"
        sql "set topn_lazy_materialization_threshold = 1024"

        def metricValue = { String profileText, String metricName ->
            def matcher = profileText =~
                    /(?m)^\s*-\s*${Pattern.quote(metricName)}:\s*(?:sum\s+)?([0-9]+(?:\.[0-9]+)?).*$/
            assert matcher.find() : "missing metric ${metricName} in profile:\n${profileText}"
            return new BigDecimal(matcher.group(1))
        }

        def metricValues = { String profileText, String metricName ->
            def matcher = profileText =~
                    /(?m)^\s*-\s*${Pattern.quote(metricName)}:\s*(?:sum\s+)?([0-9]+(?:\.[0-9]+)?).*$/
            def values = []
            while (matcher.find()) {
                values.add(new BigDecimal(matcher.group(1)))
            }
            assert !values.isEmpty() : "missing metric ${metricName} in profile:\n${profileText}"
            return values
        }

        def numericValues = { String text ->
            def matcher = text =~ /([0-9]+(?:\.[0-9]+)?)/
            def values = []
            while (matcher.find()) {
                values.add(new BigDecimal(matcher.group(1)))
            }
            return values
        }

        def metricLineValues = { String profileText, String metricName ->
            def matcher = profileText =~ /(?m)^\s*-\s*${Pattern.quote(metricName)}:\s*(.*)$/
            def values = []
            while (matcher.find()) {
                values.add(matcher.group(1).trim())
            }
            return values
        }

        def topnSecondPhaseMetricNames = [
            "TopNLazyMaterializationSecondPhaseLocalIOCount",
            "TopNLazyMaterializationSecondPhaseLocalIOBytes",
            "TopNLazyMaterializationSecondPhaseRemoteIOCount",
            "TopNLazyMaterializationSecondPhaseRemoteIOBytes",
            "TopNLazyMaterializationSecondPhaseSkipCacheIOCount",
            "TopNLazyMaterializationSecondPhaseWriteCacheBytes",
            "TopNLazyMaterializationSecondPhaseLocalIOTime",
            "TopNLazyMaterializationSecondPhaseRemoteIOTime",
            "TopNLazyMaterializationSecondPhaseWriteCacheIOTime",
            "TopNLazyMaterializationSecondPhaseRowsRead",
            "TopNLazyMaterializationSecondPhaseSegmentsRead",
            "TopNLazyMaterializationSecondPhasePerBackend",
            "TopNLazyMaterializationSecondPhasePerBackendRowsRead",
            "TopNLazyMaterializationSecondPhasePerBackendSegmentsRead",
            "TopNLazyMaterializationSecondPhasePerBackendLocalIOCount",
            "TopNLazyMaterializationSecondPhasePerBackendLocalIOBytes",
            "TopNLazyMaterializationSecondPhasePerBackendRemoteIOCount",
            "TopNLazyMaterializationSecondPhasePerBackendRemoteIOBytes",
            "TopNLazyMaterializationSecondPhasePerBackendSkipCacheIOCount",
            "TopNLazyMaterializationSecondPhasePerBackendWriteCacheBytes",
            "TopNLazyMaterializationSecondPhasePerBackendLocalIOTime",
            "TopNLazyMaterializationSecondPhasePerBackendRemoteIOTime",
            "TopNLazyMaterializationSecondPhasePerBackendWriteCacheIOTime"
        ]

        def logTopnSecondPhaseMetrics = { String name, String profileText ->
            def metrics = topnSecondPhaseMetricNames.collectEntries { metricName ->
                def values = metricLineValues(profileText, metricName)
                assert !values.isEmpty() : "missing metric ${metricName} in profile:\n${profileText}"
                [(metricName): values]
            }
            logger.info("${name} TopN lazy materialization second phase metrics: ${metrics}")
            logger.info("${name} CachedPagesNum values: ${metricLineValues(profileText, 'CachedPagesNum')}")
        }

        def runProfileQuery = { String name, String query, Closure checker ->
            profile(name) {
                run {
                    sql "/* ${name} */ ${query}"
                    sleep(1000)
                }
                check { profileString, exception ->
                    if (exception != null) {
                        logger.error("Profile failed, profile result:\n${profileString}", exception)
                        throw exception
                    }
                    assert profileString.contains("TopNLazyMaterializationSecondPhase") :
                            "missing TopN lazy materialization profile:\n${profileString}"
                    assert profileString.contains("Is  Cached:  No")
                            || profileString.contains("Is Cached: No") :
                            "query should not hit SQL/query cache:\n${profileString}"
                    logTopnSecondPhaseMetrics(name, profileString)
                    checker(profileString)
                }
            }
        }

        def assertRemoteOnlyMiss = { String profileText ->
            assert metricValue(profileText, "TopNLazyMaterializationSecondPhaseRemoteIOCount") > 0
            assert metricValue(profileText, "TopNLazyMaterializationSecondPhaseRemoteIOBytes") > 0
            assert metricValue(profileText, "TopNLazyMaterializationSecondPhaseSkipCacheIOCount") > 0
            assert metricValue(profileText, "TopNLazyMaterializationSecondPhaseWriteCacheBytes")
                    .compareTo(BigDecimal.ZERO) == 0
        }

        def assertPerBackendRowsMatchAggregate = { String profileText ->
            def aggregateRows = metricValues(profileText,
                    "TopNLazyMaterializationSecondPhaseRowsRead")
                    .inject(BigDecimal.ZERO) { sum, value -> sum + value }
            def perBackendRows = metricLineValues(profileText,
                    "TopNLazyMaterializationSecondPhasePerBackendRowsRead")
                    .collectMany { line -> numericValues(line) }
            assert !perBackendRows.isEmpty() :
                    "missing per-backend rows-read values:\n${profileText}"
            def perBackendRowsSum = perBackendRows.inject(BigDecimal.ZERO) { sum, value -> sum + value }
            assert aggregateRows.compareTo(perBackendRowsSum) == 0 :
                    "per-backend rows-read sum ${perBackendRowsSum} should equal aggregate ${aggregateRows}:\n${profileText}"
        }

        def assertLocalFullHit = { String profileText ->
            assert metricValue(profileText, "TopNLazyMaterializationSecondPhaseLocalIOCount") > 0
            assert metricValue(profileText, "TopNLazyMaterializationSecondPhaseLocalIOBytes") > 0
            assert metricValue(profileText, "TopNLazyMaterializationSecondPhaseRemoteIOCount")
                    .compareTo(BigDecimal.ZERO) == 0
            assert metricValue(profileText, "TopNLazyMaterializationSecondPhaseRemoteIOBytes")
                    .compareTo(BigDecimal.ZERO) == 0
            assert metricValue(profileText, "TopNLazyMaterializationSecondPhaseWriteCacheBytes")
                    .compareTo(BigDecimal.ZERO) == 0
        }

        sql "DROP TABLE IF EXISTS topn_lazy_remote_only_no_row_store"
        sql """
            CREATE TABLE topn_lazy_remote_only_no_row_store (
                k INT NOT NULL,
                sort_key INT NOT NULL,
                payload VARCHAR(4096) NOT NULL,
                pad VARCHAR(4096) NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "store_row_column" = "false"
            )
        """
        sql """
            INSERT INTO topn_lazy_remote_only_no_row_store
            SELECT number,
                   4096 - number,
                   repeat(cast(number as string), 128),
                   repeat('x', 256)
            FROM numbers("number" = "4096")
        """

        def noRowStoreQuery =
                "SELECT k, payload, pad FROM topn_lazy_remote_only_no_row_store ORDER BY sort_key LIMIT 16"
        explain {
            sql noRowStoreQuery
            contains("MaterializeNode")
        }

        clearFileCache()
        sql "set enable_topn_lazy_mat_phase2_no_write_file_cache = true"
        runProfileQuery("topn_lazy_remote_only_no_row_store_remote_only_miss",
                noRowStoreQuery, assertRemoteOnlyMiss)

        clearFileCache()
        sql "set enable_topn_lazy_mat_phase2_no_write_file_cache = false"
        sql "/* topn_lazy_remote_only_no_row_store_local_full_hit_warm */ ${noRowStoreQuery}"
        sleep(1000)

        sql "set enable_topn_lazy_mat_phase2_no_write_file_cache = true"
        runProfileQuery("topn_lazy_remote_only_no_row_store_local_full_hit",
                noRowStoreQuery, assertLocalFullHit)

        clearFileCache()
        sql "set enable_topn_lazy_mat_phase2_no_write_file_cache = true"
        sql "set batch_size = 1"
        try {
            runProfileQuery("topn_lazy_remote_only_no_row_store_multi_fetch_accumulate",
                    noRowStoreQuery, { profileText ->
                        assertRemoteOnlyMiss(profileText)
                        assertPerBackendRowsMatchAggregate(profileText)
                    })
        } finally {
            sql "set batch_size = 4062"
        }

        sql "DROP TABLE IF EXISTS topn_lazy_remote_only_row_store"
        sql """
            CREATE TABLE topn_lazy_remote_only_row_store (
                k INT NOT NULL,
                sort_key INT NOT NULL,
                payload VARCHAR(4096) NOT NULL,
                pad VARCHAR(4096) NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "store_row_column" = "true"
            )
        """
        sql """
            INSERT INTO topn_lazy_remote_only_row_store
            SELECT number,
                   4096 - number,
                   repeat(cast(number as string), 128),
                   repeat('x', 256)
            FROM numbers("number" = "4096")
        """

        def rowStoreQuery =
                "SELECT k, payload, pad FROM topn_lazy_remote_only_row_store ORDER BY sort_key LIMIT 16"
        explain {
            sql rowStoreQuery
            contains("MaterializeNode")
        }

        clearFileCache()
        sql "set enable_topn_lazy_mat_phase2_no_write_file_cache = true"
        runProfileQuery("topn_lazy_remote_only_row_store_remote_only_miss",
                rowStoreQuery, assertRemoteOnlyMiss)

        clearFileCache()
        sql "set enable_topn_lazy_mat_phase2_no_write_file_cache = false"
        sql "/* topn_lazy_remote_only_row_store_local_full_hit_warm */ ${rowStoreQuery}"
        sleep(1000)

        sql "set enable_topn_lazy_mat_phase2_no_write_file_cache = true"
        runProfileQuery("topn_lazy_remote_only_row_store_local_full_hit",
                rowStoreQuery, assertLocalFullHit)
    }
}
