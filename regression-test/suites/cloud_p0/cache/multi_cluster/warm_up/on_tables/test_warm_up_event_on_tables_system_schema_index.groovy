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
import org.apache.doris.regression.util.WarmupMetricsUtils

// Test points covered: ST-03, ST-05.
suite('test_warm_up_event_on_tables_system_schema_index', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_warm_up_table_filter_refresh_interval_ms=1000',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
        'disable_auto_compaction=true',
    ]
    options.cloudMode = true

    def waitLatestColumnAlter = { tableName ->
        long deadline = System.currentTimeMillis() + 60000
        def last = []
        while (System.currentTimeMillis() < deadline) {
            last = sql """SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}'
                          ORDER BY CreateTime DESC LIMIT 1"""
            if (last.isEmpty() || last[0].toString().contains("FINISHED")) {
                sleep(1000)
                return
            }
            sleep(1000)
        }
        assert false : "schema change on ${tableName} did not finish, last=${last}"
    }

    docker(options) {
        Closure sqlRunner = { String q -> sql(q) }

        def srcCluster = "warmup_source"
        def dstCluster = "warmup_target"
        def dbName = "test_on_tables_system_schema_index_db"
        def jobIds = []

        cluster.addBackend(1, srcCluster)
        cluster.addBackend(1, dstCluster)

        try {
            sql """use @${srcCluster}"""
            sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
            sql """use ${dbName}"""
            sql """CREATE TABLE IF NOT EXISTS schema_tbl (
                       id INT,
                       v INT,
                       tag STRING
                   )
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2
                   PROPERTIES ("file_cache_ttl_seconds" = "3600")"""
            sql """CREATE TABLE IF NOT EXISTS idx_tbl (
                       id INT,
                       body STRING,
                       city STRING,
                       INDEX idx_body(body) USING INVERTED
                           PROPERTIES("parser" = "english", "support_phrase" = "true")
                   )
                   DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES (
                       "file_cache_ttl_seconds" = "3600",
                       "storage_format" = "V2"
                   )"""

            def jobId = sql("""
                WARM UP CLUSTER ${dstCluster} WITH CLUSTER ${srcCluster}
                ON TABLES (
                    INCLUDE '${dbName}.schema_*',
                    INCLUDE '${dbName}.idx_*'
                )
                PROPERTIES ("sync_mode" = "event_driven", "sync_event" = "load")
            """)[0][0]
            jobIds << jobId

            def matched = WarmupMetricsUtils.waitForMatchedTables(sqlRunner, jobId,
                    ["${dbName}.schema_tbl".toString(),
                     "${dbName}.idx_tbl".toString()] as Set)
            assert matched == ["${dbName}.schema_tbl".toString(),
                               "${dbName}.idx_tbl".toString()] as Set :
                    "unexpected matched tables: ${matched}"

            WarmupMetricsUtils.clearFileCacheOnAllBackends(sqlRunner)
            def baseMetrics = WarmupMetricsUtils.getWarmupMetrics(sqlRunner, srcCluster, dstCluster)

            sql """use @${srcCluster}"""
            sql """use ${dbName}"""

            sql """INSERT INTO schema_tbl VALUES (1, 10, 'a'), (2, 20, 'b')"""
            sql """ALTER TABLE schema_tbl ADD COLUMN extra STRING DEFAULT 'x'"""
            waitLatestColumnAlter("schema_tbl")
            sql """INSERT INTO schema_tbl(id, v, tag, extra) VALUES (3, 30, 'c', 'c_extra')"""
            sql """ALTER TABLE schema_tbl RENAME COLUMN tag label"""
            waitLatestColumnAlter("schema_tbl")
            sql """INSERT INTO schema_tbl(id, v, label, extra) VALUES (4, 40, 'd', 'd_extra')"""
            sql """ALTER TABLE schema_tbl MODIFY COLUMN v BIGINT NULL"""
            waitLatestColumnAlter("schema_tbl")
            sql """ALTER TABLE schema_tbl DROP COLUMN extra"""
            waitLatestColumnAlter("schema_tbl")
            sql """INSERT INTO schema_tbl(id, v, label) VALUES (5, 50, 'e')"""

            sql """INSERT INTO idx_tbl VALUES
                   (1, 'quick brown fox', 'beijing'),
                   (2, 'slow yellow fox', 'shanghai'),
                   (3, 'quick blue whale', 'beijing')"""

            def metrics = WarmupMetricsUtils.waitForWarmupFinish(sqlRunner, srcCluster, dstCluster,
                    baseMetrics.finished + 5, 90000)
            assert metrics.failed == baseMetrics.failed : "warmup should not fail, metrics=${metrics}, base=${baseMetrics}"

            def stats = WarmupMetricsUtils.waitForJobSyncStats(sqlRunner, jobId,
                    { it.seg_num.finish_5m >= 5 && it.idx_num.finish_5m > 0
                            && it.seg_num.fail_5m == 0 && it.idx_num.fail_5m == 0 },
                    60000)
            logger.info("schema/index SyncStats: ${stats}")
            assert stats.idx_num.finish_5m > 0 : "inverted index files should be warmed, stats=${stats}"
            assert stats.idx_num.fail_5m == 0 : "inverted index warmup should not fail, stats=${stats}"

            sql """use @${dstCluster}"""
            sql """use ${dbName}"""
            def schemaRes = sql """SELECT count(*), sum(v) FROM schema_tbl"""
            assert schemaRes[0][0].toString() == "5" : "schema table count mismatch: ${schemaRes}"
            assert schemaRes[0][1].toString() == "150" : "schema table sum mismatch: ${schemaRes}"

            profile("st05_inverted_index_profile") {
                sql """set enable_profile = true"""
                sql """set profile_level = 2"""
                sql """set enable_common_expr_pushdown = true"""
                run {
                    def res = sql """/* st05_inverted_index_profile */ SELECT id FROM idx_tbl
                                      WHERE body MATCH_ALL 'quick' ORDER BY id"""
                    assert res.collect { it[0].toString() } == ["1", "3"] : "index query mismatch: ${res}"
                    sleep(1000)
                }
                check { profileString, exception ->
                    if (exception != null) {
                        throw exception
                    }
                    assert profileString.contains("InvertedIndexNumRemoteIOTotal") :
                            "profile should contain inverted index file cache counters"
                    def idxRemoteTotal = WarmupMetricsUtils.sumProfileCounter(profileString,
                            "InvertedIndexNumRemoteIOTotal")
                    def remoteTotal = WarmupMetricsUtils.sumProfileCounter(profileString, "NumRemoteIOTotal")
                    logger.info("index profile remote counters: data=${remoteTotal}, inverted_index=${idxRemoteTotal}")
                    assert idxRemoteTotal == 0 : "warmed inverted index query should not read index files remotely"
                    assert remoteTotal == 0 : "warmed inverted index query should not read data files remotely"
                }
            }
        } finally {
            for (jid in jobIds) {
                try { sql """CANCEL WARM UP JOB WHERE ID = ${jid}""" } catch (Exception ignored) {}
            }
            try {
                sql """use @${srcCluster}"""
                sql """use ${dbName}"""
                sql """DROP TABLE IF EXISTS schema_tbl"""
                sql """DROP TABLE IF EXISTS idx_tbl"""
            } catch (Exception ignored) {}
            try { sql """DROP DATABASE IF EXISTS ${dbName}""" } catch (Exception ignored) {}
        }
    }
}
