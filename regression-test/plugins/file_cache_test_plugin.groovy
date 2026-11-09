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

import org.apache.doris.regression.suite.Suite

// Use only in nonConcurrent suites: backend configuration is shared by all queries.
Suite.metaClass.withStableFileCache = { Closure action ->
    Suite suite = delegate as Suite
    // Disable upper caches so repeated reads reach BlockFileCache.
    suite.sql "set enable_file_cache = true"
    suite.sql "set disable_file_cache = false"
    suite.sql "set enable_sql_cache = false"
    suite.sql "set enable_hive_sql_cache = false"
    suite.sql "set enable_query_cache = false"
    suite.sql "set enable_parquet_file_page_cache = false"

    long monitorMs = suite.get_be_param("file_cache_background_monitor_interval_ms")
            .values().collect { it.toLong() }.max()
    int timeoutSeconds = Math.max(30, (monitorMs * 6L / 1000L) as int)
    // Host disk pressure and inherited query limits must not evict the blocks being measured.
    // Keep a real full disk visible: 100% still enters resource limit mode.
    suite.setBeConfigTemporary([
            "enable_evict_file_cache_in_advance": "false",
            "enable_file_cache_query_limit": "false"
    ]) {
        // Nest the thresholds so both application and restoration preserve enter >= exit.
        suite.setBeConfigTemporary(["file_cache_enter_disk_resource_limit_mode_percent": "100"]) {
            suite.setBeConfigTemporary(["file_cache_exit_disk_resource_limit_mode_percent": "100"]) {
                suite.awaitUntil(timeoutSeconds, 1) {
                    def modes = suite.sql """select CACHE_PATH, METRIC_NAME, METRIC_VALUE
                            from information_schema.file_cache_statistics
                            where METRIC_NAME in ('disk_resource_limit_mode', 'need_evict_cache_in_advance')
                            order by CACHE_PATH, METRIC_NAME"""
                    suite.logger.info("File cache test baseline modes: ${modes}")
                    return !modes.isEmpty() && modes.every { it[2].toString().toDouble() == 0.0 }
                }
                action.call()
            }
        }
    }
}

logger.info("Added 'withStableFileCache' function to Suite")
