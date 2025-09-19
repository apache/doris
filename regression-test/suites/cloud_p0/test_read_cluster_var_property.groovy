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

suite('test_read_cluster_var_property') {
    if (!isCloudMode()) {
        return
    }
    String userName = "test_read_cluster_var_property_user"
    String pwd = '123456'
    sql """drop user if exists ${userName}"""
    sql """CREATE USER '${userName}' IDENTIFIED BY '${pwd}'"""
    sql """GRANT ADMIN_PRIV ON *.*.* TO ${userName}"""

    def getBrpcMetrics = {ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
        def metrics = new URL(url).text
        def matcher = metrics =~ ~"${name}\\s+(\\d+)"
        if (matcher.find()) {
            def ret = matcher[0][1] as long
            logger.info("getBrpcMetrics, ${url}, name:${name}, value:${ret}")
            return ret
        } else {
            throw new RuntimeException("${name} not found for ${ip}:${port}")
        }
    }

    connect(userName, "${pwd}", context.config.jdbcUrl) {
        // test non-mow table
        try {
            def tableName = "test_read_cluster_var_property"
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE ${tableName}
                    (k int, v1 int, v2 int )
                    DUPLICATE KEY(k)
                    DISTRIBUTED BY HASH (k) 
                    BUCKETS 1  PROPERTIES(
                        "replication_num" = "1",
                        "disable_auto_compaction" = "true");
                """

            (1..20).each{ id -> 
                sql """insert into ${tableName} select number, number, number from numbers("number"="10");"""
            }

            sql "select * from ${tableName};"

            def backends = sql_return_maparray('show backends')
            def tabletStats = sql_return_maparray("show tablets from ${tableName};")
            assert tabletStats.size() == 1
            def tabletId = tabletStats[0].TabletId
            def tabletBackendId = tabletStats[0].BackendId
            def tabletBackend
            for (def be : backends) {
                if (be.BackendId == tabletBackendId) {
                    tabletBackend = be
                    break;
                }
            }
            logger.info("tablet ${tabletId} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");

            try {
                // 1. test enable_prefer_cached_rowset
                sql "set enable_prefer_cached_rowset=true;"
                def preferCachedRowsetCount = getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_prefer_cache_count")
                sql "select * from ${tableName};"
                assert getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_prefer_cache_count") == preferCachedRowsetCount + 1

                sql "set enable_prefer_cached_rowset=false;"
                preferCachedRowsetCount = getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_prefer_cache_count")
                sql "select * from ${tableName};"
                assert getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_prefer_cache_count") == preferCachedRowsetCount

                // user property has higher prioroty than session variable
                sql "set property for '${userName}' enable_prefer_cached_rowset=true;"
                preferCachedRowsetCount = getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_prefer_cache_count")
                sql "select * from ${tableName};"
                assert getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_prefer_cache_count") == 1 + preferCachedRowsetCount
            } finally {
                sql "set enable_prefer_cached_rowset=false;"
                sql "set property for '${userName}' enable_prefer_cached_rowset=false;"
            }

            try {
                // 2. test query_freshness_tolerance_ms
                sql "set query_freshness_tolerance_ms=1000;"
                def queryFreshnessTolerance = getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_with_freshness_tolerance_count")
                sql "select * from ${tableName};"
                assert getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_with_freshness_tolerance_count") == queryFreshnessTolerance + 1

                sql "set query_freshness_tolerance_ms=-1;"
                queryFreshnessTolerance = getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_with_freshness_tolerance_count")
                sql "select * from ${tableName};"
                assert getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_with_freshness_tolerance_count") == queryFreshnessTolerance

                // user property has higher prioroty than session variable
                sql "set property for '${userName}' query_freshness_tolerance_ms=2000;"
                queryFreshnessTolerance = getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_with_freshness_tolerance_count")
                sql "select * from ${tableName};"
                assert getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_with_freshness_tolerance_count") == 1 + queryFreshnessTolerance
            } finally {
                sql "set query_freshness_tolerance_ms=-1;"
                sql "set property for '${userName}' query_freshness_tolerance_ms=-1;"
            }
        } catch (Exception e) {
            logger.error("Error occurred while testing query_freshness_tolerance_ms: ${e.message}")
        } finally {
            sql "set enable_prefer_cached_rowset=false;"
            sql "set query_freshness_tolerance_ms=-1;"
            sql "set property for '${userName}' enable_prefer_cached_rowset=false;"
            sql "set property for '${userName}' query_freshness_tolerance_ms=-1;"
        }

        // test mow table
        try {
            def tableName = "test_read_cluster_var_property_mow"
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE ${tableName}
                    (k int, v1 int, v2 int )
                    UNIQUE KEY(k) DISTRIBUTED BY HASH (k) 
                    BUCKETS 1  PROPERTIES(
                        "replication_num" = "1",
                        "enable_unique_key_merge_on_write" = "true",
                        "disable_auto_compaction" = "true");
                """

            (1..20).each{ id -> 
                sql """insert into ${tableName} select number, number, number from numbers("number"="10");"""
            }

            sql "select * from ${tableName};"

            def backends = sql_return_maparray('show backends')
            def tabletStats = sql_return_maparray("show tablets from ${tableName};")
            assert tabletStats.size() == 1
            def tabletId = tabletStats[0].TabletId
            def tabletBackendId = tabletStats[0].BackendId
            def tabletBackend
            for (def be : backends) {
                if (be.BackendId == tabletBackendId) {
                    tabletBackend = be
                    break;
                }
            }
            logger.info("tablet ${tabletId} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");

            try {
                // 1. test enable_prefer_cached_rowset
                // enable_prefer_cached_rowset should not take effect on mow table
                sql "set enable_prefer_cached_rowset=true;"
                def preferCachedRowsetCount = getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_prefer_cache_count")
                sql "select * from ${tableName};"
                assert getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_prefer_cache_count") == preferCachedRowsetCount

                sql "set enable_prefer_cached_rowset=false;"
                preferCachedRowsetCount = getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_prefer_cache_count")
                sql "select * from ${tableName};"
                assert getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_prefer_cache_count") == preferCachedRowsetCount

                // user property has higher prioroty than session variable
                sql "set property for '${userName}' enable_prefer_cached_rowset=true;"
                preferCachedRowsetCount = getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_prefer_cache_count")
                sql "select * from ${tableName};"
                assert getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_prefer_cache_count") == preferCachedRowsetCount
            } finally {
                sql "set enable_prefer_cached_rowset=false;"
                sql "set property for '${userName}' enable_prefer_cached_rowset=false;"
            }

            try {
                // 2. test query_freshness_tolerance_ms
                sql "set query_freshness_tolerance_ms=1000;"
                def queryFreshnessTolerance = getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_with_freshness_tolerance_count")
                sql "select * from ${tableName};"
                assert getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_with_freshness_tolerance_count") == queryFreshnessTolerance + 1

                sql "set query_freshness_tolerance_ms=-1;"
                queryFreshnessTolerance = getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_with_freshness_tolerance_count")
                sql "select * from ${tableName};"
                assert getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_with_freshness_tolerance_count") == queryFreshnessTolerance

                // user property has higher prioroty than session variable
                sql "set property for '${userName}' query_freshness_tolerance_ms=2000;"
                queryFreshnessTolerance = getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_with_freshness_tolerance_count")
                sql "select * from ${tableName};"
                assert getBrpcMetrics(tabletBackend.Host, tabletBackend.BrpcPort, "capture_with_freshness_tolerance_count") == 1 + queryFreshnessTolerance
            } finally {
                sql "set query_freshness_tolerance_ms=-1;"
                sql "set property for '${userName}' query_freshness_tolerance_ms=-1;"
            }
        } catch (Exception e) {
            logger.error("Error occurred while testing query_freshness_tolerance_ms: ${e.message}")
            throw e
        } finally {
            sql "set enable_prefer_cached_rowset=false;"
            sql "set query_freshness_tolerance_ms=-1;"
            sql "set property for '${userName}' enable_prefer_cached_rowset=false;"
            sql "set property for '${userName}' query_freshness_tolerance_ms=-1;"
        }
    }
}