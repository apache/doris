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

// WHY: a partitioned hudi table must be able to use SqlCache. Two independent gates control external
// SqlCache eligibility, so this test satisfies both:
//   1) It is opt-in via `enable_hive_sql_cache` (default false) - set below. Without it BindRelation marks
//      every external lakehouse table unsupported and nothing is ever cached.
//   2) The table must report a real data-version token. An UNPARTITIONED hudi table lists no partitions,
//      so its token is 0 and it is never cacheable (the version<=0 fail-safe in SqlCacheContext) - hence a
//      PARTITIONED table here.
// The bug this exercises: eligibility is gated on (now_millis - table_newest_update_millis) >= quiet window,
// with now first clamped to at least table_newest_update (a guard against FE/metadata clock skew). The hudi
// connector reported its newest-update time as the raw hudi instant (yyyyMMddHHmmssSSS read as a number,
// ~2.0e16) instead of epoch millis (~1.7e12), so the clamp dragged "now" up to the instant and the difference
// was 0 FOREVER - not "0 until the window passes". A partitioned hudi table, and any query joining one, could
// therefore never be cached. The fix converts the instant to genuine wall-clock millis in the connector; a
// write still invalidates through the version token, so no stale results.
//
// The regression env's hudi data is static and long past the quiet window, so this suite is red before the
// fix and green after it, with no timing dependence.
suite("test_hudi_sqlcache", "p2,external") {
    String enabled = context.config.otherConfigs.get("enableHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }

    String catalog_name = "test_hudi_sqlcache"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hudiHmsPort = context.config.otherConfigs.get("hudiHmsPort")
    String hudiMinioPort = context.config.otherConfigs.get("hudiMinioPort")
    String hudiMinioAccessKey = context.config.otherConfigs.get("hudiMinioAccessKey")
    String hudiMinioSecretKey = context.config.otherConfigs.get("hudiMinioSecretKey")

    def assertHasCache = { String sqlStr ->
        explain {
            sql ("physical plan ${sqlStr}")
            contains("PhysicalSqlCache")
        }
    }

    // Both partition-listing paths are exercised: hive-sync reads the names from HMS and pins the instant
    // separately from the hudi-metadata path, so the unit conversion has two distinct call sites.
    for (String use_hive_sync_partition : ['true', 'false']) {
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hudiHmsPort}',
                's3.endpoint' = 'http://${externalEnvIp}:${hudiMinioPort}',
                's3.access_key' = '${hudiMinioAccessKey}',
                's3.secret_key' = '${hudiMinioSecretKey}',
                's3.region' = 'us-east-1',
                'use_path_style' = 'true',
                'use_hive_sync_partition' = '${use_hive_sync_partition}'
            );
        """

        sql """ switch ${catalog_name};"""
        sql """ use regression_hudi;"""
        sql """ set enable_fallback_to_original_planner=false """
        sql """ set enable_sql_cache=true """
        sql """ set enable_hive_sql_cache=true """

        // Populate the cache, then assert the plan is served from it.
        sql """ select count(*) from one_partition_tb """
        assertHasCache """ select count(*) from one_partition_tb """
    }

    sql """drop catalog if exists ${catalog_name};"""
}
