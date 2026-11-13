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

suite("test_remote_doris_meta_cache_weight", "p0,external") {
    String remote_doris_host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    String remote_doris_user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String remote_doris_psw = context.config.otherConfigs.get("extArrowFlightSqlPassword")

    def showres = sql "show frontends"
    String remote_doris_arrow_port = showres[0][6]
    String remote_doris_http_port = showres[0][3]
    String remote_doris_thrift_port = showres[0][5]

    sql """DROP DATABASE IF EXISTS test_remote_doris_meta_cache_weight_db"""
    sql """CREATE DATABASE IF NOT EXISTS test_remote_doris_meta_cache_weight_db"""
    sql """
        CREATE TABLE `test_remote_doris_meta_cache_weight_db`.`test_remote_doris_meta_cache_weight_t` (
            `id` INT,
            `name` STRING
        ) DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    // One catalog bounds its metadata caches by memory weight, the other one has no bound.
    sql """DROP CATALOG IF EXISTS `test_remote_doris_meta_cache_weighted`"""
    sql """
       CREATE CATALOG `test_remote_doris_meta_cache_weighted` PROPERTIES (
                'type' = 'doris',
                'fe_thrift_hosts' = '${remote_doris_host}:${remote_doris_thrift_port}',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}',
                'use_arrow_flight' = 'true',
                'meta.cache.max-weight' = '128MB'
        );
    """
    sql """DROP CATALOG IF EXISTS `test_remote_doris_meta_cache_unbounded`"""
    sql """
       CREATE CATALOG `test_remote_doris_meta_cache_unbounded` PROPERTIES (
                'type' = 'doris',
                'fe_thrift_hosts' = '${remote_doris_host}:${remote_doris_thrift_port}',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}',
                'use_arrow_flight' = 'true'
        );
    """

    // DESC loads the remote table's schema into the schema cache of each catalog.
    sql """DESC test_remote_doris_meta_cache_weighted.test_remote_doris_meta_cache_weight_db.test_remote_doris_meta_cache_weight_t"""
    sql """DESC test_remote_doris_meta_cache_unbounded.test_remote_doris_meta_cache_weight_db.test_remote_doris_meta_cache_weight_t"""

    // With only a catalog limit, each entry is bounded by it, and nothing was rejected.
    order_qt_weighted_entries """
        select entry_name, weight_bounded, max_weight, catalog_max_weight, global_max_weight, weight_reject_count
        from internal.information_schema.catalog_meta_cache_statistics
        where catalog_name = 'test_remote_doris_meta_cache_weighted' and engine_name = 'doris'
    """
    // A catalog without any memory bound reports -1 for every weight column.
    order_qt_unbounded_entries """
        select entry_name, weight_bounded, max_weight, estimated_weight, eviction_weight, weight_reject_count,
               catalog_max_weight, catalog_estimated_weight, global_max_weight, global_estimated_weight
        from internal.information_schema.catalog_meta_cache_statistics
        where catalog_name = 'test_remote_doris_meta_cache_unbounded' and engine_name = 'doris'
    """
    // The admitted schema is charged to its entry, its catalog and the FE-global total.
    def schemaWeights = sql """
        select estimated_weight, eviction_weight, catalog_estimated_weight, global_estimated_weight
        from internal.information_schema.catalog_meta_cache_statistics
        where catalog_name = 'test_remote_doris_meta_cache_weighted' and engine_name = 'doris'
          and entry_name = 'schema'
    """
    assertEquals(1, schemaWeights.size())
    long schemaWeight = schemaWeights[0][0] as long
    assertTrue(schemaWeight > 0L)
    assertEquals(0L, schemaWeights[0][1] as long)
    assertTrue((schemaWeights[0][2] as long) >= schemaWeight)
    assertTrue((schemaWeights[0][3] as long) >= (schemaWeights[0][2] as long))
}
