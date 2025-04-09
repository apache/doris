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

import org.apache.doris.regression.util.JdbcUtils

import java.util.stream.Collectors

suite("test_fallback_to_legacy_planner") {
     multi_sql """
        create database if not exists test_legacy_planner;
        use test_legacy_planner;
        set enable_nereids_planner=false;

        drop view if exists view_fallback_to_legacy_planner;
        drop table if exists test_fallback_to_legacy_planner;

        CREATE TABLE `test_fallback_to_legacy_planner`(
            `id` int NULL
        )
        ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
        
        insert into test_fallback_to_legacy_planner select number from numbers('number'='100');
        
        CREATE VIEW `view_fallback_to_legacy_planner` AS SELECT
        `default_cluster:test_legacy_planner.test_fallback_to_legacy_planner`.`id` AS `id`
        FROM `default_cluster:test_legacy_planner`.`test_fallback_to_legacy_planner`;
        """

    def assertResult = { String sqlStr, String expectLabel, int expectFragmentNum, List<List<Object>> expectResult ->
        def (explainResult, meta) = JdbcUtils.executeQueryToList(context.getConn(), "explain ${sqlStr}")
        def resultLabel = meta.getColumnLabel(1)
        log.info("explain result:\n${explainResult.get(0).get(0)}")

        def explainResultString = explainResult.stream()
            .map { it.get(0) }
            .collect(Collectors.joining("\n"))

        assertEquals(expectLabel, resultLabel)
        assertEquals(expectFragmentNum, explainResultString.findAll("PLAN FRAGMENT").size())

        test {
            sql "$sqlStr"
            result(expectResult)
        }
    }
    sql "set ignore_storage_data_distribution=true"
    sql "set enable_local_shuffle=true"
    sql "set enable_pipeline_x_engine=true"
    sql "set enable_pipeline_engine=true"

    sql "set enable_nereids_planner=false"
    assertResult(
            "select count(*) from view_fallback_to_legacy_planner",
            "Explain String(Old Planner)",
            2, // fragment num == 2
            [[100L]]
    )

    sql "set enable_nereids_planner=true"
    assertResult(
            "select count(*) from view_fallback_to_legacy_planner",
            "Explain String(Old Planner)",
            2, // fragment num == 2
            [[100L]]
    )

    sql "set enable_nereids_planner=true"
    assertResult(
            "select count(*) from test_fallback_to_legacy_planner",
            "Explain String(Nereids Planner)",
            2, // fragment num == 2
            [[100L]]
    )
}
