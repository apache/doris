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
suite("test_cte_name_reuse") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_pipeline_engine=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """
        drop table if exists test_cte_name_reuse;
    """

    sql """
        CREATE TABLE `test_cte_name_reuse` (
          `id` int(11) NULL,
          `msg` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        );
    """

    sql """
        insert into test_cte_name_reuse values(1, "1");
    """

    qt_reuse_name_with_other_outer_cte """
        with a as (select * from test_cte_name_reuse), b as (with a as (select * from test_cte_name_reuse) select * from a) select * from a, b;
    """

    qt_reuse_name_with_other_outer_cte_and_use_outer_same_name_cte """
        with a as (select * from test_cte_name_reuse), b as (with a as (select * from a) select * from a) select * from b;
    """

    qt_reuse_name_with_self_outer_cte """
        with a as (with a as (select * from test_cte_name_reuse) select * from a) select * from a;
    """
}
