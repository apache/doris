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

suite("push_down_limit_distinct_through_union") {
    sql "set parallel_pipeline_task_num=2"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set runtime_filter_mode=OFF"
    sql "SET disable_join_reorder=true"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql """
        DROP TABLE IF EXISTS push_down_limit_distinct_union_t;
    """

    sql """
        CREATE TABLE IF NOT EXISTS push_down_limit_distinct_union_t (
          `id` int NULL,
          `score` int NULL
        ) ENGINE = OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        explain shape plan select distinct *
        from (
          select *
          from (
            select id, score, score, row_number() over (order by id desc)
            from push_down_limit_distinct_union_t
          ) u1
          union all
          select *
          from (
            select id, score, score, row_number() over (order by id desc)
            from push_down_limit_distinct_union_t
          ) u2
        ) u
        limit 10;
    """
}
