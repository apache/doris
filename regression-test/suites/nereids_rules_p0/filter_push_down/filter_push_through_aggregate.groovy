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

suite("filter_push_through_aggregate") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET disable_join_reorder=true"
  
    sql """
        DROP TABLE IF EXISTS filter_push_through_aggregate
       """
    sql """
    CREATE TABLE IF NOT EXISTS filter_push_through_aggregate(
      `id` int(11) NULL,
      `msg` text NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql "set disable_nereids_rules='REWRITE_FILTER_EXPRESSION'"

    // 'a' = 'b' should not be push down aggregate.
    qt_do_not_push_no_slot_filter """
        select * from (select 'a' a, sum(1) from filter_push_through_aggregate) t where a = 'b';
    """
}

