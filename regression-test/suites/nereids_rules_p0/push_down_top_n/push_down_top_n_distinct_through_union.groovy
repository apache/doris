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

suite("push_down_top_n_distinct_through_union") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET disable_join_reorder=true"

    sql """
        DROP TABLE IF EXISTS table2;
    """

    sql """
    CREATE TABLE IF NOT EXISTS table2(
      `id` int(32) NULL,
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    qt_push_down_topn_through_union """
        explain shape plan select * from (select * from table2 t1 union select * from table2 t2) t order by id limit 10;
    """

    qt_push_down_topn_union_with_conditions """
        explain shape plan select * from (select * from table2 t1 where t1.score > 10 union select * from table2 t2 where t2.name = 'Test' union select * from table2 t3 where t3.id < 5) sub order by id limit 10;
    """

    qt_push_down_topn_union_with_order_by """
        explain shape plan select * from (select * from table2 t1 union select * from table2 t2 union select * from table2 t3 order by score) sub order by id limit 10;
    """

    qt_push_down_topn_nested_union """
        explain shape plan select * from ((select * from table2 t1 union select * from table2 t2) union (select * from table2 t3 union select * from table2 t4)) sub order by id limit 10;
    """

    qt_push_down_topn_union_after_join """
        explain shape plan select * from (select t1.id from table2 t1 join table2 t2 on t1.id = t2.id union select id from table2 t3) sub order by id limit 10;
    """

    qt_push_down_topn_union_different_projections """
        explain shape plan select * from (select id from table2 t1 union select name from table2 t2) sub order by id limit 10;
    """

    qt_push_down_topn_union_with_subquery """
        explain shape plan select * from (select id from (select * from table2 where score > 20) t1 union select id from table2 t2) sub order by id limit 10;
    """

    qt_push_down_topn_union_with_limit """
        explain shape plan select * from (select * from table2 t1 limit 5 union select * from table2 t2 limit 5) sub order by id limit 10;
    """

    qt_push_down_topn_union_complex_conditions """
        explain shape plan select * from (select * from table2 t1 where t1.score > 10 and t1.name = 'Test' union select * from table2 t2 where t2.id < 5 and t2.score < 20) sub order by id limit 10;
    """
}