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

suite("limit_push_down") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "use regression_test_nereids_rules_p0"
    sql """ SET inline_cte_referenced_threshold=0 """
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET disable_join_reorder=true"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"


    sql 'set be_number_for_test=3'
    //`limit 1, project`:
    qt_limit_project """ explain shape plan SELECT t1.id as c FROM t1 LIMIT 1; """
    //`limit 1 offset 1, project`:
    qt_limit_offset_project """ explain shape plan SELECT t1.id as c FROM t1 LIMIT 1 OFFSET 1; """
    //`limit 1, join`:
    qt_limit_join """ explain shape plan SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id LIMIT 1; """
    //`limit 1, semi join`:
    qt_limit_semi_join """ explain shape plan SELECT t1.id FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id LIMIT 1; """
   // Right Semi Join
    qt_right_semi_join """ explain shape plan SELECT t2.id FROM t1 RIGHT SEMI JOIN t2 ON t1.id = t2.id LIMIT 1; """
    // Left Anti Join
    qt_left_anti_join """ explain shape plan SELECT t1.id FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id LIMIT 1; """
    // Right Anti Join
    qt_right_anti_join """ explain shape plan SELECT t2.id FROM t1 RIGHT ANTI JOIN t2 ON t1.id = t2.id LIMIT 1; """
    // Full Outer Join
    qt_full_outer_join """ explain shape plan SELECT t1.id FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id LIMIT 1; """
    // Left Outer Join
    qt_left_outer_join """ explain shape plan SELECT t1.id FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id LIMIT 1; """
    // Right Outer Join
    qt_right_outer_join """ explain shape plan SELECT t1.id FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id LIMIT 1; """
    // Cross Join
    qt_cross_join """ explain shape plan SELECT t1.id FROM t1 CROSS JOIN t2 LIMIT 1; """
    //`limit 1 offset 1, join`:
    qt_limit_offset_join """ explain shape plan SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id LIMIT 1 OFFSET 1; """
    //`limit 1, agg & scalar agg:
    qt_limit_distinct """ explain shape plan SELECT distinct t1.id FROM t1 LIMIT 1; """
    //`limit 1 offset 1, agg & scalar agg`:
    qt_limit_offset_agg """ explain shape plan SELECT distinct t1.id c FROM t1 ORDER BY c LIMIT 1 OFFSET 1; """
    //`limit 1, agg & scalar agg join:
    qt_limit_distinct """ explain shape plan SELECT distinct t1.id FROM t1 inner join t2 on true LIMIT 1; """
    //`limit 1, agg & scalar agg join:
    qt_limit_distinct """ explain shape plan SELECT distinct t1.id FROM t1 cross join t2 LIMIT 1; """
    //`limit 1, agg & scalar agg left outer join:
    qt_limit_distinct """ explain shape plan SELECT distinct t1.id FROM t1 left outer join t2 on t1.id = t2.id LIMIT 1; """
    //`limit 1, agg & scalar agg right outer join:
    qt_limit_distinct """ explain shape plan SELECT distinct t1.id FROM t1 left outer join t2 on t1.id = t2.id LIMIT 1; """
    
    //`limit 1 offset 1, agg & scalar agg`:
    qt_limit_offset_agg """ explain shape plan SELECT distinct t1.id c FROM t1 ORDER BY c LIMIT 1 OFFSET 1; """
    //`limit 1, Set Operation`:
    qt_limit_set_operation """ explain shape plan SELECT * FROM (SELECT t1.id FROM t1 UNION SELECT t2.id FROM t2) u LIMIT 1; """
    //`limit 1 offset 1, Set Operation`:
    qt_limit_offset_set_operation """ explain shape plan SELECT * FROM (SELECT t1.id FROM t1 INTERSECT SELECT t2.id FROM t2) u LIMIT 1 OFFSET 1; """
    //`limit 1, window`:
    qt_limit_window """ explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t1 LIMIT 1; """
    //`limit 1 offset 1, window`:
    qt_limit_offset_window """ explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t1 LIMIT 1 OFFSET 1; """
    //`limit 1, filter`:
    qt_limit_filter """ explain shape plan SELECT t1.id FROM t1 WHERE id = 1 LIMIT 1; """
    //`limit 1 offset 1, filter`:
    qt_limit_offset_filter """ explain shape plan SELECT t1.id FROM t1 WHERE id = 1 LIMIT 1 OFFSET 1; """
    // `limit 1, project, filter`:
    qt_limit_project_filter """explain shape plan SELECT t1.id AS c FROM t1 WHERE t1.id > 100 LIMIT 1;"""
    // `limit 1, join, filter`:
    qt_limit_join_filter """explain shape plan SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id > 100 LIMIT 1;"""
    // `limit 1, subquery`:
    qt_limit_subquery """explain shape plan SELECT * FROM (SELECT t1.id FROM t1) AS subq LIMIT 1;"""
    // `limit 1, subquery, filter`:
    qt_limit_subquery_filter """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 WHERE t1.id > 100) AS subq LIMIT 1;"""
    // `limit 1, subquery, join`:
    qt_limit_subquery_join """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id) AS subq LIMIT 1;"""
    // `limit 1, subquery, window`:
    qt_limit_subquery_window """explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM (SELECT t1.id, t1.msg FROM t1) AS subq LIMIT 1;"""
    // `limit 1, nested subquery`:
    qt_limit_nested_subquery """explain shape plan SELECT * FROM (SELECT * FROM (SELECT t1.id FROM t1) AS subq1) AS subq2 LIMIT 1;"""
    // `limit 1, union, filter`:
    qt_limit_union_filter """explain shape plan SELECT * FROM (SELECT t1.msg FROM t1 WHERE t1.msg > 100 UNION ALL SELECT t2.id FROM t2 WHERE t2.id > 100) u LIMIT 1;"""
    // `limit 1, union, join`:
    qt_limit_union_join """explain shape plan SELECT * FROM (SELECT t1.msg FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id UNION ALL SELECT t3.msg FROM t3 LEFT OUTER JOIN t4 ON t3.id = t4.id) u LIMIT 1;"""
    // `limit 1, union, window`:
    qt_limit_union_window """explain shape plan SELECT * FROM (SELECT msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t1 UNION ALL SELECT msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t2) u LIMIT 1;"""
    // `limit 1, subquery, join, filter`:
    qt_limit_subquery_join_filter """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id > 100) AS subq LIMIT 1;"""

    // `limit 1, subquery, join, window`:
    qt_limit_subquery_join_window """explain shape plan SELECT id, msg, ROW_NUMBER() OVER (PARTITION BY subq.id ORDER BY subq.id) AS row_num FROM (SELECT t1.id, t1.msg FROM t1 left outer JOIN t2 ON t1.id = t2.id) AS subq LIMIT 1;"""

    // `limit 1, subquery, union, filter`:
    qt_limit_subquery_union_filter """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 WHERE t1.id > 100 UNION ALL SELECT t2.id FROM t2 WHERE t2.id > 100) AS subq LIMIT 1;"""

    // `limit 1, subquery, union, join`:
    qt_limit_subquery_union_join """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id UNION SELECT t3.id FROM t3 JOIN t4 ON t3.id = t4.id) AS subq LIMIT 1;"""

    // `limit 1, subquery, union, window`:
    qt_limit_subquery_union_window """explain shape plan SELECT id, msg, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS row_num FROM (SELECT id, msg FROM t1 UNION SELECT id, msg FROM t2) AS subq LIMIT 1;"""

    // `limit 1, correlated subquery`:
    qt_limit_correlated_subquery """explain shape plan SELECT t1.id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id LIMIT 1);"""

    // `limit 1, correlated subquery, join`:
    qt_limit_correlated_subquery_join """explain shape plan SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE EXISTS (SELECT 1 FROM t3 WHERE t3.id = t1.id LIMIT 1);"""

    // `limit 1, correlated subquery, window`:
    qt_limit_correlated_subquery_window """explain shape plan SELECT id, msg, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS row_num FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id LIMIT 1);"""

    // `limit 1, cte query`:
    qt_limit_cte_query """explain shape plan WITH cte AS (SELECT id FROM t1 WHERE id < 10) SELECT id FROM cte LIMIT 1;"""

    // `limit 1, cte query, join`:
    qt_limit_cte_query_join """explain shape plan WITH cte1 AS (SELECT id FROM t1 WHERE id < 10), cte2 AS (SELECT id FROM t2 WHERE id < 10) SELECT cte1.id FROM cte1 full outer JOIN cte2 ON cte1.id = cte2.id LIMIT 1;"""

    // `limit 1, cte query, window`:
    qt_limit_cte_query_window """explain shape plan WITH cte AS (SELECT id FROM t1 WHERE id < 10) SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM cte LIMIT 1;"""

    // `limit 1, project, filter`:
    qt_limit_project_filter """explain shape plan SELECT t1.id AS c FROM t1 WHERE t1.id > 100 LIMIT 1;"""

    // `limit 1, join, filter`:
    qt_limit_join_filter """explain shape plan SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id > 100 LIMIT 1;"""

    // `limit 1, subquery`:
    qt_limit_subquery """explain shape plan SELECT * FROM (SELECT t1.id FROM t1) AS subq LIMIT 1;"""

    // `limit 1, subquery, filter`:
    qt_limit_subquery_filter """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 WHERE t1.id > 100) AS subq LIMIT 1;"""

    // `limit 1, subquery, join`:
    qt_limit_subquery_join """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id) AS subq LIMIT 1;"""

    // `limit 1, subquery, window`:
    qt_limit_subquery_window """explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM (SELECT t1.id as id, t1.msg FROM t1) AS subq LIMIT 1;"""

    // `limit 1, nested subquery`:
    qt_limit_nested_subquery """explain shape plan SELECT * FROM (SELECT * FROM (SELECT t1.id FROM t1) AS subq1) AS subq2 LIMIT 1;"""

    // `limit 1, subquery, order by`:
    qt_limit_subquery_order_by """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 ORDER BY t1.id DESC) AS subq LIMIT 1;"""

    // `limit 1, subquery, order by, offset`:
    qt_limit_subquery_order_by_offset """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 ORDER BY t1.id DESC) AS subq LIMIT 1 OFFSET 1;"""

    // `limit 1, subquery, distinct`:
    qt_limit_subquery_distinct """explain shape plan SELECT DISTINCT subq.id FROM (SELECT id FROM t1) AS subq LIMIT 1;"""

    // `limit 1, cross join`:
    qt_limit_cross_join """explain shape plan SELECT t1.id FROM t1 INNER JOIN t2 on true LIMIT 1;"""

    // `limit 1, multiple left outer join`:
    qt_limit_multiple_left_outer_join """explain shape plan SELECT t1.id FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id LEFT OUTER JOIN t3 ON t1.id = t3.id LIMIT 1;"""

    // `limit 1, multiple right outer join`:
    qt_limit_multiple_right_outer_join """explain shape plan SELECT t1.id FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id RIGHT OUTER JOIN t3 ON t1.id = t3.id LIMIT 1;"""

    // `limit 1, multiple full outer join`:
    qt_limit_multiple_full_outer_join """explain shape plan SELECT t1.id FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id FULL OUTER JOIN t3 ON t1.id = t3.id LIMIT 1;"""

    // `limit 1, multiple cross join`:
    qt_limit_multiple_cross_join """explain shape plan SELECT t1.id FROM t1 CROSS JOIN t2 CROSS JOIN t3 LIMIT 1;"""

    // `limit 1, left outer join, right outer join`:
    qt_limit_left_outer_join_right_outer_join """explain shape plan SELECT t1.id FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id RIGHT OUTER JOIN t3 ON t1.id = t3.id LIMIT 1;"""

    // `limit 1, left outer join, full outer join`:
    qt_limit_left_outer_join_full_outer_join """explain shape plan SELECT t1.id FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id FULL OUTER JOIN t3 ON t1.id = t3.id LIMIT 1;"""

    // `limit 1, left outer join, cross join`:
    qt_limit_left_outer_join_cross_join """explain shape plan SELECT t1.id FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id CROSS JOIN t3 LIMIT 1;"""

    // `limit 1, right outer join, full outer join`:
    qt_limit_right_outer_join_full_outer_join """explain shape plan SELECT t1.id FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id FULL OUTER JOIN t3 ON t1.id = t3.id LIMIT 1;"""

    // `limit 1, right outer join, cross join`:
    qt_limit_right_outer_join_cross_join """explain shape plan SELECT t1.id FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id CROSS JOIN t3 LIMIT 1;"""

    // `limit 1, full outer join, cross join`:
    qt_limit_full_outer_join_cross_join """explain shape plan SELECT t1.id FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id CROSS JOIN t3 LIMIT 1;"""

    // `limit 1, left outer join, right outer join, full outer join`:
    qt_limit_left_outer_join_right_outer_join_full_outer_join """explain shape plan SELECT t1.id FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id RIGHT OUTER JOIN t3 ON t1.id = t3.id FULL OUTER JOIN t4 ON t1.id = t4.id LIMIT 1;"""

    // `limit 1, left outer join, right outer join, cross join`:
    qt_limit_left_outer_join_right_outer_join_cross_join """explain shape plan SELECT t1.id FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id RIGHT OUTER JOIN t3 ON t1.id = t3.id CROSS JOIN t4 LIMIT 1;"""

    // `limit 1, left outer join, full outer join, cross join`:
    qt_limit_left_outer_join_full_outer_join_cross_join """explain shape plan SELECT t1.id FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id FULL OUTER JOIN t3 ON t1.id = t3.id CROSS JOIN t4 LIMIT 1;"""

    // `limit 1, right outer join, full outer join, cross join`:
    qt_limit_right_outer_join_full_outer_join_cross_join """explain shape plan SELECT t1.id FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id FULL OUTER JOIN t3 ON t1.id = t3.id CROSS JOIN t4 LIMIT 1;"""

    // `limit 1, left outer join, right outer join, full outer join, cross join`:
    qt_limit_left_outer_join_right_outer_join_full_outer_join_cross_join """explain shape plan SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id RIGHT OUTER JOIN t3 ON t1.id = t3.id FULL OUTER JOIN t4 ON t1.id = t4.id inner JOIN t4 on TRUE LIMIT 1;"""
}