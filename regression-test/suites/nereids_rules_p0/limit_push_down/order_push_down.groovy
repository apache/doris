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

suite("order_push_down") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "use regression_test_nereids_rules_p0"
    sql """ SET inline_cte_referenced_threshold=0 """
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET disable_join_reorder=true"
    sql 'set be_number_for_test=3'
    
    //`limit 1 offset 1 + sort, project`:
    qt_limit_offset_sort_project """ explain shape plan SELECT t1.id FROM t1 ORDER BY id LIMIT 1 OFFSET 1; """
    //`limit 1 + sort, join`:
    qt_limit_sort_join """ explain shape plan SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 1; """
    //`limit 1 + sort, semi join`:
    qt_limit_sort_semi_join """ explain shape plan SELECT t1.id FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 1; """
    // Right Semi Join with Order By
    qt_right_semi_join_order """ explain shape plan SELECT t2.id FROM t1 RIGHT SEMI JOIN t2 ON t1.id = t2.id ORDER BY t2.id LIMIT 1; """
    // Left Anti Join with Order By
    qt_left_anti_join_order """ explain shape plan SELECT t1.id FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 1; """
    // Right Anti Join with Order By
    qt_right_anti_join_order """ explain shape plan SELECT t2.id FROM t1 RIGHT ANTI JOIN t2 ON t1.id = t2.id ORDER BY t2.id LIMIT 1; """
    // Full Outer Join with Order By
    qt_full_outer_join_order """ explain shape plan SELECT t1.id FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 1; """
    // Left Outer Join with Order By
    qt_left_outer_join_order """ explain shape plan SELECT t1.id FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 1; """
    // Right Outer Join with Order By
    qt_right_outer_join_order """ explain shape plan SELECT t2.id FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id ORDER BY t2.id LIMIT 1; """
    // Cross Join with Order By
    qt_cross_join_order """ explain shape plan SELECT t1.id FROM t1 CROSS JOIN t2 ORDER BY t1.id LIMIT 1; """
    qt_limit_offset_sort_join """ explain shape plan SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.id LIMIT 1 OFFSET 1; """
    // `limit 1 + sort, agg & scalar agg and having`:
    qt_limit_sort_agg_having """ explain shape plan SELECT distinct id c FROM t1 ORDER BY c LIMIT 1; """
    //`limit 1 offset 1, agg & scalar agg and having`:
    qt_limit_offset_agg_having """ explain shape plan SELECT distinct id c FROM t1 ORDER BY c LIMIT 1 OFFSET 1; """
    //`limit 1 offset 1 + sort, agg & scalar agg and having`:
    qt_limit_offset_sort_agg_having """ explain shape plan SELECT distinct id c FROM t1 ORDER BY c LIMIT 1 OFFSET 1; """
    //`limit 1, agg & scalar agg join:
    qt_limit_distinct """ explain shape plan SELECT distinct t1.msg c FROM t1 inner join t2 on true ORDER BY c LIMIT 1; """
    //`limit 1, agg & scalar agg join:
    qt_limit_distinct """ explain shape plan SELECT distinct t1.msg c FROM t1 cross join t2 ORDER BY c LIMIT 1; """
    //`limit 1, agg & scalar agg left outer join:
    qt_limit_distinct """ explain shape plan SELECT distinct t1.msg c FROM t1 left outer join t2 on t1.id = t2.id ORDER BY c LIMIT 1; """
    //`limit 1, agg & scalar agg right outer join:
    qt_limit_distinct """ explain shape plan SELECT distinct t1.msg c FROM t1 left outer join t2 on t1.id = t2.id ORDER BY c LIMIT 1; """
    //`limit 1, window`:
    qt_limit_window """ explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t1 LIMIT 1; """
    //`limit 1 + sort, window`:
    qt_limit_sort_window """ explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t1 ORDER BY id LIMIT 1; """
    //`limit 1 offset 1, window`:
    qt_limit_offset_window """ explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t1 LIMIT 1 OFFSET 1; """
    // `limit 1 offset 1 + sort, window`:
    qt_limit_offset_sort_window """ explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t1 ORDER BY id LIMIT 1 OFFSET 1; """
    //`limit 1 + sort, filter`:
    qt_limit_sort_filter """ explain shape plan SELECT t1.id FROM t1 WHERE id = 1 ORDER BY id LIMIT 1; """
    //`limit 1 offset 1 + sort, filter`:
    qt_limit_offset_sort_filter """ explain shape plan SELECT t1.id FROM t1 WHERE id = 1 ORDER BY id LIMIT 1; """
    // `limit 1, subquery with order by`:
    qt_limit_subquery_order_by_inside_limit_outside """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 order by id) AS subq LIMIT 1;"""
    // `limit 1, subquery with order by`:
    qt_limit_subquery_all_inside """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 order by id LIMIT 1) AS subq ;"""
    // `LIMIT` with Set Operation and `ORDER BY`:
    qt_limit_set_operation """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 UNION SELECT t2.id FROM t2) u ORDER BY id LIMIT 1;"""
    // `LIMIT` with Set Operation and `ORDER BY`:
    qt_limit_outside_order_inside_set_operation """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 UNION SELECT t2.id FROM t2 ORDER BY id) u  LIMIT 1;"""
    // `LIMIT` with Set Operation and `ORDER BY`:
    qt_limit_inside_set_operation """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 UNION SELECT t2.id FROM t2 ORDER BY id LIMIT 1) u;"""

    // `LIMIT` with Set Operation and `OFFSET` with `ORDER BY`:
    qt_limit_offset_set_operation """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 INTERSECT SELECT t2.id FROM t2) u ORDER BY id LIMIT 1 OFFSET 1;"""

    // `LIMIT` with Window function and `ORDER BY`:
    qt_limit_window """explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t1 ORDER BY id LIMIT 1;"""

    // `LIMIT` with Window function and `OFFSET` with `ORDER BY`:
    qt_limit_offset_window """explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t1 ORDER BY id LIMIT 1 OFFSET 1;"""

    // `LIMIT` with Filter and `ORDER BY`:
    qt_limit_filter """explain shape plan SELECT t1.id FROM t1 WHERE id = 1 ORDER BY id LIMIT 1;"""

    // `LIMIT` with Filter and `OFFSET` with `ORDER BY`:
    qt_limit_offset_filter """explain shape plan SELECT t1.id FROM t1 WHERE id = 1 ORDER BY id LIMIT 1 OFFSET 1;"""

    // `LIMIT` with Projection, Filter, and `ORDER BY`:
    qt_limit_project_filter """explain shape plan SELECT t1.id AS c FROM t1 WHERE t1.id > 100 ORDER BY t1.id LIMIT 1;"""

    // `LIMIT` with Join, Filter, and `ORDER BY`:
    qt_limit_join_filter """explain shape plan SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id > 100 ORDER BY t1.id LIMIT 1;"""

    // `LIMIT` with Subquery and `ORDER BY`:
    qt_limit_subquery """explain shape plan SELECT * FROM (SELECT t1.id FROM t1) AS subq ORDER BY id LIMIT 1;"""

    // `LIMIT` with Subquery, Filter, and `ORDER BY`:
    qt_limit_subquery_filter """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 WHERE t1.id > 100) AS subq ORDER BY id LIMIT 1;"""

    // `LIMIT` with Subquery, Join, and `ORDER BY`:
    qt_limit_subquery_join """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id) AS subq ORDER BY id LIMIT 1;"""

    // `LIMIT` with Subquery, Window function, and `ORDER BY`:
    qt_limit_subquery_window """explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM (SELECT t1.id, t1.msg FROM t1) AS subq ORDER BY id LIMIT 1;"""

    // `LIMIT` with Nested Subquery and `ORDER BY`:
    qt_limit_nested_subquery """explain shape plan SELECT * FROM (SELECT * FROM (SELECT t1.id FROM t1) AS subq1) AS subq2 ORDER BY id LIMIT 1;"""

    // `LIMIT` with Union, Filter, and `ORDER BY`:
    qt_limit_union_filter """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 WHERE t1.id > 100 UNION SELECT t2.id FROM t2 WHERE t2.id > 100) u ORDER BY id LIMIT 1;"""

    // `LIMIT` with Union, Join, and `ORDER BY`:
    qt_limit_union_join """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id UNION SELECT t3.id FROM t3 LEFT OUTER JOIN t4 ON t3.id = t4.id) u ORDER BY id LIMIT 1;"""

    // `LIMIT` with Union, Window function, and `ORDER BY`:
    qt_limit_union_window """explain shape plan SELECT * FROM (SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t1 UNION SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t2) u ORDER BY id LIMIT 1;"""

    // `LIMIT` with Subquery, Join, Filter, and `ORDER BY`:
    qt_limit_subquery_join_filter """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id > 100) AS subq ORDER BY id LIMIT 1;"""

    // `LIMIT` with Subquery, Join, Window function, and `ORDER BY`:
    qt_limit_subqueryjoin_window """explain shape plan SELECT id, msg, ROW_NUMBER() OVER (PARTITION BY subq.id ORDER BY subq.id) AS row_num FROM (SELECT t1.id, t1.msg FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id) AS subq ORDER BY id LIMIT 1;"""

    // `LIMIT` with Subquery, Union, Filter, and `ORDER BY`:
    qt_limit_subquery_union_filter """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 WHERE t1.id > 100 UNION SELECT t2.id FROM t2 WHERE t2.id > 100) AS subq ORDER BY id LIMIT 1;"""

    // `LIMIT` with Subquery, Union, Join, and `ORDER BY`:
    qt_limit_subquery_union_join """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id UNION SELECT t3.id FROM t3 JOIN t4 ON t3.id = t4.id) AS subq ORDER BY id LIMIT 1;"""

    // `LIMIT` with Subquery, Union, Window function, and `ORDER BY`:
    qt_limit_subquery_union_window """explain shape plan SELECT id, msg, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS row_num FROM (SELECT id, msg FROM t1 UNION SELECT id, msg FROM t2) AS subq ORDER BY id LIMIT 1;"""

    // `LIMIT` with Correlated Subquery and `ORDER BY`:
    qt_limit_correlated_subquery """explain shape plan SELECT t1.id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id ORDER BY id LIMIT 1);"""

    // `LIMIT` with Correlated Subquery, Join, and `ORDER BY`:
    qt_limit_correlated_subquery_join """explain shape plan SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE EXISTS (SELECT 1 FROM t3 WHERE t3.id = t1.id ORDER BY id LIMIT 1);"""

    // `limit 1, correlated subquery, window`:
    qt_limit_correlated_subquery_window """explain shape plan SELECT id, msg, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS row_num FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id LIMIT 1) ORDER BY id;"""

    // `limit 1, cte query`:
    qt_limit_cte_query """explain shape plan WITH cte AS (SELECT id FROM t1 WHERE id < 10) SELECT id FROM cte ORDER BY id LIMIT 1;"""

    // `limit 1, cte query`:
    qt_limit_cte_outside_query """explain shape plan WITH cte AS (SELECT id FROM t1 WHERE id < 10 ORDER BY id) SELECT id FROM cte LIMIT 1;"""

    // `limit 1, cte query`:
    qt_limit_cte_outside_query """explain shape plan WITH cte AS (SELECT id FROM t1 WHERE id < 10 ORDER BY id LIMIT 1) SELECT id FROM cte;"""

    // `limit 1, cte query, join`:
    qt_limit_cte_query_join """explain shape plan WITH cte1 AS (SELECT id FROM t1 WHERE id < 10), cte2 AS (SELECT id FROM t2 WHERE id < 10) SELECT cte1.id FROM cte1 full outer JOIN cte2 ON cte1.id = cte2.id ORDER BY cte1.id LIMIT 1;"""

    // `limit 1, cte query, window`:
    qt_limit_cte_query_window """explain shape plan WITH cte AS (SELECT id FROM t1 WHERE id < 10) SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM cte ORDER BY id LIMIT 1;"""

    // `limit 1, project, filter`:
    qt_limit_project_filter """explain shape plan SELECT t1.id AS c FROM t1 WHERE t1.id > 100 ORDER BY t1.id LIMIT 1;"""

    // `limit 1, join, filter`:
    qt_limit_join_filter """explain shape plan SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id > 100 ORDER BY t1.id LIMIT 1;"""

    // `limit 1, subquery, join`:
    qt_limit_subquery_join """explain shape plan SELECT * FROM (SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id) AS subq ORDER BY subq.id LIMIT 1;"""

    // `limit 1, subquery, window`:
    qt_limit_subquery_window """explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM (SELECT t1.id as id, t1.msg FROM t1) AS subq ORDER BY id LIMIT 1;"""

    // `limit 1, nested subquery`:
    qt_limit_nested_subquery """explain shape plan SELECT * FROM (SELECT * FROM (SELECT t1.id FROM t1) AS subq1) AS subq2 LIMIT 1;"""

    // `limit 1, subquery, distinct`:
    qt_limit_subquery_distinct """explain shape plan SELECT DISTINCT subq.id FROM (SELECT id FROM t1) AS subq ORDER BY subq.id LIMIT 1;"""

    // `limit 1, cross join`:
    qt_limit_cross_join """explain shape plan SELECT t1.id FROM t1 INNER JOIN t2 on true ORDER BY t1.id LIMIT 1;"""

    // `limit 1, multiple left outer join`:
    qt_limit_multiple_left_outer_join """explain shape plan SELECT t1.id FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id LEFT OUTER JOIN t3 ON t1.id = t3.id ORDER BY t1.id LIMIT 1;"""

    // `limit 1, multiple right outer join`:
    qt_limit_multiple_right_outer_join """explain shape plan SELECT t1.id FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id RIGHT OUTER JOIN t3 ON t1.id = t3.id ORDER BY t1.id LIMIT 1;"""

    // `limit 1, multiple full outer join`:
    qt_limit_multiple_full_outerjoin """explain shape plan SELECT t1.id FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id FULL OUTER JOIN t3 ON t1.id = t3.id ORDER BY t1.id LIMIT 1;"""

    // `limit 1, subquery, cross join`:
    qt_limit_subquery_cross_join """explain shape plan SELECT * FROM (SELECT t1.id FROM t1) AS subq CROSS JOIN t2 ORDER BY subq.id LIMIT 1;"""

    // `limit 1, subquery, multiple join`:
    qt_limit_subquery_multiple_join """explain shape plan SELECT * FROM (SELECT t1.id FROM t1) AS subq JOIN t2 ON subq.id = t2.id JOIN t3 ON subq.id = t3.id ORDER BY subq.id LIMIT 1;"""

    // `limit 1, subquery, multiple join, nested subquery`:
    qt_limit_subquery_multiple_join_nested_subquery """explain shape plan SELECT * FROM (SELECT * FROM (SELECT t1.id FROM t1) AS subq1) AS subq2 JOIN t2 ON subq2.id = t2.id JOIN t3 ON subq2.id = t3.id ORDER BY subq2.id LIMIT 1;"""

    // `limit 1, subquery, multiple join, nested subquery, distinct`:
    qt_limit_subquery_multiple_join_nested_subquery_distinct """explain shape plan SELECT DISTINCT subq2.id FROM (SELECT * FROM (SELECT t1.id FROM t1) AS subq1) AS subq2 JOIN t2 ON subq2.id = t2.id JOIN t3 ON subq2.id = t3.id ORDER BY subq2.id LIMIT 1;"""

    // `limit 1, subquery, multiple join, nested subquery, distinct, filter`:
    qt_limit_subquery_multiple_join_nested_subquery_distinct_filter """explain shape plan SELECT DISTINCT subq2.id FROM (SELECT * FROM (SELECT t1.id FROM t1 WHERE t1.id > 100) AS subq1) AS subq2 JOIN t2 ON subq2.id = t2.id JOIN t3 ON subq2.id = t3.id ORDER BY subq2.id LIMIT 1;"""
}