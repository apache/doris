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
    sql "SET enable_fallback_to_original_planner=false"
    sql "use regression_test_nereids_rules_p0"
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
    //`limit 1, agg & scalar agg and having`:
    qt_limit_agg_having """ explain shape plan SELECT COUNT(t1.id) FROM t1 LIMIT 1; """
    //`limit 1 offset 1, agg & scalar agg and having`:
    qt_limit_offset_agg_having """ explain shape plan SELECT  COUNT(distinct id) c FROM t1 ORDER BY c LIMIT 1 OFFSET 1; """
    //`limit 1, Set Operation`:
    qt_limit_set_operation """ explain shape plan SELECT t1.id FROM t1 UNION SELECT t2.id FROM t2 LIMIT 1; """
    //`limit 1 offset 1, Set Operation`:
    qt_limit_offset_set_operation """ explain shape plan SELECT t1.id FROM t1 INTERSECT SELECT t2.id FROM t2 LIMIT 1 OFFSET 1; """
    //`limit 1, window`:
    qt_limit_window """ explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t1 LIMIT 1; """
    //`limit 1 offset 1, window`:
    qt_limit_offset_window """ explain shape plan SELECT id, msg, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t1 LIMIT 1 OFFSET 1; """
    //`limit 1, filter`:
    qt_limit_filter """ explain shape plan SELECT t1.id FROM t1 WHERE id = 1 LIMIT 1; """
    //`limit 1 offset 1, filter`:
    qt_limit_offset_filter """ explain shape plan SELECT t1.id FROM t1 WHERE id = 1 LIMIT 1 OFFSET 1; """
}