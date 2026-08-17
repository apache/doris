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

suite('push_down_filter_through_set_operation_with_unique_function') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    // UNION DISTINCT: rand() filter must NOT be pushed into branches,
    // otherwise membership / dedup semantics change.
    qt_union_distinct_keep_rand '''
        explain shape plan
        select id from ((select id from t1) union distinct (select id from t2)) u
        where rand() > 0.1
        '''

    // INTERSECT: rand() filter must NOT be pushed — intersect depends on
    // full branch membership before dedup.
    qt_intersect_keep_rand '''
        explain shape plan
        select id from ((select id from t1) intersect (select id from t2)) u
        where rand() > 0.1
        '''

    // EXCEPT: rand() filter must NOT be pushed.
    qt_except_keep_rand '''
        explain shape plan
        select id from ((select id from t1) except (select id from t2)) u
        where rand() > 0.1
        '''

    // UNION ALL: per-output-row == per-branch-input-row (1:1), push is safe.
    // Expect rand() filter to be pushed into each branch as before.
    qt_union_all_push_rand '''
        explain shape plan
        select id from ((select id from t1) union all (select id from t2)) u
        where rand() > 0.1
        '''

    // Mixed: non-unique predicate goes down, unique-fn stays up.
    qt_union_distinct_split '''
        explain shape plan
        select id from ((select id from t1) union distinct (select id from t2)) u
        where id = 1 and rand() > 0.1
        '''

    qt_intersect_split '''
        explain shape plan
        select id from ((select id from t1) intersect (select id from t2)) u
        where id = 1 and rand() > 0.1
        '''

    qt_except_split '''
        explain shape plan
        select id from ((select id from t1) except (select id from t2)) u
        where id = 1 and rand() > 0.1
        '''
}
