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

suite('project_other_join_condition_for_nlj_with_unique_function') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET disable_join_reorder=true'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    // LEFT JOIN with a mixed-slots + rand() non-equal other-condition —
    // the `t1.id + rand()` expression must NOT be pre-aliased into a Project
    // below NLJ, otherwise rand() is evaluated per-left-row instead of per-pair.
    qt_left_join_rand_one_side '''
        explain shape plan
        select t1.id, t2.id
        from t1 left join t2 on t1.id + rand() < t2.id
        '''

    // INNER CROSS-style NLJ with rand() on one side expression.
    qt_cross_rand_one_side '''
        explain shape plan
        select t1.id, t2.id
        from t1 join t2 on t1.id + rand() > t2.id
        '''

    // Both sides contain rand() — both sub-expressions must stay inline in the
    // other condition, neither should be pre-aliased into a child Project.
    qt_cross_rand_both_sides '''
        explain shape plan
        select t1.id, t2.id
        from t1 join t2 on t1.id + rand() > t2.id + rand()
        '''
}
