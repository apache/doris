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

suite('push_project_into_union_with_unique_function') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject,PhysicalOneRowRelation,PhysicalUnion'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    qt_push_down_1 '''
        explain shape plan select a + random() as b, a + random() as c from (select 100.0 as a union all select 200.0 as a) t
        '''

    qt_push_down_2 '''
        explain shape plan select a + b from (select random() as a, 2 * random() as b union all select 10 * random() as a, 20 * random() as b) t
        '''

    qt_push_down_3 '''
        explain shape plan select a + 100, b + 100 from (select random() as a, 2 * random() as b union all select 10 * random() as a, 20 * random() as b) t
        '''

    qt_no_push_down_1 '''
        explain shape plan select a as b, a as c from (select random() as a union all select 200.0 as a) t
        '''

    qt_no_push_down_2 '''
        explain shape plan select a + 10 + a as b from (select random() as a union all select 200.0 as a) t
        '''

    qt_no_push_down_3 '''
        explain shape plan select a as b, a + 10 as c from (select random() as a union all select 200.0 as a) t
        '''
}
