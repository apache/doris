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

suite('test_nonfoldable') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    qt_filter_through_project_1 '''
        explain shape plan select * from (select id + 100 as a, id + 200 as b, id + 300 as c from t1) t where a > 999 and b > 999
        '''

    qt_filter_through_project_2 '''
        explain shape plan select * from (select id + random(1, 10) + 100 as a, id + 200 as b, id + 300 as c from t1) t where a > 999 and b > 999
        '''

    qt_filter_through_project_3 '''
        explain shape plan select * from (select id + random(1, 10) + 100 as a, id + random(1, 10) + 200 as b, id + random(1, 10) + 300 as c from t1) t where a > 999 and b > 999
        '''

    qt_filter_through_project_4 '''
        explain shape plan select * from (select id + 100 as a, id + 200 as b, id + 300 as c from t1) t where a + random(1, 10) > 999 and b + random(1, 10) > 999
        '''

    qt_filter_through_project_5 '''
        explain shape plan select * from (select id + 100 as a, id + 200 as b, id + 300 as c from t1) t where a > 999 and b > 999 limit 10
        '''

    qt_filter_through_project_6 '''
        explain shape plan select * from (select id + random(1, 10) + 100 as a, id + 200 as b, id + 300 as c from t1) t where a > 999 and b > 999 limit 10
        '''

    qt_filter_through_project_7 '''
        explain shape plan select * from (select id + random(1, 10) + 100 as a, id + random(1, 10) + 200 as b, id + random(1, 10) + 300 as c from t1) t where a > 999 and b > 999 limit 10
        '''

    qt_filter_through_project_8 '''
        explain shape plan select * from (select id + 100 as a, id + 200 as b, id + 300 as c from t1) t where a + random(1, 10) > 999 and b + random(1, 10) > 999 limit 10
        '''

    qt_merge_project_1 '''
        explain shape plan select a as b, a as c from (select id + 100 as a from t1) t
        '''

    qt_merge_project_2 '''
        explain shape plan select a as b, a as c from (select id + random(1, 10) as a from t1) t
        '''

    qt_merge_project_3 '''
        explain shape plan select a as b from (select id + random(1, 10) as a from t1) t
        '''

    qt_merge_project_4 '''
        explain shape plan select a + 10 + a as b from (select id + random(1, 10) as a from t1) t
        '''

    qt_merge_project_5 '''
        explain shape plan select a as b, a + 10 as c from (select id + random(1, 10) as a from t1) t
        '''
}
