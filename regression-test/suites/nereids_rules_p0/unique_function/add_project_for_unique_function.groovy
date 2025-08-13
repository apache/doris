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

suite('add_project_for_unique_function') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject,PhysicalOneRowRelation'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    // no project
    qt_one_row_relation_1 '''
        explain shape plan select random(1, 100), uuid_to_int(uuid())
        '''

    qt_one_row_relation_2 '''
        explain shape plan select random(1, 100) between 10 and 20, uuid_to_int(uuid())
        '''

    qt_one_row_relation_3 '''
        explain shape plan select random(1, 100) between 10 and 20, uuid_to_int(uuid()) between 111 and 222
        '''

    qt_project_1 '''
        explain shape plan select id + random(1, 100) > 20, id * 200 from t1
        '''

    qt_project_2 '''
        explain shape plan select id + random(1, 100) between 10 and 20, id * 200 from t1
        '''

    qt_select_1 '''
        explain shape plan select id from t1 where id + random(1, 100) >= 10
        '''

    qt_select_2 '''
        explain shape plan select id from t1 where id + random(1, 100) between 10 and 20
        '''

    qt_join_1 '''
        explain shape plan select * from t1 join t2 on
            t1.id + t2.id + random(1, 100) between 10 and 20
            and t2.id * random(1, 100) between 100 and 200
            and random(1, 100) between 1 and 10
        '''
}
