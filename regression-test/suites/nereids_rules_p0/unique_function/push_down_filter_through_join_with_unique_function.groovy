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

suite('push_down_filter_through_join_with_unique_function') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET disable_join_reorder=true'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    qt_push_down_filter_through_join_1 '''
        explain shape plan
        select t1.id, t2.id
        from t1 join t2
        where rand() > 0.1
        '''

    qt_push_down_filter_through_join_2 '''
        explain shape plan
        select t1.id, t2.id
        from t1 join t2
        where rand() > 0.1 and t1.id = t2.id + 10 and t1.id > 100
        '''

    qt_push_down_filter_through_join_3 '''
        explain shape plan
        select t1.id, t2.id
        from t1 join t2
        where t1.id + rand(1, 100) > 100
        '''

    // sql 'SET disable_join_reorder=false'
    // Well the reorder join need disable_join_reorder=false,
    // But if we enable this var, the test is not stable,
    // the p0 test pipeline may change the two table join order sometimes.

    qt_reorder_join_1 '''
         explain shape plan
         select t1.id, t2.id
         from t1 join t2
         where t1.id + rand(1, 100) = t2.id and t1.id * 2 = t2.id * 5
         '''

    qt_reorder_join_2 '''
         explain shape plan
         select t1.id, t2.id, t3.id
         from t1, t2, t2 as t3
         where t1.id + rand(1, 100) = t3.id and t1.id * 2 = t2.id * 5
         '''

    // if set disable_join_reorder=false,
    // | PhysicalResultSink
    //  --filter((random() > 10.0))                                                                                                              |
    //  ----NestedLoopJoin[CROSS_JOIN]                                                                                                           | | ------PhysicalProject                                                                                                                    |
    //  --------hashJoin[INNER_JOIN broadcast] hashCondition=((expr_(cast(id as BIGINT) * 2) = expr_(cast(id as BIGINT) * 5))) otherCondition=() |
    //  ----------PhysicalProject                                                                                                                |
    //  ------------PhysicalOlapScan[t1]                                                                                                         |
    //  ----------PhysicalProject                                                                                                                |
    //  ------------PhysicalOlapScan[t2(t3)]                                                                                                     |
    //  ------PhysicalProject                                                                                                                    |
    //  --------PhysicalOlapScan[t2]                                                                                                            |
    qt_reorder_join_3 '''
         explain shape plan
         select t1.id, t2.id, t3.id
         from t1, t2, t2 as t3
         where random() > 10 and t1.id * 2 = t3.id * 5
         '''
}
