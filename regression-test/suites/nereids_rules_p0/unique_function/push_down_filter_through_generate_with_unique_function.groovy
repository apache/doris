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

suite('push_down_filter_through_generate_with_unique_function') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    // unique function in filter must NOT be pushed below LogicalGenerate,
    // otherwise call count / state of the unique function changes semantically.
    qt_filter_through_generate_unique_1 '''
        explain shape plan
        select e1 from t1 lateral view explode_numbers(3) tmp1 as e1
        where rand() > 0.1
    '''

    // mixed: deterministic conjunct pushable, unique conjunct in same WHERE must stay above generate.
    // Here `t1.id > 10` should be pushed below generate while `t1.id + rand(1,100) > 5` (a conjunct
    // that does reference a base slot, so old code would have pushed it) must stay above.
    qt_filter_through_generate_unique_2 '''
        explain shape plan
        select e1 from t1 lateral view explode_numbers(3) tmp1 as e1
        where t1.id > 10 and t1.id + rand(1,100) > 5
    '''

    // unique function combined with base slot in the same conjunct.
    qt_filter_through_generate_unique_3 '''
        explain shape plan
        select e1 from t1 lateral view explode_numbers(3) tmp1 as e1
        where t1.id + rand(1,100) > 5
    '''
}
