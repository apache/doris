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

suite('push_down_join_other_condition_with_unique_function') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    // A `rand() > 0.5` predicate has empty input slots, so the legacy
    // `child.getOutputSet().containsAll(emptySet)` check would arbitrarily push the
    // predicate into one join side. Block such empty-slot unique predicates.
    qt_join_other_unique_1 '''
        explain shape plan
        select * from t1 join t2 on t1.id = t2.id and rand() > 0.5
    '''

    // Mixed: a deterministic ON-condition referencing only the left side is pushed;
    // the empty-slot unique conjunct must stay in the join.
    qt_join_other_unique_2 '''
        explain shape plan
        select * from t1 join t2 on t1.id = t2.id and t1.id > 3 and rand() > 0.5
    '''

    // uuid() with empty slots in ON condition should not be pushed either.
    qt_join_other_unique_3 '''
        explain shape plan
        select * from t1 left join t2 on t1.id = t2.id and uuid() is not null
    '''

    // When the unique-function conjunct has side-specific input slots, push-down is
    // allowed: output cardinality expectation is preserved and pre-join evaluation
    // is what users typically want.
    qt_join_other_unique_4 '''
        explain shape plan
        select * from t1 join t2 on t1.id = t2.id and t1.id + rand() > 0.5
    '''
}
