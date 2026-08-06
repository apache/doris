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

suite('push_down_filter_through_window_with_unique_function') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET enable_fallback_to_original_planner=false'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    // A `rand() > 0.5` predicate has empty input slots, so the legacy
    // `commonPartitionKeys.containsAll(emptySet)` check would push it below the window node.
    // That changes which rows belong to each partition and breaks every window function.
    qt_filter_through_window_unique_1 '''
        explain shape plan
        select id, msg, rn from (
            select id, msg, row_number() over (partition by id order by msg) rn from t1
        ) v
        where rand() > 0.5
    '''

    // Mixed: deterministic predicate on the partition key remains pushable,
    // unique-fn conjunct must stay above the window.
    qt_filter_through_window_unique_2 '''
        explain shape plan
        select * from (
            select id, msg, row_number() over (partition by id order by msg) rn from t1
        ) v
        where id > 5 and rand() > 0.5
    '''

    // Conjunct that references the partition key together with a unique function
    // (input slots are subset of partition keys so the legacy check matched).
    qt_filter_through_window_unique_3 '''
        explain shape plan
        select * from (
            select id, msg, row_number() over (partition by id order by msg) rn from t1
        ) v
        where id + rand(1, 100) > 5
    '''
}
