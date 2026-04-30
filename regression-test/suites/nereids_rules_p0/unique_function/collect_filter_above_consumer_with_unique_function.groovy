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

suite('collect_filter_above_consumer_with_unique_function') {
    sql 'SET enable_nereids_planner=true'
    sql 'SET runtime_filter_mode=OFF'
    sql 'SET enable_fallback_to_original_planner=false'
    // keep CTE materialized so CollectFilterAboveConsumer actually runs.
    sql 'SET inline_cte_referenced_threshold=0'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "SET detail_shape_nodes='PhysicalProject'"
    sql 'SET disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    // filter with unique function above a CTE consumer must NOT be collected and
    // pushed into the CTE producer; otherwise the producer would be re-filtered
    // and other consumers would see inconsistent rows.
    qt_collect_filter_above_consumer_unique_1 '''
        explain shape plan
        with cte1 as (select id, msg from t1)
        select * from cte1 where rand() > 0.1
        union all
        select * from cte1 where rand() > 0.2
    '''

    // mixed conjuncts: deterministic parts can be collected and pushed into the CTE producer,
    // unique parts must stay above the consumer only. Use predicates that survive simplification
    // so the collected-into-producer OR shape is visible.
    qt_collect_filter_above_consumer_unique_2 '''
        explain shape plan
        with cte1 as (select id, msg from t1)
        select * from cte1 where id > 10 and rand() > 0.1
        union all
        select * from cte1 where id > 100 and rand() > 0.2
    '''
}
