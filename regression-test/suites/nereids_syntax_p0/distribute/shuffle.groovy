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

suite("shuffle") {
    createTestTable "test_shuffle"

    multi_sql """
        set enable_nereids_distribute_planner=true;
        set enable_pipeline_x_engine=true;
        set enable_local_shuffle=false;
        set force_to_local_shuffle=false;
        """

    order_qt_4_phase_agg """
        select /*+SET_VAR(disable_nereids_rules='TWO_PHASE_AGGREGATE_WITH_MULTI_DISTINCT,TWO_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI,THREE_PHASE_AGGREGATE_WITH_COUNT_DISTINCT_MULTI,THREE_PHASE_AGGREGATE_WITH_DISTINCT,FOUR_PHASE_AGGREGATE_WITH_DISTINCT')*/
        id, count(distinct value)
        from test_shuffle
        group by id
        """
}
