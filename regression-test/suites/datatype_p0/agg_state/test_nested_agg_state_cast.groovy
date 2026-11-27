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

suite("test_nested_agg_state_cast") {
    sql "SET enable_agg_state = true"

    def state = "sum_state(CAST(1 AS INT))"
    def stateType = "AGG_STATE<sum(INT NOT NULL)>"

    order_qt_array """
        SELECT sum_finalize(element_at(CAST(array(${state}) AS ARRAY<${stateType}>), 1))
    """

    order_qt_map """
        SELECT sum_finalize(element_at(TRY_CAST(map(1, ${state}) AS MAP<TINYINT, ${stateType}>), 1))
    """

    order_qt_struct """
        SELECT sum_finalize(struct_element(
            CAST(named_struct('s', nullable(${state})) AS STRUCT<s:${stateType}>), 's'))
    """

    test {
        sql """EXPLAIN ANALYZED PLAN SELECT CAST(array(${state}) AS ARRAY<AGG_STATE<sum(BIGINT NOT NULL)>>)"""
        exception "can not cast from origin type"
    }
}
