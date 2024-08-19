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

suite("colocate_union_numbers") {
    multi_sql """
        set enable_nereids_distribute_planner=false;
        set enable_pipeline_x_engine=true;
        set disable_join_reorder=true;
        set enable_local_shuffle=false;
        set force_to_local_shuffle=false;
        """

    def extractFragment = { String sqlStr, String containsString, Closure<Integer> checkExchangeNum ->
        explain {
            sql sqlStr
            check { result ->
                log.info("Explain result:\n${result}")

                assertTrue(result.contains(containsString))

                def fragmentContainsJoin = result.split("PLAN FRAGMENT")
                        .toList()
                        .stream()
                        .filter { it.contains(containsString) }
                        .findFirst()
                        .get()

                log.info("Fragment:\n${fragmentContainsJoin}")

                checkExchangeNum(fragmentContainsJoin.count("VEXCHANGE"))
            }
        }
    }

    def sqlStr = """
        select * from numbers('number'='3')a
        union all
        select * from numbers('number'='4')b
        """

    extractFragment(sqlStr, "VUNION") { exchangeNum ->
        assertTrue(exchangeNum == 2)
    }

    multi_sql """
        set enable_nereids_distribute_planner=true;
        set enable_pipeline_x_engine=true;
        set disable_join_reorder=true;
        """

    extractFragment(sqlStr, "VUNION") { exchangeNum ->
        assertTrue(exchangeNum == 0)
    }

    order_qt_union_all sqlStr
}
