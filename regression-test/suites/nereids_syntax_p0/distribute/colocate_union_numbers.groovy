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
        """

    def sqlStr = """
        select * from numbers('number'='3')a
        union all
        select * from numbers('number'='4')b
        """

    explain {
        sql sqlStr
        check { explainStr ->
            log.info(explainStr)

            // union all with two exchange
            assertTrue(explainStr.count("VEXCHANGE") > 1)
            assertTrue(explainStr.count("VDataGenScanNode") == 2)
        }
    }

    multi_sql """
        set enable_nereids_distribute_planner=true;
        set enable_pipeline_x_engine=true;
        set disable_join_reorder=true;
        """

    explain {
        sql "distributed plan ${sqlStr}"
        check { explainStr ->
            log.info(explainStr)

            // only contains one instance
            assertTrue(explainStr.count("StaticAssignedJob") == 1)
            assertTrue(explainStr.count(" DataGenScanNode{") == 2)
        }
    }

    order_qt_union_all sqlStr
}
