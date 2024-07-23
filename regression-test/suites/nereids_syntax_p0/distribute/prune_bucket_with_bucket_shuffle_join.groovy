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

suite("prune_bucket_with_bucket_shuffle_join") {
    multi_sql """
        drop table if exists test_outer_join1;
        CREATE TABLE IF NOT EXISTS test_outer_join1 (
         c0 DECIMALV3(8,3)
        )
        DISTRIBUTED BY HASH (c0) BUCKETS 10 PROPERTIES ("replication_num" = "1");
        
        drop table if exists test_outer_join2;
        CREATE TABLE IF NOT EXISTS test_outer_join2 (
          c0 DECIMALV3(8,3)
        )
        DISTRIBUTED BY HASH (c0) BUCKETS 10 PROPERTIES ("replication_num" = "1");
        INSERT INTO test_outer_join1 (c0) VALUES (1), (3);
        INSERT INTO test_outer_join2 (c0) VALUES (2), (3);
        
        sync;
        
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

    String sqlStr = """
            SELECT * FROM
            (select * from test_outer_join1 where c0 =1)a
            RIGHT OUTER JOIN
            (select * from test_outer_join2)b
            ON a.c0 = b.c0
            """

    extractFragment(sqlStr, "RIGHT OUTER JOIN(PARTITIONED)") { exchangeNum ->
        assertTrue(exchangeNum > 1)
    }

    multi_sql """
        set enable_nereids_distribute_planner=true;
        set enable_pipeline_x_engine=true;
        set disable_join_reorder=true;
        """

    extractFragment(sqlStr, "RIGHT OUTER JOIN(BUCKET_SHUFFLE)") { exchangeNum ->
        assertTrue(exchangeNum == 1)
    }

    explain {
        sql "distributed plan ${sqlStr}"
        check { explainStr ->
            log.info("Distributed plan:\n${explainStr}")

            // some tablets of left table are pruned
            assertTrue(explainStr.count("tablet ") < 20)
        }
    }

    order_qt_fillup_bucket sqlStr
}
