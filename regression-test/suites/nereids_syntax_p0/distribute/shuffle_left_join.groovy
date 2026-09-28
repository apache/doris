import java.util.stream.Collectors

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

suite("shuffle_left_join") {
    // The point of this suite is the left-to-right bucket shuffle: with the nereids distribute
    // planner on, the aggregated left side is shuffled onto the right table's storage buckets, so
    // the join becomes a bucket shuffle join that saves one exchange versus a plain partitioned
    // shuffle. The planner chooses between the two candidates by cost, which depends on the scan
    // row count and on the bucket-shuffle downgrade gate. Both are pinned below so the asserted
    // plan is stable:
    //   - `analyze ... with sync` fixes the row count (otherwise it is reported asynchronously
    //     after the insert, and the plan flips depending on whether the report has landed yet);
    //   - `bucket_shuffle_downgrade_ratio=0` disables the downgrade that turns bucket shuffle back
    //     into a partitioned shuffle when the bucket count is small relative to the instance count,
    //     which otherwise makes the plan depend on the number of backends.
    multi_sql """
        drop table if exists test_shuffle_left;
        
        CREATE TABLE `test_shuffle_left` (
          id int,
          id2 int
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        
        insert into test_shuffle_left values (1, 1), (2, 2), (3, 4);
        
        sync;

        analyze table test_shuffle_left with sync;

        set enable_nereids_distribute_planner=false;
        set enable_pipeline_x_engine=true;
        set disable_join_reorder=true;
        set enable_local_shuffle=false;
        set force_to_local_shuffle=false;
        set bucket_shuffle_downgrade_ratio=0;
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
            select *
            from
            (
              select id2
              from test_shuffle_left
              group by id2
            ) a
            inner join [shuffle]
            test_shuffle_left b
            on a.id2=b.id;
        """

    extractFragment(sqlStr, "INNER JOIN(PARTITIONED)") { exchangeNum ->
        assertTrue(exchangeNum == 2)
    }

    order_qt_shuffle_left_and_right sqlStr

    multi_sql """
        set enable_nereids_distribute_planner=true;
        set enable_pipeline_x_engine=true;
        set disable_join_reorder=true;
        """

    def variables = sql "show variables"
    def variableString = variables.stream()
        .map { it.toString() }
        .collect(Collectors.joining("\n"))
    logger.info("Variables:\n${variableString}")

    extractFragment(sqlStr, "INNER JOIN(BUCKET_SHUFFLE)") { exchangeNum ->
        assertTrue(exchangeNum == 1)
    }

    explain {
        sql "plan $sqlStr"
        check { explainStr ->
            log.info("explain plan:\n${explainStr}")
        }
    }

    def rows = sql sqlStr
    def rowsString = rows.stream()
            .map { it.toString() }
            .collect(Collectors.joining("\n"))
    logger.info("Rows:\n${rowsString}")


    order_qt_shuffle_left sqlStr
}
