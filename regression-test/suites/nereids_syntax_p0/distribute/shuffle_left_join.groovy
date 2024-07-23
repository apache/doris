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
