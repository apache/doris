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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("dist_expr_list") {
    sql """
    drop table if exists agg_cse_shuffle;
    create table agg_cse_shuffle(
        a int, b int, c int
    )distributed by hash(a) buckets 3
    properties("replication_num" = "1")
    ;
    insert into agg_cse_shuffle values (1, 2, 3), (3, 4, 5);
    """

    explain {
        sql """
            select max(
                case 
                    when (abs(b) > 10) then a+1
                    when (abs(b) < 10) then a+2
                    else NULL
                end
            ) x
            from agg_cse_shuffle
            where c > 0;
            """
        notContains "distribute expr lists: a"
    }
    /*
    after ProjectAggregateExpressionsForCse, a#6 is not output slot of scanNode, and hence it should not be distr expr.

    expect explain string
    |   1:VAGGREGATE (update serialize)(114)                                                                                                                                   |
    |   |  output: partial_max(CASE WHEN (abs(b) > 10) THEN (cast(a as BIGINT) + 1) WHEN (abs(b) < 10) THEN (cast(a as BIGINT) + 2) ELSE NULL END[#8])[#9]                     |
    |   |  group by:                                                                                                                                                           |
    |   |  sortByGroupKey:false                                                                                                                                                |
    |   |  cardinality=1                                                                                                                                                       |
    |   |  distribute expr lists:                                                                                                                                              |
    |   |                                                                                                                                                                      |
    |   0:VOlapScanNode(95)                              


    the bad case: distribute expr lists: a[#6] 
    explain string
    |   1:VAGGREGATE (update serialize)(114)                                                                                                                                   |
    |   |  output: partial_max(CASE WHEN (abs(b) > 10) THEN (cast(a as BIGINT) + 1) WHEN (abs(b) < 10) THEN (cast(a as BIGINT) + 2) ELSE NULL END[#8])[#9]                     |
    |   |  group by:                                                                                                                                                           |
    |   |  sortByGroupKey:false                                                                                                                                                |
    |   |  cardinality=1                                                                                                                                                       |
    |   |  distribute expr lists: a[#6]     <==                                                                                                                                   |
    |   |                                                                                                                                                                      |
    |   0:VOlapScanNode(95)
    |      TABLE: rqg.agg_cse_shuffle(agg_cse_shuffle), PREAGGREGATION: ON                                                                                                     |
    |      partitions=1/1 (agg_cse_shuffle)                                                                                                                                    |
    |      tablets=3/3, tabletList=1761200234884,1761200234886,1761200234888                                                                                                   |
    |      cardinality=2, avgRowSize=1182.5, numNodes=2                                                                                                                        |
    |      pushAggOp=NONE                                                                                                                                                      |
    |      final projections: a[#2], b[#3], CASE WHEN (abs(b)[#4] > 10) THEN (cast(a as BIGINT)[#5] + 1) WHEN (abs(b)[#4] < 10) THEN (cast(a as BIGINT)[#5] + 2) ELSE NULL END |
    |      final project output tuple id: 2                                                                                                                                    |
    |      intermediate projections: a[#0], b[#1], abs(b[#1]), CAST(a[#0] AS bigint)                                                                                           |
    |      intermediate tuple id: 1                                                                 
    */

    explain {
        sql """
            select max(
                case 
                    when (abs(b) > 10) then a+1
                    when (abs(b) < 10) then a+2
                    else NULL
                end
            ),
            max(a)
            from agg_cse_shuffle;
            """
        contains "distribute expr lists: a"
    /*
        expect explain string
        |   1:VAGGREGATE (update serialize)(100)                                                                                                                                                              |
        |   |  output: partial_max(CASE WHEN (abs(b) > 10) THEN (cast(a as BIGINT) + 1) WHEN (abs(b) < 10) THEN (cast(a as BIGINT) + 2) ELSE NULL END[#8])[#9], partial_max(a[#6])[#10]                       |
        |   |  group by:                                                                                                                                                                                      |
        |   |  sortByGroupKey:false                                                                                                                                                                           |
        |   |  cardinality=1                                                                                                                                                                                  |
        |   |  distribute expr lists: a[#6]                                                                                                                                                                   |
        |   |  tuple ids: 3                                                                                                                                                                                   |
        |   |                                                                                                                                                                                                 |
        |   0:VOlapScanNode(81)     
    */
    }
}