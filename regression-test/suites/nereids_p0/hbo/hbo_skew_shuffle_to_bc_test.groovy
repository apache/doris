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

suite("hbo_skew_shuffle_to_bc_test", "nonConcurrent") {
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql """drop table if exists hbo_skew_shuffle_to_bc_test1;"""
    sql """create table hbo_skew_shuffle_to_bc_test1(a int, b int) distributed by hash(a, b) buckets 32 properties("replication_num"="1");"""
    sql """insert into hbo_skew_shuffle_to_bc_test1 select number, 1 from numbers("number" = "100");"""
    sql """insert into hbo_skew_shuffle_to_bc_test1 select number, number from numbers("number" = "1000000");"""
    sql """analyze table hbo_skew_shuffle_to_bc_test1 with full with sync;"""

    sql """drop table if exists hbo_skew_shuffle_to_bc_test2;"""
    sql """create table hbo_skew_shuffle_to_bc_test2(a int, b int) distributed by hash(a, b) buckets 32 properties("replication_num"="1");"""
    sql """insert into hbo_skew_shuffle_to_bc_test2 select number, number from numbers("number" = "200000");"""
    sql """insert into hbo_skew_shuffle_to_bc_test2 select 1, 1 from numbers("number" = "200000");"""
    sql """insert into hbo_skew_shuffle_to_bc_test2 select number, number from numbers("number" = "600000");"""
    sql """analyze table hbo_skew_shuffle_to_bc_test2 with full with sync;"""

    // increase the parallel to make enough skew between different instances
    sql "set parallel_pipeline_task_num=100;"
    sql "set hbo_rfsafe_threshold=1.0;"
    sql """ ADMIN SET ALL FRONTENDS CONFIG ("hbo_slow_query_threshold_ms" = "10"); """
    sql "set enable_hbo_optimization=false;"
    sql "set global enable_hbo_info_collection=true;"
    /**
     +---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                                 |
     +---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 8.8669105036E7                                                                                                                                                                                                                                                           |
     | PhysicalResultSink[348] ( outputExprs=[__count_0#4] )                                                                                                                                                                                                                           |
     | +--PhysicalHashAggregate[343]@6 ( stats=1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#9) AS `count(*)`#4], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )                       |
     |    +--PhysicalDistribute[338]@8 ( stats=1, distributionSpec=DistributionSpecGather )                                                                                                                                                                                            |
     |       +--PhysicalHashAggregate[333]@8 ( stats=1, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#9], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )                    |
     |          +--PhysicalProject[328]@5 ( stats=10,000,000, projects=[1 AS `1`#8] )                                                                                                                                                                                                  |
     |             +--PhysicalHashJoin[323]@4 ( stats=10,000,000, type=INNER_JOIN, hashCondition=[(b#1 = b#3)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[b#3->[b#1](ndv/size = 6028862/8388608) , RF1[b#3->[b#1](ndv/size = 6028862/8388608) ] )                       |
     |                |--PhysicalDistribute[307]@1 ( stats=10,001,000, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[1], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[1]], exprIdToEquivalenceSet={1=0} ) ) |
     |                |  +--PhysicalProject[302]@1 ( stats=10,001,000, projects=[b#1] )                                                                                                                                                                                                |
     |                |     +--PhysicalOlapScan[hbo_skew_shuffle_to_bc_test1]@0 ( stats=10,001,000, RFs= RF0 RF1 )                                                                                                                                                                     |
     |                +--PhysicalDistribute[318]@3 ( stats=10,000,000, distributionSpec=DistributionSpecHash ( orderedShuffledColumns=[3], shuffleType=EXECUTION_BUCKETED, tableId=-1, selectedIndexId=-1, partitionIds=[], equivalenceExprIds=[[3]], exprIdToEquivalenceSet={3=0} ) ) |
     |                   +--PhysicalProject[313]@3 ( stats=10,000,000, projects=[b#3] )                                                                                                                                                                                                |
     |                      +--PhysicalOlapScan[hbo_skew_shuffle_to_bc_test2]@2 ( stats=10,000,000 )
     +---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select count(1) from hbo_skew_shuffle_to_bc_test1 t1, hbo_skew_shuffle_to_bc_test2 t2 where t1.b = t2.b;"
        contains("stats=1,000,100, distributionSpec=DistributionSpecHash")
        contains("stats=1,000,000, distributionSpec=DistributionSpecHash")
    }

    sql "select count(1) from hbo_skew_shuffle_to_bc_test1 t1, hbo_skew_shuffle_to_bc_test2 t2 where t1.b = t2.b;"
    sleep(3000);
    sql "set enable_hbo_optimization=true;"
    /**
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | Explain String(Nereids Planner)                                                                                                                                                                                                                                    |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     | cost = 6.081327812596689E9                                                                                                                                                                                                                                          |
     | PhysicalResultSink[340] ( outputExprs=[__count_0#4] )                                                                                                                                                                                                               |
     | +--PhysicalHashAggregate[335]@6 ( stats=(hbo)1, aggPhase=GLOBAL, aggMode=BUFFER_TO_RESULT, maybeUseStreaming=false, groupByExpr=[], outputExpr=[count(partial_count(*)#9) AS `count(*)`#4], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false )      |
     |    +--PhysicalDistribute[330]@8 ( stats=(hbo)100, distributionSpec=DistributionSpecGather )                                                                                                                                                                         |
     |       +--PhysicalHashAggregate[325]@8 ( stats=(hbo)100, aggPhase=LOCAL, aggMode=INPUT_TO_BUFFER, maybeUseStreaming=false, groupByExpr=[], outputExpr=[partial_count(*) AS `partial_count(*)`#9], partitionExpr=Optional[[]], topnFilter=false, topnPushDown=false ) |
     |          +--PhysicalProject[320]@5 ( stats=2,010,002,000, projects=[1 AS `1`#8] )                                                                                                                                                                                   |
     |             +--PhysicalHashJoin[315]@4 ( stats=(hbo)2,010,002,000, type=INNER_JOIN, hashCondition=[(b#1 = b#3)], otherCondition=[], markCondition=[], runtimeFilters=[RF0[b#3->[b#1](ndv/size = 6028862/8388608) , RF1[b#3->[b#1](ndv/size = 6028862/8388608) ] )   |
     |                |--PhysicalProject[299]@1 ( stats=10,001,000, projects=[b#1] )                                                                                                                                                                                       |
     |                |  +--PhysicalOlapScan[hbo_skew_shuffle_to_bc_test1]@0 ( stats=10,001,000, RFs= RF0 RF1 )                                                                                                                                                            |
     |                +--PhysicalDistribute[310]@3 ( stats=10,000,000, distributionSpec=DistributionSpecReplicated )                                                                                                                                                       |
     |                   +--PhysicalProject[305]@3 ( stats=10,000,000, projects=[b#3] )                                                                                                                                                                                    |
     |                      +--PhysicalOlapScan[hbo_skew_shuffle_to_bc_test2]@2 ( stats=10,000,000 )                                                                                                                                                                     |
     +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     */
    explain {
        sql "physical plan select count(1) from hbo_skew_shuffle_to_bc_test1 t1, hbo_skew_shuffle_to_bc_test2 t2 where t1.b = t2.b;"
        contains("stats=1,000,100, projects=[b#1]")
        contains("stats=1,000,000, distributionSpec=DistributionSpecReplicated")
        contains("stats=(hbo)21,000,200, type=INNER_JOIN")
        contains("stats=(hbo)1, aggPhase=GLOBAL")
    }

}
